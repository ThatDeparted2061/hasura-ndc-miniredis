//! A Hasura data connector (NDC spec v0.2) for Redis-compatible stores.

pub mod config;
pub mod query;
pub mod schema;
pub mod store;

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use ndc_sdk::connector::{self, Connector, ConnectorSetup, MutationError, QueryError};
use ndc_sdk::json_response::JsonResponse;
use ndc_sdk::models;
use redis::aio::MultiplexedConnection;
use serde_json::Value;

use crate::config::{Collection, Configuration};
use crate::query::{Plan, Vars};

#[derive(Clone, Default)]
pub struct MiniRedis;

#[derive(Clone)]
pub struct State {
    conn: MultiplexedConnection,
    fetched: prometheus::IntCounterVec,
}

#[async_trait]
impl ConnectorSetup for MiniRedis {
    type Connector = Self;

    async fn parse_configuration(&self, dir: &Path) -> connector::Result<Configuration> {
        config::load(dir).map_err(|e| connector::ErrorResponse::from(e))
    }

    async fn try_init_state(&self, c: &Configuration, metrics: &mut prometheus::Registry) -> connector::Result<State> {
        let client = redis::Client::open(c.redis_url.as_str()).map_err(connector::ErrorResponse::from_error)?;
        let conn = client.get_multiplexed_async_connection().await.map_err(connector::ErrorResponse::from_error)?;
        let fetched = prometheus::IntCounterVec::new(
            prometheus::Opts::new("ndc_miniredis_rows_fetched_total", "Hashes read from Redis, by how they were found"),
            &["plan"],
        )
        .map_err(connector::ErrorResponse::from_error)?;
        metrics.register(Box::new(fetched.clone())).map_err(connector::ErrorResponse::from_error)?;
        Ok(State { conn, fetched })
    }
}

fn collection<'a>(c: &'a Configuration, name: &str) -> Result<&'a Collection, QueryError> {
    c.collections.get(name).ok_or_else(|| QueryError::new_invalid_request(&format!("unknown collection {name}")))
}

fn redis_err(e: redis::RedisError) -> connector::ErrorResponse {
    connector::ErrorResponse::from_error(e)
}

impl MiniRedis {
    async fn run(c: &Configuration, state: &State, req: &models::QueryRequest, vars: &Vars) -> connector::Result<models::RowSet> {
        let col = collection(c, req.collection.as_str())?;
        let mut conn = state.conn.clone();
        let (ids, plan) = match query::plan(req.query.predicate.as_ref(), vars)? {
            Plan::Keys(ids) => (ids, "keys"),
            Plan::Scan => (store::all_ids(&mut conn, col).await.map_err(redis_err)?, "scan"),
        };
        let rows = store::fetch(&mut conn, col, &ids).await.map_err(redis_err)?;
        state.fetched.with_label_values(&[plan]).inc_by(rows.len() as u64);
        Ok(query::execute(rows, &req.query, vars)?)
    }
}

#[async_trait]
impl Connector for MiniRedis {
    type Configuration = Configuration;
    type State = State;

    fn connector_name() -> &'static str {
        "ndc-miniredis"
    }

    fn connector_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    fn fetch_metrics(_: &Configuration, _: &State) -> connector::Result<()> {
        Ok(())
    }

    async fn get_capabilities() -> models::Capabilities {
        schema::capabilities()
    }

    async fn get_schema(c: &Configuration) -> connector::Result<JsonResponse<models::SchemaResponse>> {
        Ok(schema::schema(c).into())
    }

    async fn query_explain(c: &Configuration, _: &State, req: models::QueryRequest) -> connector::Result<JsonResponse<models::ExplainResponse>> {
        let col = collection(c, req.collection.as_str())?;
        let vars = req.variables.as_ref().and_then(|v| v.first().cloned()).unwrap_or_default();
        let fetch = match query::plan(req.query.predicate.as_ref(), &vars)? {
            Plan::Keys(ids) => format!("HGETALL {} key(s) by id: {}", ids.len(), ids.iter().take(5).map(|i| col.key(i)).collect::<Vec<_>>().join(", ")),
            Plan::Scan => format!("SMEMBERS {}, then pipelined HGETALL of every {}* hash", col.index_key(), col.key_prefix),
        };
        let mut details = BTreeMap::from([("fetch".to_string(), fetch)]);
        details.insert("in connector".into(), "filter, order_by, limit/offset, aggregates".into());
        Ok(models::ExplainResponse { details }.into())
    }

    async fn mutation_explain(_: &Configuration, _: &State, _: models::MutationRequest) -> connector::Result<JsonResponse<models::ExplainResponse>> {
        Err(connector::NotSupported::new().into())
    }

    async fn query(c: &Configuration, state: &State, req: models::QueryRequest) -> connector::Result<JsonResponse<models::QueryResponse>> {
        if !req.collection_relationships.is_empty() {
            return Err(QueryError::new_unsupported_operation(&"relationships are not supported").into());
        }
        let mut out = vec![];
        match &req.variables {
            None => out.push(Self::run(c, state, &req, &Vars::new()).await?),
            // ponytail: one Redis read per variable set; batch the key lookups if the engine sends many
            Some(sets) => {
                for vars in sets {
                    out.push(Self::run(c, state, &req, vars).await?);
                }
            }
        }
        Ok(models::QueryResponse(out).into())
    }

    async fn mutation(c: &Configuration, state: &State, req: models::MutationRequest) -> connector::Result<JsonResponse<models::MutationResponse>> {
        let mut results = vec![];
        for op in &req.operations {
            let models::MutationOperation::Procedure { name, arguments, fields } = op;
            let (verb, coll) = name
                .as_str()
                .split_once('_')
                .ok_or_else(|| MutationError::new_invalid_request(&format!("unknown procedure {name}")))?;
            let col = c.collections.get(coll).ok_or_else(|| MutationError::new_invalid_request(&format!("unknown procedure {name}")))?;
            let id = arguments
                .get("id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| MutationError::new_invalid_request(&"id must be a non-empty string"))?
                .to_string();
            let mut conn = state.conn.clone();
            let row = match verb {
                "insert" => {
                    let mut set = vec![];
                    for (field, ty) in &col.columns {
                        let v = arguments.get(field.as_str()).unwrap_or(&Value::Null);
                        let rendered = ty.render(v);
                        if rendered.is_none() && !v.is_null() {
                            return Err(MutationError::new_invalid_request(&format!("{field} must be a {}", ty.name())).into());
                        }
                        set.push((field.clone(), rendered));
                    }
                    store::upsert(&mut conn, col, &id, &set).await.map_err(redis_err)?;
                    store::fetch(&mut conn, col, std::slice::from_ref(&id)).await.map_err(redis_err)?.pop()
                }
                "delete" => {
                    let before = store::fetch(&mut conn, col, std::slice::from_ref(&id)).await.map_err(redis_err)?.pop();
                    store::delete(&mut conn, col, &id).await.map_err(redis_err)?;
                    before
                }
                _ => return Err(MutationError::new_invalid_request(&format!("unknown procedure {name}")).into()),
            };
            let result = match (row, fields) {
                (None, _) => Value::Null,
                (Some(r), Some(models::NestedField::Object(o))) => serde_json::to_value(query::project(&o.fields, &r)?).map_err(connector::ErrorResponse::from_error)?,
                (Some(r), None) => Value::Object(r),
                (Some(_), Some(_)) => return Err(MutationError::new_unsupported_operation(&"procedures return an object").into()),
            };
            results.push(models::MutationOperationResults::Procedure { result });
        }
        Ok(models::MutationResponse { operation_results: results }.into())
    }
}
