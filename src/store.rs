//! Reading and writing rows as hashes over the Redis protocol.

use std::collections::HashMap;

use redis::aio::MultiplexedConnection;
use serde_json::{Map, Value};

use crate::config::Collection;

pub type Row = Map<String, Value>;

/// Ids of every row: the collection's index set.
pub async fn all_ids(conn: &mut MultiplexedConnection, col: &Collection) -> redis::RedisResult<Vec<String>> {
    let mut ids: Vec<String> = redis::cmd("SMEMBERS").arg(col.index_key()).query_async(conn).await?;
    ids.sort();
    Ok(ids)
}

/// Fetch rows by id with pipelined HGETALLs. Ids whose hash is gone (a
/// stale index entry) are skipped.
pub async fn fetch(conn: &mut MultiplexedConnection, col: &Collection, ids: &[String]) -> redis::RedisResult<Vec<Row>> {
    let mut rows = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(512) {
        let mut pipe = redis::pipe();
        for id in chunk {
            pipe.cmd("HGETALL").arg(col.key(id));
        }
        let hashes: Vec<HashMap<String, String>> = pipe.query_async(conn).await?;
        for (id, h) in chunk.iter().zip(hashes) {
            if h.is_empty() {
                continue;
            }
            rows.push(to_row(col, id, &h));
        }
    }
    Ok(rows)
}

pub fn to_row(col: &Collection, id: &str, h: &HashMap<String, String>) -> Row {
    let mut row = Map::new();
    row.insert("id".into(), Value::String(id.to_string()));
    for (name, ty) in &col.columns {
        row.insert(name.clone(), h.get(name).map(|raw| ty.parse(raw)).unwrap_or(Value::Null));
    }
    row
}

/// Insert or replace a row: set the given fields, drop the null ones, then
/// add the id to the index (an index entry without a hash is tolerated, the
/// reverse would hide the row).
pub async fn upsert(conn: &mut MultiplexedConnection, col: &Collection, id: &str, fields: &[(String, Option<String>)]) -> redis::RedisResult<()> {
    let key = col.key(id);
    let set: Vec<(&str, &str)> = fields.iter().filter_map(|(k, v)| v.as_deref().map(|v| (k.as_str(), v))).collect();
    let unset: Vec<&str> = fields.iter().filter(|(_, v)| v.is_none()).map(|(k, _)| k.as_str()).collect();
    let mut pipe = redis::pipe();
    if !set.is_empty() {
        let mut c = redis::cmd("HSET");
        c.arg(&key);
        for (k, v) in &set {
            c.arg(*k).arg(*v);
        }
        pipe.add_command(c).ignore();
    } else {
        // a row needs at least one field to exist as a hash
        pipe.cmd("HSET").arg(&key).arg("__exists").arg("1").ignore();
    }
    if !unset.is_empty() {
        pipe.cmd("HDEL").arg(&key).arg(&unset).ignore();
    }
    pipe.cmd("SADD").arg(col.index_key()).arg(id).ignore();
    pipe.query_async::<()>(conn).await
}

pub async fn delete(conn: &mut MultiplexedConnection, col: &Collection, id: &str) -> redis::RedisResult<()> {
    redis::pipe()
        .cmd("SREM").arg(col.index_key()).arg(id).ignore()
        .cmd("DEL").arg(col.key(id)).ignore()
        .query_async::<()>(conn)
        .await
}
