//! Query execution. Rows are fetched from Redis, then filtered, ordered,
//! paginated, aggregated and projected here.
//!
//! The one thing pushed down to Redis is the key: a predicate that pins `id`
//! (`id _eq x`, `id _in [...]`, at the top level or inside an `and`) fetches
//! just those hashes instead of the whole collection.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use indexmap::IndexMap;
use ndc_sdk::connector::QueryError;
use ndc_sdk::models::{self, Aggregate, ComparisonTarget, ComparisonValue, Expression, Field, OrderByTarget, OrderDirection};
use serde_json::{Number, Value};

use crate::store::Row;

pub type Vars = BTreeMap<models::VariableName, Value>;

/// How rows will be fetched.
#[derive(Debug, PartialEq)]
pub enum Plan {
    Keys(Vec<String>),
    Scan,
}

pub fn plan(predicate: Option<&Expression>, vars: &Vars) -> Result<Plan, QueryError> {
    Ok(match predicate {
        Some(e) => keys(e, vars)?.map(Plan::Keys).unwrap_or(Plan::Scan),
        None => Plan::Scan,
    })
}

fn keys(e: &Expression, vars: &Vars) -> Result<Option<Vec<String>>, QueryError> {
    match e {
        Expression::BinaryComparisonOperator { column: ComparisonTarget::Column { name, field_path: None, .. }, operator, value } if name.as_str() == "id" => {
            let v = value_of(value, vars)?;
            Ok(match (operator.as_str(), v) {
                ("_eq", Value::String(s)) => Some(vec![s]),
                ("_in", Value::Array(xs)) => {
                    let mut ids: Vec<String> = xs.into_iter().filter_map(|x| x.as_str().map(str::to_string)).collect();
                    ids.sort();
                    ids.dedup();
                    Some(ids)
                }
                _ => None,
            })
        }
        Expression::And { expressions } => {
            for e in expressions {
                if let Some(ids) = keys(e, vars)? {
                    return Ok(Some(ids)); // the rest of the conjunction is still evaluated on these rows
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn value_of(v: &ComparisonValue, vars: &Vars) -> Result<Value, QueryError> {
    match v {
        ComparisonValue::Scalar { value } => Ok(value.clone()),
        ComparisonValue::Variable { name } => vars
            .get(name)
            .cloned()
            .ok_or_else(|| QueryError::new_invalid_request(&format!("variable {name} is not set"))),
        ComparisonValue::Column { .. } => Err(QueryError::new_unsupported_operation(&"comparing against another column is not supported")),
    }
}

fn column<'a>(target: &ComparisonTarget, row: &'a Row) -> Result<&'a Value, QueryError> {
    match target {
        ComparisonTarget::Column { name, field_path: None, .. } => Ok(row.get(name.as_str()).unwrap_or(&Value::Null)),
        ComparisonTarget::Column { .. } => Err(QueryError::new_unsupported_operation(&"nested field paths are not supported")),
        ComparisonTarget::Aggregate { .. } => Err(QueryError::new_unsupported_operation(&"comparing aggregates is not supported")),
    }
}

/// Total order over the JSON values a column can hold: null first, then by value.
pub fn compare(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Number(x), Value::Number(y)) => x.as_f64().unwrap_or(0.0).total_cmp(&y.as_f64().unwrap_or(0.0)),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => a.to_string().cmp(&b.to_string()),
    }
}

pub fn eval(e: &Expression, row: &Row, vars: &Vars) -> Result<bool, QueryError> {
    match e {
        Expression::And { expressions } => {
            for e in expressions {
                if !eval(e, row, vars)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expression::Or { expressions } => {
            for e in expressions {
                if eval(e, row, vars)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expression::Not { expression } => Ok(!eval(expression, row, vars)?),
        Expression::UnaryComparisonOperator { column: c, operator: models::UnaryComparisonOperator::IsNull } => Ok(column(c, row)?.is_null()),
        Expression::BinaryComparisonOperator { column: c, operator, value } => {
            let left = column(c, row)?;
            let right = value_of(value, vars)?;
            binary(operator.as_str(), left, &right)
        }
        Expression::ArrayComparison { .. } => Err(QueryError::new_unsupported_operation(&"array comparisons are not supported")),
        Expression::Exists { .. } => Err(QueryError::new_unsupported_operation(&"exists is not supported")),
    }
}

fn binary(op: &str, left: &Value, right: &Value) -> Result<bool, QueryError> {
    if left.is_null() {
        return Ok(false); // SQL-like: a missing field matches no comparison
    }
    let ord = || compare(left, right);
    let text = |f: fn(&str, &str) -> bool| match (left, right) {
        (Value::String(l), Value::String(r)) => f(l, r),
        _ => false,
    };
    Ok(match op {
        "_eq" => ord() == Ordering::Equal,
        "_lt" => ord() == Ordering::Less,
        "_lte" => ord() != Ordering::Greater,
        "_gt" => ord() == Ordering::Greater,
        "_gte" => ord() != Ordering::Less,
        "_in" => match right {
            Value::Array(xs) => xs.iter().any(|x| compare(left, x) == Ordering::Equal),
            _ => return Err(QueryError::new_invalid_request(&"_in needs an array")),
        },
        "_contains" => text(|l, r| l.contains(r)),
        "_icontains" => text(|l, r| l.to_lowercase().contains(&r.to_lowercase())),
        "_starts_with" => text(|l, r| l.starts_with(r)),
        other => return Err(QueryError::new_invalid_request(&format!("unknown comparison operator {other}"))),
    })
}

/// Filter, order and paginate rows, then compute aggregates and project fields.
pub fn execute(mut rows: Vec<Row>, q: &models::Query, vars: &Vars) -> Result<models::RowSet, QueryError> {
    if let Some(p) = &q.predicate {
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            if eval(p, &r, vars)? {
                kept.push(r);
            }
        }
        rows = kept;
    }
    // Rows arrive ordered by id, so an explicit order_by stays deterministic on ties.
    if let Some(order_by) = &q.order_by {
        let mut keys = vec![];
        for el in &order_by.elements {
            match &el.target {
                OrderByTarget::Column { name, path, field_path: None, .. } if path.is_empty() => keys.push((name.as_str().to_string(), el.order_direction)),
                _ => return Err(QueryError::new_unsupported_operation(&"only ordering by a column of the collection is supported")),
            }
        }
        rows.sort_by(|a, b| {
            for (k, dir) in &keys {
                let o = compare(a.get(k).unwrap_or(&Value::Null), b.get(k).unwrap_or(&Value::Null));
                let o = if *dir == OrderDirection::Desc { o.reverse() } else { o };
                if o != Ordering::Equal {
                    return o;
                }
            }
            Ordering::Equal
        });
    }
    let offset = q.offset.unwrap_or(0) as usize;
    let rows: Vec<Row> = rows.into_iter().skip(offset).take(q.limit.map(|l| l as usize).unwrap_or(usize::MAX)).collect();

    let aggregates = match &q.aggregates {
        Some(aggs) => {
            let mut out = IndexMap::new();
            for (name, a) in aggs {
                out.insert(name.clone(), aggregate(a, &rows)?);
            }
            Some(out)
        }
        None => None,
    };
    let projected = match &q.fields {
        Some(fields) => {
            let mut out = Vec::with_capacity(rows.len());
            for r in &rows {
                out.push(project(fields, r)?);
            }
            Some(out)
        }
        None => None,
    };
    if q.groups.is_some() {
        return Err(QueryError::new_unsupported_operation(&"grouping is not supported"));
    }
    Ok(models::RowSet { aggregates, rows: projected, groups: None })
}

pub fn project(fields: &IndexMap<models::FieldName, Field>, row: &Row) -> Result<IndexMap<models::FieldName, models::RowFieldValue>, QueryError> {
    let mut out = IndexMap::new();
    for (alias, f) in fields {
        match f {
            Field::Column { column, fields: None, .. } => {
                out.insert(alias.clone(), models::RowFieldValue(row.get(column.as_str()).cloned().unwrap_or(Value::Null)));
            }
            Field::Column { .. } => return Err(QueryError::new_unsupported_operation(&"columns are scalars; nested selection is not supported")),
            Field::Relationship { .. } => return Err(QueryError::new_unsupported_operation(&"relationships are not supported")),
        }
    }
    Ok(out)
}

fn non_null<'a>(rows: &'a [Row], c: &models::FieldName) -> Vec<&'a Value> {
    rows.iter().filter_map(|r| r.get(c.as_str())).filter(|v| !v.is_null()).collect()
}

fn aggregate(a: &Aggregate, rows: &[Row]) -> Result<Value, QueryError> {
    Ok(match a {
        Aggregate::StarCount {} => Value::from(rows.len()),
        Aggregate::ColumnCount { column, distinct, field_path: None, .. } => {
            let mut vals = non_null(rows, column);
            if *distinct {
                vals.sort_by(|a, b| compare(a, b));
                vals.dedup_by(|a, b| compare(a, b) == Ordering::Equal);
            }
            Value::from(vals.len())
        }
        Aggregate::SingleColumn { column, function, field_path: None, .. } => {
            let vals = non_null(rows, column);
            match function.as_str() {
                "min" => vals.into_iter().min_by(|a, b| compare(a, b)).cloned().unwrap_or(Value::Null),
                "max" => vals.into_iter().max_by(|a, b| compare(a, b)).cloned().unwrap_or(Value::Null),
                "sum" | "avg" => {
                    if vals.is_empty() {
                        return Ok(Value::Null);
                    }
                    let all_int = vals.iter().all(|v| v.is_i64());
                    let sum: f64 = vals.iter().filter_map(|v| v.as_f64()).sum();
                    if function.as_str() == "avg" {
                        Number::from_f64(sum / vals.len() as f64).map(Value::Number).unwrap_or(Value::Null)
                    } else if all_int {
                        // BigInt (Int64) results are serialised as strings
                        Value::String(vals.iter().filter_map(|v| v.as_i64()).sum::<i64>().to_string())
                    } else {
                        Number::from_f64(sum).map(Value::Number).unwrap_or(Value::Null)
                    }
                }
                other => return Err(QueryError::new_invalid_request(&format!("unknown aggregate function {other}"))),
            }
        }
        _ => return Err(QueryError::new_unsupported_operation(&"nested field aggregates are not supported")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(v: Value) -> Row {
        v.as_object().unwrap().clone()
    }

    fn q(v: Value) -> models::Query {
        serde_json::from_value(v).unwrap()
    }

    fn rows() -> Vec<Row> {
        vec![
            row(json!({"id": "1", "name": "Asha", "age": 31, "city": "Bengaluru"})),
            row(json!({"id": "2", "name": "Ravi", "age": 24, "city": "Mumbai"})),
            row(json!({"id": "3", "name": "Meera", "age": null, "city": "Bengaluru"})),
            row(json!({"id": "4", "name": "Kabir", "age": 45, "city": "Delhi"})),
        ]
    }

    #[test]
    fn filters_orders_and_paginates() {
        let query = q(json!({
            "fields": {"name": {"type": "column", "column": "name"}},
            "predicate": {"type": "or", "expressions": [
                {"type": "binary_comparison_operator", "column": {"type": "column", "name": "age"}, "operator": "_gte", "value": {"type": "scalar", "value": 30}},
                {"type": "unary_comparison_operator", "column": {"type": "column", "name": "age"}, "operator": "is_null"}
            ]},
            "order_by": {"elements": [{"order_direction": "desc", "target": {"type": "column", "name": "age", "path": []}}]},
            "limit": 2, "offset": 0
        }));
        let rs = execute(rows(), &query, &Vars::new()).unwrap();
        let names: Vec<Value> = rs.rows.unwrap().into_iter().map(|r| r["name"].0.clone()).collect();
        assert_eq!(names, vec![json!("Kabir"), json!("Asha")]); // Meera (null age) sorts last when descending
    }

    #[test]
    fn aggregates_respect_the_filter() {
        let query = q(json!({
            "aggregates": {
                "n": {"type": "star_count"},
                "cities": {"type": "column_count", "column": "city", "distinct": true},
                "avg_age": {"type": "single_column", "column": "age", "function": "avg"},
                "oldest": {"type": "single_column", "column": "age", "function": "max"}
            },
            "predicate": {"type": "binary_comparison_operator", "column": {"type": "column", "name": "city"}, "operator": "_in", "value": {"type": "scalar", "value": ["Bengaluru", "Delhi"]}}
        }));
        let a = execute(rows(), &query, &Vars::new()).unwrap().aggregates.unwrap();
        assert_eq!(a["n"], json!(3));
        assert_eq!(a["cities"], json!(2));
        assert_eq!(a["avg_age"], json!(38.0)); // (31 + 45) / 2, nulls ignored
        assert_eq!(a["oldest"], json!(45));
    }

    #[test]
    fn pushes_id_lookups_down_to_keys() {
        let vars = Vars::from([("ids".into(), json!(["7", "3", "7"]))]);
        let e: Expression = serde_json::from_value(json!({"type": "and", "expressions": [
            {"type": "binary_comparison_operator", "column": {"type": "column", "name": "city"}, "operator": "_eq", "value": {"type": "scalar", "value": "Delhi"}},
            {"type": "binary_comparison_operator", "column": {"type": "column", "name": "id"}, "operator": "_in", "value": {"type": "variable", "name": "ids"}}
        ]})).unwrap();
        assert_eq!(plan(Some(&e), &vars).unwrap(), Plan::Keys(vec!["3".into(), "7".into()]));
        let scan: Expression = serde_json::from_value(json!({"type": "binary_comparison_operator", "column": {"type": "column", "name": "city"}, "operator": "_eq", "value": {"type": "scalar", "value": "Delhi"}})).unwrap();
        assert_eq!(plan(Some(&scan), &vars).unwrap(), Plan::Scan);
    }
}
