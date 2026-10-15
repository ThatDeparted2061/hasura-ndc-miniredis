//! Introspect collections from live data and print a configuration.json:
//! `introspect <redis-url> users=user: orders=order: > config/configuration.json`
//!
//! For each `name=prefix` it samples up to 200 rows from the collection's
//! index set and infers every field's type: Int if every value is a 32-bit
//! integer, Float if every value is a number, Boolean for true/false/1/0, else String.

use std::collections::{BTreeMap, HashMap};

use ndc_miniredis::config::{Collection, Configuration, ScalarType};

fn infer(values: &[&str]) -> ScalarType {
    let all = |f: fn(&str) -> bool| values.iter().all(|v| f(v));
    if all(|v| v == "true" || v == "false") {
        ScalarType::Boolean
    } else if all(|v| v.parse::<i32>().is_ok()) {
        ScalarType::Int
    } else if all(|v| v.parse::<f64>().is_ok()) {
        ScalarType::Float
    } else {
        ScalarType::String
    }
}

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let args: Vec<String> = std::env::args().collect();
    let url = args.get(1).cloned().unwrap_or_else(|| "redis://127.0.0.1:6380".into());
    let mut conn = redis::Client::open(url.as_str())?.get_multiplexed_async_connection().await?;
    let mut collections = BTreeMap::new();
    for spec in &args[2..] {
        let (name, prefix) = spec.split_once('=').expect("collections are name=prefix");
        let mut col = Collection { description: None, key_prefix: prefix.into(), index_key: None, columns: BTreeMap::new() };
        // ponytail: SMEMBERS then the first 200; mini-redis-go has no SRANDMEMBER
        let ids: Vec<String> = redis::cmd("SMEMBERS").arg(col.index_key()).query_async(&mut conn).await?;
        let mut pipe = redis::pipe();
        for id in ids.iter().take(200) {
            pipe.cmd("HGETALL").arg(col.key(id));
        }
        let rows: Vec<HashMap<String, String>> = pipe.query_async(&mut conn).await?;
        let mut seen: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for r in &rows {
            for (k, v) in r {
                if k != "id" && !k.starts_with("__") {
                    seen.entry(k.clone()).or_default().push(v.clone());
                }
            }
        }
        for (field, vals) in &seen {
            col.columns.insert(field.clone(), infer(&vals.iter().map(String::as_str).collect::<Vec<_>>()));
        }
        col.description = Some(format!("Introspected from {} sampled rows", rows.len()));
        eprintln!("{name}: {} rows sampled, {} columns", rows.len(), col.columns.len());
        collections.insert(name.to_string(), col);
    }
    println!("{}", serde_json::to_string_pretty(&Configuration { redis_url: url, collections }).unwrap());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infers_the_narrowest_type() {
        assert_eq!(infer(&["1", "42", "-7"]), ScalarType::Int);
        assert_eq!(infer(&["1", "4.5"]), ScalarType::Float);
        assert_eq!(infer(&["true", "false"]), ScalarType::Boolean);
        assert_eq!(infer(&["3000000000"]), ScalarType::Float); // too big for Int32
        assert_eq!(infer(&["Pune", "1"]), ScalarType::String);
    }
}
