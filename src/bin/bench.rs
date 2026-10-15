//! Latency of the same reads three ways: straight to Redis, through the
//! connector's NDC /query, and through the v3 engine's GraphQL.
//! `bench <redis-url> <connector-url> <engine-url> [iterations]`; prints JSON.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

fn pct(mut xs: Vec<Duration>, p: f64) -> f64 {
    xs.sort();
    xs[((xs.len() as f64 * p) as usize).min(xs.len() - 1)].as_secs_f64() * 1000.0
}

async fn time<F, Fut>(n: usize, mut f: F) -> Value
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    for i in 0..n.min(50) {
        f(i).await; // warm up connections and caches
    }
    let mut lat = Vec::with_capacity(n);
    let mut rows = 0;
    for i in 0..n {
        let t = Instant::now();
        rows = f(i).await;
        lat.push(t.elapsed());
    }
    json!({"p50_ms": pct(lat.clone(), 0.5), "p95_ms": pct(lat.clone(), 0.95), "p99_ms": pct(lat, 0.99), "rows": rows})
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (redis_url, ndc, engine) = (&args[1], &args[2], &args[3]);
    let n: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1000);
    let mut conn = redis::Client::open(redis_url.as_str()).unwrap().get_multiplexed_async_connection().await.unwrap();
    let http = reqwest::Client::new();
    let users: usize = redis::cmd("SCARD").arg("user:__index").query_async(&mut conn).await.unwrap();
    let id = |i: usize| format!("{}", i % users + 1);

    let post = |url: String, body: Value| {
        let http = http.clone();
        async move { http.post(url).json(&body).send().await.unwrap().json::<Value>().await.unwrap() }
    };

    // 1. one row by primary key
    let by_id_redis = time(n, |i| {
        let mut c = conn.clone();
        let k = format!("user:{}", id(i));
        async move {
            let h: HashMap<String, String> = redis::cmd("HGETALL").arg(k).query_async(&mut c).await.unwrap();
            h.len().min(1)
        }
    })
    .await;
    let by_id_ndc = time(n, |i| {
        post(format!("{ndc}/query"), json!({"collection": "users", "arguments": {}, "collection_relationships": {}, "query": {
            "fields": {"id": {"type": "column", "column": "id"}, "name": {"type": "column", "column": "name"}, "city": {"type": "column", "column": "city"}},
            "predicate": {"type": "binary_comparison_operator", "column": {"type": "column", "name": "id"}, "operator": "_eq", "value": {"type": "scalar", "value": id(i)}}}}))
        .then_len(|v| v[0]["rows"].as_array().map_or(0, Vec::len))
    })
    .await;
    let by_id_gql = time(n, |i| {
        post(format!("{engine}/graphql"), json!({"query": format!("{{ usersById(id: \"{}\") {{ id name city }} }}", id(i))}))
            .then_len(|v| usize::from(!v["data"]["usersById"].is_null()))
    })
    .await;

    // 2. filter + order + limit over the whole collection (a scan)
    let m = (n / 10).max(30);
    let scan_redis = time(m, |_| {
        let mut c = conn.clone();
        async move {
            let ids: Vec<String> = redis::cmd("SMEMBERS").arg("user:__index").query_async(&mut c).await.unwrap();
            let mut pipe = redis::pipe();
            for i in &ids {
                pipe.cmd("HGETALL").arg(format!("user:{i}"));
            }
            let rows: Vec<HashMap<String, String>> = pipe.query_async(&mut c).await.unwrap();
            let mut pune: Vec<i64> = rows.iter().filter(|r| r.get("city").map(String::as_str) == Some("Pune")).filter_map(|r| r.get("age")?.parse().ok()).collect();
            pune.sort_unstable_by(|a, b| b.cmp(a));
            pune.len().min(10)
        }
    })
    .await;
    let scan_ndc = time(m, |_| {
        post(format!("{ndc}/query"), json!({"collection": "users", "arguments": {}, "collection_relationships": {}, "query": {
            "fields": {"id": {"type": "column", "column": "id"}, "age": {"type": "column", "column": "age"}},
            "predicate": {"type": "binary_comparison_operator", "column": {"type": "column", "name": "city"}, "operator": "_eq", "value": {"type": "scalar", "value": "Pune"}},
            "order_by": {"elements": [{"order_direction": "desc", "target": {"type": "column", "name": "age", "path": []}}]},
            "limit": 10}}))
        .then_len(|v| v[0]["rows"].as_array().map_or(0, Vec::len))
    })
    .await;
    let scan_gql = time(m, |_| {
        post(format!("{engine}/graphql"), json!({"query": "{ users(where: {city: {_eq: \"Pune\"}}, order_by: [{age: Desc}], limit: 10) { id age } }"}))
            .then_len(|v| v["data"]["users"].as_array().map_or(0, Vec::len))
    })
    .await;

    println!("{}", serde_json::to_string_pretty(&json!({
        "users": users, "iterations": n, "scan_iterations": m,
        "by_id": {"redis": by_id_redis, "connector": by_id_ndc, "engine": by_id_gql},
        "scan_filter_order_limit": {"redis": scan_redis, "connector": scan_ndc, "engine": scan_gql}
    })).unwrap());
}

trait ThenLen: std::future::Future<Output = Value> + Sized {
    async fn then_len(self, f: impl FnOnce(&Value) -> usize) -> usize {
        f(&self.await)
    }
}
impl<F: std::future::Future<Output = Value>> ThenLen for F {}
