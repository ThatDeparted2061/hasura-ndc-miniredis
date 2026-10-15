**ndc-test: 29 checks passed, 0 failed.** Through Hasura's v3 engine, a lookup by id takes 0.58 ms p50 against 0.29 ms straight to mini-redis-go, and a filtered, ordered scan of 10,000 hashes takes 81 ms against 122 ms done by hand.

# ndc-miniredis

[![CI](https://github.com/ThatDeparted2061/hasura-ndc-miniredis/actions/workflows/ci.yml/badge.svg)](https://github.com/ThatDeparted2061/hasura-ndc-miniredis/actions/workflows/ci.yml)

A Rust data connector for Hasura's v3 engine, built on the [NDC spec](https://hasura.github.io/ndc-spec/) v0.2 with Hasura's [`ndc-sdk-rs`](https://github.com/hasura/ndc-sdk-rs) v0.9. It turns a Redis-compatible store into a GraphQL API with filters, ordering, pagination, aggregates and mutations. The target is [mini-redis-go](https://github.com/ThatDeparted2061/mini-redis-go), a Redis server written from scratch; it also works unchanged against real Redis.

```graphql
{ users(where: {_and: [{city: {_eq: "Pune"}}, {age: {_gte: 30}}]}, order_by: [{age: Desc}], limit: 10) { id name age } }
mutation { insertUsers(id: "u-1", name: "Tara", city: "Pune", age: 33) { id name } }
```

## Results

`make demo` builds mini-redis-go from its repository, seeds 10,000 users and 50,000 orders, and starts the connector and the v3 engine in Docker. It then runs Hasura's connector test suite and times the same reads three ways:

| Query | Path | p50 | p95 |
|---|---|---:|---:|
| `usersById(id)` | direct `HGETALL` | 0.29 ms | 0.42 ms |
| | connector `/query` | 0.42 ms | 0.53 ms |
| | engine GraphQL | 0.58 ms | 0.74 ms |
| `users(where: city = Pune, order_by: age desc, limit: 10)` | direct: `SMEMBERS` + pipelined `HGETALL`, filter and sort in the client | 121.75 ms | 181.58 ms |
| | connector `/query` | 77.22 ms | 86.46 ms |
| | engine GraphQL | 81.16 ms | 89.96 ms |

2000 id lookups and 200 scans per path, one at a time, over 10,000 users, from the host into `docker compose` on a laptop.

The full `ndc-test` output is in [`bench/ndc-test.txt`](bench/ndc-test.txt). It covers capabilities, schema validation, select top N, predicates over every comparison operator with values taken from the data, sorting, and `star_count`, `column_count` and `single_column` aggregates.

**Reading the numbers.** By id, the engine adds about 0.3 ms over a raw `HGETALL`: GraphQL parsing and planning, plus one NDC request to the connector. On the scan, the engine path beats "direct". The direct client has to pull all 10,000 hashes across Docker's port forward to filter them, while the connector does the scan next to Redis and returns 10 rows. Push-down matters too: `usersById` and any `id: {_eq | _in}` filter become `HGETALL`s of those keys, not a scan (see `/query/explain`).

## How it maps Redis to tables

A **collection** is every hash whose key is `prefix + id`, so `users` is `user:1`, `user:2` and so on. Each hash field is a column; `id` is the key suffix. mini-redis-go, like any store without `KEYS`/`SCAN`, can't enumerate keys, so each collection keeps its ids in a set (`user:__index`). The insert and delete procedures maintain it.

```json
{
  "redis_url": "redis://127.0.0.1:6380",
  "collections": {
    "users": { "key_prefix": "user:", "columns": { "name": "String", "city": "String", "age": "Int", "score": "Float", "active": "Boolean" } }
  }
}
```

You don't have to write that by hand: `introspect redis://… users=user: orders=order:` samples rows and infers each column's type.

| NDC feature | Support |
|---|---|
| Schema | Collections, object types, scalar types `String`, `Int` (Int32), `Float`, `Boolean`; uniqueness constraint on `id` |
| Predicates | `_eq` `_in` `_lt` `_lte` `_gt` `_gte` on every type; `_contains` `_icontains` `_starts_with` on strings; `is_null`; `and` / `or` / `not` |
| Ordering, pagination | Any columns, asc/desc; `limit`/`offset` (unordered results are ordered by id, so pages are stable) |
| Aggregates | `star_count`, `column_count` (distinct too), `min`/`max`, `sum`/`avg` on numbers |
| Variables | Yes: the engine batches remote-relationship lookups this way |
| Mutations | `insert_<collection>` (upsert; null removes a field) and `delete_<collection>` procedures |
| Explain | `/query/explain` says whether a query is a key lookup or a scan |
| Not supported | Relationships, nested fields, grouping |

`metadata <config-dir> <connector-url>` writes the engine's OpenDD metadata from the same configuration, with the connector's schema embedded, so the GraphQL API can't drift from the connector.

## Run it

```bash
make demo                      # compose up, ndc-test, latency bench -> bench/RESULTS.md
open http://localhost:3280     # GraphQL endpoint: /graphql

# or by hand, against a mini-redis-go (or Redis) on :6380
cargo run --release --bin seed -- redis://127.0.0.1:6380 10000 50000
cargo run --release -- serve --configuration config --port 8100
cargo run --release -- test --configuration config      # Hasura's ndc-test suite
```

## Layout

| Path | |
|---|---|
| `src/lib.rs` | The `Connector` implementation: query, mutation, explain |
| `src/query.rs` | Planning (key push-down), predicates, ordering, pagination, aggregates |
| `src/schema.rs` | NDC schema and capabilities |
| `src/store.rs` | Hashes over the Redis protocol, pipelined |
| `src/bin/` | `seed`, `introspect`, `metadata` (OpenDD generator), `bench` |

## Limits

- Anything that isn't a key lookup is a full scan of the collection followed by filtering in the connector, which is what Redis offers without secondary indexes. Per-column index sets would make equality filters cheap.
- Writes are two commands (`HSET`, `SADD`) without `MULTI`, which mini-redis-go doesn't have. The order is chosen so a crash in between leaves a stale index entry, which reads skip, rather than a hidden row.
- `Int` is 32-bit so values stay JSON numbers. NDC carries 64-bit integers as strings, which is how `sum` over `Int` is returned (`BigInt`).
