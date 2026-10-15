**ndc-test: 29 checks passed, 0 failed.** Through Hasura's v3 engine, a lookup by id takes 0.58 ms p50 against 0.29 ms straight to mini-redis-go, and a filtered, ordered scan of 10,000 hashes takes 81 ms against 122 ms done by hand.

| Query | Path | p50 | p95 |
|---|---|---:|---:|
| `usersById(id)` | direct `HGETALL` | 0.29 ms | 0.42 ms |
| | connector `/query` | 0.42 ms | 0.53 ms |
| | engine GraphQL | 0.58 ms | 0.74 ms |
| `users(where: city = Pune, order_by: age desc, limit: 10)` | direct: `SMEMBERS` + pipelined `HGETALL`, filter and sort in the client | 121.75 ms | 181.58 ms |
| | connector `/query` | 77.22 ms | 86.46 ms |
| | engine GraphQL | 81.16 ms | 89.96 ms |

2000 id lookups and 200 scans per path, one at a time, over 10,000 users, from the host into `docker compose` on a laptop.
