#!/usr/bin/env bash
# Bring up mini-redis-go + connector + v3 engine, run Hasura's ndc-test suite
# against the connector, then time the same reads three ways. Writes bench/RESULTS.md.
set -euo pipefail
cd "$(dirname "$0")/.."
N=${N:-2000}
docker compose up -d --build
for _ in $(seq 1 120); do curl -sf localhost:3280/graphql -H 'content-type: application/json' -d '{"query":"{ usersById(id: \"1\") { id } }"}' | grep -q '"id"' && break; sleep 1; done
cargo build --release --bins
REDIS_URL=redis://127.0.0.1:6380 target/release/ndc-miniredis test --configuration config 2>&1 | sed 's/\x1b\[[0-9;]*m//g' > bench/ndc-test.txt || true
target/release/bench redis://127.0.0.1:6380 http://localhost:8100 http://localhost:3280 "$N" > bench/latency.json
python3 - <<'PY'
import json, re
t = open("bench/ndc-test.txt").read()
ok, fail = len(re.findall(r" OK$", t, re.M)), len(re.findall(r" FAIL$", t, re.M))
d = json.load(open("bench/latency.json"))
row = lambda q, k: f"{d[q][k]['p50_ms']:.2f} ms | {d[q][k]['p95_ms']:.2f} ms"
b, s = d["by_id"], d["scan_filter_order_limit"]
lines = [
    f"**ndc-test: {ok} checks passed, {fail} failed.** Through Hasura's v3 engine, a lookup by id takes "
    f"{b['engine']['p50_ms']:.2f} ms p50 against {b['redis']['p50_ms']:.2f} ms straight to mini-redis-go, and a filtered, "
    f"ordered scan of {d['users']:,} hashes takes {s['engine']['p50_ms']:.0f} ms against {s['redis']['p50_ms']:.0f} ms done by hand.",
    "",
    "| Query | Path | p50 | p95 |", "|---|---|---:|---:|",
    f"| `usersById(id)` | direct `HGETALL` | {row('by_id', 'redis')} |",
    f"| | connector `/query` | {row('by_id', 'connector')} |",
    f"| | engine GraphQL | {row('by_id', 'engine')} |",
    f"| `users(where: city = Pune, order_by: age desc, limit: 10)` | direct: `SMEMBERS` + pipelined `HGETALL`, filter and sort in the client | {row('scan_filter_order_limit', 'redis')} |",
    f"| | connector `/query` | {row('scan_filter_order_limit', 'connector')} |",
    f"| | engine GraphQL | {row('scan_filter_order_limit', 'engine')} |",
    "",
    f"{d['iterations']} id lookups and {d['scan_iterations']} scans per path, one at a time, over {d['users']:,} users, from the host into `docker compose` on a laptop.",
]
open("bench/RESULTS.md", "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
PY
