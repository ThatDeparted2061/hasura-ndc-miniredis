.PHONY: demo test up down metadata
demo:       ## docker compose up, ndc-test, latency bench -> bench/RESULTS.md
	bench/run.sh
test:
	cargo test
up:
	docker compose up -d --build
down:
	docker compose down -v
metadata:   ## regenerate engine metadata for a connector on the host
	cargo run --release --bin metadata -- config http://host.docker.internal:8100 > engine/metadata.json
