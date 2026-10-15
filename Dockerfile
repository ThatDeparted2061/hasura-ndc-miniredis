FROM rust:1.96-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin ndc-miniredis --bin seed --bin metadata

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/ndc-miniredis /src/target/release/seed /src/target/release/metadata /usr/local/bin/
COPY config /etc/connector
ENV HASURA_CONFIGURATION_DIRECTORY=/etc/connector REDIS_URL=redis://redis:6380
EXPOSE 8100
ENTRYPOINT ["ndc-miniredis"]
CMD ["serve", "--port", "8100"]
