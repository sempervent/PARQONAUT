# syntax=docker/dockerfile:1

FROM rust:1.99-slim-bookworm AS builder
WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY xtask ./xtask
COPY benches ./benches
COPY fuzz ./fuzz
COPY fixtures/api ./fixtures/api

RUN cargo build --release --locked -p parqonaut-cli --bin prqnt

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 parqonaut \
    && useradd --system --uid 10001 --gid 10001 --home-dir /var/lib/parqonaut --create-home parqonaut

COPY --from=builder /src/target/release/prqnt /usr/local/bin/prqnt

RUN mkdir -p /var/lib/parqonaut/state /var/lib/parqonaut/data /var/lib/parqonaut/out \
    && chown -R parqonaut:parqonaut /var/lib/parqonaut

USER parqonaut:parqonaut
WORKDIR /var/lib/parqonaut
VOLUME ["/var/lib/parqonaut/state"]

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/api/v1/health/ready || exit 1

ENTRYPOINT ["/usr/local/bin/prqnt"]
CMD ["serve", "--listen", "0.0.0.0:8080", "--state-dir", "/var/lib/parqonaut/state"]
