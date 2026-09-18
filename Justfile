set shell := ["bash", "-cu"]

setup:
    rustup component add clippy rustfmt

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
    cargo test --workspace

check:
    cargo check --workspace

build:
    cargo build --workspace

bench:
    cargo bench --workspace || true

docs:
    @echo "See docs/architecture.md and docs/adr/"

ci: fmt-check lint test

demo:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp=$(mktemp -d)
    echo "a,b" > "$tmp/input.csv"
    echo "1,2" >> "$tmp/input.csv"
    cargo run -p parqonaut-cli -- convert "$tmp/input.csv" -o "$tmp/streamed.parquet" --out-format parquet
    cargo run -p parqonaut-cli -- scan "$tmp/streamed.parquet"
    cargo run -p parqonaut-cli -- rewrite "$tmp/streamed.parquet" "$tmp/rewritten.parquet" --compression zstd
    cargo run -p parqonaut-cli -- scan "$tmp/rewritten.parquet" --json
    echo "Demo complete in $tmp"
