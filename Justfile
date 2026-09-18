set shell := ["bash", "-cu"]

# Cargo 1.98+ rejects `jobs = 0` in ~/.cargo/config.toml. Override when unset.
export CARGO_BUILD_JOBS := env_var_or_default("CARGO_BUILD_JOBS", `bash -lc 'nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4'`)

setup:
    rustup component add clippy rustfmt

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v cargo-clippy >/dev/null 2>&1; then
      cargo-clippy clippy --workspace --all-targets --all-features -- -D warnings
    else
      cargo clippy --workspace --all-targets --all-features -- -D warnings
    fi

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

phase2-fixtures:
    cargo run -p parqonaut-repair --bin generate-phase2-fixtures -- fixtures/phase2

phase3-fixtures:
    cargo run -p parqonaut-repair --bin generate-phase3-fixtures -- fixtures/phase3

phase3-demo:
    #!/usr/bin/env bash
    set -euo pipefail
    just phase3-fixtures
    echo "=== doctor ==="
    cargo run -p parqonaut-cli -- doctor fixtures/phase3/frankenlake-v2 \
        --policy fixtures/phase3/policy.toml
    echo "=== plan ==="
    cargo run -p parqonaut-cli -- plan fixtures/phase3/frankenlake-v2 \
        --policy fixtures/phase3/policy.toml \
        --output target/frankenlake-v2.plan.json
    echo "=== repair (safe only, no authorization) ==="
    rm -rf target/frankenlake-v2-repaired target/frankenlake-v2-repaired-safe
    cargo run -p parqonaut-cli -- repair fixtures/phase3/frankenlake-v2 \
        --plan target/frankenlake-v2.plan.json \
        --output target/frankenlake-v2-repaired-safe
    echo "=== repair (authorized schema ops) ==="
    rm -rf target/frankenlake-v2-repaired
    review_ids=$(python3 - <<'PY'
    import json
    plan = json.load(open("target/frankenlake-v2.plan.json"))
    for op in plan["operations"]:
        if op["safety"] == "review_required":
            print(op["operation_id"])
    PY
    )
    repair_args=(repair fixtures/phase3/frankenlake-v2 --plan target/frankenlake-v2.plan.json --output target/frankenlake-v2-repaired)
    for id in $review_ids; do repair_args+=(--authorize "$id"); done
    cargo run -p parqonaut-cli -- "${repair_args[@]}"
    echo "=== verify ==="
    cargo run -p parqonaut-cli -- verify fixtures/phase3/frankenlake-v2 target/frankenlake-v2-repaired \
        --manifest target/frankenlake-v2-repaired/.parqonaut-manifest.json
    echo "=== check (expect exit 4: blocked schema conflict remains) ==="
    cargo run -p parqonaut-cli -- check target/frankenlake-v2-repaired \
        --policy fixtures/phase3/ci-policy.toml || test $? -eq 4

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
