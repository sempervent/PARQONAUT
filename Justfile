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

naming-check:
    scripts/check-active-naming.sh

ci: fmt-check lint test naming-check

api-test:
    cargo test -p paraclete-service --tests
    cargo test -p paraclete-store --test auth_tokens

repair-fixtures:
    cargo xtask fixtures repair

schema-fixtures:
    cargo xtask fixtures schema

orchestration-fixtures:
    cargo xtask fixtures orchestration

golden-plans:
    cargo xtask golden-plans

schema-demo:
    #!/usr/bin/env bash
    set -euo pipefail
    just schema-fixtures
    echo "=== doctor ==="
    cargo run -p parqonaut-cli --bin prqnt -- doctor fixtures/schema/frankenlake-v2 \
        --policy fixtures/schema/policy.toml
    echo "=== plan ==="
    cargo run -p parqonaut-cli --bin prqnt -- plan fixtures/schema/frankenlake-v2 \
        --policy fixtures/schema/policy.toml \
        --output target/frankenlake-v2.plan.json
    echo "=== repair (safe only, no authorization) ==="
    rm -rf target/frankenlake-v2-repaired target/frankenlake-v2-repaired-safe
    cargo run -p parqonaut-cli --bin prqnt -- repair fixtures/schema/frankenlake-v2 \
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
    repair_args=(repair fixtures/schema/frankenlake-v2 --plan target/frankenlake-v2.plan.json --output target/frankenlake-v2-repaired)
    for id in $review_ids; do repair_args+=(--authorize "$id"); done
    cargo run -p parqonaut-cli --bin prqnt -- "${repair_args[@]}"
    echo "=== verify ==="
    cargo run -p parqonaut-cli --bin prqnt -- verify fixtures/schema/frankenlake-v2 target/frankenlake-v2-repaired \
        --manifest target/frankenlake-v2-repaired/.parqonaut-manifest.json
    echo "=== check (expect exit 4: blocked schema conflict remains) ==="
    cargo run -p parqonaut-cli --bin prqnt -- check target/frankenlake-v2-repaired \
        --policy fixtures/schema/ci-policy.toml || test $? -eq 4

batch-demo:
    #!/usr/bin/env bash
    set -euo pipefail
    just orchestration-fixtures
    rm -rf target/batch-shipwreck-out
    echo "=== batch check (SHIPWRECK includes intentionally bad datasets) ==="
    cargo run -p parqonaut-cli --bin prqnt -- batch check --config fixtures/orchestration/shipwreck/batch.toml || test $? -eq 1
    echo "=== batch plan ==="
    cargo run -p parqonaut-cli --bin prqnt -- batch plan --config fixtures/orchestration/shipwreck/batch.toml \
        --output target/shipwreck.batch-plan.json
    python3 - <<'PY'
    import json, pathlib
    plan = json.load(open("target/shipwreck.batch-plan.json"))
    out = pathlib.Path("target/batch-shipwreck-out").resolve()
    out.mkdir(parents=True, exist_ok=True)
    plan["output_root"] = str(out / "repaired")
    plan["run_root"] = plan["output_root"]
    for ds in plan["datasets"]:
        ds_id = ds["dataset_id"]["0"] if isinstance(ds["dataset_id"], dict) else ds["dataset_id"]
        ds["output_path"] = str(out / "repaired" / ds_id)
    json.dump(plan, open("target/shipwreck.batch-plan.json", "w"), indent=2)
    PY
    echo "=== batch repair ==="
    cargo run -p parqonaut-cli --bin prqnt -- batch repair --plan target/shipwreck.batch-plan.json --jobs 2 || test $? -eq 2
    run_dir=$(find target/batch-shipwreck-out/repaired/.parqonaut/runs -mindepth 1 -maxdepth 1 -type d | head -1)
    echo "=== batch status ==="
    cargo run -p parqonaut-cli --bin prqnt -- batch status --run-dir "$run_dir"
    echo "=== batch verify ==="
    cargo run -p parqonaut-cli --bin prqnt -- batch verify --run-dir "$run_dir" || true
    echo "=== aggregate report ==="
    test -f "$run_dir/report.json"
    head -40 "$run_dir/report.json"

s3-up:
    scripts/s3-test/up.sh

s3-down:
    scripts/s3-test/down.sh

s3-fixtures:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/s3-test/env.sh
    scripts/s3-test/fixtures.sh

s3-test:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/s3-test/env.sh
    cargo test -p parqonaut-storage --features s3 fogbank -- --nocapture

s3-demo:
    #!/usr/bin/env bash
    set -euo pipefail
    just s3-up
    source scripts/s3-test/env.sh
    just s3-fixtures
    echo "=== scan remote healthy dataset ==="
    cargo run -p parqonaut-cli --bin prqnt --features s3 -- scan "s3://${FOGBANK_BUCKET}/datasets/healthy/"
    echo "=== plan remote repair ==="
    cargo run -p parqonaut-cli --bin prqnt --features s3 -- plan "s3://${FOGBANK_BUCKET}/datasets/healthy/" \
        --output target/s3-demo-plan.json
    echo "=== fogbank integration tests ==="
    just s3-test

s3-resume-demo:
    scripts/s3-test/resume-demo.sh

mixed-storage-demo:
    scripts/s3-test/batch-demo.sh

batch-resume-demo:
    #!/usr/bin/env bash
    set -euo pipefail
    just orchestration-fixtures
    rm -rf target/batch-resume-out
    cargo run -p parqonaut-cli --bin prqnt -- batch plan --config fixtures/orchestration/shipwreck/batch.toml \
        --output target/shipwreck-resume.plan.json
    python3 - <<'PY'
    import json, pathlib
    plan = json.load(open("target/shipwreck-resume.plan.json"))
    out = pathlib.Path("target/batch-resume-out").resolve()
    out.mkdir(parents=True, exist_ok=True)
    plan["output_root"] = str(out / "repaired")
    plan["run_root"] = plan["output_root"]
    for ds in plan["datasets"]:
        ds_id = ds["dataset_id"]["0"] if isinstance(ds["dataset_id"], dict) else ds["dataset_id"]
        ds["output_path"] = str(out / "repaired" / ds_id)
    json.dump(plan, open("target/shipwreck-resume.plan.json", "w"), indent=2)
    PY
    echo "=== start (interrupt after 2 datasets) ==="
    cargo run -p parqonaut-cli --bin prqnt -- batch repair --plan target/shipwreck-resume.plan.json \
        --jobs 1 --interrupt-after 2 || test $? -eq 130
    run_dir=$(find target/batch-resume-out/repaired/.parqonaut/runs -mindepth 1 -maxdepth 1 -type d | head -1)
    echo "=== status after interrupt ==="
    cargo run -p parqonaut-cli --bin prqnt -- batch status --run-dir "$run_dir"
    echo "=== resume ==="
    cargo run -p parqonaut-cli --bin prqnt -- batch resume --run-dir "$run_dir" --jobs 1 || test $? -eq 2
    echo "=== verify ==="
    cargo run -p parqonaut-cli --bin prqnt -- batch verify --run-dir "$run_dir" || true

demo:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp=$(mktemp -d)
    echo "a,b" > "$tmp/input.csv"
    echo "1,2" >> "$tmp/input.csv"
    cargo run -p parqonaut-cli --bin prqnt -- convert "$tmp/input.csv" -o "$tmp/streamed.parquet" --out-format parquet
    cargo run -p parqonaut-cli --bin prqnt -- scan "$tmp/streamed.parquet"
    cargo run -p parqonaut-cli --bin prqnt -- rewrite "$tmp/streamed.parquet" "$tmp/rewritten.parquet" --compression zstd
    cargo run -p parqonaut-cli --bin prqnt -- scan "$tmp/rewritten.parquet" --json
    echo "Demo complete in $tmp"
