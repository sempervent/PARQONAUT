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

phase4-fixtures:
    cargo run -p parqonaut-orchestrator --bin generate-phase4-fixtures -- fixtures/phase4/shipwreck

phase4-demo:
    #!/usr/bin/env bash
    set -euo pipefail
    just phase4-fixtures
    rm -rf target/phase4-shipwreck-out
    echo "=== batch check (SHIPWRECK includes intentionally bad datasets) ==="
    cargo run -p parqonaut-cli -- batch check --config fixtures/phase4/shipwreck/batch.toml || test $? -eq 1
    echo "=== batch plan ==="
    cargo run -p parqonaut-cli -- batch plan --config fixtures/phase4/shipwreck/batch.toml \
        --output target/shipwreck.batch-plan.json
    python3 - <<'PY'
    import json, pathlib
    plan = json.load(open("target/shipwreck.batch-plan.json"))
    out = pathlib.Path("target/phase4-shipwreck-out").resolve()
    out.mkdir(parents=True, exist_ok=True)
    plan["output_root"] = str(out / "repaired")
    plan["run_root"] = plan["output_root"]
    for ds in plan["datasets"]:
        ds_id = ds["dataset_id"]["0"] if isinstance(ds["dataset_id"], dict) else ds["dataset_id"]
        ds["output_path"] = str(out / "repaired" / ds_id)
    json.dump(plan, open("target/shipwreck.batch-plan.json", "w"), indent=2)
    PY
    echo "=== batch repair ==="
    cargo run -p parqonaut-cli -- batch repair --plan target/shipwreck.batch-plan.json --jobs 2 || test $? -eq 2
    run_dir=$(find target/phase4-shipwreck-out/repaired/.parqonaut/runs -mindepth 1 -maxdepth 1 -type d | head -1)
    echo "=== batch status ==="
    cargo run -p parqonaut-cli -- batch status --run-dir "$run_dir"
    echo "=== batch verify ==="
    cargo run -p parqonaut-cli -- batch verify --run-dir "$run_dir" || true
    echo "=== aggregate report ==="
    test -f "$run_dir/report.json"
    head -40 "$run_dir/report.json"

phase5-up:
    scripts/phase5/up.sh

phase5-down:
    scripts/phase5/down.sh

phase5-fixtures:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/phase5/env.sh
    scripts/phase5/fixtures.sh

phase5-test:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/phase5/env.sh
    cargo test -p parqonaut-storage --features s3 fogbank -- --nocapture

phase5-demo:
    #!/usr/bin/env bash
    set -euo pipefail
    just phase5-up
    source scripts/phase5/env.sh
    just phase5-fixtures
    echo "=== scan remote healthy dataset ==="
    cargo run -p parqonaut-cli --features s3 -- scan "s3://${FOGBANK_BUCKET}/datasets/healthy/"
    echo "=== plan remote repair ==="
    cargo run -p parqonaut-cli --features s3 -- plan "s3://${FOGBANK_BUCKET}/datasets/healthy/" \
        --output target/phase5-plan.json
    echo "=== fogbank integration tests ==="
    just phase5-test

phase5-resume-demo:
    scripts/phase5/resume-demo.sh

phase5-batch-demo:
    scripts/phase5/batch-demo.sh

phase4-resume-demo:
    #!/usr/bin/env bash
    set -euo pipefail
    just phase4-fixtures
    rm -rf target/phase4-resume-out
    cargo run -p parqonaut-cli -- batch plan --config fixtures/phase4/shipwreck/batch.toml \
        --output target/shipwreck-resume.plan.json
    python3 - <<'PY'
    import json, pathlib
    plan = json.load(open("target/shipwreck-resume.plan.json"))
    out = pathlib.Path("target/phase4-resume-out").resolve()
    out.mkdir(parents=True, exist_ok=True)
    plan["output_root"] = str(out / "repaired")
    plan["run_root"] = plan["output_root"]
    for ds in plan["datasets"]:
        ds_id = ds["dataset_id"]["0"] if isinstance(ds["dataset_id"], dict) else ds["dataset_id"]
        ds["output_path"] = str(out / "repaired" / ds_id)
    json.dump(plan, open("target/shipwreck-resume.plan.json", "w"), indent=2)
    PY
    echo "=== start (interrupt after 2 datasets) ==="
    cargo run -p parqonaut-cli -- batch repair --plan target/shipwreck-resume.plan.json \
        --jobs 1 --interrupt-after 2 || test $? -eq 130
    run_dir=$(find target/phase4-resume-out/repaired/.parqonaut/runs -mindepth 1 -maxdepth 1 -type d | head -1)
    echo "=== status after interrupt ==="
    cargo run -p parqonaut-cli -- batch status --run-dir "$run_dir"
    echo "=== resume ==="
    cargo run -p parqonaut-cli -- batch resume --run-dir "$run_dir" --jobs 1 || test $? -eq 2
    echo "=== verify ==="
    cargo run -p parqonaut-cli -- batch verify --run-dir "$run_dir" || true

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
