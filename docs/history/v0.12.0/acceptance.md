# PARQONAUT v0.12.0 Release Acceptance & Independent Audit Passes

**Target milestone:** `v0.12.0`  
**Branch:** `feat/v0.12-production-hardening`  
**Baseline commit:** `3f5afbefc29cfa55266b4af8dbe700df2a0b9ff5` (`main`)

## Independent Audit Passes (A–H)

| Pass | Scope | Key Findings & Disposition | Verdict |
|---|---|---|---|
| **A. Data Integrity & Transaction Boundaries** | `parqonaut-store`, `parqonaut-service` | Enqueue, claim, cancel, success, and failure status updates commit atomically with lifecycle/terminal `application_job_events`; `JobEventRecorder` drains `critical_tx` + `progress_tx` in one flusher; `Idempotency-Key` hashes canonical JSON with bounded expiry cleanup (`V12-02`, `V12-03`, `V12-05`, `V12-06`). | **PASS** |
| **B. SQLite & PostgreSQL Migrations & Backup Recovery** | `parqonaut-store/migrations/`, `migration_upgrade.rs` | Tested v0.8 (`scan_jobs`), v0.9/v0.10 (`application_jobs`), and v0.11 forward migrations, WAL `VACUUM INTO` + `PRAGMA integrity_check = ok`, interrupted DDL failure + backup restoration, and Postgres 16 upgrade + `pg_dump`/`pg_restore` (`V12-08`). | **PASS** |
| **C. API, CLI, & Durable Contract Compatibility** | `parqonaut-cli`, `parqonaut-service/src/openapi/`, `parqonaut-repair` | Added `/api/v1/health/{live,ready}` to OpenAPI, fixed `prqnt merge <dir>` (`V12-11`), enforced `ExecutionManifest` & `DurableJobPayload` version checks (`V12-12`), and documented v1.0 stability policy in `compatibility.md`. | **PASS** |
| **D. Streaming, Memory, & Performance** | `parqonaut-bench`, `benches/stream-throughput/throughput.rs` | Replaced placeholder benchmark with real Criterion benchmarks + release `perf_baseline` across 12 workloads; verified `< 256 MiB` RSS delta on 250k CSV->Parquet (`--mem-budget 64`) and 200k ZSTD rewrite (`V12-07`). | **PASS** |
| **E. Fuzzing & Malformed/Malicious Input Handling** | `fuzz/`, `fuzz_smoke.rs`, `proptest_*`, `chaos_backend`, `fault_injection` | Fuzzing `fuzz_arrow_ipc` discovered & fixed `arrow-ipc 54.3.1` `capacity overflow` panic (`V12-09`); `proptest_contracts` caught & fixed `sanitize_event_message` length & secret leak (`V12-10`); all 9 fuzz targets + fault-injection tests pass. | **PASS** |
| **F. Security, Authentication, & Dependencies** | `Cargo.lock`, `deny.toml`, `dependabot.yml`, `error.rs`, `ui/` | Committed root `Cargo.lock`, enforced `--locked` CI gates, verified `uv.lock`, sanitized internal `Store`/`Internal` HTTP error messages, confirmed zero XSS sinks and strict CSP (`V12-04`, `V12-10`). Note: `cargo-audit`/`cargo-deny` binaries are not pre-installed locally. | **PASS** |
| **G. Packaging, Release Automation, & Clean Install** | `.github/workflows/release.yml`, `Dockerfile`, `compose.yaml` | Multi-target `.github/workflows/release.yml` (Linux x86_64/aarch64, macOS arm64/x86_64, SHA-256, SBOM, provenance attestations, clean-env smoke) and non-root `Dockerfile`/`compose.yaml`. Local clean-env archive smoke passed. | **PASS** |
| **H. Documentation & Operational Reproducibility** | `docs/operations.md`, `docs/installation.md`, `README.md`, `CHANGELOG.md`, `docs/history/v0.12.0/*` | Added 15-section `docs/operations.md`, updated installation/roadmap/changelog/CLI docs, verified `just docs-check` (`mdbook`, OpenAPI check, CLI check, link check) and `just naming-check`. | **PASS** |

## Verification Matrix (Executed Commands)

| Gate | Command | Result |
|---|---|---|
| Formatting | `just fmt-check` (`cargo fmt --all -- --check`) | **PASS** |
| Lint | `just lint` (`cargo-clippy --workspace --all-targets --all-features -- -D warnings`) | **PASS** (0 warnings) |
| Naming | `just naming-check` (`check-active-naming.sh` + selftest) | **PASS** |
| Columnar guardrails | `just columnar-check` | **PASS** |
| Docs & OpenAPI/CLI drift | `just docs-check` | **PASS** |
| Workspace tests (`--locked`) | `cargo test --locked --workspace` (all 20 crates incl. `migration_upgrade`, `cli_contracts`, `bounded_memory`, `fuzz_smoke`, `proptest_*`, `fault_injection`, `events_and_idempotency`) | **PASS** |
| Storage chaos suite | `cargo test --locked -p parqonaut-storage --features test-util --test chaos_backend` | **PASS** (12/12) |
| Python plugin SDK | `uv lock --check && uv sync --frozen && uv run ruff check . && uv run ruff format --check . && uv run python -m pytest` | **PASS** (7/7) |
| Browser & Console UI | `just ui-test` (`ui_dashboard` + `scripts/ui-test.mjs` 3/3 + headless Chrome `console-demo.sh`) | **PASS** |
| Terminal Console demo | `just tui-demo` | **PASS** |
| PostgreSQL 16 live tests & `pg_dump`/`pg_restore` | `postgres_upgrade_from_v0_9_v0_10_schema` (1/1) + `pg_dump`/`pg_restore` + `postgres_store_integration` (4/4) + `http_api_postgres` (2/2) | **PASS** |
| RustFS S3 live tests | `s3_operator_e2e` (1/1) + `s3_integration` (4/4) + `write_s3_roundtrip` (4/4) + `plugin_s3_cancellation` (2/2) | **PASS** |
| Release build & clean-archive smoke | `cargo build --workspace --release --locked` + tar + SHA-256 verify + isolated extract run (`--version`, `--help`, `plugin list`, `tui --help`, `serve --print-openapi`, CSV->Parquet + `--json scan`) | **PASS** |
| Criterion bench compile & release profile | `cargo bench --locked -p parqonaut-bench --no-run` + `cargo run --release --locked -p parqonaut-bench --bin perf_baseline` | **PASS** |

## Readiness Decision

**READY for v0.12.0** (0 unresolved P0/P1 findings; CI verification on the final commit SHA tracked in the PR).

## Deferred Risks & Recommended v1.0.0 RC Gates

1. Run `cargo audit` and `cargo deny check` in CI once security-audit runner tooling is installed alongside `deny.toml`.
2. Execute extended multi-hour `cargo-fuzz` campaigns on Linux (`x86_64-unknown-linux-gnu`) across all 9 targets in `fuzz/` before cutting `v1.0.0-rc.1`.
3. Verify `.github/workflows/release.yml` across all 4 matrix runners (Linux x86_64/aarch64, macOS arm64/x86_64) via `workflow_dispatch` on a pre-release RC tag prior to `v1.0.0`.
4. Note single-node worker model: `prqnt serve` uses in-process background workers; multi-node horizontal worker coordination remains explicitly out of scope for v0.12.0/v1.0.0 unless designed separately.
