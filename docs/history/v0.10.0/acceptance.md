# PARQONAUT v0.10.0 acceptance

Release-readiness verification for `feat/plugin-execution` (PR [#12](https://github.com/sempervent/PARQONAUT/pull/12)).

## Tested commit & CI evidence

- **Tested code commit SHA:** `94442a9e139791d967aeb5a07e80cf80a66ef8f9`
- **CI (`rust`, `API integration (SQLite)`, `Postgres integration`, `S3 integration (RustFS)`):** [Run 44209487021](https://github.com/sempervent/PARQONAUT/actions/runs/44209487021) (PASS)
- **Plugin integration:** [Run 44209487006](https://github.com/sempervent/PARQONAUT/actions/runs/44209487006) & [Run 44209479694](https://github.com/sempervent/PARQONAUT/actions/runs/44209479694) (PASS)
- **Documentation:** [Run 44209487019](https://github.com/sempervent/PARQONAUT/actions/runs/44209487019) (PASS)

## Environment

- Local verification host: `darwin` (arm64), Rust workspace + `uv` Python 3.13 (`pydantic`, `pyarrow` from `python/parqonaut_plugins/uv.lock`), Docker `postgres:16-alpine` (ephemeral local Postgres) and `ghcr.io/rustfs/rustfs:1.0.0` (RustFS S3-compatible backend).
- CI verification: GitHub Actions `ubuntu-latest` (`CI`, `Plugin integration`, `Documentation` workflows on PR #12).

## Verification gates

| Category | Command / Gate | Result |
|----------|----------------|--------|
| Python SDK (`uv`, `ruff`, `pytest`) | `cd python/parqonaut_plugins && uv sync --frozen && uv run ruff check . && uv run ruff format --check . && uv run python -m pytest` | PASS (7 passed) |
| Formatting & clippy | `cargo fmt --all -- --check && just lint` | PASS |
| Active naming purity | `just naming-check` | PASS |
| Columnar boundary guard | `just columnar-check` | PASS |
| Plugin protocol & host (incl. lifecycle, timeout, cancel, stderr, digest, env) | `cargo test -p parqonaut-plugin-protocol && cargo test -p parqonaut-plugin-host` | PASS (5 + 63 tests) |
| Transform & CLI plugin gates | `cargo test -p parqonaut-transform --test plugin_transform --test plugin_zero_intermediate --test plugin_progress --test plugin_runtime_gates && cargo test -p parqonaut-cli --test plugin_integration` | PASS (14 + 6 tests) |
| Full workspace test suite | `cargo test --workspace` | PASS |
| Server plugin policy & durable jobs (SQLite) | `cargo test -p parqonaut-service --test server_plugin_policy --test server_plugin_durable -- --test-threads=1` | PASS (8 + 10 tests) |
| Postgres store & HTTP API plugin jobs (repeated + concurrent) | `cargo test -p parqonaut-store --test postgres_store_integration -- --ignored --test-threads=1 && cargo test -p parqonaut-service --test http_api_postgres -- --ignored` | PASS (3 + 2 tests, repeated) |
| Four-leg S3 matrix, cancellation & mid-stream abort (RustFS) | `cargo test -p parqonaut-transform --features s3 --test plugin_storage_matrix --test plugin_s3_cancellation -- --nocapture --test-threads=1` | PASS (1 matrix [4 legs] + 2 tests) |
| Server plugin E2E demo | `just plugin-server-demo` | PASS |
| S3 plugin E2E demo | `just plugin-s3-demo` | PASS |
| Docs, CLI reference & OpenAPI golden | `just docs-check` | PASS |

Detailed findings and independent review passes (A–F) are recorded in [parallel-audit](parallel-audit.md).
