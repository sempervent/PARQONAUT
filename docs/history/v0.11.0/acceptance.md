# PARQONAUT v0.11.0 Acceptance

Verification record for `feat/v0.11-operator-console`.

## Environment

- **Local host:** `darwin` arm64, Rust workspace `0.11.0`, Node `v24.21.0`, headless Google Chrome (CDP), Python 3.13 (`uv.lock`), Docker `postgres:16-alpine`, `ghcr.io/rustfs/rustfs:1.0.0`.
- **CI host:** GitHub Actions `ubuntu-latest` (`CI`, `Plugin integration`, `Documentation` workflows).

## Verification Matrix

| Category | Command | Result |
|----------|---------|--------|
| Formatting, clippy, naming, columnar | `just fmt-check && just lint && just naming-check && just columnar-check` | PASS |
| Store events, retention, idempotency (SQLite) | `cargo test -p parqonaut-store --test job_events_and_idempotency` | PASS (4 passed) |
| Service events, SSE, RBAC, idempotency, UI CSP | `cargo test -p parqonaut-service --test events_and_idempotency --test ui_dashboard --test openapi_contract` | PASS (12 passed) |
| Shared operator client & full E2E w/ restart | `cargo test -p parqonaut-client --tests` | PASS (5 passed) |
| TUI reducer, RBAC, 80×24 render, terminal restore | `cargo test -p parqonaut-tui` & `just tui-demo` | PASS (4 passed) |
| Browser JS unit + headless Chrome E2E | `UI_BROWSER_REQUIRED=1 just ui-test` | PASS (2 Node tests + Chrome CDP smoke + restart) |
| PostgreSQL store & HTTP API + events parity | `cargo test -p parqonaut-store --test postgres_store_integration -- --ignored --test-threads=1 && cargo test -p parqonaut-service --test http_api_postgres -- --ignored` | PASS (4 + 2 passed) |
| RustFS S3 repair + durable events E2E | `cargo test -p parqonaut-service --features s3 --test s3_operator_e2e -- --nocapture --test-threads=1` | PASS (1 passed) |
| Full workspace tests | `cargo test --workspace` | Executed on CI + local |
| Docs, CLI ref, OpenAPI golden, mdbook | `just docs-check` | PASS |

Detailed findings and audit passes are documented in [parallel-audit](parallel-audit.md) and [architecture](architecture.md).
