# PARQONAUT v0.7.0 acceptance record

Release: **0.7.0**  
Contract baseline: **`APPLICATION_CONTRACT_SHA=134b828679b4f1e648bb8336e77fd1378bcc04a2`**  
Integration branch tip at acceptance: **`a3ad60d`**

| Criterion | Status | Evidence |
|-----------|--------|----------|
| Shared application layer (`parqonaut-app`) | PASS | Crate + `contracts.rs`; CLI repair/batch tests |
| CLI repair/batch through app | PASS | `repair.rs`, `batch.rs`, `repair_app_delegation` test |
| HTTP through app | PASS | `application_http.rs`, handlers call service → app |
| Generic durable jobs | PASS | `application_jobs`, `JobKind` in types/store |
| Scan job | PASS | `POST /api/v1/scans`, worker dispatch |
| Repair job | PASS | `POST /api/v1/repairs` |
| Batch repair / resume jobs | PASS | batch HTTP + worker kinds |
| Job cancellation | PASS | cancel endpoint + store flags |
| SQLite migration | PASS | store tests + api-integration |
| Postgres migration | PASS | postgres-integration CI job |
| Restart recovery | PASS | `just api-restart-demo`, recovery tests |
| S3 API / storage | PASS | s3-integration CI; S3StorageBackend tests |
| Storage policy | PASS | policy tests + server.toml in api-test |
| RBAC | PASS | http_api, admin_tokens, roles |
| OpenAPI golden | PASS | `openapi_contract.rs` |
| CLI/API parity | PASS (core) | Shared app layer; smoke + integration tests |
| Worker bounds | PASS | serve rejects `--workers 0` |
| Full local tests | PASS | `cargo test --workspace` (CI rust job) |
| All CI jobs | PASS | rust, api-integration, postgres-integration, s3-integration @ run 35466259298 |
| Clean install | PASS | `cargo install --path crates/parqonaut-cli` → `prqnt` only |

**Verdict:** All mandatory items **PASS** for v0.7.0 application-server release.
