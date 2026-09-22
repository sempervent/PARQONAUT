# PARQONAUT v0.10.0 acceptance

All mandatory categories **PASS** for v0.10.0 release (no mandatory deferrals).

| Category | Result |
|----------|--------|
| Namespace purity | PASS |
| Protocol v1 | PASS |
| Manifest / catalog / digest | PASS |
| Scan analyzer runtime | PASS |
| Batch Arrow IPC runtime | PASS |
| Schema preservation | PASS |
| Backpressure / cancellation | PASS |
| Four-leg storage matrix | PASS |
| Secret isolation | PASS |
| CLI plugin management | PASS |
| Transform spec plugins | PASS |
| Server disabled by default | PASS |
| Server explicit roots | PASS |
| Server allowlist | PASS |
| Catalog API | PASS |
| HTTP names-only request | PASS |
| Durable resolved selections | PASS |
| Digest pinning | PASS |
| Recovery unchanged plugin | PASS |
| Recovery stale plugin rejection | PASS |
| Removed plugin rejection | PASS |
| Policy-changed rejection | PASS |
| Server plugin cancellation | PASS |
| SQLite plugin job | PASS |
| Postgres plugin job | PASS (CI `postgres-integration`) |
| Server secret canary | PASS |
| Storage policy pre-spawn rejection | PASS |
| Plugin CI | PASS |
| S3 plugin CI | PASS |
| Workspace fmt / clippy / test / build | PASS |
| Python gates | PASS |
| Demos (incl. plugin-server-demo) | PASS |
| Docs / OpenAPI | PASS |
| Parallel audit | PASS |

Evidence: CI on PR #12, local `just ci`, `just plugin-test`, `just plugin-server-demo`, and [parallel-audit](parallel-audit.md).
