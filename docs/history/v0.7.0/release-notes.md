# PARQONAUT v0.7.0 — Application Server

## Highlights

- **`prqnt serve`** — local HTTP application server with `/api/v1` (OpenAPI, metrics, health live/ready).
- **Shared application layer** — CLI and HTTP both call **`parqonaut-app`** for scan, repair, and batch workflows.
- **Durable jobs** — scan, repair, batch repair, and batch resume with lease/recovery, cancellation, and SQLite/Postgres persistence.
- **Security defaults** — loopback bind, RBAC bearer tokens (`prqnt_` prefix), storage location policy, bootstrap via `PRQNT_BOOTSTRAP_ADMIN_TOKEN`.
- **S3** — API and workers use the same storage backends; RustFS-backed CI for object storage.

## Deferred (not in this release)

- Distributed / remote workers
- Idempotency-Key header support
- SDK generation, web UI, TUI
- TLS termination in `prqnt serve`
- Plugin execution on the server

See [CHANGELOG](https://github.com/sempervent/PARQONAUT/blob/main/CHANGELOG.md#070---2026-09-19) for full details.
