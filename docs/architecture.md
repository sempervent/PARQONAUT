# Architecture overview

PARQONAUT is a Rust-first toolkit for exploring, diagnosing, repairing, and orchestrating Parquet datasets — locally or on S3-compatible storage.

## Application layer (CLI and HTTP)

Scan, repair, and batch **orchestration** share one application facade:

```text
prqnt CLI ───┐
             ├──► parqonaut-app ──► scan / repair engines, orchestrator, storage
HTTP/workers ┘

parqonaut-app
  ├─ scan, diagnose, plan, check, repair, verify
  └─ batch check, plan, repair, status, resume, verify

Direct CLI utilities (intentionally outside parqonaut-app):
  inspect, rewrite, convert  →  transform / stream crates
```

`prqnt serve` runs in-process workers that dispatch durable jobs by kind into the same `parqonaut-app` entrypoints.

## Control plane and storage

```text
paraclete-store  →  SQLite / Postgres (jobs, tokens, runs)
parqonaut-storage →  S3StorageBackend (s3://)
paraclete-service →  HTTP router, auth, metrics, OpenAPI
```

## Workspace map (selected)

| Crate | Role |
|-------|------|
| `parqonaut-app` | Transport-neutral use cases |
| `parqonaut-cli` | `prqnt` binary |
| `paraclete-core` | Forensic scan engine |
| `parqonaut-repair` | Planning and repair execution |
| `parqonaut-orchestrator` | Batch journal and scheduler |
| `paraclete-store` | Durable metadata |
| `paraclete-service` | `/api/v1` server library |
| `parqonaut-transform` / `parqonaut-stream` | Direct Parquet/CSV utilities |

Historical crate names (`paraclete-*`) reflect provenance; see [Provenance](./provenance.md) and [ADR index](./adr/README.md).

## Arrow/Parquet stacks

Three stacks coexist (scan vs transform vs stream). Convergence is deferred — [ADR-0002](./adr/ADR-0002-coexisting-arrow-stacks.md).

## Further reading

- [Storage architecture](./storage-architecture.md)
- [Durable jobs](./job-model.md)
- [HTTP API](./api.md)
