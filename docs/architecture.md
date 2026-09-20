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

## Columnar execution (v0.9+)

Product engines share **Apache Arrow / Parquet 54.3.1** and `parqonaut-columnar` batch-stream contracts (`BatchSource`, `BatchSink`, bounded `BatchStream`). Stream conversion, transform, repair data movers, and storage-backed I/O converge on `RecordBatch` pipelines. See [ADR-0015](./adr/ADR-0015-columnar-stack-convergence.md) and [Pipelines](./pipelines.md).

Prior multi-stack layout is documented in [ADR-0002](./adr/ADR-0002-coexisting-arrow-stacks.md) (superseded).

## Further reading

- [Storage architecture](./storage-architecture.md)
- [Durable jobs](./job-model.md)
- [HTTP API](./api.md)
