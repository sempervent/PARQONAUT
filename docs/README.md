# PARQONAUT

**Parquet Analysis, Rewriting, Quality, Orchestration, Navigation, Auditing, Unification & Transformation**

PARQONAUT helps teams **explore**, **diagnose**, **repair**, and **orchestrate** Parquet datasets — from a single file to multi-dataset fleets on local disk or S3-compatible object storage.

The public command-line tool is **`prqnt`**. The optional **application server** (`prqnt serve`) exposes the same core workflows over **`/api/v1`**.

## What you can do today

| Area | Examples |
|------|----------|
| **Scan** | Forensic inventory, findings, dataset identity |
| **Repair** | Evidence-bound plans, safe vs review-required operations, staged output |
| **Schema** | Reconciliation policies, explicit authorization for risky changes |
| **Batch** | Multi-dataset plans, bounded concurrency, resume, verification |
| **Object storage** | `s3://` scan, repair, and batch with `S3StorageBackend` |
| **Server** | Durable async jobs (scan, repair, batch), RBAC, OpenAPI |

## Install

```bash
git clone https://github.com/sempervent/PARQONAUT
cd PARQONAUT
cargo build --release -p parqonaut-cli --bin prqnt
```

See [Installation](./installation.md) for `cargo install --git …` and feature flags.

## Quick examples

```bash
prqnt scan ./dataset

prqnt doctor ./dataset

prqnt repair ./dataset \
  --plan repair-plan.json \
  --output repaired/

prqnt batch plan \
  --config batch.toml \
  --output batch-plan.json

prqnt scan s3://bucket/prefix/

prqnt serve
```

## Where to go next

- [Getting started](./getting-started.md) — first successful scan and repair
- [CLI overview](./cli.md) — commands and exit semantics
- [Application server](./server.md) — `prqnt serve`, jobs, and storage policy
- [Architecture](./architecture.md) — how CLI, HTTP, and `parqonaut-app` fit together

## Current boundaries (v0.7)

PARQONAUT shares application logic between CLI and HTTP via **`parqonaut-app`**. Deep CLI/API fingerprint parity is enforced primarily through that shared layer rather than one exhaustive cross-transport test matrix. Full HTTP repair-over-S3 end-to-end coverage is still growing. **`prqnt doctor`** is a CLI-oriented workflow and is not exposed as a separate durable API job in v0.7.
