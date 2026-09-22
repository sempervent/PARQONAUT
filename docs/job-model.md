# Durable jobs

`prqnt serve` persists long-running work in **`application_jobs`** (SQLite or Postgres via `parqonaut-store`).

## Job kinds (durable async)

| Kind | Trigger (examples) |
|------|---------------------|
| **scan** | `POST /api/v1/scans` |
| **repair** | `POST /api/v1/repairs` |
| **batch_repair** | `POST /api/v1/batches/repairs` |
| **batch_resume** | `POST /api/v1/batches/{run_id}/resume` |

## States

```text
queued → running → succeeded | failed | canceled
```

Workers claim jobs with leases and heartbeats. Expired leases are recovered on server startup (requeue or fail with attempt limits).

## Cancellation

`POST /api/v1/jobs/{job_id}/cancel` — queued jobs cancel immediately; running jobs set a cooperative cancel flag.

## Synchronous API operations

Diagnose, plan, check, verify, and batch status/check/plan/verify remain synchronous in v0.7 unless deliberately promoted later.

See [HTTP API](./api.md) and [prqnt serve](./server.md).
