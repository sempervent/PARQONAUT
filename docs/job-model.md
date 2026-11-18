# Durable jobs & operational events

`prqnt serve` persists long-running work in **`application_jobs`** and ordered operational history in **`application_job_events`** (SQLite or Postgres via `parqonaut-store`).

## Job kinds (durable async)

| Kind | Trigger |
|------|---------|
| **scan** | `POST /api/v1/scans` / `POST /api/v1/jobs/scans` |
| **repair** | `POST /api/v1/repairs` |
| **batch_repair** | `POST /api/v1/batches/repairs` |
| **batch_resume** | `POST /api/v1/batches/{run_dir}/resume` |

## Authoritative state & event journal

```text
queued → running → succeeded | failed | canceled
```

`application_jobs.status` is the single authoritative state machine. `application_job_events` records monotonic per-job `seq` events (`job_queued`, `job_claimed`, `job_recovered`, `cancel_requested`, `job_cancelled`, `execution_started`, `inputs_discovered`, `operation_started`, `operation_completed`, `output_committed`, `verification_started`, `verification_completed`, `plugin_*`, `execution_completed`, `execution_failed`).

- **Indeterminate vs finite progress:** Completion percentage is reported only when `datasets_total > 0` or `files_total > 0`. Unknown totals report an indeterminate stage rather than fabricating progress from elapsed time.
- **Retention & backpressure:** High-frequency non-critical progress updates flow through a bounded channel (`PARQONAUT_EVENT_CHANNEL_CAPACITY`) and coalesce when full; oldest non-terminal progress rows are pruned past `PARQONAUT_MAX_EVENTS_PER_JOB` while lifecycle/terminal events are preserved.
- **Reconnection:** Clients reconnect to `GET /api/v1/jobs/{job_id}/events/stream` with `Last-Event-ID` or `?after_seq=` (or poll `GET /api/v1/jobs/{job_id}/events`) without duplicate application.

## Cancellation & recovery

`POST /api/v1/jobs/{job_id}/cancel` cancels `queued` jobs immediately and sets a cooperative cancel flag on `running` jobs. Workers renew leases with heartbeats; expired leases are recovered on startup (`job_recovered` / `job_retries_exhausted`).

See [HTTP API](./api.md) and [prqnt serve](./server.md).
