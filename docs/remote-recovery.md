# Remote recovery

Recovery covers **batch resume**, **server job lease recovery**, and **S3 dataset staleness** handling.

- **Batch** — [Resume and recovery](./resume-recovery.md) (journal, locks, stale source).
- **Server jobs** — [Durable jobs](./job-model.md) (lease expiry, requeue, attempt limits).
- **Object storage** — fingerprints recomputed before mutating work; mismatches fail closed.

HTTP **`POST /api/v1/batches/{run_id}/resume`** submits a durable batch resume job with the same application semantics as `prqnt batch resume`.
