# Phase 4 concurrency audit

## Scope

Batch orchestration in `parqonaut-orchestrator`: bounded scheduler, SQLite journal, output lockfiles, overlap prevention, cancellation, and resume.

## Scheduler bounds

- `BatchScheduler` uses a Tokio `Semaphore` sized to `batch.max_concurrency` (CLI `--jobs` override).
- Peak concurrency is recorded via `ConcurrencyMetrics` for observability and tests.
- Cancellation stops **scheduling** new dataset tasks; in-flight tasks run to completion at the repair executor boundary.

## Journal synchronization

- `RunJournal` exposes a synchronous API backed by SQLite.
- Each SQLite operation runs on a **dedicated worker thread** with its own single-thread Tokio runtime (`journal/sqlite.rs::tokio_task_block`).
- The batch executor additionally wraps hot-path journal writes in `std::thread::spawn` to avoid blocking the async scheduler thread during dataset execution.
- This design is intentionally conservative: journal writes are not on the critical path for CPU-bound Parquet rewrite, and the approach avoids nested-runtime panics when called from async CLI handlers.

## SQLite access

- One connection pool per run journal file (`{output_root}/.parqonaut/runs/{run_id}/journal.sqlite`).
- WAL mode, `foreign_keys=ON`, schema managed by `sqlx` migrations (`migrations/20250918120000_init.sql`).
- No cross-run sharing of journal files.

## Lock acquisition

- Per-dataset exclusive lockfile: `{output}.parqonaut.lock` containing the owning `run_id`.
- `create_new` semantics reject concurrent writers to the same output tree.
- Locks are released on scope drop after each dataset attempt.
- Dry-run does **not** acquire locks or create lockfiles.

## Overlapping datasets

- Planning derives outputs from `batch.output_root` + optional per-dataset `output` segment.
- Duplicate output targets and source/output or cross-dataset path nesting are rejected before execution (`paths.rs`, `overlap.rs`).
- Output mappings and fingerprints are embedded in the durable `BatchPlan`.

## Concurrent outputs

- Different datasets may execute concurrently up to the jobs bound when outputs are disjoint (required).
- Two datasets must never share an output path or nested output tree.

## Output commit boundaries

- Single-dataset repairs commit via Phase 3 staged publication inside `parqonaut-repair` (staging dir → verify → promote).
- Batch orchestration treats a dataset as `Succeeded` only after `RepairExecutor::execute` returns successfully.
- If the process dies during `Running`, resume conservatively reclassifies to `FailedRecoverable` and may retry; incomplete staging directories may remain for inspection per ADR-0010.

## Cancellation

- First SIGINT sets a cooperative `CancelFlag`.
- Scheduler stops enqueueing new datasets; active repairs finish at the repair executor boundary (no mid-row Parquet transaction).
- Run journal records `cancelled_at`; non-terminal datasets remain resumable where policy allows.

## Deadlock / starvation

- No lock ordering across datasets: each dataset lock is independent.
- Semaphore prevents unbounded task fan-out.
- Journal worker threads are short-lived per operation; no persistent lock inversion between journal and dataset locks.

## jobs=1 vs jobs=N

- Semantics are equivalent for terminal dataset outcomes; only scheduling order and peak concurrency differ.
- Integration tests assert both modes produce repaired outputs for the SHIPWCK fleet.

## Verdict

The current scheduler + thread-offloaded journal + per-output lockfile model is **sound and bounded** for Phase 4. No refactor to full async journal APIs is required unless cross-process journal contention becomes a requirement.
