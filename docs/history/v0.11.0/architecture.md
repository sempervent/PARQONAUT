# PARQONAUT v0.11.0 Architecture & Recon: Operator Console

## Baseline Reconnaissance (from `main` @ `1631a3c20f13eca8f715959fbd1b2ebd79916cce`)

Existing capabilities already implemented and verified:

- **Durable jobs (`parqonaut-store`, `parqonaut-service`):** `application_jobs` table across SQLite & Postgres with `scan`, `repair`, `batch_repair`, and `batch_resume` kinds, lease/heartbeat recovery, and cooperative cancellation (`POST /api/v1/jobs/{job_id}/cancel`).
- **Scan, runs, findings, diff:** `POST /api/v1/scans`, `GET /api/v1/runs/{run_id}`, paginated `/assets` and `/findings` projections, target history, `/api/v1/diff`.
- **Repair planning & execution (`parqonaut-repair`, `parqonaut-app`):** `POST /api/v1/diagnose`, `/plans`, `/checks`, `/repairs`, `/verifications` with `RepairSafety` (`Safe`, `ReviewRequired`, `Blocked`, `Destructive`), plan fingerprint binding, and explicit `authorize[]` IDs.
- **Batch orchestration (`parqonaut-orchestrator`):** `POST /api/v1/batches/check`, `/plans`, `/repairs`, `GET /api/v1/batches/{run_dir}`, `/resume`, `/verify` backed by per-run SQLite journals.
- **Plugins (v0.10.0):** `GET /api/v1/plugins`, `GET /api/v1/plugins/{name}`, digest-pinned scan analyzer execution.
- **Progress contract (`parqonaut-workflow`):** `ProgressEvent`, `ProgressEventKind`, `ProgressMetrics`, `ProgressObserver` (used by stream/transform; not yet persisted on durable server jobs).

### Gaps Addressed in v0.11.0

1. **No durable per-job event journal:** Workers write only coarse job status (`queued` → `running` → terminal). Clients cannot inspect ordered step/operation history or resume live progress after reconnect.
2. **No repair/batch execution progress wiring:** `RepairExecutor` and `BatchExecutor` emit tracing logs rather than `ProgressObserver` events.
3. **No duplicate-submission protection:** Repeated `POST` clicks on `/scans`, `/repairs`, or `/batches/repairs` create duplicate jobs.
4. **No run listing or job-scoped batch status endpoint:** Clients need `GET /api/v1/runs`, `GET /api/v1/overview`, and `GET /api/v1/jobs/{job_id}/batch` (for active and completed batch jobs).
5. **No interactive interfaces:** Missing shared operator HTTP/SSE client, `prqnt tui`, and bundled Axum `/ui/` dashboard.

## Architectural Decisions

### 1. Shared Progress & Durable Event Journal

- Extend `parqonaut_workflow::{ProgressEventKind, ProgressMetrics}` so engines, workers, store, API, TUI, and Web UI share **one** event vocabulary.
- Persist events in `application_job_events` (SQLite + Postgres migrations) with `event_id` (UUID), `job_id`, monotonic per-job `seq` (allocated atomically via `UPDATE application_jobs SET last_event_seq = last_event_seq + 1 ... RETURNING last_event_seq`), `schema_version`, `occurred_at`, `kind`, `stage`, `is_terminal`, `metrics_json`, sanitized `message`, and optional `run_id`, `plugin_name`, `plugin_digest`.
- **Authoritative state:** `application_jobs.status` remains the single source of truth; events record history and progress transitions.
- **Boundedness & retention:** `JobEventRecorder` routes high-frequency non-terminal progress updates (`BatchProcessed`, `FileStarted`, `FileCompleted`, `PluginProgress`, `CheckpointSaved`) through a bounded `mpsc` channel using `try_send` (never blocking workers). Overflow increments a per-job coalesced counter and emits a `ProgressCoalesced` summary when space frees or before terminal completion. Critical lifecycle and terminal events are always persisted directly. Per-job retention (`PARQONAUT_MAX_EVENTS_PER_JOB`, default `500`) prunes oldest non-terminal rows while keeping terminal/lifecycle rows intact.
- **Indeterminate progress:** `ProgressMetrics::completion_fraction()` returns `Some` only when `files_total > 0` or `datasets_total > 0`; unknown workloads remain `None` (indeterminate) and never fabricate percentages from elapsed time.

### 2. HTTP Event Stream & Idempotency

- `GET /api/v1/jobs/{job_id}/events?after_seq=&limit=&kind=&stage=` for paginated cursor replay.
- `GET /api/v1/jobs/{job_id}/events/stream` for authenticated SSE (`Authorization: Bearer` header only; supports `Last-Event-ID` and `after_seq` cursor deduplication, lag recovery from SQL/Postgres store, and clean close on terminal job).
- `Idempotency-Key` header on `POST /api/v1/scans`, `/jobs/scans`, `/repairs`, `/batches/repairs`, `/batches/{run_dir}/resume` backed by `idempotency_keys (scope, idempotency_key)` with `request_sha256` verification (`202` replay on match, `409 Conflict` on mismatched payload).

### 3. Shared Operator Client (`parqonaut-client`)

- One typed Rust client wrapping `/api/v1` (REST + streaming SSE over `Authorization: Bearer`) and shared console state/RBAC helpers (`OperatorClientError` distinguishing `Connection`, `Unauthorized`, `Forbidden`, `Operation`, `Transient`).
- Consumed directly by `prqonaut tui` and integration tests.

### 4. Browser Dashboard Technology Choice: Embedded Native ES Modules vs TypeScript/Vite

Evaluated:
- **Option A:** Separate TypeScript/Vite SPA compiled into `dist/` and embedded. Requires Node/npm in Rust CI or committing compiled JS bundles prone to merge conflicts and build drift.
- **Option B (Chosen):** Modular server-hosted HTML, CSS, and standard ES modules (`crates/parqonaut-service/src/ui/`) embedded directly in the `parqonaut-service` binary via `include_str!`.
  - **Rationale:** Zero build-toolchain coupling for `cargo build` / `prqnt`, zero external CDNs, strict `Content-Security-Policy` (`script-src 'self'; style-src 'self'; connect-src 'self'`), DOM construction exclusively via `createElement` + `textContent` (eliminating `innerHTML` XSS vectors), in-memory bearer token only (never `localStorage` or URLs), and authenticated `fetch()` SSE streaming with custom headers.

### 5. Security & Trust Boundaries

- Static `/ui/` assets serve no operational data; all `/api/v1/*` endpoints (except `/api/v1/health*`) enforce bearer auth and RBAC (`Reader`, `Operator`, `Admin`).
- `prqnt serve` binds `127.0.0.1:8080` by default.
- TUI reads tokens from `PARQONAUT_API_TOKEN` (or `--token-env` / `--token-file` / interactive obscured prompt) — never `--token` CLI flags.
- Repair execution in both TUI and Web UI enforces explicit per-ID opt-in for `ReviewRequired` operations, locks `Blocked`/`Destructive` operations, displays source/output/plan identity before confirmation, and relies on server-side revalidation.
