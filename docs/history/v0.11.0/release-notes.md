# PARQONAUT v0.11.0 - Operator Console and Interactive Operations

PARQONAUT v0.11.0 turns the server, repair, batch, and plugin engines into an interactive operational tool across both terminal (`prqnt tui`) and browser (`/ui/`), backed by a durable job-event journal.

## Highlights

- **Durable job-event journal & SSE stream:** SQLite and Postgres `application_job_events` with per-job monotonic `seq` cursors, bounded recorder backpressure coalescing, retention pruning preserving terminal/lifecycle events, `GET /api/v1/jobs/{job_id}/events`, and authenticated `GET /api/v1/jobs/{job_id}/events/stream` (`Last-Event-ID` reconnect)
- **Progress coverage without fabricated percentages:** Scan, repair (`execute_with_progress`), batch (`run_with_progress`), cancellation, and lease recovery emit real progress; unknown workloads report indeterminate stages
- **Shared operator client (`parqonaut-client`):** Typed HTTP/SSE client and transport-neutral console & repair-review state model
- **Interactive terminal console (`prqnt tui`):** Ratatui/Crossterm 6-view console with keyboard navigation (`Tab`, `j/k`, `Enter`, `/`, `r`, `?`, `Esc`, `q`), masked token entry / env / `--token-file`, and panic-safe RAII terminal restoration
- **Bundled `/ui/` web dashboard:** Strict CSP (`script-src 'self'`), in-memory bearer token only, XSS-safe DOM construction, authenticated `fetch` SSE with polling fallback, light/dark themes, and explicit repair confirmation dialogs
- **Duplicate submission safety:** Durable `Idempotency-Key` (`idempotency_keys` table, `409 Conflict` on mismatched payload) on scan, repair, batch repair, and batch resume endpoints

See [architecture](architecture.md), [acceptance](acceptance.md), and [parallel-audit](parallel-audit.md).
