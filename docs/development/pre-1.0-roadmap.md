# PARQONAUT pre-1.0 roadmap

Planned capability sequence before **v1.0.0**. Each item must ship as **implemented** or be **explicitly removed** with documented rationale before 1.0.

## v0.8.0 — Transform + stream completion

- `prqnt partition`, `merge`, `split`, `transform --spec`
- Declarative transform specs (schema version, validation, dry-run, execution report)
- Real stream schema discovery, unification, batch alignment, conflict policies
- Resumable `prqnt convert` with durable checkpoint identity and stale detection
- Progress event contract and terminal/non-TTY behavior
- Local/S3 coverage where supported; v0.9 convergence recon artifacts

## v0.9.0 — Columnar engine convergence

- One supported Arrow/Parquet stack
- Shared `RecordBatch` contracts across engines
- Eliminate arrow2/parquet2 split where practical
- In-memory cross-engine pipelines; remove unnecessary disk handoffs

## v0.10.0 — Plugin execution bridge (shipped, commit `1631a3c`)

- Plugin protocol v1 (`parqonaut-plugin-protocol`), Python SDK (`parqonaut_plugins`), subprocess host (`parqonaut-plugin-host`)
- CLI + opt-in server integration, digest-pinned durable scan jobs, Local/S3 batch plugin matrix, `parqonaut-*` namespace migration

## v0.11.0 — Interactive interfaces (current)

- Durable job-event journal, SSE + polling progress stream, `Idempotency-Key` submission safety
- Shared operator client (`parqonaut-client`), interactive TUI (`prqnt tui`), and bundled Axum web dashboard (`/ui/`)
- Live jobs, findings, explicit repair review & authorization, batch monitoring, and read-only plugin catalog

## v0.12.x — 1.0 hardening

- Public contract audit, CLI/API stability, migration guarantees, performance, fuzz/property tests, security audit, docs completeness, upgrade testing, release candidates

## v1.0.0 — Stable PARQONAUT

Roadmap may be refined when evidence demands it; deferrals must be recorded in release notes and this document.
