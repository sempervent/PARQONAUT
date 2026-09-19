# Changelog

All notable changes to PARQONAUT are documented here.

## [0.7.0] - 2026-09-19

Unified application server: CLI and HTTP share `parqonaut-app`; durable jobs for scan, repair, and batch orchestration.

### ADDED

- **`prqnt serve`** — PARQONAUT HTTP API at `/api/v1` (OpenAPI, metrics, health live/ready).
- **`parqonaut-app`** — transport-neutral scan, repair, and batch use cases.
- Durable **`application_jobs`** model (scan, repair, batch_repair, batch_resume) with lease/recovery and cancellation.
- Server storage policy (allowed local roots, S3 buckets/prefixes); `PRQNT_BOOTSTRAP_ADMIN_TOKEN`.
- API integration scripts (`scripts/api-test/`), `just api-test`, `api-demo`, `api-restart-demo`.
- CI jobs **`api-integration`** and **`postgres-integration`**.

### CHANGED

- CLI repair/batch commands delegate to **`parqonaut-app`** (scan already did).
- OpenAPI title **PARQONAUT API**; metrics namespace **`parqonaut_`**; new bearer tokens use **`prqnt_`** prefix.
- SQLite **`scan_jobs`** migrated to **`application_jobs`** (data preserved).

### SECURITY

- Default bind **127.0.0.1**; location policy enforced before storage I/O.

### DEFERRED

- Idempotency-Key, distributed workers, SDKs, web UI/TUI, TLS termination, plugin execution.

## [0.6.0] - 2026-09-19

Interface normalization: one public executable (`prqnt`), capability-oriented repository layout, and removal of legacy product entrypoints.

### BREAKING

- The `parqonaut` executable is renamed to **`prqnt`**. Update scripts, CI, and documentation accordingly. There is no compatibility alias.

### REMOVED

- `paraclete-http` legacy HTTP binary (service crate remains library-only; `prqnt serve` deferred).
- Product-scoped fixture generator binaries (`generate-phase2-fixtures`, `generate-phase3-fixtures`, `generate-phase4-fixtures`, `generate-phase5-fogbank-fixtures`, `generate-golden-plans`).

### CHANGED

- Developer automation consolidated under **`cargo xtask`** (fixtures, golden plans).
- Active tests, fixtures, scripts, Just targets, and CI jobs use capability names (`scan`, `repair`, `schema`, `orchestration`, `object-storage`, `s3-integration`) instead of numbered implementation phases.
- `paraclete-core` scan modules renamed: `scan_rules`, `scan_findings`, `schema_findings`; `evaluate_scan_rules` replaces `evaluate_phase1_rules`.
- CLI help describes PARQONAUT capabilities without predecessor engine branding.

### ADDED

- `scripts/check-active-naming.sh` and `just naming-check` (also wired into `just ci` and GitHub Actions).

[0.6.0]: https://github.com/sempervent/parqonaut/releases/tag/v0.6.0

## [0.4.1] - 2026-09-18

Release metadata correction only — no functional changes from v0.4.0.

### Fixed

- Workspace crate version aligned with the published `v0.4.0` release tag (`0.3.0` → `0.4.1`)
- `CHANGELOG.md` updated with missing release notes for v0.2.0–v0.4.0
- `README.md` updated to describe current Phase 4 capabilities and batch workflow

[0.4.1]: https://github.com/sempervent/parqonaut/releases/tag/v0.4.1

## [0.4.0] - 2026-09-18

Multi-dataset batch orchestration with bounded concurrency, durable journal, resume, and verification.

### Added

- `parqonaut-orchestrator` crate — batch plan contracts, scheduler, SQLite run journal, locking, resume
- `prqnt batch check|plan|repair|status|resume|verify` CLI commands with JSON output
- SHIPWRECK Phase 4 fixture fleet and integration tests
- Plan-bound authorization, output path mapping, and overlap detection for batch runs
- Cooperative SIGINT cancellation and idempotent resume semantics
- ADRs 0011–0014 and Phase 4 documentation
- `data/dummy.parquet` sample dataset for scan/inspect tests

[0.4.0]: https://github.com/sempervent/parqonaut/releases/tag/v0.4.0

## [0.3.0] - 2026-09-18

Policy-governed schema reconciliation and explicit authorization for review-required repairs.

### Added

- Deterministic schema compatibility model and lossless schema widening
- Explicit `--authorize` for ReviewRequired repair operations
- `prqnt doctor --policy`, `prqnt check`, `prqnt plan diff`
- Durable repair plan contract v1, execution manifests, staged publication
- FRANKENLAKE v2 laboratory fixtures and Phase 3 demo
- ADRs 0007–0010 and Phase 3 documentation

[0.3.0]: https://github.com/sempervent/parqonaut/releases/tag/v0.3.0

## [0.2.0] - 2026-09-18

Evidence-driven diagnose, plan, repair, and verify for single datasets.

### Added

- `parqonaut-repair` crate — scan → diagnose → plan → repair → verify pipeline
- Dataset fingerprints and stale-plan rejection
- Repair safety classification (Safe, ReviewRequired, Blocked)
- `prqnt doctor`, `prqnt plan`, `prqnt repair`, `prqnt verify`
- FRANKENLAKE Phase 2 fixtures and golden repair plans
- ADRs 0004–0006 and Phase 2 documentation

[0.2.0]: https://github.com/sempervent/parqonaut/releases/tag/v0.2.0

## [0.1.0] - 2026-09-18

Initial unified release consolidating Paraclete, parqknife, and streaming-parquet (maw).

### Added

- Cargo workspace with Paraclete-derived scan chassis (`paraclete-*` crates)
- `parqonaut-transform` — parqknife-derived inspect/rewrite/filter pipeline (Apache Arrow 54)
- `parqonaut-stream` — maw-derived streaming CSV/Parquet conversion (arrow2)
- Unified `prqnt` CLI with `scan`, `inspect`, `rewrite`, and `convert` commands
- End-to-end demo: CSV → Parquet → scan → rewrite → rescan (`just demo`)
- Integration tests covering all three engine families
- Fixtures, Python plugin contracts, and migration documentation
- GitHub Actions CI (fmt, clippy, test)
- Provenance and architecture ADRs

[0.1.0]: https://github.com/sempervent/parqonaut/releases/tag/v0.1.0
