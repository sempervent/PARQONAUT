# Server stack recon (v0.7.0 planning)

Planning history for PARQONAUT v0.7.0 (`prqnt serve`). May move to `docs/history/v0.7.0/` at release.

## Existing crates (pre-v0.7)

| Area | Crate | Notes |
|------|-------|-------|
| HTTP + workers | `paraclete-service` | Axum `/api/v1`, in-process scan job worker, OpenAPI via utoipa |
| Persistence | `paraclete-store` | SQLite + Postgres, `scan_jobs` lease/recovery, auth tokens (hashed) |
| Scan engine | `paraclete-core` | Local forensic scan |
| Remote scan/repair | `parqonaut-repair` / `parqonaut-storage` | `s3://` datasets, publication, repair execution |
| Batch | `parqonaut-orchestrator` | Journal on disk; not duplicated in service DB |

## Gaps addressed in v0.7.0

- **Single executable:** `prqnt serve` (no `paraclete-http` binary).
- **Shared application layer:** `parqonaut-app` for transport-neutral scan (and expanding repair/batch).
- **Branding:** OpenAPI title `PARQONAUT API`, metrics `parqonaut_*`, new tokens `prqnt_*` prefix.
- **S3 on HTTP:** Remove local-only scan restriction when location policy allows.
- **Server policy:** Configurable `allowed_local_roots` / S3 bucket-prefix allow lists.
- **Health:** `/api/v1/health/live` and `/api/v1/health/ready` (DB ping).

## Still evolving post-freeze

- Generic `job_kind` column and repair/batch async job kinds (scan jobs generalized incrementally).
- Full repair/plan/check/batch HTTP surface and CLI/API parity tests.
- Job cancel endpoint and idempotency keys.

## APPLICATION_CONTRACT_SHA

Record at interface-freeze commit on `feat/application-server` (see `application-server-agent-plan-v0.7.md`).
