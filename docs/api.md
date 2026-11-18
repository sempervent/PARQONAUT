# PARQONAUT HTTP API (`/api/v1`)

PARQONAUT ships the supported API path prefix **`/api/v1`** (distinct from product semver in OpenAPI `info.version`).

## Endpoint groups

| Group | Examples |
|-------|----------|
| Health & overview | `/api/v1/health`, `/api/v1/health/live`, `/api/v1/health/ready`, `/api/v1/overview` |
| Web dashboard | `/ui/` (static shell only; strict CSP, no operational data without auth) |
| Meta | `/metrics`, `/api/v1/openapi.json` |
| Identity | `/api/v1/whoami` |
| Scans & jobs | `POST /api/v1/scans`, `POST /api/v1/jobs/scans`, `GET /api/v1/jobs`, `GET /api/v1/jobs/{job_id}`, `POST /api/v1/jobs/{job_id}/cancel` |
| Job events & SSE | `GET /api/v1/jobs/{job_id}/events`, `GET /api/v1/jobs/{job_id}/events/stream` (`Last-Event-ID` / `after_seq`) |
| Runs & findings | `GET /api/v1/runs`, `/api/v1/runs/{id}`, report/assets/findings, target run lists, `/api/v1/diff` |
| Repair | `POST /api/v1/diagnose`, `/api/v1/plans`, `/api/v1/checks`, `/api/v1/repairs`, `/api/v1/verifications` |
| Batches | `POST /api/v1/batches/check`, `/plans`, `/repairs`, `GET /api/v1/batches/{run_dir}`, `GET /api/v1/jobs/{job_id}/batch`, `/resume`, `/verify` |
| Plugins | `GET /api/v1/plugins`, `GET /api/v1/plugins/{name}` (read-only allowlisted catalog) |
| Admin | `/api/v1/admin/tokens` … |

Bearer token required via `Authorization: Bearer <token>` on all `/api/v1` routes except `/api/v1/health*`. Never send bearer tokens in URL query parameters.

Mutation submissions (`POST /api/v1/scans`, `/jobs/scans`, `/repairs`, `/batches/repairs`, `/batches/{run_dir}/resume`) accept `Idempotency-Key` (`1..=128` printable ASCII chars): matching payload replays the existing job; mismatched payload returns `409 Conflict` (`conflict`).

OpenAPI title: **PARQONAUT API**; `info.version` tracks the workspace crate version.
