# PARQONAUT HTTP API (`/api/v1`)

PARQONAUT ships the supported API path prefix **`/api/v1`** (distinct from product semver in OpenAPI `info.version`).

## Endpoint groups

| Group | Examples |
|-------|----------|
| Health | `/api/v1/health`, `/api/v1/health/live`, `/api/v1/health/ready` |
| Meta | `/metrics`, `/api/v1/openapi.json` |
| Identity | `/api/v1/whoami` |
| Scans & jobs | `POST /api/v1/scans`, `POST /api/v1/scans/sync`, `GET /api/v1/jobs`, `GET /api/v1/jobs/{id}` |
| Runs | `/api/v1/runs/{id}`, report/assets/findings, target run lists, `/api/v1/diff` |
| Admin | `/api/v1/admin/tokens` … |

Bearer token required on protected routes. OpenAPI title: **PARQONAUT API**; `info.version` tracks the workspace crate version.

Direct CLI utilities (`inspect`, `rewrite`, `convert`) intentionally have no HTTP twins in v0.7.x.
