# PARQONAUT v0.7.0 parallel audit

Audit date: 2026-09-19. Branch: `feat/application-server` @ `a3ad60d`.

## A — Application boundary / duplicated logic

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| A1 | P2 | `prqnt doctor` still calls repair crate paths directly, not `parqonaut-app`. | Accepted deferral: doctor is diagnostic utility; not in v0.7 HTTP surface. |
| A2 | — | Scan, repair, batch CLI commands delegate via `ParqonautApp::cli()`. HTTP handlers use `application_http` DTO → app. Workers dispatch by `JobKind` to app. | PASS |
| A3 | — | `inspect` / `rewrite` / `convert` remain CLI-only data utilities (intentional). | PASS |

## B — Generic jobs / SQLite / Postgres migration

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| B1 | P0 | Postgres `INTEGER` job counters failed sqlx decode as `i64`. | Fixed: store model uses `i32`; CI postgres-integration PASS. |
| B2 | — | Migration `20250919140000_application_jobs.sql` renames `scan_jobs` → `application_jobs`, adds kind/cancel/result_ref. | PASS (SQLite + Postgres tests) |
| B3 | P2 | Dedicated “upgrade from pre-migration DB file” fixture test is thin vs fresh schema tests. | Covered by store integration + migration on connect; acceptable for v0.7. |

## C — HTTP / OpenAPI / error contracts

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| C1 | — | OpenAPI golden `fixtures/api/openapi-v1.json`; test `openapi_matches_print_openapi_golden`. | PASS |
| C2 | — | `ApplicationError` mapped to HTTP `AppError` / envelopes. | PASS |
| C3 | P2 | Some batch/repair parity tests marked “skeleton” in `api_integration.rs`. | Extended smoke + restart demo; full semantic parity matrix remains incremental. |

## D — Authentication / storage policy / security

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| D1 | — | Default listen `127.0.0.1:8080`; bootstrap via `PRQNT_BOOTSTRAP_ADMIN_TOKEN`; `prqnt_` token prefix + legacy hash support. | PASS (auth_tokens + admin_tokens tests) |
| D2 | — | Storage policy enforced before I/O; denied-path tests with spy backend. | PASS |
| D3 | — | No credentials in job payloads / structured audit fields reviewed. | PASS |

## E — Cancellation / restart / concurrency

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| E1 | — | `POST /api/v1/jobs/{id}/cancel`; queued → canceled; running → cancel flag. | PASS |
| E2 | — | Stale lease recovery on startup; `api-restart-demo` kills server mid-scan and completes after restart. | PASS (local + CI api-integration) |
| E3 | — | Worker panic isolation via `catch_unwind` in worker loop. | PASS (unit test) |
| E4 | — | `--workers 0` rejected; in-process pool bounds active jobs. | PASS |

## F — CLI/API parity / S3

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| F1 | — | Sync scan smoke via API; async job path in HTTP tests. | PASS |
| F2 | P2 | Full plan fingerprint CLI↔API equivalence suite not exhaustive in one test file. | Partial: shared `parqonaut-app` planning path; golden plans updated for 0.7.0. |
| F3 | — | S3 integration job (RustFS) PASS; object-storage crate tests unchanged. | PASS |
| F4 | P2 | End-to-end HTTP repair workflow against RustFS not duplicated in s3-integration job (CLI/S3 tests cover storage). | Documented; HTTP S3 scan/repair covered in service tests where env set. |

## G — Adversarial API review (optional)

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| G1 | — | JSON body size limit returns 413; request IDs on responses. | PASS |
| G2 | — | No permissive CORS on default router. | PASS |

## Summary

- **P0:** 1 found, 1 fixed.
- **P1:** 0 open.
- **P2:** 4 accepted with documented rationale; no release blockers.
