# Deployment, Container, PostgreSQL, S3 & Plugin Validation (`v1.0.0-rc.1`)

## 1. Standalone Hardened Container (`Dockerfile`)

- **Builder Alignment:** Updated `Dockerfile` builder image to `rust:1.99-slim-bookworm` to match locked `aws-*` MSRV (`1.94.1+`), added `/var/lib/parqonaut/out`, and hardened `.dockerignore` (`**/target/`, `fuzz/Cargo.lock`, `.env*`, `**/*.sqlite*`, `**/*.profraw`).
- **Build & Runtime Verification (`parqonaut:rc-validation`):**
  - Non-root execution confirmed (`id -u` = `10001`, `id -g` = `10001`).
  - Read-only rootfs verified (`--read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m --memory 512m --cpus 1.0`).
  - Healthcheck `/api/v1/health/ready` reported `healthy` (`200 {"status":"ready"}`).
  - Auth fail-closed confirmed without bootstrap token (`401 unauthorized`) and authenticated with `PRQNT_BOOTSTRAP_ADMIN_TOKEN`.
  - Secret-free image layers (`docker history`) and logs verified.
  - `SIGKILL` crash + restart retained durable state; `SIGTERM` (`docker stop -t 15`) exited cleanly with code `0`.

## 2. SQLite Docker Compose (`compose.yaml`)

- **Runtime Fixes Verified:**
  - `prqnt serve` reads `PARQONAUT_DATABASE_URL` (ignoring empty `""`), `PARQONAUT_ALLOWED_LOCAL_ROOTS`, `PARQONAUT_ALLOWED_S3_BUCKETS`, and `PARQONAUT_ALLOWED_S3_PREFIXES`.
  - `StoragePolicy::validate_local_path` now resolves the deepest existing ancestor for non-existent repair output targets (`resolve_lexical_or_canonical`) while rejecting `..` traversal.
  - `compose.yaml` exposes `parqonaut_out` (`/var/lib/parqonaut/out:rw`) separate from read-only inputs (`/var/lib/parqonaut/data:ro`) and binds `127.0.0.1:${PARQONAUT_PORT:-8080}:8080`.
- **Live End-to-End Flow (`PARQONAUT_PORT=18080`):**
  - `GET /api/v1/whoami` (`admin`) succeeded (`database_backend="sqlite"`).
  - Idempotent `POST /api/v1/scans` + SSE (`job_event` + `job_status`) discovered `13` findings on `frankenlake-v2`.
  - `POST /api/v1/plans` -> `POST /api/v1/repairs` (with authorized `ReviewRequired` ops) -> `POST /api/v1/verifications` succeeded (`outcome: "verified"`).
  - Container restart retained all 2 jobs and 1 run; output artifacts persisted cleanly under `/var/lib/parqonaut/out/repaired-ds`.

## 3. PostgreSQL Docker Compose (`compose.yaml` + `compose.postgres.yaml --profile postgres`)

- **Wiring & Security:** `compose.postgres.yaml` connects `parqonaut` to `postgres` (`condition: service_healthy`), passes discrete `PARQONAUT_PG_HOST`/`PORT`/`USER`/`PASSWORD`/`DB` options without URI string interpolation (so `@ : / # ? %` credentials work verbatim), enforces required `PARQONAUT_PG_PASSWORD` (no weak default), and never falls back to SQLite when PostgreSQL fails.
- **Live Verification (`scripts/postgres-compose-test.sh` / `just postgres-compose-test`):**
  - Startup logged `database_backend="postgres"` with **zero** SQLite file created in `/var/lib/parqonaut/state`, verified with both simple (`simplepass123`) and special-character (`p@ss:w/o#rd?100%rc1`) passwords, and verified fail-closed startup (no SQLite fallback) on missing or wrong passwords.
  - Applied all 3 embedded Postgres migrations (`_sqlx_migrations`: `20250415120000`, `20250919140000`, `20261117120000`).
  - Verified auth bootstrap, durable scan job, monotonic event ordering (`[1, 2, 3, 4, 5]`), SSE `after_seq=2` replay (`[3, 4, 5]`), `Idempotency-Key` replay (`202` same `job_id`), and payload mismatch (`409 conflict`).
  - Direct `psql` inspection confirmed `tokens=1, jobs=1, events=5, runs=1, idem_keys=1`.
  - `SIGKILL` crash + restart retained full state in Postgres.
  - `pg_dump --format=custom` -> `createdb parqonaut_restored` -> `pg_restore` restored `jobs=1, events=5`.
  - Database outage test: stopping `postgres` kept `/api/v1/health/live` at `200 {"status":"ok"}` while `/api/v1/health/ready` returned `HTTP 503 {"error":{"code":"service_unavailable","message":"store dependency unavailable","details":{}}}` without leaking credentials; restarting `postgres` auto-recovered `/api/v1/health/ready` to `200 ready` and `/api/v1/jobs` without restarting `parqonaut`.
  - Graceful `SIGTERM` (`docker compose ... stop -t 15 parqonaut`) exited with code `0`; logs contained zero secret strings.

## 4. RustFS S3 & Python Plugin Matrix

- **Automated S3 & Plugin Suites against Live RustFS (`http://127.0.0.1:9000`):**
  - `just plugin-test`: Python `ruff` + `pytest` (7/7), `parqonaut-plugin-protocol`, `parqonaut-plugin-host` (68 tests), `parqonaut-transform` (`plugin_progress` 3/3, `plugin_runtime_gates` 6/6, `plugin_transform` 4/4, `plugin_zero_intermediate` 1/1, `plugin_s3_cancellation` 2/2, `plugin_storage_matrix` Local/S3 4-leg matrix), `parqonaut-cli` `plugin_integration` (6/6) — **ALL PASS**.
  - `parqonaut-storage --features "columnar s3"`: `columnar_s3_integration` (1/1), `fogbank_integration` (3/3), `multipart_boundary` (1/1, 80.5s), `s3_integration` (4/4), `s3_raw_write` (1/1), `write_s3_roundtrip` (4/4) — **ALL PASS**.
  - `parqonaut-transform --features s3`: `remote_matrix_s3` (3/3), `remote_pipeline_matrix` (6/6 four-leg matrix) — **ALL PASS**.
  - `parqonaut-service --features s3`: `s3_operator_e2e` (1/1), `server_plugin_durable` (10/10), `server_plugin_policy` (8/8) — **ALL PASS**.
  - `parqonaut-stream --features storage`: `remote_convert_s3` (2/2) — **ALL PASS**.
- **Live `prqnt --features s3` CLI Smoke on RustFS:**
  - `prqnt plan` + `prqnt repair` on `s3://fogbank/datasets/healthy/` -> `s3://parqonaut-test/rc1-repair-...` committed `.parqonaut/versions/.../data/.parqonaut-manifest.json` cleanly.
  - `prqnt transform --spec` with `normalize-strings` Python plugin (`s3://fogbank/...` -> `s3://parqonaut-test/...`) verified `500` rows readable via `prqnt scan ... --json`.
