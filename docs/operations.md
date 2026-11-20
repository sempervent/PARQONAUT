# Operations Runbook

Operational guide for running `prqnt serve` as a long-running service. Note: PARQONAUT uses an **in-process worker pool** per server instance (no external distributed worker queue in v1.0.0-rc.1).

## 1. Installation and Upgrade

Install from a verified release archive (`tar.gz` + `SHA256SUMS`), container image (`Dockerfile`), or `cargo build --release --locked -p parqonaut-cli --bin prqnt`.

Before upgrading an existing instance:

1. Quiesce incoming traffic or drain the server cleanly via `SIGTERM`.
2. Back up the database (SQLite WAL `VACUUM INTO` or PostgreSQL `pg_dump`) **before** starting the new binary.
3. Start the new `prqnt serve` binary; embedded `sqlx` forward migrations apply automatically at startup.
4. Verify `GET /api/v1/health/ready` returns `200 {"status":"ready"}`.

## 2. Configuration Precedence

`prqnt serve` resolves settings in the following order (highest precedence first):

1. Explicit CLI flags (`--listen`, `--database`, `--state-dir`, `--workers`, `--config`).
2. Environment variables (`PARQONAUT_DATABASE_URL`, `PRQNT_BOOTSTRAP_ADMIN_TOKEN`, `PARQONAUT_ALLOWED_LOCAL_ROOTS`, `PARQONAUT_ALLOWED_S3_BUCKETS`, `PARQONAUT_ALLOWED_S3_PREFIXES`, `PARQONAUT_PLUGIN_*`, `PARQONAUT_MAX_EVENTS_PER_JOB`, `PARQONAUT_EVENT_CHANNEL_CAPACITY`, `PARQONAUT_IDEMPOTENCY_TTL_SECS`).
3. TOML config file (`--config server.toml`).
4. Built-in defaults (`127.0.0.1:8080`, SQLite in `<state-dir>/database/parqonaut.sqlite`, 2 workers, plugins disabled, 500 retained events/job, 24h idempotency TTL).

## 3. Backup and Restoration

### SQLite (WAL mode)

`SqliteScanStore` runs in `WAL` mode with `foreign_keys=ON`. **Do not** copy a live `.sqlite` file with plain `cp` while transactions or WAL checkpoints are active.

Online consistent backup:

```bash
sqlite3 /var/lib/parqonaut/state/database/parqonaut.sqlite "VACUUM INTO '/var/backups/parqonaut-$(date -u +%Y%m%dT%H%M%SZ).sqlite';"
sqlite3 /var/backups/parqonaut-*.sqlite "PRAGMA integrity_check;"
```

Restore to a clean or failed instance (stop `prqnt serve` first, remove `-wal` / `-shm` sidecars, restore the verified backup):

```bash
rm -f /var/lib/parqonaut/state/database/parqonaut.sqlite{,-wal,-shm}
cp /var/backups/parqonaut-backup.sqlite /var/lib/parqonaut/state/database/parqonaut.sqlite
```

### PostgreSQL

Custom-format logical backup (compatible with PostgreSQL 16+):

```bash
pg_dump --format=custom --no-owner --no-acl --file=/var/backups/parqonaut.dump "${DATABASE_URL}"
```

Restore into a clean target database:

```bash
createdb parqonaut_restored
pg_restore --clean --if-exists --no-owner --no-acl --dbname=parqonaut_restored /var/backups/parqonaut.dump
```

Version constraint: use `pg_dump` / `pg_restore` from a PostgreSQL client version `>=` the source server version (tested with PostgreSQL 16).

When running via Docker Compose with PostgreSQL, supply the overlay file (`compose.postgres.yaml`) and an explicit `PARQONAUT_PG_PASSWORD`:

```bash
PARQONAUT_PG_PASSWORD="<strong-password>" \
PRQNT_BOOTSTRAP_ADMIN_TOKEN="$(openssl rand -hex 32)" \
  docker compose -f compose.yaml -f compose.postgres.yaml --profile postgres up -d
```

Set `PARQONAUT_PORT` to override the default host binding (`127.0.0.1:8080`).

## 4. Authentication Bootstrap and Token Rotation

Bootstrap an initial admin token once on an empty store:

```bash
export PRQNT_BOOTSTRAP_ADMIN_TOKEN="$(openssl rand -hex 32)"
prqnt serve --state-dir /var/lib/parqonaut/state
```

Create least-privilege tokens (`reader`, `operator`, `admin`) via `POST /api/v1/admin/tokens` and rotate periodically via `POST /api/v1/admin/tokens/{token_id}/rotate` (which atomically mints the replacement secret, disables the old token, and records `replaced_by_token_id`). Disable compromised tokens immediately with `POST /api/v1/admin/tokens/{token_id}/disable`.

## 5. Database Migrations

`SqliteScanStore::connect` and `PostgresScanStore::connect` execute embedded forward-only `sqlx` migrations at startup. Automatic schema downgrades are **not** supported; restore from your pre-upgrade backup if a rollback is required.

## 6. Server Startup and Graceful Shutdown

- Startup fails fast if database migrations fail, `--config` is invalid, or allowlisted plugin manifests cannot be verified.
- On `SIGINT` or `SIGTERM`, `prqnt serve` stops accepting new HTTP connections, signals background workers to stop claiming new jobs, waits up to 10 seconds for in-flight jobs to reach a safe boundary, and flushes queued `JobEventRecorder` events to storage before exiting.

## 7. TLS and Reverse Proxy Expectations

`prqnt serve` speaks plaintext HTTP and binds `127.0.0.1:8080` by default. For non-loopback deployments, terminate TLS at a reverse proxy (Caddy, Nginx, Envoy, or cloud ALB) and **disable proxy response buffering and compression** on `GET /api/v1/jobs/{job_id}/events/stream` (`Content-Type: text/event-stream`).

## 8. S3 Credential Provisioning

Provide standard AWS SDK environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, optional `AWS_SESSION_TOKEN` / IAM role / `PARQONAUT_S3_ENDPOINT` + `PARQONAUT_S3_PATH_STYLE=true`). Restrict server access via `--config` `[storage]` `allowed_s3_buckets` and `allowed_s3_prefixes`. Child plugin subprocesses run with a scrubbed environment and **never** inherit AWS or database credentials.

## 9. Plugin Runtime Installation

Plugins are disabled on `prqnt serve` by default. To enable opt-in server plugins:

1. Provision a dedicated Python `>=3.12` virtualenv with `python/parqonaut_plugins` (`uv sync --frozen`).
2. Set `PARQONAUT_PLUGIN_PYTHON`, `PARQONAUT_PLUGIN_ROOTS`, and `PARQONAUT_PLUGIN_SDK_PATH` (or `[plugins]` in `--config`), plus an explicit `allowed` plugin list.
3. Digest pinning validates plugin contents at enqueue and again before worker execution.

## 10. Logging Configuration

Structured logs use `tracing-subscriber` controlled via `RUST_LOG` (default `prqnt=info`). Audit events emit stable structured fields (`job_id`, `run_id`, `target_kind`, `token_id`) without raw bearer secrets or credentialed URLs.

## 11. Metrics and Health Checks

- `GET /api/v1/health/live`: process liveness (`200 {"status":"ok"}`).
- `GET /api/v1/health/ready`: executes `store().ping().await` (`200 {"status":"ready"}` when DB is reachable; `500` when unavailable).
- `GET /metrics` (and `/api/v1/metrics`): Prometheus exposition (`parqonaut_jobs_*`, `parqonaut_job_events_coalesced_total`, `parqonaut_http_requests_total`, `parqonaut_store_errors_total`). Protect unauthenticated `/metrics` at the network/reverse-proxy edge.

## 12. Event Retention

- `PARQONAUT_MAX_EVENTS_PER_JOB` (default `500`, min `10`): caps persisted progress rows per job while preserving initial (`job_queued`), recovery, cancellation, and terminal events.
- `PARQONAUT_EVENT_CHANNEL_CAPACITY` (default `128`): bounded in-memory non-critical channel; overflows increment `coalesced_events`.
- `PARQONAUT_IDEMPOTENCY_TTL_SECS` (default `86400`): retention window for `idempotency_keys` (pruned in bounded batches on insert).

## 13. Capacity Planning

- Streaming conversions (`prqnt convert`) and rewrites operate on bounded Arrow `RecordBatch` buffers (under 50 MiB peak RSS on 100k–250k row benchmarks).
- Increase `--workers` modestly (typically `2`–`4`) when running concurrent scan/repair workloads; keep SQLite under single-instance local state or use PostgreSQL for higher concurrent read/write throughput.

## 14. Interrupted Job Recovery

- Running jobs hold a worker lease (`leased_until`) renewed via periodic heartbeats.
- On restart or lease expiry (`recover_stale_scan_jobs`), stale `running` jobs are atomically requeued with a durable `job_recovered` event up to `max_attempts`, or marked `failed` (`job_retries_exhausted`) with no partial run or S3 multipart commit.
- Interrupted batch runs remain resumable via `prqnt batch resume --run-dir <dir>` or `POST /api/v1/batches/{run_dir}/resume`.

## 15. Incident Troubleshooting

- **Corrupted local SQLite state:** Stop `prqnt serve`, remove `-wal`/`-shm`, verify integrity with `sqlite3 ... "PRAGMA integrity_check;"`, and restore from the latest `VACUUM INTO` backup.
- **Failed schema upgrade:** Stop `prqnt serve`, restore the pre-upgrade SQLite or PostgreSQL backup, and verify migration logs before retrying.
- **Stale/stuck batch execution:** Check `GET /api/v1/jobs/{job_id}/batch` or `prqnt batch status --run-dir <dir>`, cancel active job if needed, and resume with `prqnt batch resume`.
