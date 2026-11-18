# PARQONAUT server (`prqnt serve`) & Operator Console

PARQONAUT exposes the supported HTTP API and bundled web dashboard through:

```bash
prqnt serve
```

## Defaults

- **Bind:** `127.0.0.1:8080` (loopback). Use `--listen 0.0.0.0:8080` only behind TLS termination on trusted networks.
- **Web dashboard:** `http://127.0.0.1:8080/ui/` (bundled static assets with strict CSP; all operational `/api/v1/*` data requires bearer auth).
- **Terminal console:** `prqnt tui --url http://127.0.0.1:8080` (connects via `PARQONAUT_API_TOKEN`, `--token-env`, `--token-file`, or an interactive masked prompt).
- **State:** `PRQNT_STATE_DIR` or platform data dir (`…/prqnt`) with SQLite under `database/`.
- **Workers:** `--workers N` in-process job executors (default `2`).

## Configuration precedence

1. CLI flags (`--listen`, `--database`, `--state-dir`, `--workers`)
2. Environment (`PRQNT_STATE_DIR`, `PRQNT_BOOTSTRAP_ADMIN_TOKEN`, legacy `PARQONAUT_BOOTSTRAP_TOKEN`)
3. Optional `--config` TOML (`[server]` / `[storage]` / `[plugins]` sections)

## Event retention & backpressure tuning

| Variable | Default | Purpose |
|----------|---------|---------|
| `PARQONAUT_MAX_EVENTS_PER_JOB` | `500` | Maximum non-terminal progress events retained per job (terminal/lifecycle events are preserved) |
| `PARQONAUT_EVENT_CHANNEL_CAPACITY` | `64` | Bounded async recorder buffer; excess non-critical updates coalesce instead of blocking workers |
| `PARQONAUT_IDEMPOTENCY_TTL_SECS` | `86400` | Lifetime of `Idempotency-Key` records in SQLite/Postgres |

## Bootstrap auth

Set `PRQNT_BOOTSTRAP_ADMIN_TOKEN` once when starting an empty database; the token is stored hashed. Do not pass secrets as CLI flags or URL query parameters.

## OpenAPI

- Live: `GET /api/v1/openapi.json`
- Offline: `prqnt serve --print-openapi`

See [api.md](api.md) for endpoint groups, [job-model.md](job-model.md) for events/recovery, and [server-storage-policy.md](server-storage-policy.md) for location rules.
