# PARQONAUT server (`prqnt serve`)

v0.7.0 exposes the supported HTTP API only through:

```bash
prqnt serve
```

## Defaults

- **Bind:** `127.0.0.1:8080` (loopback). Use `--listen 0.0.0.0:8080` only with a reverse proxy/TLS at the edge.
- **State:** `PRQNT_STATE_DIR` or platform data dir (`…/prqnt`) with SQLite under `database/`.
- **Workers:** `--workers N` in-process job executors (default `2`).

## Configuration precedence

1. CLI flags (`--listen`, `--database`, `--state-dir`, `--workers`)
2. Environment (`PRQNT_STATE_DIR`, `PRQNT_BOOTSTRAP_ADMIN_TOKEN`, legacy `PARACLETE_BOOTSTRAP_TOKEN`)
3. Optional `--config` TOML (`[server]` / `[storage]` policy sections)

## Bootstrap auth

Set `PRQNT_BOOTSTRAP_ADMIN_TOKEN` once when starting an empty database; the token is stored hashed. Do not pass secrets on the command line.

## OpenAPI

- Live: `GET /api/v1/openapi.json`
- Offline: `prqnt serve --print-openapi`

See [api.md](api.md) for endpoint groups and [server-storage-policy.md](server-storage-policy.md) for location rules.
