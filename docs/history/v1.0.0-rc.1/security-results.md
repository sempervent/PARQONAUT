# Security & Supply-Chain Qualification Results (`v1.0.0-rc.1`)

## 1. Rust Advisory & License Audits

Executed against committed root `Cargo.lock` and `deny.toml`:

| Check | Command | Outcome | Notes |
|---|---|---|---|
| RustSec vulnerability audit | `cargo audit --ignore RUSTSEC-2026-0459` | **PASS** (0 vulnerabilities) | 2 unmaintained warnings (`fxhash`, `paste` via transitive crates); `RUSTSEC-2026-0459` documented narrow exception below |
| Cargo Deny policy check | `cargo deny check` | **PASS** | `advisories ok, bans ok, licenses ok, sources ok` |

### Documented `RUSTSEC-2026-0459` Exception (`libsqlite3-sys`)

- **Advisory:** `RUSTSEC-2026-0459` (bundled SQLite `3.49.x` via `libsqlite3-sys 0.30.1` pinned by `sqlx-sqlite 0.9.1`).
- **Scope & Exposure Assessment:** PARQONAUT opens SQLite **exclusively** for its own trusted local state database (`parqonaut.sqlite`) and batch run journals (`journal.sqlite`). Untrusted user files (CSV, Parquet, JSON, Arrow IPC, S3 objects) are **never** parsed or opened by SQLite, and arbitrary user-supplied SQL is never executed.
- **Disposition:** Narrowly ignored in `deny.toml` (`advisories.ignore`) and CI security job with explicit comment tracking upgrade once `sqlx` adopts a newer `libsqlite3-sys` release.

## 2. Python SDK (`python/parqonaut_plugins`) Audit

| Check | Outcome | Details |
|---|---|---|
| `uv lock --check` | **PASS** | Lockfile consistent with `pyproject.toml` (`pyarrow 26.0.1`) |
| `uv sync --frozen` | **PASS** | Deterministic environment resolution |
| `uvx pip-audit` | **PASS** | **0 known vulnerabilities** across resolved Python dependencies |
| `uv run ruff check .` & `ruff format --check .` | **PASS** | Zero lint or formatting violations |
| `uv run python -m pytest` | **PASS** | 7/7 tests passing |

## 3. Application, Container & Workflow Security Controls

- **CI Enforcement (`.github/workflows/ci.yml`):** Added pinned `security-audit` job running `cargo audit --ignore RUSTSEC-2026-0459` and `cargo deny check` on every push/PR with `permissions: contents: read`.
- **Workflow Hygiene:** Verified least-privilege `permissions` across `.github/workflows/ci.yml`, `plugin-integration.yml`, `docs.yml`, `release.yml`, and `fuzz-extended.yml` (all `workflow_dispatch` / tag-gated publish isolation).
- **Container Hardening (`Dockerfile`, `compose.yaml`, `compose.postgres.yaml`):**
  - Multi-stage non-root runtime (`uid=10001`, `gid=10001`).
  - Verified with `--read-only` root filesystem (`--tmpfs /tmp`), `--memory 512m --cpus 1.0`.
  - Removed weak default Postgres password fallback from `compose.yaml` (`PARQONAUT_PG_PASSWORD:?` required when Postgres profile is active).
  - Secret-free image layers and runtime logs verified against `PRQNT_BOOTSTRAP_ADMIN_TOKEN` and `PARQONAUT_PG_PASSWORD`.
- **HTTP Error & Event Sanitization:** Internal `StoreError` and `Internal` errors return generic messages (`internal storage error` / `internal server error`) with raw details kept server-side; `AppError` and `sanitize_event_message` redact `Bearer <token>`, credentialed URLs, `password=`/`secret=`/`token=`/`api_key=` pairs, and absolute paths.
- **UI Security:** Strict CSP (`default-src 'self'; script-src 'self'; frame-ancestors 'none'`), in-memory bearer token only, zero `innerHTML`/`eval` sinks.
- **Plugin Trust Boundary:** Opt-in allowlist + SHA-256 digest pinning at enqueue and execution + scrubbed child env (blocks `DATABASE_URL`, `AWS_*`, `PRQNT_BOOTSTRAP_ADMIN_TOKEN`, `GITHUB_TOKEN`, `PYTHONSTARTUP`). Documented trusted-plugin model (not an OS sandbox against hostile local code).
