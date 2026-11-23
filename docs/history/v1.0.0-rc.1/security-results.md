# Security & Supply-Chain Qualification Results (`v1.0.0-rc.1`)

## 1. Rust Advisory & License Audits

Executed against committed root `Cargo.lock` and `deny.toml`:

| Check | Command | Outcome | Notes |
|---|---|---|---|
| RustSec vulnerability audit | `cargo audit` | **PASS** (0 vulnerabilities, 0 ignored) | 2 unmaintained warnings (`chrono` `RUSTSEC-2026-0543` transitive via `sqlx-core`/`object_store`, `unsafe-libyaml` `RUSTSEC-2024-0449` via `serde_yaml`) |
| Cargo Deny policy check | `cargo deny check` | **PASS** | `advisories ok, bans ok, licenses ok, sources ok` |

### Remediation of `RUSTSEC-2026-0459` & `RUSTSEC-2026-0592`

- **`RUSTSEC-2026-0459` (`libsqlite3-sys`, CVE-2026-119375):** Previously `Cargo.lock` pinned `libsqlite3-sys 0.30.1` (bundled SQLite `3.49.x`) even though `sqlx-sqlite 0.9.1` already permits `libsqlite3-sys >=0.30.1, <0.41.0`. Upgraded `Cargo.lock` via `cargo update -p libsqlite3-sys` to **`libsqlite3-sys 0.40.0`**, which bundles patched SQLite **`3.54.1`**. Removed the `RUSTSEC-2026-0459` ignore from `deny.toml` and `.github/workflows/ci.yml`.
- **`RUSTSEC-2026-0592` (`url` stack overflow in `Url::origin()`):** Upgraded `Cargo.lock` via `cargo update -p url@2.5.8 --precise 2.5.9` (`url 2.5.9`, `form_urlencoded 1.2.3`).
- **Residual SQLite Hygiene:** PARQONAUT still opens SQLite exclusively for operator-owned local state (`parqonaut.sqlite`) and batch run journals (`journal.sqlite`), never on untrusted user Parquet/CSV/S3 files.

## 2. Python SDK (`python/parqonaut_plugins`) Audit

| Check | Outcome | Details |
|---|---|---|
| `uv lock --check` | **PASS** | Lockfile consistent with `pyproject.toml` (`pyarrow 26.0.1`) |
| `uv sync --frozen` | **PASS** | Deterministic environment resolution |
| `uvx pip-audit` | **PASS** | **0 known vulnerabilities** across resolved Python dependencies |
| `uv run ruff check .` & `ruff format --check .` | **PASS** | Zero lint or formatting violations |
| `uv run python -m pytest` | **PASS** | 7/7 tests passing |

## 3. Application, Container & Workflow Security Controls

- **CI Enforcement (`.github/workflows/ci.yml`):** Added pinned `security-audit` job running `cargo audit` and `cargo deny check` (0 ignored vulnerabilities) on every push/PR with `permissions: contents: read`.
- **Workflow Hygiene:** Verified least-privilege `permissions` across `.github/workflows/ci.yml`, `plugin-integration.yml`, `docs.yml`, `release.yml`, and `fuzz-extended.yml` (all `workflow_dispatch` / tag-gated publish isolation), and pinned `syft` `v1.57.2` installation in `release.yml` via dual SHA-256 verification instead of `curl | sh`.
- **Container Hardening (`Dockerfile`, `compose.yaml`, `compose.postgres.yaml`):**
  - Multi-stage non-root runtime (`uid=10001`, `gid=10001`).
  - Verified with `--read-only` root filesystem (`--tmpfs /tmp`), `--memory 512m --cpus 1.0`.
  - Removed weak default Postgres password fallback from `compose.yaml` (`PARQONAUT_PG_PASSWORD:?` required when Postgres profile is active).
  - Secret-free image layers and runtime logs verified against `PRQNT_BOOTSTRAP_ADMIN_TOKEN` and `PARQONAUT_PG_PASSWORD`.
- **HTTP Error & Event Sanitization:** Internal `StoreError` and `Internal` errors return generic messages (`internal storage error` / `internal server error`) with raw details kept server-side; `AppError` and `sanitize_event_message` redact `Bearer <token>`, credentialed URLs, `password=`/`secret=`/`token=`/`api_key=` pairs, and absolute paths.
- **UI Security:** Strict CSP (`default-src 'self'; script-src 'self'; frame-ancestors 'none'`), in-memory bearer token only, zero `innerHTML`/`eval` sinks.
- **Plugin Trust Boundary:** Opt-in allowlist + SHA-256 digest pinning at enqueue and execution + scrubbed child env (blocks `DATABASE_URL`, `AWS_*`, `PRQNT_BOOTSTRAP_ADMIN_TOKEN`, `GITHUB_TOKEN`, `PYTHONSTARTUP`). Documented trusted-plugin model (not an OS sandbox against hostile local code).
