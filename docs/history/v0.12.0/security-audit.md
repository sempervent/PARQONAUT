# Security & Supply-Chain Audit (v0.12.0)

## 1. Application Security Boundaries

| Surface | Mechanism | Verification |
|---|---|---|
| Bearer authentication | SHA-256 hashed opaque tokens (`auth_tokens`); constant-length lookup; `last_used_at` + rotation tracking; plaintext never stored | `auth_tokens.rs`, `admin_tokens.rs` |
| RBAC | `Reader` (GET), `Operator` (mutations + jobs), `Admin` (`/api/v1/admin/tokens*`) enforced by middleware & handler checks | `http_api.rs`, `admin_tokens.rs`, `events_and_idempotency.rs` |
| TUI & browser token lifetime | TUI masks prompt / env / file; browser dashboard stores token strictly in JS memory (`ParqonautApi`), never in `localStorage`/`sessionStorage`/cookies | `scripts/ui-test.mjs`, `ui_dashboard.rs` |
| XSS & CSP | Strict CSP (`default-src 'self'; script-src 'self'; frame-ancestors 'none'`), `no-store`, `nosniff`; zero `innerHTML`/`eval` sinks in `ui/*.js` | `ui_dashboard.rs`, static audit |
| Local & S3 storage policies | `StoragePolicy` enforces `allowed_local_roots`, `allowed_s3_buckets`, and `allowed_s3_prefixes` before scan/repair/plugin execution | `api_integration.rs`, `server_plugin_durable.rs` |
| Error & log sanitization | `sanitize_event_message` & `AppError::into_response` redact `Bearer <secret>`, credentialed URLs (`://user:pass@`), `password=`/`secret=`/`token=`/`api_key=` pairs, and internal `Store`/`Internal` errors | `proptest_contracts.rs`, `server_plugin_durable.rs` |
| Subprocess plugin model | Explicit opt-in (`PARQONAUT_PLUGIN_ROOTS` + allowlist), digest pinning at enqueue and execution, scrubbed child env (blocks `DATABASE_URL`, `AWS_*`, `PRQNT_BOOTSTRAP_ADMIN_TOKEN`, `GITHUB_TOKEN`, `PYTHONSTARTUP`), bounded stdout/stderr/IPC frames | `server_plugin_durable.rs`, `batch_adversarial.rs`, `scan_adversarial.rs` |

*Trust boundary note:* PARQONAUT maintains a **trusted-plugin model**. Subprocess isolation scrubs secrets, enforces timeouts, kills process-group descendants, and validates IPC schemas/bounds, but does **not** constitute an OS/container sandbox against hostile local code.

## 2. Fuzzing & Malformed-Input Findings

- **`V12-09` (Fixed, P1):** Fuzzing `fuzz_arrow_ipc` revealed that `arrow-ipc 54.3.1` `StreamReader::try_new` converts signed `i32` metadata length to `usize` (`vec![0; meta_len as usize]`), panicking with `capacity overflow` on negative lengths (e.g. `0xfffffffe`) and attempting multi-gigabyte allocations on crafted `bodyLength` fields. Fixed via `validate_ipc_stream_framing` + `decode_ipc_stream_bytes` in `crates/parqonaut-plugin-host/src/batch_ipc.rs` before `StreamReader` invocation.
- **`V12-10` (Fixed, P1):** Property testing (`proptest_contracts`) caught that `sanitize_event_message` exceeded `max_bytes` by 3 (`"..."`) and did not redact `Bearer <token>` or `key=secret` forms. Fixed in `crates/parqonaut-types/src/job_events.rs` and `crates/parqonaut-service/src/error.rs`.
- All 9 fuzz targets (`fuzz/fuzz_targets/*.rs`) pass deterministic smoke campaigns (`fuzz_smoke`) with zero panics or uncontrolled allocations.

## 3. Dependency & Supply-Chain Controls

- **Rust Lockfile:** Committed root `Cargo.lock` (`git add -f Cargo.lock`); `--locked` enforced across CI (`ci.yml`, `plugin-integration.yml`) and release builds (`release.yml`).
- **License & Advisory Policy:** Added `deny.toml` permitting OSI-approved licenses (`MIT`, `Apache-2.0`, `BSD-*`, `ISC`, `Zlib`, `Unicode-3.0`, `MPL-2.0`, `CDLA-Permissive-2.0`), disallowing unknown registries/git sources, and configuring RustSec advisory checks.
- **Python Lockfile:** Verified `python/parqonaut_plugins/uv.lock` with `uv lock --check`, `uv sync --frozen`, `ruff check`, `ruff format --check`, and `pytest` (7/7 passing).
- **Update Automation:** Added `.github/dependabot.yml` covering `cargo`, `pip` (`/python/parqonaut_plugins`), `github-actions`, and `docker` weekly updates.
- **CI Permissions & Provenance:** Added explicit least-privilege `permissions: contents: read` on `ci.yml`, `plugin-integration.yml`, `docs.yml`, and `release.yml`, with `release.yml` generating `SHA256SUMS`, `cargo-metadata-sbom.json`, and `actions/attest-build-provenance@v2` gated behind the `release` environment.
- **Local audit tool availability note:** `cargo-audit` and `cargo-deny` binaries are not pre-installed on the local development workstation; `deny.toml` and `Cargo.lock` are committed so CI/security runners with `cargo-deny` / `cargo-audit` enforce them deterministically.
