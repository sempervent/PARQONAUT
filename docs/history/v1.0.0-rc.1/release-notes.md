# PARQONAUT v1.0.0-rc.1 Release Notes

**Release candidate:** `v1.0.0-rc.1`  
**Milestone:** First Release Candidate for PARQONAUT v1.0.0  
**Prior merged milestones on `main`:** `v0.10.0`, `v0.11.0`, `v0.12.0` (latest prior published GitHub Release tag: [`v0.9.2`](https://github.com/sempervent/PARQONAUT/releases/tag/v0.9.2))

PARQONAUT `v1.0.0-rc.1` completes end-to-end release qualification across distribution, security audits, Linux `x86_64` libFuzzer campaigns, container/PostgreSQL/S3 deployments, public contract freeze, performance regression envelopes, and 12-scenario failure/recovery drills.

## Highlights & Qualification Fixes

- **4-Platform Release Pipeline & Provenance (`.github/workflows/release.yml`):**
  - Updated macOS matrix runners to supported `macos-15` (ARM64) and `macos-15-intel` (`x86_64`) alongside `ubuntu-24.04` and `ubuntu-24.04-arm`.
  - Enforced strict separation between `workflow_dispatch` validation runs and tag-push draft release publication (`refs/tags/v*`).
  - Enforced strict artifact identity validation in `.github/workflows/release.yml` (`Verify Release Identity` job), 12-step per-platform archive smoke verification (`scripts/release/verify-artifact.sh`, including live `prqnt serve` + `/ui/`), real CycloneDX 1.6 SBOM generation (`scripts/release/build-and-validate-sbom.py`, `parqonaut-v<version>.cdx.json`), `SHA256SUMS`, and `actions/attest-build-provenance@v2` verification.
- **Security & Supply-Chain Enforcement:**
  - Verified `cargo audit` (0 vulnerabilities, 0 ignored — `RUSTSEC-2026-0459` remediated via `libsqlite3-sys 0.40.0` / SQLite `3.54.1`; `RUSTSEC-2026-0592` remediated via `url 2.5.9`), `cargo deny check`, and Python SDK `uvx pip-audit` (0 vulnerabilities).
  - Hardened `.github/workflows/release.yml` Syft installation with dual SHA-256 verification instead of `curl | sh`, and enforced `security-audit` in `.github/workflows/ci.yml`.
- **Sustained Linux `x86_64` Fuzzing (`.github/workflows/fuzz-extended.yml`):**
  - Executed extended `scripts/fuzz/run-extended.sh` campaigns across all 9 libFuzzer targets (`docs/history/v1.0.0-rc.1/fuzz-results.md`).
- **SQLite Concurrency, Docker, PostgreSQL & URI Remediation:**
  - Switched `SqliteScanStore` write transactions (`sqlite_store.rs`, `job_events_store.rs`, `auth_store.rs`) to `BEGIN IMMEDIATE` to eliminate snapshot-upgrade `SQLITE_BUSY` errors under concurrent progress writes and job cancellation (`650bae6`).
  - Updated `Dockerfile` builder to `rust:1.99-slim-bookworm`, excluded build/fuzz artifacts in `.dockerignore`, wired discrete `PARQONAUT_PG_*` options (`PostgresConnectConfig` / `PgConnectOptions`, special-character `@ : / # ? %` safe, no SQLite fallback), returned `HTTP 503 service_unavailable` on `/api/v1/health/ready` outage, fixed `batch verify` `file://` URI handling (`parqonaut-orchestrator/src/verify.rs`), added `s3://` + `StoragePolicy` validation to `prqnt verify`, and fixed `StoragePolicy::validate_local_path` for non-existent repair output targets.
- **Public Contract Freeze & Performance Envelopes:**
  - Froze CLI, `/api/v1` OpenAPI, RBAC, SSE, `Idempotency-Key`, durable artifact versions, and forward-only SQLite/Postgres migrations (`docs/history/v1.0.0-rc.1/contract-freeze.md`).
  - Fixed Criterion `repair_and_store` output directory bug (`benches/stream-throughput/throughput.rs`) and verified bounded memory (`< 256 MiB` delta) and release throughput (`docs/history/v1.0.0-rc.1/performance-regression.md`).

## Verification Summary

- `cargo fmt --all -- --check`, `just lint`, `cargo test --workspace --locked`, `just naming-check`, `just columnar-check`, `just docs-check`
- `cargo audit`, `cargo deny check`, `just plugin-test` (including `uvx pip-audit`, `ruff`, `pytest`, RustFS S3 matrix)
- `just console-demo`, `just tui-demo`, `just ui-test`
- Standalone Docker, SQLite Compose, PostgreSQL 16 Compose, RustFS S3 & Python plugin smoke, 12/12 failure-recovery drills, and 4-platform GitHub Actions release validation
