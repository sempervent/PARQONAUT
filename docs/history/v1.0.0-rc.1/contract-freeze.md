# Public Contract Freeze (`v1.0.0-rc.1`)

This document freezes the public and durable contract surface for PARQONAUT `v1.0.0-rc.1` toward `v1.0.0`, classifying each interface as **Stable**, **Versioned**, **Experimental**, or **Internal**.

## 1. Stability Tiers

- **Stable:** Backward-compatible across `v1.x`. Breaking removals or incompatible shape changes require `v2.0`. Additive optional fields are allowed.
- **Versioned:** Protected by an explicit `schema_version` / `protocol_version` / `parqonaut_manifest_version` field. Unsupported versions fail closed; minor additive extensions remain backward-compatible within the major contract version.
- **Experimental / Opt-in:** Functional and tested, but schema extensions or ergonomic adjustments remain possible before general commitment.
- **Internal:** Implementation detail of workspace library crates (`parqonaut-core`, `parqonaut-store`, `parqonaut-service`, etc.); follows crate SemVer rather than public API stability.

## 2. CLI Surface (`prqnt`) — **Stable**

Verified by `crates/parqonaut-cli/tests/cli_contracts.rs` and `cargo xtask docs cli --check`:

| Surface | Stability | Contract & Exit Codes |
|---|---|---|
| Subcommands (`scan`, `plugin`, `inspect`, `partition`, `merge`, `split`, `transform`, `rewrite`, `diagnose`, `plan`, `plan-diff`, `check`, `repair`, `verify`, `doctor`, `batch`, `serve`, `tui`, `convert`) | **Stable** | Required flags, `--json` output envelopes, global JSON error `{"ok":false,"error":"..."}` on `stderr` |
| Exit codes (`check`) | **Stable** | `0` clean, `2`–`5` policy thresholds, `1` runtime error |
| Exit codes (`batch repair` / `batch resume` / `batch check`) | **Stable** | `0` ok, `1` invalid config/input, `2` dataset failure(s), `130` interrupted |
| Exit codes (`serve`, `tui`, general) | **Stable** | `0` clean exit, `1` error |
| Storage URI scope | **Stable** | `scan`, `diagnose`, `plan`, `check`, `repair`, `verify`, `transform --spec`, `convert` support `s3://` URIs (`scan` enumerates prefixes); `inspect`, `rewrite`, `merge`, `split`, `partition` operate on local paths (use `transform --spec` for S3) |

## 3. HTTP API (`/api/v1`) — **Stable**

Verified by `crates/parqonaut-service/tests/openapi_contract.rs`, `fixtures/api/openapi-v1.json`, and `cargo xtask docs openapi --check`:

| Contract Area | Stability | Specification |
|---|---|---|
| Routes & OpenAPI 3.1 (`fixtures/api/openapi-v1.json`) | **Stable** | Health (`/api/v1/health`, `/health/live`, `/health/ready` returning `200` when DB reachable and `503 service_unavailable` on store outage), `/metrics`, `/api/v1/metrics`, `/api/v1/overview`, `/api/v1/whoami`, `/api/v1/plugins*`, `/api/v1/scans*`, `/api/v1/jobs*`, `/api/v1/runs*`, `/api/v1/targets/{kind}/runs`, `/api/v1/diff`, `/api/v1/admin/tokens*`, `/api/v1/diagnose`, `/api/v1/plans`, `/api/v1/checks`, `/api/v1/repairs`, `/api/v1/verifications`, `/api/v1/batches/*` |
| Error Envelope (`ErrorBody`) | **Stable** | `{"error":{"code":"<snake_case>","message":"...","details":{}}}` across `ErrorCode` variants (`invalid_request`, `invalid_json_request`, `run_not_found`, `job_not_found`, `token_not_found`, `target_not_found`, `scan_failed`, `store_error`, `service_unavailable`, `internal_error`, `conflict`, `unauthorized`, `forbidden`, `location_not_allowed`, `plugin_*`) |
| Auth & RBAC (`http/auth.rs`) | **Stable** | Bearer token (`Authorization: Bearer <token>`, `WWW-Authenticate: Bearer realm="parqonaut"` on `401`); `Reader` (GET), `Operator` (`POST` mutations excluding admin), `Admin` (`/api/v1/admin/tokens*`) |
| `Idempotency-Key` | **Stable** | 1..=128 printable ASCII; canonical JSON payload hash; `202` replay of existing `job_id` on match, `409 conflict` on mismatch; TTL `PARQONAUT_IDEMPOTENCY_TTL_SECS` (default 86,400s) |
| SSE Stream (`/api/v1/jobs/{job_id}/events/stream`) | **Stable** | `text/event-stream` (excluded from `CompressionLayer`), frames `id: <seq>`, `event: job_event` (`JobEvent`), `event: job_status` (`{"job_id","status","last_seq"}`), cursor resume via `Last-Event-ID` or `?after_seq=`, terminal catch-up before close |

## 4. Durable Artifacts & Wire Protocols — **Versioned**

| Artifact / Protocol | Version Field & Value | Fail-Closed Check |
|---|---|---|
| `ScanReport` | `contract_schema="0.5.0"`, `report_format="0.5.0"` | `validate_report` |
| `RepairPlan` | `schema_version = 1` (`PLAN_SCHEMA_VERSION`) | `RepairPlan::validate_version` / `canonical_plan_json` |
| `ExecutionManifest` | `parqonaut_manifest_version = 1` (`MANIFEST_VERSION`) | `ExecutionManifest::read_json` |
| `BatchConfig` / `BatchPlan` / `BatchReport` | `schema_version = 1` | `BatchConfig::from_toml` / `BatchPlan::from_json` |
| `BatchRunJournal` (SQLite) | `parqonaut_journal_meta.schema_version = 1` (plus forward DDL) | `SqliteRunJournal` |
| `DurableJobPayload` | `schema_version = 1` | `decode_durable_payload` |
| `JobEvent` | `schema_version = 1` (`JOB_EVENT_SCHEMA_VERSION`), `seq` 1-based monotonic per `job_id` | Typed `ProgressEventKind` + `JobStage` |
| `TransformSpec` | `schema-version: 1` (`TRANSFORM_SPEC_SCHEMA_VERSION`) | `parse_spec` |
| `StreamCheckpoint` (`convert --resume`) | `schema_version = 1` | Source & execution identity verification |
| `PluginManifest` & Protocol v1 | `protocol_version = 1` (`PLUGIN_PROTOCOL_VERSION`) | `PluginProtocolVersion::validate`, `PluginManifest::validate`, host IPC length checks |
| `DatasetFingerprint` (local) / `RemoteFingerprint` (S3) | `version = 1` (`FINGERPRINT_VERSION` / `REMOTE_FINGERPRINT_VERSION`) | SHA-256 over canonical metadata entries (`parqonaut-dataset-fp-v1\0` / `parqonaut-remote-fp-v1\0`); stale drift blocks repair |
| S3 Publication Layout | `parqonaut_publication_version = 1` (`PUBLICATION_CONTRACT_VERSION`) | `.parqonaut/writer.lock`, `.parqonaut/versions/{id}/{state.json,manifest.json,data/*,COMMITTED}`, `.parqonaut/CURRENT`; committed iff `COMMITTED` exists |

## 5. Database Migrations — **Versioned (Forward-Only)**

- **SQLite (`crates/parqonaut-store/migrations/`):** 9 forward-only migrations (`20250415120000` through `20261117120000`); `WAL` mode, `foreign_keys=ON`, `synchronous=NORMAL`, `busy_timeout=5s`, `BEGIN IMMEDIATE` write transactions (`650bae6`).
- **PostgreSQL (`crates/parqonaut-store/migrations/postgres/`):** 3 consolidated forward-only migrations (`20250415120000`, `20250919140000`, `20261117120000`).
- **Policy:** Applied migrations are immutable; downgrades are unsupported (restore pre-upgrade backup).

## 6. Deferred / Experimental Boundaries

- **Web Dashboard (`/ui/`) HTML/CSS/JS DOM structure:** Internal operator UI (`Experimental` DOM layout, backed by **Stable** `/api/v1` endpoints).
- **Generic HTTP server transform jobs & distributed multi-node worker queue:** Explicitly deferred post-`v1.0.0`.
- **Internal Rust crate APIs:** **Internal** (SemVer minor increments may refine Rust types without breaking `prqnt` or `/api/v1`).
