# Public Contract Compatibility Policy & Audit (v0.12.0)

This document inventories PARQONAUT's public contracts as of v0.12.0 and defines the stability policy intended for **v1.0.0**.

## 1. CLI Contract (`prqnt`)

Verified by `crates/parqonaut-cli/tests/cli_contracts.rs` and `docs/reference/cli.md` (`cargo xtask docs cli --check`).

| Subcommand | Promise | JSON Contract (`--json`) | Exit Codes |
|---|---|---|---|
| `scan` | Forensic local/`s3://` inspection (`quick`, `standard`, `deep`, `--plugin`) | `ScanReport` JSON (`metadata`, `summary`, `assets`, `findings`) | `0` ok, `1` error |
| `plugin {list,inspect,validate}` | Discover/inspect/validate plugins without execution | Array / object JSON | `0` ok, `1` error |
| `inspect` | Parquet schema and row-group metadata summary | Text output (human) | `0` ok, `1` error |
| `rewrite`, `partition`, `merge`, `split` | Streaming Parquet manipulation (`merge` accepts files, directories, or globs) | — | `0` ok, `1` error |
| `transform` | Declarative YAML pipeline (`--check`, `--dry-run`) | `TransformReport` JSON | `0` ok, `1` error |
| `convert` | Streaming CSV/Parquet conversion (`--plan`, `--dry-run`, `--resume`, `--json-progress`) | Line-delimited JSON progress when `--json-progress` | `0` ok, `1` error |
| `diagnose`, `plan`, `plan-diff` | Evidence-driven planning (`--canonical` deterministic JSON) | `DiagnosisReport` / `RepairPlan` / `PlanDiff` | `0` ok, `1` error |
| `check` | Non-mutating CI compliance gate | `CheckReport` JSON | `0` clean, `2`–`5` policy thresholds, `1` runtime error |
| `repair`, `verify`, `doctor` | Plan-bound execution & invariant verification | `Doctor` supports `--json` | `0` ok, `1` error |
| `batch {check,plan,repair,status,resume,verify}` | Multi-dataset batch orchestration & journal recovery | `BatchCheckReport` / `BatchPlan` / `BatchStatus` / `BatchVerifyReport` | `0` ok, `1` config/invalid, `2` failed dataset(s) |
| `serve` | HTTP API (`/api/v1`) + `/ui/` (`--print-openapi`) | OpenAPI 3.1 JSON on `--print-openapi` | `0` clean exit, `1` startup failure |
| `tui` | Interactive terminal operator console | — | `0` clean exit, `1` connection/auth error |

Global `--json` error envelope on `stderr` exits `1` with `{"ok":false,"error":"..."}`.

### Changes in v0.12.0

- **Fixed defect (`V12-11`):** `prqnt merge <directory> --output <file>` now walks subdirectories recursively for `.parquet` files as documented in `--help`. No breaking changes.

## 2. HTTP API & OpenAPI Contract (`/api/v1`)

Verified by `crates/parqonaut-service/tests/openapi_contract.rs`, `http_api.rs`, `events_and_idempotency.rs`, `admin_tokens.rs`, and `fixtures/api/openapi-v1.json`.

- **All Axum routes covered in OpenAPI 3.1:** `/metrics`, `/api/v1/metrics`, `/api/v1/health`, `/api/v1/health/live`, `/api/v1/health/ready`, `/api/v1/openapi.json`, `/api/v1/overview`, `/api/v1/whoami`, `/api/v1/plugins`, `/api/v1/plugins/{name}`, `/api/v1/scans`, `/api/v1/scans/sync`, `/api/v1/jobs/scans`, `/api/v1/jobs`, `/api/v1/jobs/{job_id}`, `/api/v1/jobs/{job_id}/events`, `/api/v1/jobs/{job_id}/events/stream`, `/api/v1/jobs/{job_id}/batch`, `/api/v1/jobs/{job_id}/cancel`, `/api/v1/runs`, `/api/v1/runs/{run_id}`, `/api/v1/runs/{run_id}/report`, `/api/v1/runs/{run_id}/assets`, `/api/v1/runs/{run_id}/findings`, `/api/v1/targets/{target_kind}/runs`, `/api/v1/diff`, `/api/v1/admin/tokens*`, `/api/v1/diagnose`, `/api/v1/plans`, `/api/v1/checks`, `/api/v1/repairs`, `/api/v1/verifications`, `/api/v1/batches/*`.
- **RBAC:** `Reader` (GET read-only), `Operator` (mutations + job control), `Admin` (`/api/v1/admin/tokens*`). Unauthenticated requests return `401`, insufficient roles return `403`.
- **Error Envelope:** `{"error":{"code":"...","message":"...","details":{}}}` across `400/401/403/404/409/413/415/422/500`.
- **Idempotency:** `Idempotency-Key` (1..=128 printable ASCII) on scan/repair/batch-repair/batch-resume. Canonical JSON SHA-256 comparison returns existing `202` job or `409 Conflict` on mismatch.
- **SSE Framing:** `id: <seq>`, `event: job_event`, `event: job_status` (after draining all persisted events up to terminal status), `Last-Event-ID` / `?after_seq=` deduplication, uncompressed `text/event-stream`.

### Changes in v0.12.0

- Added `/api/v1/health/live` and `/api/v1/health/ready` to OpenAPI (`fixtures/api/openapi-v1.json`) to match existing Axum routes (`V12-12`).
- Sanitized internal `Store`/`Internal` HTTP error messages to prevent leaking database URLs or local paths (`V12-10`).

## 3. Durable Formats & Versioning

| Format | Version Field & Current Value | Unknown-Version Behavior | Evidence |
|---|---|---|---|
| `ScanReport` | `contract_schema` = `"0.5.0"`, `report_format` = `"0.5.0"` | `validate_report` rejects unsupported versions | `parqonaut-types` |
| `RepairPlan` | `schema_version` = `1` | `RepairPlan::from_json` rejects unsupported versions | `golden_plans.rs` |
| `ExecutionManifest` | `parqonaut_manifest_version` = `1` | `ExecutionManifest::read_json` rejects unsupported versions | `manifest.rs` |
| `BatchConfig` / `BatchPlan` | `schema_version` = `1` | `BatchConfig::from_toml` / `BatchPlan::from_json` reject unsupported versions | `parqonaut-orchestrator` |
| `DurableJobPayload` | `schema_version` = `1` | `decode_durable_payload` rejects unsupported versions | `service.rs` |
| `PluginManifest` (Protocol v1) | `protocol_version` = `1` | `PluginManifest::validate` & host reject unsupported versions | `parqonaut-plugin-protocol` |
| `JobEvent` | `schema_version` = `1` | Typed enum deserialization with forward-safe `#[serde(default)]` metrics | `parqonaut-types` |
| `CheckpointState` (`convert --resume`) | `schema_version` = `1` | Rejects mismatched version and source drift | `parqonaut-stream/src/state.rs` |
| `TransformSpec` | `schema-version` = `1` | `parse_spec` rejects unsupported versions | `parqonaut-transform` |
| SQLite Batch Journal | `parqonaut_journal_meta.schema_version` = `2` | `journal_migrations.rs` verifies v1 -> v2 upgrade | `parqonaut-orchestrator` |

## 4. v1.0.0 Stability Commitment Policy

At **v1.0.0**, PARQONAUT commits to backward compatibility across:
1. All `prqnt` subcommand names, required flags, exit codes (`0/1/2/3/4/5`), and `--json` output shapes.
2. `/api/v1/*` HTTP paths, RBAC roles, error envelope codes, `Idempotency-Key` semantics, and SSE `job_event`/`job_status` framing.
3. Versioned durable JSON/TOML/YAML artifacts (`RepairPlan`, `BatchPlan`, `ExecutionManifest`, `ScanReport`, `PluginManifest` v1, `JobEvent`) with explicit additive-only minor extensions and major version bumps for breaking changes.
4. Forward-only SQLite and PostgreSQL schema migrations without data loss.

Internal Rust library crate APIs (e.g., `parqonaut-service`, `parqonaut-store`, `parqonaut-core`) follow SemVer minor releases until explicitly marked stable, while `prqnt` CLI and `/api/v1` carry the v1.0 stability promise.
