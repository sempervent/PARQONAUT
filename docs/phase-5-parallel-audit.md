# Phase 5 parallel read-only audit (feat/object-storage)

Six concurrent scopes (A–F). P0 items were fixed or absent; P1/P2 recorded for follow-up.

| Scope | P0 | P1 highlights | Disposition |
|-------|-----|---------------|-------------|
| A Storage / credentials | 0 | Repair `S3Config::from_env()` drift | **Fixed** (`storage_scan.rs`) |
| B Footer / bounded memory | 0 | Metadata cap, LIST vs HEAD size | Documented; P0 50 GiB regression test retained |
| C S3 / RustFS CI | 0 | Multipart retry class via `io::Error` | Accepted for v0.5.0; chaos + contract tests |
| D Fingerprints / stale | 0 | TOCTOU list-after-scan; S3 version_id on replace | Parquet fingerprint path enforced at execute |
| E Publication / locks | 0 | Lock release on partial publish | Known; resume demo exercises happy path |
| F Orchestrator / verify | 0 | Post-repair verify wrong run_id | **Fixed** (`run.rs`) |

RustFS CI: pinned `ghcr.io/rustfs/rustfs:1.0.0@sha256:ba0a1b53e36f321c0d46f3867104abef169f7bc59c467c664ddac87e7ddc9a8b`, job `phase5-s3-integration`.
