# Phase 5 acceptance matrix (v0.5.0)

| Item | Status | Evidence |
|------|--------|----------|
| Bounded footer parsing (50 GiB declared, small footer) | PASS | `footer_inspection_bounded_for_huge_declared_object_size` |
| Production S3 runtime (`BatchStorageRuntime::new` → S3) | PASS | `production_runtime_selects_s3_backend_for_remote_locations` |
| RustFS integration CI (`phase5-s3-integration`) | PASS | `.github/workflows/ci.yml`, pinned `ghcr.io/rustfs/rustfs:1.0.0@sha256:ba0a1…` |
| S3 smoke (PUT/HEAD/GET/LIST/DELETE) | PASS | `scripts/phase5/s3-smoke.sh` in CI |
| FOGBANK + chaos integration (no skip when `PARQONAUT_S3_INTEGRATION=1`) | PASS | `crates/parqonaut-storage/tests/` |
| Remote batch verification | PASS | `crates/parqonaut-orchestrator/src/verify.rs` + `remote_output_committed` |
| Workspace Clippy (`-D warnings`, all features) | PASS | CI `rust` job |
| Resume demo | PASS | `just phase5-resume-demo` |
| Mixed fleet batch demo (`jobs=3`) | PASS | `just phase5-batch-demo` |
| Credential redaction in S3 config debug | PASS | `s3_config_has_no_secret_fields` |
| Full workspace tests | PASS | CI `rust` job |

CI S3-compatible backend: **RustFS 1.0.0** (not application-coupled; `S3StorageBackend` remains the production abstraction).
