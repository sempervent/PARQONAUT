# PARQONAUT v1.0.0-rc.1 Release Candidate Acceptance Record

**Target release candidate:** `v1.0.0-rc.1`  
**Qualification branch:** `release/v1.0.0-rc.1-qualification`  
**Baseline commit:** `e6e6940ba03b6d3a155bb442813b6baf6ddfe02d` (`main`)

## Gate Summary (RC-01 through RC-14)

| Gate | Scope | Evidence Artifact | Verdict |
|---|---|---|---|
| **RC-01..RC-05** | Release workflow runners, validation/publish split, pinned Syft SHA-256, strict identity, 12-step smoke, CycloneDX SBOM & build provenance | [release-matrix.md](release-matrix.md), baseline run [`44658428683`](https://github.com/sempervent/PARQONAUT/actions/runs/44658428683), `v1.0.0-rc.1` run [`45103052595`](https://github.com/sempervent/PARQONAUT/actions/runs/45103052595) | **PASS** |
| **RC-06** | `cargo audit` (0 vulns, 0 ignored), `cargo deny check`, Python `uvx pip-audit`, CI security job, container/error hygiene | [security-results.md](security-results.md) | **PASS** |
| **RC-07** | Extended Linux `x86_64` nightly `cargo-fuzz` libFuzzer across all 9 targets (`18,076,045` execs in `1,574 s` + `8,936,647` baseline, 0 crashes) | [fuzz-results.md](fuzz-results.md), `.github/workflows/fuzz-extended.yml` | **PASS** |
| **RC-08..RC-10** | Hardened `Dockerfile`, SQLite Compose, PostgreSQL 16 Compose (`PARQONAUT_PG_*` special-char creds), RustFS S3 & Python plugins, `v0.12.0` migration & 9-dataset fleet E2E | [deployment-validation.md](deployment-validation.md) | **PASS** |
| **RC-11** | Public contract freeze (`prqnt` CLI, `/api/v1` OpenAPI, RBAC, SSE, durable schemas, migrations, publication) | [contract-freeze.md](contract-freeze.md) | **PASS** |
| **RC-12** | Release `perf_baseline`, Criterion `throughput`, `bounded_memory`, SQLite event ingest analysis, operating envelopes | [performance-regression.md](performance-regression.md) | **PASS** |
| **RC-13** | 12-scenario failure & recovery drill matrix across native, Docker, Compose, Postgres, and RustFS | [failure-recovery.md](failure-recovery.md) | **PASS** |
| **RC-14** | Documentation package, stale-version cleanup, `1.0.0-rc.1` version bump, full CI & release verification | [qualification-plan.md](qualification-plan.md), [release-notes.md](release-notes.md) | **PASS** |

## Sign-off Checklist

- **Open P0 defects:** 0
- **Open P1 defects:** 0
- **Public GitHub tag / Release created:** **No** (qualification PR awaiting human review, merge, and release tag decision)
- **Recommendation:** **READY FOR `v1.0.0-rc.1` PR REVIEW**
