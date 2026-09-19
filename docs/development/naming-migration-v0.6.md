# Naming migration map (v0.6.0)

Frozen inventory for the interface-normalization release. Historical phase references remain in `CHANGELOG.md`, `docs/history/**`, and `docs/provenance.md`.

| Old name | New name | Category | Reason | Historical exception? |
|----------|----------|----------|--------|------------------------|
| `parqonaut` (binary) | `prqnt` | Public executable | Single product CLI | Document in CHANGELOG only |
| `paraclete-http` | *(removed)* | Legacy binary | HTTP deferred; library retained | — |
| `generate-phase2-fixtures` | `cargo xtask fixtures repair` | Developer tool | Not a product surface | — |
| `generate-phase3-fixtures` | `cargo xtask fixtures schema` | Developer tool | Not a product surface | — |
| `generate-phase4-fixtures` | `cargo xtask fixtures orchestration` | Developer tool | Not a product surface | — |
| `generate-phase5-fogbank-fixtures` | `cargo xtask fixtures object-storage` | Developer tool | Not a product surface | — |
| `generate-golden-plans` | `cargo xtask golden-plans` | Developer tool | Not a product surface | — |
| `phase1_rules.rs` | `scan_rules.rs` | Source module | Scan quality rules | — |
| `phase2_findings.rs` | `scan_findings.rs` | Source module | Scan-time findings | — |
| `phase3_findings.rs` | `schema_findings.rs` | Source module | Schema/grouping findings | — |
| `evaluate_phase1_rules` | `evaluate_scan_rules` | Symbol | Capability-oriented API | — |
| `fixtures/phase1/` | `fixtures/scan/` | Fixtures | Scan scenarios | — |
| `fixtures/phase2/` | `fixtures/repair/` | Fixtures | Repair scenarios | — |
| `fixtures/phase3/` | `fixtures/schema/` | Fixtures | Schema scenarios (FRANKENLAKE) | — |
| `fixtures/phase4/` | `fixtures/orchestration/` | Fixtures | Batch scenarios (SHIPWRECK) | — |
| `fixtures/phase5/` | `fixtures/object-storage/` | Fixtures | FOGBANK / S3 lab | — |
| `scripts/phase5/` | `scripts/s3-test/` | Scripts | S3 integration harness | — |
| `docker-compose.phase5.yml` | `docker-compose.s3-test.yml` | Infra | S3 test compose | — |
| `phase3-demo` | `schema-demo` | Just target | Capability demo | — |
| `phase4-demo` | `batch-demo` | Just target | Capability demo | — |
| `phase4-resume-demo` | `batch-resume-demo` | Just target | Capability demo | — |
| `phase5-up/down/fixtures/test/demo/...` | `s3-*`, `mixed-storage-demo` | Just target | S3 lab workflows | — |
| `phase5-s3-integration` | `s3-integration` | CI job | Stable capability naming | — |
| `docs/phase-*.md` (active) | capability docs or `docs/history/v0.x.x/` | Docs | History vs architecture | Yes in `docs/history/**` |
