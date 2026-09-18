# Phase 3 closeout audit

Audit date: 2026-09-18. Each row maps a Phase 3 acceptance requirement to repository evidence.

| Requirement | Status | Evidence | Fix |
|-------------|--------|----------|-----|
| Structured schema diff | **PASS** | `crates/parqonaut-repair/src/schema/diff.rs` | — |
| Compatibility lattice | **PASS** | `schema/compatibility.rs`, `tests/schema_compat.rs` | — |
| Schema policy TOML | **PASS** | `schema_policy.rs`, `fixtures/phase3/policy.toml` | — |
| Explicit authorization | **PASS** | `authorization.rs`, `--authorize` CLI | — |
| Operation audit metadata | **PASS** | `OperationAuditRecord` includes `plan_id`, `dataset_fingerprint`, `policy_fingerprint`, `executor_version` | Added in closeout |
| Golden plan fixtures (6 cases) | **PASS** | `fixtures/plans/*.canonical.json`, `generate-golden-plans` bin | Regenerated in closeout |
| Rename map execution | **PASS** | `apply_rename_rules`, `rewrite_parquet_with_rename`, `fixtures/phase3/rename-map` | Wired in closeout |
| `doctor --policy` | **PASS** | CLI `Doctor { policy }`, `run_doctor` uses `EffectivePolicy::from_toml` | Added in closeout |
| Stale fingerprint rejection | **PASS** | `golden_plans::stale_fingerprint_rejected`, `stale-fingerprint.case.json` | Test added in closeout |
| Plan contract v1 | **PASS** | `PLAN_SCHEMA_VERSION = 1`, `golden_plans.rs` | — |
| Execution manifest | **PASS** | `manifest.rs`, `.parqonaut-manifest.json` | — |
| FRANKENLAKE v2 | **PASS** | `fixtures/phase3/frankenlake-v2`, `just phase3-demo` | — |
| No LLM/AI | **PASS** | No AI dependencies in repair/diagnose paths | — |
| Cargo `jobs=0` quirk | **DOCUMENTED** | `docs/development-environment.md` | User `~/.cargo/config.toml` had invalid `jobs=0` for Cargo 1.98+; Justfile uses `CARGO_BUILD_JOBS` fallback |

## Regenerate goldens

```bash
just phase3-fixtures
cargo run -p parqonaut-repair --bin generate-golden-plans
```

## Verification commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
just ci
just phase3-demo
```
