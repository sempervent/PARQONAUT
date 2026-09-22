# Legacy name audit (v0.10.0)

PARQONAUT v0.10.0 renames active `paraclete-*` workspace crates to `parqonaut-*` and removes predecessor branding from current product surfaces. This document classifies remaining predecessor strings so they are not “fixed” accidentally.

## Active references removed

- Workspace crates: `paraclete-{types,core,report,store,service}` → `parqonaut-{types,core,report,store,service}` (directories, package names, Rust `use` paths).
- Public service type: `ParacleteService` → `ParqonautService`.
- Active docs: `README.md`, `docs/architecture.md`, `docs/job-model.md`, `docs/server.md`, `docs/contributing.md`, `docs/repair-planning.md`, `docs/development/plugin-execution-boundaries.md`, `docs/development/columnar-convergence-plan.md`.
- CI/Justfile/xtask/scripts: package targets and Postgres test env `PARQONAUT_TEST_PG_URL` / `parqonaut_test`.
- Transform CLI scratch prefix `.parqknife.tmp.*` → `.parqonaut.tmp.*`; clap command name `prqnt-transform`.
- OpenAPI golden `fixtures/api/openapi-v1.json` regenerated with `parqonaut_types` doc links and `parqonaut-test` engine revision example.

Enforced by `scripts/check-active-naming.sh` (+ `scripts/check-active-naming-selftest.sh` via `just naming-check`).

## Historical references intentionally retained

| Location | Why |
|----------|-----|
| `docs/provenance.md` | Factual import map from predecessor repos; destination paths updated to `parqonaut-*`. |
| `docs/migration-analysis.md` | Phase-1 consolidation analysis; predecessor names name the source repos. |
| `docs/history/**` | Release notes and acceptance records for shipped versions. |
| `docs/development/*recon*` | Time-stamped recon snapshots (v0.7–v0.10). |
| `docs/development/naming-migration-v0.6.md` | v0.6 naming migration record. |
| `docs/development/application-server-agent-plan-v0.7.md` | v0.7 planning snapshot. |
| `docs/adr/ADR-0001-consolidation-strategy.md`, `ADR-0002-coexisting-arrow-stacks.md`, `ADR-0003-unified-cli-architecture.md` | ADRs describing consolidation decisions at import time. |
| `docs/release-notes-v0.6.0.md` | Historical release note mentioning removed `paraclete-http` binary. |
| `CHANGELOG.md` | Immutable release history. |

## Legal/provenance references intentionally retained

| Location | Why |
|----------|-----|
| `NOTICE` | Copyright and attribution. |
| `THIRD_PARTY_LICENSES.md` | Predecessor project names, upstream SHAs, and licenses; **current paths** point at `crates/parqonaut-*` and `python/parqonaut_plugins`. |

## Persistence compatibility (no cosmetic SQL rename)

No SQL table or index names were renamed solely for branding in v0.10.0. Migration file comments were neutralized where they were non-durable documentation only.

## Rollback reference

Pre-rename runtime green checkpoint: **`PRE_PARQONAUT_RENAME_GREEN_COMMIT=bba9ccc`** (`test(plugin): complete batch runtime acceptance`).
