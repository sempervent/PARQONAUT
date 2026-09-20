# ADR-0002: Temporary coexistence of Arrow/Parquet implementations

## Status

Superseded by [ADR-0015](./ADR-0015-columnar-stack-convergence.md) (v0.9.0 columnar convergence). Historical record retained.

Accepted (2026-09-17)

## Context

Predecessors use incompatible stacks:

| Project | Stack |
|---------|-------|
| Paraclete | `parquet` 53 |
| parqknife | `arrow`/`parquet` 54 |
| maw | `arrow2` 0.18 / `parquet2` 0.17 |

Forcing unification in the consolidation milestone risks regressions without measured benefit.

## Decision

Keep three stacks isolated in separate workspace crates. Do not reimplement working engines solely for dependency aesthetics.

Document the split in `docs/migration-analysis.md`. Measure convergence cost/benefit before any migration project.

## Consequences

- Cross-engine pipelines may require file handoff at Parquet boundaries (acceptable for Phase 1).
- CI must compile all three stacks in one workspace (resolved via separate crates).
