# PARQONAUT v0.8.0 — Transform and Streaming Workflows

## Highlights

- **Transform:** `prqnt partition`, `prqnt merge`, `prqnt split`, and declarative `prqnt transform --spec` multi-step pipelines with typed plan compilation, `--check`, and `--dry-run`.
- **Streaming:** Schema discovery, unification, and batch alignment in `prqnt convert` with strict / widen / stringify policies.
- **Resumability:** Checkpoint schema v1 with input fingerprints, stale detection, staged output, and interruption recovery.
- **Progress:** Shared `ProgressEvent` contract with observer implementations and `--json-progress` on convert.
- **Documentation:** Transform, streaming, and pre-1.0 roadmap pages on GitHub Pages.

## Arrow / Parquet convergence

Stack unification across repair, transform, and stream remains **planned for v0.9.0** (`docs/development/columnar-convergence-plan.md`).

## Upgrade from v0.7.x

- Workspace version is **0.8.0**; repair plan goldens include updated `parqonaut_version`.
- No breaking changes to repair plan or batch journal contracts.
