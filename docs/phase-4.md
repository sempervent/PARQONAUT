# Phase 4 — batch orchestration

Phase 4 adds **multi-dataset batch orchestration** on top of the Phase 3 single-dataset repair engine.

## Goals

- Bounded parallel dataset repair (`--jobs N`)
- Durable SQLite run journal with resume
- Failure isolation per dataset
- Plan-bound authorization and output path safety
- Cooperative cancellation (SIGINT)

## Crate boundary

```text
parqonaut-repair       → one dataset
parqonaut-orchestrator → many datasets, journal, scheduler
parqonaut-cli          → `parqonaut batch …` commands
```

See ADR-0011 through ADR-0014.

## Commands

```bash
parqonaut batch check   --config batch.toml
parqonaut batch plan    --config batch.toml --output plan.json
parqonaut batch repair  --plan plan.json [--jobs N] [--dry-run]
parqonaut batch status  --run-dir .parqonaut/runs/<run-id>
parqonaut batch resume  --run-dir .parqonaut/runs/<run-id> [--jobs N]
parqonaut batch verify  --run-dir .parqonaut/runs/<run-id>
```

## SHIPWRECK fixture fleet

`fixtures/phase4/shipwreck/` — reproducible torture lab (`just phase4-fixtures`).

Demos: `just phase4-demo`, `just phase4-resume-demo`.

## Related docs

- [Batch execution](batch-execution.md)
- [Resume and recovery](resume-recovery.md)
- [Concurrency audit](phase-4-concurrency-audit.md)
- [Acceptance matrix](phase-4-acceptance.md)
