# Batch batch orchestration

Batch orchestration adds **multi-dataset batch orchestration** on top of the single-dataset repair engine.

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
parqonaut-cli          → `prqnt batch …` commands
```

See ADR-0011 through ADR-0014.

## Commands

```bash
prqnt batch check   --config batch.toml
prqnt batch plan    --config batch.toml --output plan.json
prqnt batch repair  --plan plan.json [--jobs N] [--dry-run]
prqnt batch status  --run-dir .parqonaut/runs/<run-id>
prqnt batch resume  --run-dir .parqonaut/runs/<run-id> [--jobs N]
prqnt batch verify  --run-dir .parqonaut/runs/<run-id>
```

## SHIPWRECK fixture fleet

`fixtures/orchestration/shipwreck/` — reproducible torture lab (`just orchestration-fixtures`).

Demos: `just batch-demo`, `just batch-resume-demo`.

## Related docs

- [Batch execution](batch-execution.md)
- [Resume and recovery](resume-recovery.md)
- [Concurrency audit](phase-4-concurrency-audit.md)
- [Acceptance matrix](phase-4-acceptance.md)
