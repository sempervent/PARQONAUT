# ADR-0012: Journaling and resume semantics

## Status

Accepted

## Context

Batch runs must survive process crash, SIGINT, and operator retry without duplicating committed repairs or silently changing output destinations.

## Decision

Each run persists:

- `{output_root}/.parqonaut/runs/{run_id}/journal.sqlite`
- `batch-plan.json` (authorized plan snapshot)
- `report.json` (aggregate terminal report)

Dataset states use an explicit transition graph. Datasets left `running` after crash are recovered to `failed_recoverable`. Resume validates plan identity (plan id, config/plan digests, output paths, repair plan ids, policy fingerprints) and skips `succeeded` datasets. A second resume after full completion is an idempotent no-op.

Dry-run does **not** create a journal.

## Alternatives considered

- Ephemeral in-memory state only: rejected.
- Re-planning on resume without fingerprint checks: rejected (silent policy drift).

## Consequences

- Operators must preserve the run directory to resume.
- Configuration changes after planning require a new plan and run.
