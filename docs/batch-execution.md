# Batch execution

## Configuration

Batch jobs are declared in TOML (`schema_version = 1`):

```toml
[batch]
name = "nightly"
max_concurrency = 4
output_root = "repaired"

[[datasets]]
id = "telemetry"
path = "data/telemetry"
policy = "policies/telemetry.toml"
```

## Planning

`prqnt batch plan` scans each dataset, builds embedded `RepairPlan` objects, assigns deterministic output paths under `output_root`, and emits a durable `BatchPlan` JSON artifact.

Plan identity includes:

- `batch_plan_id` (derived from config + dataset plan IDs + fingerprints)
- `config_fingerprint`
- `output_root`
- per-dataset output paths and repair plan IDs

## Execution layout

```text
{output_root}/
  {dataset-id}/           ← repaired dataset output
  .parqonaut/runs/{run-id}/
    batch-plan.json       ← plan snapshot
    journal.sqlite        ← durable state
    report.json           ← aggregate report
```

## Concurrency

- CLI `--jobs N` overrides `batch.max_concurrency`
- `--jobs 0` is rejected
- Peak concurrent datasets is recorded in the execution outcome and `report.json`
- Dry-run performs no filesystem mutations (no journal, locks, or outputs)

## Exit codes (batch repair / resume)

| Code | Meaning |
|------|---------|
| 0 | Success (all datasets terminal without error class) |
| 1 | Operational/validation failure |
| 2 | Partial batch failure (some datasets failed/blocked/stale) |
| 130 | Interrupted (SIGINT); journal persisted, resumable |

## Failure isolation

One dataset failure does not stop unrelated datasets. Terminal error classes (`StaleSource`, `Blocked`, corrupt input, lock held) are recorded per dataset in the journal.
