# Resume and recovery

## When to resume

After interruption (SIGINT, crash) or partial failure, use:

```bash
parqonaut batch resume --run-dir {output_root}/.parqonaut/runs/{run-id}
```

Status from a **new process** reads the same journal:

```bash
parqonaut batch status --run-dir …
```

## Resume steps

1. Load plan snapshot and journal from run directory
2. Validate plan matches persisted run identity (`plan_digest`, fingerprints, output root)
3. Validate journal dataset rows match plan (output paths, repair plan IDs)
4. Recover in-flight `Running` rows → `FailedRecoverable` with recovery metadata
5. Skip `Succeeded` datasets (no repair re-execution)
6. Continue pending / recoverable datasets under the jobs bound

## Idempotency

When all plan datasets have terminal journal states, resume is a **no-op** (exit 0, no repair work). A second resume after full completion does not mutate outputs.

## Source staleness

Before each dataset executes, the orchestrator recomputes the source fingerprint. If it diverges from the plan-time fingerprint:

- State → `StaleSource`
- No repair runs for that dataset
- Other datasets continue

Resume/verify also reject plan snapshots that no longer match run identity.

## Locks

Each output directory uses `{output}.parqonaut.lock` recording the owning `run_id`.

- Different run → `LockHeld` error
- Same run after crash → stale lock may be reacquired for resume

## Cancellation

First SIGINT sets a cooperative cancel flag:

- No new datasets scheduled (re-checked inside scheduler spawn loop)
- In-flight repairs finish at the repair executor boundary
- Journal records `cancelled_at`
- Exit code 130

Incomplete datasets remain resumable; successful outputs are not rolled back.
