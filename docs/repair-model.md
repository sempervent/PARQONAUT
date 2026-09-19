# PARQONAUT repair model 

Repair adds an evidence-driven **diagnose → plan → repair → verify** pipeline on top of the
scan and transform engines.

## Flow

```text
dataset → scan → diagnose → repair rules → plan → validate fingerprint → execute (Safe only) → rescan → verify
```

Every repair operation in a plan references:

- one or more **finding fingerprints** (from scan + diagnosis),
- **evidence** payloads copied from those findings,
- **preconditions** checked before execution,
- **expected outcomes** recorded for verification.

No operation is emitted from policy alone without a matching diagnosed condition.

## Commands

| Command | Role |
|---------|------|
| `prqnt diagnose` | Scan + repair-oriented findings |
| `prqnt plan` | Structured repair plan (JSON or human) |
| `prqnt repair` | Execute plan to a separate output directory |
| `prqnt verify` | Compare before/after scans |
| `prqnt doctor` | Diagnose + plan; with `--repair`, run Safe ops and verify |

## Safety classes

See [ADR-0005](../docs/adr/ADR-0005-repair-safety-classification.md).

- **Safe** — auto-executable (recompress, merge compatible small files, resize row groups).
- **ReviewRequired** — present in plans; never auto-executed (schema alignment, casts, renames).
- **Destructive** — represented only; never auto-executed (drops, filters, deletes).

## Source immutability

Repair **never** mutates the source dataset. All repairs write to a user-specified output tree.
Staging directories (`.parqonaut-staging-<uuid>/`) are retained on failure.

## Determinism

Plan **operation IDs** and **plan_id** are derived from canonical JSON hashes of finding-linked
content and policy parameters. Execution IDs remain random UUIDs.

See [ADR-0004](../docs/adr/ADR-0004-evidence-driven-repair-planning.md) and
[ADR-0006](../docs/adr/ADR-0006-dataset-fingerprint-stale-plan-rejection.md).
