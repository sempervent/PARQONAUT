# Repair plan contract

Repair plans are durable, versioned JSON suitable for CI, code review, and replay.

## Version

Every plan includes `"schema_version": 1`. Unsupported versions are rejected at load time.

## Stable fields

| Field | Purpose |
|-------|---------|
| `plan_id` | Deterministic hash of fingerprint, policy, operations |
| `dataset_fingerprint` | Stale-plan rejection |
| `policy_fingerprint` | Policy contract binding |
| `parqonaut_version` | Producer version |
| `operations[].depends_on` | Explicit execution ordering |
| `schema_diff` / `schema_conflicts` | Structured schema state |

Volatile fields (`generated_at`, finding UUIDs) are stripped from canonical serialization.

## Commands

```bash
prqnt plan dataset/ --policy policy.toml --output plan.json
prqnt plan dataset/ --canonical
prqnt plan diff plan-a.json plan-b.json
prqnt repair dataset/ --plan plan.json --authorize R004 --output out/
```

Golden fixtures under `fixtures/plans/` guard against accidental contract changes.
