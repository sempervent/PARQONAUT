# Streaming conversion

`prqnt convert` runs the schema-aware streaming engine (`parqonaut-stream`).

## Schema discovery and unification

Inputs are discovered recursively (unless `--no-recursive`). Schemas are unified before writing:

| Policy | Flag | Behavior |
|--------|------|----------|
| Strict | `--schema-conflicts strict` (default) | Reject incompatible types |
| Widen | `--schema-conflicts widen` | Deterministic numeric/date widening |
| Stringify | `--stringify-conflicts` | Coerce conflicts to UTF-8 strings |

Shared semantic vectors live in `fixtures/schema/compatibility-vectors.json` and are tested against stream unification.

## Resumable conversion

```bash
prqnt convert 'data/*.csv' -o out.parquet --out-format parquet \
  --state target/convert-state.json

# After interruption
prqnt convert 'data/*.csv' -o out.parquet --out-format parquet \
  --state target/convert-state.json --resume
```

Checkpoints bind input fingerprints (path, size, mtime), output location, format, schema policy, compression, and batch settings. Stale sources produce `STALE CHECKPOINT` (no silent restart). Incomplete output is staged under `<output>.staging/` until all inputs finish.

CSV and Parquet resume granularity in v0.8: **completed-file** boundary (row-group resume planned).

## Progress

Human progress renders on TTY stderr when progress is enabled and `--quiet` / JSON-only modes are off. Automation may consume JSON Lines progress via `--json-progress` (see CLI reference).
