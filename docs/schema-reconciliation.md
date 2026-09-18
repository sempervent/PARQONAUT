# Schema reconciliation

PARQONAUT resolves schema drift using deterministic comparison, a compatibility lattice, and explicit policy. No inference or AI is used.

## Structured schema diff

Each field difference records:

- `path` — dot-separated field path
- `left` / `right` — normalized descriptors (physical type + nullability)
- `difference` — e.g. `compatible_numeric_widening`, `nullable_difference`, `incompatible_type_change`
- `compatibility` — `lossless`, `conditionally_lossless`, `potentially_lossy`, or `unsupported`

## Policy

Configure in TOML (embedded in repair plans):

```toml
[schema]
allow_numeric_widening = true
allow_nullable_widening = true
allow_column_reorder = true
allow_missing_nullable_columns = false

[schema.rename]
cust_id = "customer_id"
```

Renames are explicit only. PARQONAUT never guesses similar column names.

## Target schema

1. **Explicit** — `--target-schema schema.json` (highest authority)
2. **Dataset-derived** — canonical schema when all conflicts resolve losslessly under policy

If types cannot be resolved deterministically (e.g. `utf8` vs `int64` for the same column), PARQONAUT emits `UnresolvableSchemaConflict` and blocks automatic repair.

## Authorization

Compatible widening and nullable widening are `ReviewRequired`. Execute with:

```bash
parqonaut repair dataset/ --plan plan.json --authorize <operation_id> --output repaired/
```

Safe operations run under `automatic-safe-policy` without extra flags.
