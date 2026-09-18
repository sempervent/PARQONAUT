# Phase 2 — diagnose, repair, verify

Phase 2 proves PARQONAUT can reason mechanically from scan evidence, prescribe bounded repairs,
execute Safe operations without touching source data, and verify results.

## Crate layout

| Crate | Responsibility |
|-------|----------------|
| `parqonaut-repair` | Diagnosis, rules, plans, fingerprint, execution orchestration, verification |
| `parqonaut-transform` | Parquet merge + rewrite (library calls, no subprocesses) |
| `paraclete-core` / `paraclete-types` | Scan engine + durable finding/plan contracts |
| `parqonaut-cli` | `doctor`, `diagnose`, `plan`, `repair`, `verify` commands |

## Fixtures

Generate pathological datasets:

```bash
cargo run -p parqonaut-repair --bin generate-phase2-fixtures -- fixtures/phase2
```

| Fixture | Purpose |
|---------|---------|
| `healthy/` | No Safe repairs expected |
| `small-files/` | Merge candidate |
| `tiny-row-groups/` | Row-group normalization |
| `mixed-compression/` | Recompress candidate |
| `schema-drift/` | ReviewRequired only |
| `frankenlake/` | Integration: all of the above |

## Acceptance scenario

```bash
parqonaut doctor fixtures/phase2/frankenlake
parqonaut doctor fixtures/phase2/frankenlake --repair --output target/frankenlake-repaired
parqonaut plan target/frankenlake-repaired   # 0 Safe ops; schema drift remains ReviewRequired
```

## Deferred (not Phase 2)

- LLM diagnosis, destructive auto-repair, distributed workers, web UI, table formats (Iceberg/Delta), Python plugins.
