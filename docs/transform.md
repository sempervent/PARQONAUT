# Transform workflows

PARQONAUT v0.8 adds first-class Parquet transform commands and declarative multi-step specs.

## Commands

```bash
# Hive-style partition (LRU-backed writer cap)
prqnt partition input.parquet -o out/ --partition-by country,date

# Merge compatible Parquet inputs
prqnt merge 'parts/*.parquet' -o merged.parquet

# Split by target size
prqnt split large.parquet -o parts/ --target-size-mb 512

# Declarative pipeline (YAML)
prqnt transform --spec fixtures/transform/specs/rewrite-partition.yaml
prqnt transform --spec workflow.yaml --check
prqnt transform --spec workflow.yaml --dry-run --json
```

## Spec format

Sequential steps only (v0.8): step *N* output feeds step *N+1* unless an operation declares its own source. Intermediate results live under `.parqonaut-spec-<execution-id>/` and are removed after success.

Canonical example (`fixtures/transform/specs/rewrite-partition.yaml`):

```yaml
schema-version: 1
input: fixtures/transform/partition-basic/input.parquet
output: target/spec-demo/processed
steps:
  - operation:
      type: rewrite
      compression: zstd
  - operation:
      type: partition
      partition-by:
        - country
```

`--check` validates without mutation. `--dry-run` resolves the typed execution plan (JSON report includes structured `plan`).

## Object storage

Transform spec and partition/merge/split commands are **local-path only in v0.8**. Remote dataset repair and publication continue to use `parqonaut-storage` via scan/repair/batch (see [Object storage](./object-storage.md)).
