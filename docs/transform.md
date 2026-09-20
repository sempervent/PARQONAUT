# Transform workflows

PARQONAUT v0.9 runs Parquet transforms on the shared **`parqonaut-columnar`** batch pipeline (Apache Arrow/Parquet **54.3.1**). Streamable transform spec steps can fuse in memory with **zero intermediate Parquet staging**; barriers (for example **split**) appear explicitly in dry-run plans.

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

Sequential steps only: step *N* output feeds step *N+1* unless an operation declares its own source. When the compiled plan is **fully streamable**, steps run in memory and no `.parqonaut-spec-*` directory is created. Non-fusible steps (barriers) may still materialize under `.parqonaut-spec-<execution-id>/` until the run completes.

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

**Rewrite**, **merge**, **split**, **partition**, and **fused streamable specs** resolve **independent source and sink backends** (`ColumnarPipelineIo`): local and `s3://` legs compose in all four combinations. Parquet outputs stream through a bounded encoder and multipart upload — no whole-object buffering (see [Object storage](./object-storage.md) and [Pipelines](./pipelines.md)).
