# Shared columnar pipelines (v0.9)

PARQONAUT executes data movement through **`parqonaut-columnar`**:

```text
BatchSource → [bounded BatchStream] → transforms → BatchSink
```

Default channel capacity is **4** batches (`DEFAULT_STREAM_CHANNEL_CAPACITY`).

## Fused transform specs

Multi-step transform YAML compiles into **fused segments** when operations are streamable. Staging directories under `.parqonaut-spec-*` appear only when a **barrier** operation (for example `split` or rewrite metadata mutation) requires materialization. Dry-run JSON includes `pipeline.stages` boundaries.

## Storage-backed I/O

`parqonaut-storage` (feature `columnar`) provides `StorageParquetBatchSource` / `StorageParquetBatchSink` using range reads and multipart writes — no whole-object download for metadata.

## Demos

```bash
just pipeline-demo      # local CSV drift → unify → fused partition
just pipeline-s3-demo   # RustFS rewrite (requires just s3-up)
```

See [Architecture](./architecture.md) and [Transform](./transform.md).
