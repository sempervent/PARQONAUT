# Remote routing recon (v0.9.1)

Baseline: v0.9.0 shipped unified Arrow/Parquet 54 columnar pipelines with storage-backed **rewrite/merge** only when a single `StorageBackend` matched both legs (mixed local↔S3 failed or used the wrong backend).

| Capability | Source | Destination | v0.9.0 backend selection | Limitation | v0.9.1 adapter |
|------------|--------|-------------|--------------------------|------------|----------------|
| convert | local CSV/glob | local path | `parqonaut-stream` `PathBuf` writers | No `s3://` output | `BatchSource`/`BatchSink` + stream storage-out |
| convert | local | `s3://` | N/A | Not wired | Stream → `StorageParquetBatchSink` |
| convert | `s3://` Parquet | local | N/A | Not wired | `StorageParquetBatchSource` → stream/local sink |
| convert | `s3://` | `s3://` | N/A | Not wired | Dual-backend batch pipe |
| convert | `s3://` CSV | any | N/A | Unsupported (typed) | — |
| rewrite | any | any | Single caller-supplied backend | L↔S3 needs two backends | `ColumnarPipelineIo::backend_for` per leg |
| merge | any | any | Same as rewrite | Same | Dual-backend merge relay |
| split | local file | local dir | `File::open` + local `ParquetWriter` | No remote | Storage rolling split sink |
| split | any remote leg | any | N/A | Local-only | `split_parquet_storage` |
| partition | local file | local dir | Loads all batches / local LRU writers | No remote | `partition_parquet_storage` |
| partition | any remote leg | any | N/A | Local-only | Storage LRU partition sinks |
| fused spec | local | local | `LocalParquetBatchSource` + local partition | `StorageKind::S3` rejected in fusion | Fused segment via `ColumnarPipelineIo` |
| fused spec | mixed / S3 | mixed / S3 | N/A | Error in `fused_execute` | Same fusion, storage sources/sinks |

Call paths:

- `prqnt convert` → `parqonaut-cli/src/stream.rs` → `parqonaut-stream` pipeline (`PathBuf` output).
- `prqnt split|partition|merge|rewrite` → `parqonaut-transform` CLI → local engine or `remote.rs` (single backend).
- `prqnt transform --spec` → `spec/execute.rs` / `fused_execute.rs` (local-only fusion for remote kinds).

v0.9.1 introduces **`ColumnarPipelineIo`** (local + S3 factories) and resolves source/destination backends independently for all columnar workflows above.
