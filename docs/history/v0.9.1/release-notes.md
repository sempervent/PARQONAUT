# PARQONAUT v0.9.1 — Remote pipeline completeness

## Highlights

- **Bounded streaming Parquet publication to S3** — RecordBatch streams encode through a sync Parquet writer, bounded byte channels, and multipart upload finalization (no whole-object buffering).
- **Complete local/S3 routing** for transform and stream workflows via independent source and sink backends (`ColumnarPipelineIo`).
- **Remote convert**, **split**, **partition**, **rewrite**, **merge**, and **fused specs** across all four storage legs (local→local, local→S3, S3→local, S3→S3).
- **`just pipeline-s3-demo`** — end-to-end RustFS pipeline: CSV schema drift → S3 convert → fused rewrite/partition → scan/verify with zero remote intermediates.
- **Authoritative 6×4 capability matrix** integration tests on RustFS (`remote_pipeline_matrix`).

## Fixes

- S3 multipart `poll_shutdown` now always runs `CompleteMultipartUpload` / single-part finalize instead of returning early with an empty pending queue.
- `prqnt convert` remote Parquet output uses `write_parquet_batch_stream` instead of in-memory `conditional_create`.

## Verification

See [acceptance.md](./acceptance.md) and [parallel-audit.md](./parallel-audit.md).
