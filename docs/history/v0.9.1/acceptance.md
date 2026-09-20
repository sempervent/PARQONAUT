# v0.9.1 acceptance record

| Gate | Status |
|------|--------|
| Streaming S3 Parquet publication | PASS (CI `write_s3_roundtrip`, `s3_raw_write`) |
| No full-object S3 output buffering | PASS (architecture: bounded batch + byte channels + multipart parts) |
| Multipart visibility (pre/post finalize) | PASS (`write_stream_not_visible_until_finalized`) |
| Multipart failure / retry | PASS (`write_stream_multipart_failure_then_clean_retry`) |
| Memory-bound proof | PASS (output ≫ part size; peak ≤ O(part × in-flight)) |
| Convert 4 legs | PASS (`remote_convert_s3`, matrix) |
| Rewrite 4 legs | PASS (`remote_pipeline_matrix`) |
| Merge 4 legs | PASS |
| Split 4 legs | PASS |
| Partition 4 legs | PASS |
| Fused spec 4 legs | PASS |
| Zero intermediate local I/O (fused) | PASS (`intermediate_files_created == 0`) |
| Zero intermediate S3 objects (fused demo) | PASS (`pipeline-s3-demo` inventory) |
| Bounded batch queue | PASS (8) |
| Bounded partition writers | PASS (existing caps) |
| Bounded multipart state | PASS (single buffer + in-flight part) |
| Schema parity across legs | PASS (matrix row counts + fixtures) |
| Credential redaction | PASS (existing canaries) |
| `pipeline-s3-demo` | PASS (CI / local with RustFS) |
| Full demo battery | PASS (CI `just ci` + workflow) |
| Repair/batch remote regression | PASS (storage tests + demos) |
| Workspace fmt/clippy/test/build | PASS (CI `rust`) |
| `columnar-check`, `naming-check`, `docs-check` | PASS (CI) |
| `s3-integration` CI | PASS |
| Parallel audit | PASS ([parallel-audit.md](./parallel-audit.md)) |
| GitHub Pages | PASS (post-merge docs workflow) |
