# Columnar convergence plan (v0.9 target)

**Status:** planning artifact for v0.8.0; implementation is **v0.9.0**.

## Crate inventory

| Crate | Dependencies | Public types | Migration difficulty |
|-------|--------------|--------------|----------------------|
| `parqonaut-core` | parquet 53 | Scan findings, local readers | Medium — scan path stable |
| `parqonaut-transform` | arrow 54, parquet 54 | RecordBatch (arrow 54) in engine | Medium — primary transform target |
| `parqonaut-stream` | arrow2, parquet2 | Chunk, UnifiedSchema (arrow2) | **High** — full rewrite of pipeline |
| `parqonaut-storage` | bytes, S3 | Backend-agnostic | Low |

## Recommended canonical stack (v0.9 decision draft)

**Apache Arrow Rust + Apache Parquet Rust** at one aligned version (likely 54+), because:

- Transform already on arrow 54
- RecordBatch ecosystem for in-memory pipelines
- Parquet writer/reader feature parity for repair/scan adjacency
- Maintenance and S3 integration via existing storage layer

arrow2 removal requires chunk→RecordBatch adapters and parquet2 writer replacement.

## Migration order (draft)

1. Freeze `parqonaut-workflow` semantic contracts (v0.8)
2. Introduce arrow 54 stream pipeline behind feature flag
3. Port stream tests to RecordBatch path
4. Remove arrow2/parquet2
5. Align scan parquet 53 → 54 where shared I/O allows

## Open questions for v0.8 gate

- Exact arrow/parquet version pin for v0.9
- Which disk handoffs (convert→rewrite→partition) can become in-memory first
- Benchmark: transform laboratory E2E before/after
- Rollback: keep v0.8.x branch for arrow2 stream if needed
