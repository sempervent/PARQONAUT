# PARQONAUT v0.5.0 — Remote Object Storage

Phase 5 delivers transactional S3-compatible object storage for scan, repair, batch orchestration, and publication.

## Highlights

- **S3 storage backend** with conditional writes, ranged reads, multipart streaming, and remote publication (`COMMITTED` / `CURRENT`).
- **Mixed fleet batch orchestration** — local and `s3://` sources and outputs with durable SQLite journals and resume.
- **FOGBANK** integration fixtures and chaos coverage for storage retries and publication safety.
- **Bounded remote Parquet footer parsing** — no allocation proportional to declared object size.
- **CI**: `phase5-s3-integration` job using pinned **RustFS 1.0.0** (S3-compatible), explicit `docker run`, health `/health/ready`, AWS CLI smoke (including large-object multipart).

## Upgrade notes

- Prefer `PARQONAUT_S3_ENDPOINT`, `PARQONAUT_S3_PATH_STYLE`, and `PARQONAUT_S3_BUCKET`; legacy `MINIO_*` env vars remain as aliases for local scripts.
- Production remote repair uses `S3StorageBackend` via `BatchStorageRuntime::new()`; in-memory remote backends are test-only.
