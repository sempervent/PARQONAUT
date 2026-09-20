# Object storage (S3-compatible)

PARQONAUT uses **`S3StorageBackend`** for remote datasets. URIs use the standard form:

```text
s3://bucket/prefix/
```

Supported workflows include **scan**, **plan**, **repair**, **batch**, **transform** (rewrite, merge, split, partition, fused specs), **`prqnt convert`**, and server jobs when storage policy allows the bucket/prefix.

## Streaming Parquet publication (v0.9.1)

Remote Parquet outputs follow:

```text
RecordBatch stream → Parquet encoder (blocking task) → bounded byte channel → multipart S3 upload → finalized object
```

Create-only destinations use **`conditional_create`**; direct `write_stream` targets replace on successful finalize. Failed uploads do not commit a readable destination object.

## RustFS in tests

CI and local integration tests use **RustFS** (pinned container image) as an S3-compatible backend — not a separate product API. Example environment (fake credentials for local lab only):

```bash
export PARQONAUT_S3_ENDPOINT=http://127.0.0.1:9000
export PARQONAUT_S3_PATH_STYLE=true
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
just s3-demo
```

## Server storage policy

`prqnt serve` enforces **`allowed_s3_buckets`** and **`allowed_s3_prefixes`** before any I/O. See [Storage policy](./server-storage-policy.md).

## Architecture

See [Storage architecture](./storage-architecture.md) and [Remote publication](./remote-publication.md).
