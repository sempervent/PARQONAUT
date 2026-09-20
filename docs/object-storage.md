# Object storage (S3-compatible)

PARQONAUT uses **`S3StorageBackend`** for remote datasets. URIs use the standard form:

```text
s3://bucket/prefix/
```

Supported workflows include **scan**, **plan**, **repair**, **batch**, **transform rewrite/merge** on `s3://` Parquet (columnar batch stream via `parqonaut-storage`), and server jobs when storage policy allows the bucket/prefix.

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
