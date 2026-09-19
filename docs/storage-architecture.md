# Storage architecture

Contract version: `STORAGE_CONTRACT_VERSION = 1` in `parqonaut-storage`.

## Boundaries

```text
parqonaut-storage     → DatasetLocation, StorageBackend, publication (later)
parqonaut-repair      → single-dataset mechanics via storage traits
parqonaut-orchestrator → fleet orchestration via storage traits
parqonaut-cli         → command surface only
```

AWS SDK types must not appear outside `parqonaut-storage` S3 backend modules.

## DatasetLocation

Structured backend identity parsed from:

```text
/path/to/data
./relative
file:///absolute/path
s3://bucket
s3://bucket/
s3://bucket/prefix/
```

Credentials are **never** embedded in URIs. Serialization is deterministic JSON with tagged `backend` field.

## ObjectLocation

Single object reference (`file://…` display form or `s3://bucket/key`).

## ObjectMetadata

Backend-neutral metadata:

- `size`
- opaque `etag` (never interpreted as MD5)
- optional `version_id`
- optional `last_modified`

## StorageBackend

Async trait providing:

| Operation | Notes |
|-----------|-------|
| `list` | Lexically stable ordering; pagination via continuation tokens |
| `head` | Metadata only |
| `read_range` | Bounded bytes for Parquet footer reads |
| `read_stream` | Streaming reads; no whole-object buffering |
| `write_stream` | Streaming writes |
| `conditional_create` | Create-if-absent |
| `conditional_replace` | Replace-if-etag/version matches |
| `delete_owned_object` | PARQONAUT-owned cleanup only |

## StorageCapabilities

Explicit feature flags (`range_reads`, `multipart_upload`, `conditional_create`, etc.). Backends must not silently downgrade safety.

## Streaming model

- Single abstraction: `ObjectReadStream` / `ObjectWriteStream` (Tokio `AsyncRead`/`AsyncWrite` based)
- Backpressure via async read/write; at most one in-flight buffer per stream
- Large objects must use streams or ranged reads, not unbounded `Vec<u8>`

## Errors and retry

`StorageError` classifies:

```text
NotFound, PermissionDenied, Authentication, Conflict, PreconditionFailed,
Transient, Unavailable, InvalidLocation, UnsupportedCapability, Io, Other
```

Each error exposes `retry_class()` → `NeverRetry` | `Retryable`.

## Conditional operations

Publication and locking depend on explicit conditional create/replace — not generic overwrite flags.

## Credential boundary

- Credentials load via AWS provider chain (S3 backend only)
- Never serialized in plans, journals, manifests, or logs
- `redact` module strips secrets from URIs and debug output

## Contract tests

`contract::storage_backend_contract` runs against each backend implementation (`MemoryStorageBackend` in crate tests; Local and S3 backends add their own).
