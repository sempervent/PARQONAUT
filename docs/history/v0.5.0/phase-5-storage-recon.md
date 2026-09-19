# Phase 5 storage reconnaissance

Baseline: `v0.4.1` / `main` @ `480b031`.

## Existing S3-related code

| Location | Status | Notes |
|----------|--------|-------|
| `crates/parqonaut-transform/src/io/s3.rs` | **Stub / incomplete** | `S3InputSource`, `S3OutputSink` via `aws-sdk-s3` + `aws-config` |
| `crates/parqonaut-transform/Cargo.toml` | Dependency present | `aws-sdk-s3`, `aws-config` (rustls) |
| `crates/parqonaut-repair` | Transitive AWS deps | Pulled via transform for scan/rewrite paths |
| `parqonaut-cli` | No S3 URIs | All commands assume local paths today |
| `parqonaut-orchestrator` | Local paths only | `paths.rs`, `lock.rs` use filesystem semantics |

## parqknife S3 stub assessment (`io/s3.rs`)

**Reusable concepts:**
- `s3://bucket/key` URL parsing pattern
- `InputSource` / `OutputSink` trait boundary in transform I/O

**Not reusable as-is:**
- `S3ReadAdapter` — placeholder; buffers nothing, returns empty reads
- `S3WriteAdapter` — buffers in RAM, no multipart, no flush upload
- No range reads, HEAD, LIST, conditional writes, or credential redaction
- No custom endpoint / path-style configuration
- Full-object GET only (when implemented) — violates metadata-only scan requirement

**Verdict:** Do **not** extend the transform stub. Extract any useful URI parsing into `parqonaut-storage`, then migrate consumers. Deprecate or replace `parqonaut-transform/src/io/s3.rs` once the storage crate is authoritative.

## Phase 4 local semantics to preserve

| Component | Local assumption today |
|-----------|------------------------|
| `parqonaut-repair` | `camino::Utf8Path`, directory scan, staged local publication |
| `parqonaut-orchestrator` | Output locks as `{path}.parqonaut.lock`, SQLite journal on local FS |
| Fingerprints | Filesystem inventory + file metadata |
| Batch overlap | Canonical local path nesting checks |

Phase 5 must introduce `DatasetLocation` at the storage boundary without breaking local behavior.

## Recommended new crate

```text
crates/parqonaut-storage/
  location.rs      — DatasetLocation, ObjectLocation, URI parse/canonicalize
  backend.rs     — StorageBackend trait, StorageCapabilities
  local.rs       — LocalStorageBackend (wrap existing semantics)
  s3.rs          — S3StorageBackend (aws-sdk-s3)
  inventory.rs   — deterministic LIST pagination
  fingerprint.rs — remote dataset identity
  publication.rs — immutable version + commit marker + CURRENT
  lock.rs        — conditional remote lock objects
  metrics.rs     — request/byte counters
  redact.rs      — credential-safe display/logging
```

## Integration order

1. Lead freezes `DatasetLocation` + `StorageBackend` contracts
2. Local backend + contract tests (parity with today)
3. S3 backend against MinIO
4. Range-read Parquet footer adapter
5. Repair/orchestrator integration
6. FOGBANK fixtures + fault injection

## MinIO test strategy

- `docker-compose.phase5.yml` (or `compose.phase5.yaml`) — not required in default unit-test CI
- Dedicated GitHub Actions job with service container
- No paid AWS account required for mandatory tests

## Non-goals (Phase 5)

- Distributed workers, Redis/Kafka, HTTP server
- Arrow-stack convergence
- Broad bucket GC / retention policies
