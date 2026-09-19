# Phase 5 FOGBANK fixtures

FOGBANK is the local MinIO laboratory for S3-compatible storage tests. Fixtures are
generated on demand and optionally uploaded to the `fogbank` bucket.

## Prerequisites

- Docker (for MinIO; images pulled from `quay.io/minio/*` because Docker Hub `minio/*` may be unavailable)
- Rust toolchain with `s3` + `fogbank-fixtures` features on `parqonaut-storage`

## Quick start

```bash
just phase5-up
source scripts/phase5/env.sh
just phase5-fixtures
```

Environment (also set by `scripts/phase5/env.sh`):

| Variable | Default |
|----------|---------|
| `MINIO_ENDPOINT` | `http://127.0.0.1:9000` |
| `AWS_ACCESS_KEY_ID` | `minioadmin` |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` |
| `FOGBANK_BUCKET` | `fogbank` |
| `MINIO_BUCKET` | `parqonaut-test` (contract / integration tests) |

## Scenarios

| Prefix | Purpose |
|--------|---------|
| `datasets/healthy/` | Multi-file healthy remote dataset |
| `datasets/large-parquet/` | Single large Parquet object for range-read / footer tests |
| `datasets/stale-source/` | Baseline snapshots; mutate after plan capture (see `local/stale-source/MUTATION.md`) |

## Layout

```text
fixtures/phase5/
  README.md           (this file)
  fogbank.toml        (scenario catalog)
  local/              (generated Parquet sources; safe to regenerate)
  manifests/          (remote fingerprint JSON after upload)
```

## Regenerate without upload

```bash
FOGBANK_UPLOAD=0 scripts/phase5/fixtures.sh
```

## Integration tests

With MinIO running and env sourced:

```bash
cargo test -p parqonaut-storage --features s3 fogbank -- --nocapture
```

Without `MINIO_ENDPOINT`, fogbank tests skip gracefully.
