# FOGBANK object-storage fixtures

FOGBANK is the S3-compatible laboratory (RustFS in CI) for remote scan, repair, and batch tests.
Fixtures are generated on demand and optionally uploaded to the `fogbank` bucket.

## Prerequisites

- Docker (RustFS or other S3-compatible server)
- Rust toolchain with `s3` features on `parqonaut-storage` / `parqonaut-cli`

## Quick start

```bash
just s3-up
source scripts/s3-test/env.sh
just s3-fixtures
```

Environment (also set by `scripts/s3-test/env.sh`):

| Variable | Default |
|----------|---------|
| `PARQONAUT_S3_ENDPOINT` | `http://127.0.0.1:9000` |
| `AWS_ACCESS_KEY_ID` | `rustfsadmin` |
| `AWS_SECRET_ACCESS_KEY` | `rustfsadmin` |
| `FOGBANK_BUCKET` | `fogbank` |

Generate local Parquet under `fixtures/object-storage/local/`:

```bash
cargo xtask fixtures object-storage
```

Upload to the test bucket (server must be running):

```bash
PARQONAUT_XTASK_UPLOAD=1 cargo xtask fixtures object-storage fixtures/object-storage/local
```
