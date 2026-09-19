# Installation

PARQONAUT ships as a single executable: **`prqnt`**.

There is no Homebrew formula or crates.io package for PARQONAUT at this time. Supported paths are **source build** and **`cargo install --git`**.

## Build from source

```bash
git clone https://github.com/sempervent/PARQONAUT
cd PARQONAUT
cargo build --release -p parqonaut-cli --bin prqnt
```

The binary is at `target/release/prqnt`.

Developer automation:

```bash
just setup   # rustfmt + clippy components
just build
just test
```

## Install with Cargo (git)

```bash
cargo install --git https://github.com/sempervent/PARQONAUT --bin prqnt
```

This compiles the latest `main` (or the ref you pin with `--tag v0.7.1`). Ensure Rust stable is installed.

## S3 / object storage features

S3-compatible backends (AWS, MinIO, **RustFS** in tests) use the `parqonaut-storage` S3 backend. The CLI binary includes S3 support in the default workspace build used for releases in this repository.

For local integration tests, see [Object storage](./object-storage.md) and `just s3-demo`.

## Application server

`prqnt serve` uses SQLite by default under `--state-dir`. Postgres is supported via `--database` (Postgres URL). See [prqnt serve](./server.md).
