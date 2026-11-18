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

## Application server & Operator Console

`prqnt serve` uses SQLite by default under `--state-dir` and serves both `/api/v1` and the bundled web dashboard at `/ui/`. Postgres is supported via `--database`.

Connect from the terminal using:

```bash
export PARQONAUT_API_TOKEN="<your-bearer-token>"
prqnt tui --url http://127.0.0.1:8080
```

### Troubleshooting

- **Disconnected or unauthorized in TUI / Web UI:** Verify `prqnt serve` is running (`curl http://127.0.0.1:8080/api/v1/health`), ensure `Authorization: Bearer` token is valid (`--token-env`, `--token-file`, or interactive masked prompt), and check role (`Reader` is read-only; `Operator` or `Admin` required for scan/repair/batch/cancel).
- **SSE live updates behind reverse proxies:** Disable response buffering/compression on `/api/v1/jobs/*/events/stream` (`text/event-stream`). PARQONAUT's built-in `CompressionLayer` explicitly excludes `text/event-stream`, and clients automatically resume via `Last-Event-ID` or fall back to polling `GET /api/v1/jobs/{job_id}/events`.
- **Headless browser test (`just ui-test`):** Requires Node 22+ and a Chromium/Chrome binary (runs automatically when `CHROME_BIN` or standard Chrome/Chromium paths exist).

See [prqnt serve](./server.md) and [Durable jobs & events](./job-model.md).
