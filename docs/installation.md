# Installation

PARQONAUT ships as a single executable: **`prqnt`**, including the CLI, HTTP server (`prqnt serve`), terminal operator console (`prqnt tui`), and bundled browser dashboard (`/ui/`). No separate Node.js runtime is required to run `prqnt`.

## Pre-built release archives

Versioned release archives (`parqonaut-v<version>-<target>.tar.gz`) and `SHA256SUMS` are built via `.github/workflows/release.yml` for:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `aarch64-apple-darwin` (macOS Apple Silicon)
- `x86_64-apple-darwin` (macOS Intel)

Verify the checksum, build provenance attestation, and install:

```bash
sha256sum -c SHA256SUMS --ignore-missing
gh attestation verify parqonaut-v1.0.0-rc.1-x86_64-unknown-linux-gnu.tar.gz --repo sempervent/PARQONAUT
tar -xzf parqonaut-v1.0.0-rc.1-x86_64-unknown-linux-gnu.tar.gz
sudo install -m 0755 parqonaut-v1.0.0-rc.1-x86_64-unknown-linux-gnu/prqnt /usr/local/bin/prqnt
prqnt --version
```

Release bundles also include a validated CycloneDX 1.6 SBOM (`parqonaut-v1.0.0-rc.1.cdx.json`) and `cargo-metadata-v1.0.0-rc.1.json`.

## Container image (`prqnt serve`)

Build and run the non-root multi-stage container with Docker Compose:

```bash
# Default SQLite state backend:
PRQNT_BOOTSTRAP_ADMIN_TOKEN="$(openssl rand -hex 32)" docker compose up --build -d

# Optional PostgreSQL 16 backend (requires explicit PARQONAUT_PG_PASSWORD):
PARQONAUT_PG_PASSWORD="<strong-password>" \
PRQNT_BOOTSTRAP_ADMIN_TOKEN="$(openssl rand -hex 32)" \
  docker compose -f compose.yaml -f compose.postgres.yaml --profile postgres up --build -d
```

See [Operations runbook](./operations.md) for backup, authentication bootstrap, and reverse-proxy configuration.

## Build from source

```bash
git clone https://github.com/sempervent/PARQONAUT
cd PARQONAUT
cargo build --release --locked -p parqonaut-cli --bin prqnt
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
cargo install --git https://github.com/sempervent/PARQONAUT --locked --bin prqnt
```

## Optional Python plugin runtime

Python plugins (`parqonaut_plugins`) are an optional runtime dependency. Core CLI commands, streaming conversion, repair, and `/ui/` work without Python; invoking `--plugin` when Python is unavailable reports actionable diagnostics (`plugin Python runtime is unavailable`).

To install and enable the Python plugin SDK (`python >= 3.12`):

```bash
(cd python/parqonaut_plugins && uv sync --frozen)
export PARQONAUT_PLUGIN_PYTHON="$PWD/python/parqonaut_plugins/.venv/bin/python"
export PARQONAUT_PLUGIN_ROOTS="$PWD/fixtures/plugins"
export PARQONAUT_PLUGIN_SDK_PATH="$PWD/python/parqonaut_plugins/src"
prqnt plugin list
```

## S3 / object storage features

S3-compatible backends (AWS, MinIO, **RustFS** in tests) use the `parqonaut-storage` S3 backend. The release `prqnt` binary includes S3 support by default.

## Application server & Operator Console

`prqnt serve` uses SQLite by default under `--state-dir` and serves both `/api/v1` and `/ui/`. PostgreSQL is supported via `--database`.

Connect from the terminal using:

```bash
export PARQONAUT_API_TOKEN="<your-bearer-token>"
prqnt tui --url http://127.0.0.1:8080
```

### Troubleshooting

- **Disconnected or unauthorized in TUI / Web UI:** Verify `prqnt serve` is running (`curl http://127.0.0.1:8080/api/v1/health/ready`), ensure `Authorization: Bearer` token is valid (`--token-env`, `--token-file`, or interactive masked prompt), and check role (`Reader` is read-only; `Operator` or `Admin` required for mutations).
- **SSE live updates behind reverse proxies:** Disable response buffering/compression on `/api/v1/jobs/*/events/stream` (`text/event-stream`). PARQONAUT's built-in `CompressionLayer` explicitly excludes `text/event-stream`, and clients automatically resume via `Last-Event-ID` or fall back to polling `GET /api/v1/jobs/{job_id}/events`.
- **Headless browser test (`just ui-test`):** Requires Node 22+ and a Chromium/Chrome binary.

See [prqnt serve](./server.md), [Operations runbook](./operations.md), and [Durable jobs & events](./job-model.md).
