# PARQONAUT

**Parquet Analysis, Rewriting, Quality, Orchestration, Navigation, Auditing, Unification & Transformation**

PARQONAUT is a Rust-first toolkit for exploring, diagnosing, streaming, transforming, and repairing Parquet-oriented datasets — from single files to multi-dataset fleets.

It consolidates [Paraclete](https://github.com/sempervent/paraclete), [parqknife](https://github.com/sempervent/parqknife), and [streaming-parquet (maw)](https://github.com/sempervent/streaming-parquet) into one workspace. See [docs/provenance.md](docs/provenance.md) for migration sources.

**Current release:** v0.7.0 — unified `prqnt` CLI plus `prqnt serve` HTTP API; local and S3-capable scan/repair/batch workflows.

PARQONAUT provides the **`prqnt`** command.

## What works today

| Command | Capability | Description |
|---------|------------|-------------|
| `prqnt scan <path>` | scan | Forensic scan of a file, directory, or `s3://` prefix |
| `prqnt serve` | server | PARQONAUT HTTP API (`/api/v1`) with durable async scan jobs |
| `prqnt inspect <file>` | transform | Parquet schema and row-group metadata |
| `prqnt rewrite <in> <out> [--compression zstd]` | transform | Rewrite Parquet with optional recompression |
| `prqnt convert <inputs...> -o <out>` | stream | Stream CSV → Parquet (or concatenate CSV) |
| `prqnt doctor <path> [--policy policy.toml]` | repair | Scan → diagnose → plan (optional `--repair`) |
| `prqnt plan <path> [--policy policy.toml]` | repair | Generate durable repair plan JSON |
| `prqnt repair <path> --plan plan.json --output out/` | repair | Execute plan; `--authorize <op_id>` for ReviewRequired |
| `prqnt verify before/ after/ [--manifest manifest.json]` | repair | Verify invariants and optional manifest |
| `prqnt check <path> [--policy ci-policy.toml]` | repair | CI gate (non-mutating; exit codes 0/2/3/4/5) |
| `prqnt plan diff plan-a.json plan-b.json` | repair | Compare plans for review/CI |
| `prqnt batch check --config batch.toml` | batch | Validate batch config (non-mutating) |
| `prqnt batch plan --config batch.toml --output plan.json` | batch | Build durable multi-dataset batch plan |
| `prqnt batch repair --plan plan.json [--jobs N]` | batch | Execute batch repair with bounded concurrency |
| `prqnt batch status --run-dir <run-dir>` | batch | Read durable run journal status |
| `prqnt batch resume --run-dir <run-dir>` | batch | Resume interrupted batch run |
| `prqnt batch verify --run-dir <run-dir>` | batch | Verify batch outputs from journal |

Local paths and **`s3://`** dataset URIs are supported when built with S3 features (see object-storage docs and `just s3-demo`).

### Predecessor contributions

| Source | Crates | CLI surface |
|--------|--------|-------------|
| **Paraclete** | `paraclete-*` | `scan` |
| **parqknife** | `parqonaut-transform` | `inspect`, `rewrite` |
| **maw** | `parqonaut-stream` | `convert` |

Three Arrow/Parquet stacks coexist behind crate boundaries (documented in [ADR-0002](docs/adr/ADR-0002-coexisting-arrow-stacks.md)).

## Build

Requires Rust stable with `clippy` and `rustfmt` components.

```bash
git clone https://github.com/sempervent/parqonaut.git
cd parqonaut
cargo build --release
```

Or use the [Justfile](Justfile):

```bash
just setup    # install rustfmt + clippy
just build
just test
just ci       # fmt + clippy + test
```

## Quick start

```bash
# Forensic scan
cargo run -p parqonaut-cli --bin prqnt -- scan fixtures/scan/single_parquet/data.parquet

# HTTP server (loopback default)
cargo run -p parqonaut-cli --bin prqnt -- serve --state-dir /tmp/prqnt-serve-demo

# Recompress Parquet
cargo run -p parqonaut-cli --bin prqnt -- rewrite \
  fixtures/scan/single_parquet/data.parquet /tmp/out.parquet --compression zstd

# CSV → Parquet
printf 'id,name\n1,alpha\n2,beta\n' > /tmp/sample.csv
cargo run -p parqonaut-cli --bin prqnt -- convert /tmp/sample.csv -o /tmp/sample.parquet --out-format parquet
```

## Batch orchestration

Repair many independent datasets with bounded parallelism, durable SQLite journal, resume, and per-dataset failure isolation:

```bash
# Validate configuration
cargo run -p parqonaut-cli --bin prqnt -- batch check --config fixtures/orchestration/shipwreck/batch.toml

# Generate batch plan
cargo run -p parqonaut-cli --bin prqnt -- batch plan \
  --config fixtures/orchestration/shipwreck/batch.toml \
  --output /tmp/shipwreck.plan.json

# Execute with up to 4 concurrent datasets
cargo run -p parqonaut-cli --bin prqnt -- batch repair --plan /tmp/shipwreck.plan.json --jobs 4

# Inspect run state and verify outputs
cargo run -p parqonaut-cli --bin prqnt -- batch status --run-dir <run-dir>
cargo run -p parqonaut-cli --bin prqnt -- batch verify --run-dir <run-dir>
```

See [docs/batch-orchestration.md](docs/batch-orchestration.md) and [docs/batch-execution.md](docs/batch-execution.md).

## Demos

```bash
just demo              # Stream demo: CSV → Parquet → scan → rewrite → rescan
just schema-demo       # Schema: FRANKENLAKE schema reconciliation
just batch-demo       # Batch: SHIPWRECK batch fleet
just batch-resume-demo  # Batch: interrupt and resume
```

## Not yet implemented

- Web dashboard / TUI
- Plugin execution bridge
- parqknife: partition, merge, split, and spec-file workflows beyond current transform surface
- Stream pipeline: resumability, progress UI wiring, full schema unification
- Arrow/Parquet dependency convergence across engines
- In-memory cross-engine pipelines

See [CHANGELOG.md](CHANGELOG.md) for release history.

## Documentation

- [Server (`prqnt serve`)](docs/server.md)
- [HTTP API](docs/api.md)
- [Architecture](docs/architecture.md)
- [Schema reconciliation](docs/schema-reconciliation.md)
- [Batch orchestration](docs/batch-orchestration.md)
- [Batch execution](docs/batch-execution.md)
- [Resume and recovery](docs/resume-recovery.md)
- [Schema reconciliation policy](docs/schema-reconciliation.md)
- [Repair plan contract](docs/plan-contract.md)
- [CI policy / check command](docs/ci-policy.md)
- [Migration analysis](docs/migration-analysis.md)
- [Provenance](docs/provenance.md)
- [Third-party licenses](THIRD_PARTY_LICENSES.md)
- [ADRs](docs/adr/)

## License

MIT — see [LICENSE](LICENSE) and [NOTICE](NOTICE).

`crates/parqonaut-transform` (parqknife lineage) declares `MIT OR Apache-2.0` in its `Cargo.toml`; [LICENSE-APACHE-2.0](LICENSE-APACHE-2.0) is provided for the Apache option. Details in [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
