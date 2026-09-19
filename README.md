# PARQONAUT

**Parquet Analysis, Rewriting, Quality, Orchestration, Navigation, Auditing, Unification & Transformation**

PARQONAUT is a Rust-first toolkit for exploring, diagnosing, streaming, transforming, and repairing Parquet-oriented datasets — from single files to multi-dataset fleets.

It consolidates [Paraclete](https://github.com/sempervent/paraclete), [parqknife](https://github.com/sempervent/parqknife), and [streaming-parquet (maw)](https://github.com/sempervent/streaming-parquet) into one workspace. See [docs/provenance.md](docs/provenance.md) for migration sources.

**Current release:** v0.4.x — local filesystem scan, repair, verification, and batch orchestration.

## What works today

| Command | Engine | Description |
|---------|--------|-------------|
| `parqonaut scan <path>` | Paraclete | Forensic scan of a file or directory (assets, datasets, findings) |
| `parqonaut inspect <file>` | parqknife | Parquet schema and row-group metadata |
| `parqonaut rewrite <in> <out> [--compression zstd]` | parqknife | Rewrite Parquet with optional recompression |
| `parqonaut convert <inputs...> -o <out>` | maw | Stream CSV → Parquet (or concatenate CSV) |
| `parqonaut doctor <path> [--policy policy.toml]` | repair | Scan → diagnose → plan (optional `--repair`) |
| `parqonaut plan <path> [--policy policy.toml]` | repair | Generate durable repair plan JSON |
| `parqonaut repair <path> --plan plan.json --output out/` | repair | Execute plan; `--authorize <op_id>` for ReviewRequired |
| `parqonaut verify before/ after/ [--manifest manifest.json]` | repair | Verify invariants and optional manifest |
| `parqonaut check <path> [--policy ci-policy.toml]` | repair | CI gate (non-mutating; exit codes 0/2/3/4/5) |
| `parqonaut plan diff plan-a.json plan-b.json` | repair | Compare plans for review/CI |
| `parqonaut batch check --config batch.toml` | orchestrator | Validate batch config (non-mutating) |
| `parqonaut batch plan --config batch.toml --output plan.json` | orchestrator | Build durable multi-dataset batch plan |
| `parqonaut batch repair --plan plan.json [--jobs N]` | orchestrator | Execute batch repair with bounded concurrency |
| `parqonaut batch status --run-dir <run-dir>` | orchestrator | Read durable run journal status |
| `parqonaut batch resume --run-dir <run-dir>` | orchestrator | Resume interrupted batch run |
| `parqonaut batch verify --run-dir <run-dir>` | orchestrator | Verify batch outputs from journal |

All paths above are **local filesystem** today. Remote object storage (`s3://`) is planned for v0.5.0.

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
# Forensic scan (Paraclete engine)
cargo run -p parqonaut-cli -- scan fixtures/phase1/single_parquet/data.parquet

# Recompress Parquet (parqknife engine)
cargo run -p parqonaut-cli -- rewrite \
  fixtures/phase1/single_parquet/data.parquet /tmp/out.parquet --compression zstd

# CSV → Parquet (maw engine)
printf 'id,name\n1,alpha\n2,beta\n' > /tmp/sample.csv
cargo run -p parqonaut-cli -- convert /tmp/sample.csv -o /tmp/sample.parquet --out-format parquet
```

## Batch orchestration

Repair many independent datasets with bounded parallelism, durable SQLite journal, resume, and per-dataset failure isolation:

```bash
# Validate configuration
cargo run -p parqonaut-cli -- batch check --config fixtures/phase4/shipwreck/batch.toml

# Generate batch plan
cargo run -p parqonaut-cli -- batch plan \
  --config fixtures/phase4/shipwreck/batch.toml \
  --output /tmp/shipwreck.plan.json

# Execute with up to 4 concurrent datasets
cargo run -p parqonaut-cli -- batch repair --plan /tmp/shipwreck.plan.json --jobs 4

# Inspect run state and verify outputs
cargo run -p parqonaut-cli -- batch status --run-dir <run-dir>
cargo run -p parqonaut-cli -- batch verify --run-dir <run-dir>
```

See [docs/phase-4.md](docs/phase-4.md) and [docs/batch-execution.md](docs/batch-execution.md).

## Demos

```bash
just demo              # Phase 1: CSV → Parquet → scan → rewrite → rescan
just phase3-demo       # Phase 3: FRANKENLAKE schema reconciliation
just phase4-demo       # Phase 4: SHIPWRECK batch fleet
just phase4-resume-demo  # Phase 4: interrupt and resume
```

## Not yet implemented

- **Remote object storage** (`s3://` URIs) — Phase 5
- `parqonaut server` (HTTP service) and TUI
- Plugin execution bridge
- parqknife: partition, merge, split, and spec-file workflows (S3 I/O deferred to Phase 5 storage layer)
- maw: resumability, progress UI wiring, full schema unification in the stream pipeline
- Arrow/Parquet dependency convergence across engines
- In-memory cross-engine pipelines

See [CHANGELOG.md](CHANGELOG.md) for release history.

## Documentation

- [Architecture](docs/architecture.md)
- [Phase 3 — schema reconciliation](docs/phase-3.md)
- [Phase 4 — batch orchestration](docs/phase-4.md)
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
