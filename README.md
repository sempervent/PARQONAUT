# PARQONAUT

**Parquet Analysis, Rewriting, Quality, Orchestration, Navigation, Auditing, Unification & Transformation**

PARQONAUT is a Rust-first toolkit for exploring, diagnosing, streaming, transforming, and repairing Parquet-oriented datasets.

It consolidates [Paraclete](https://github.com/sempervent/paraclete), [parqknife](https://github.com/sempervent/parqknife), and [streaming-parquet (maw)](https://github.com/sempervent/streaming-parquet) into one workspace. See [docs/provenance.md](docs/provenance.md) for migration sources.

## What works today

| Command | Engine | Description |
|---------|--------|-------------|
| `parqonaut scan <path>` | Paraclete | Forensic scan of a file or directory (assets, datasets, findings) |
| `parqonaut inspect <file>` | parqknife | Parquet schema and row-group metadata |
| `parqonaut rewrite <in> <out> [--compression zstd]` | parqknife | Rewrite Parquet with optional recompression |
| `parqonaut convert <inputs...> -o <out>` | maw | Stream CSV → Parquet (or concatenate CSV) |
| `parqonaut doctor <path>` | repair | Scan → diagnose → plan (optional `--repair`) |
| `parqonaut plan <path> [--policy policy.toml]` | repair | Generate durable repair plan JSON |
| `parqonaut repair <path> --plan plan.json --output out/` | repair | Execute plan; `--authorize <op_id>` for ReviewRequired |
| `parqonaut verify before/ after/ [--manifest manifest.json]` | repair | Verify invariants and optional manifest |
| `parqonaut check <path> [--policy ci-policy.toml]` | repair | CI gate (non-mutating; exit codes 0/2/3/4/5) |
| `parqonaut plan diff plan-a.json plan-b.json` | repair | Compare plans for review/CI |

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

## End-to-end demo

Runs the full Phase 1 pipeline without extending scope:

```text
CSV → convert → Parquet → scan → rewrite → rescan
```

```bash
just demo
```

This creates a temporary CSV, converts it to Parquet, scans it, rewrites with zstd compression, and rescans with JSON output.

### Phase 3 schema reconciliation demo

```bash
just phase3-demo
```

Runs the FRANKENLAKE v2 laboratory: diagnose → plan → partial repair → authorized schema repair → verify → CI check. See [docs/phase-3.md](docs/phase-3.md).

## Not yet implemented

These are intentionally **not** available in v0.1.0:

- `parqonaut server` (HTTP service) and TUI
- Plugin execution bridge
- parqknife: partition, merge, split, S3 I/O, spec-file workflows
- maw: resumability, progress UI wiring, full schema unification in the stream pipeline
- Arrow/Parquet dependency convergence across engines
- In-memory cross-engine pipelines

See [CHANGELOG.md](CHANGELOG.md) for the v0.1.0 release notes.

## Documentation

- [Architecture](docs/architecture.md)
- [Phase 3 — schema reconciliation](docs/phase-3.md)
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
