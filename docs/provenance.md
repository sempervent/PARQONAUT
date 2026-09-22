# PARQONAUT Provenance

PARQONAUT consolidates three predecessor projects owned by `sempervent`. The original repositories are **not** archived or modified by this migration.

## Migration Date

2026-09-17 (baseline tagged **v0.1.0** on 2026-09-18)

## Originating Repositories

| Project | URL | Original Purpose | License (declared) |
|---------|-----|------------------|-------------------|
| **Paraclete** | https://github.com/sempervent/paraclete | Forensic dataset scanning, evidence-backed findings, SQLite persistence, HTTP scan service, plugin protocol | MIT |
| **parqknife** | https://github.com/sempervent/parqknife | Parquet transformation toolkit (inspect, rewrite, filter, pipeline) | `MIT OR Apache-2.0` in `Cargo.toml` |
| **streaming-parquet (maw)** | https://github.com/sempervent/streaming-parquet | Bounded-memory streaming CSV/Parquet concat and conversion | MIT |

## Source Commit SHAs

| Source | HEAD at import |
|--------|----------------|
| paraclete | `4dd0316f76811370bdd38f7b3778beed7b71a1c6` |
| parqknife | `a179c9a8d5ddd0c2cbc055e631135470d63a20a7` |
| streaming-parquet | `4de2dbe2f97bfe5a43cf53c94f05eb11e1bfa4ad` |

## Imported Components

### From Paraclete (architectural chassis)

| Source path | PARQONAUT destination |
|-------------|----------------------|
| `crates/paraclete-types/` | `crates/parqonaut-types/` |
| `crates/paraclete-core/` | `crates/parqonaut-core/` |
| `crates/paraclete-report/` | `crates/parqonaut-report/` |
| `crates/paraclete-plugin-protocol/` | `crates/parqonaut-plugin-protocol/` |
| `crates/paraclete-store/` | `crates/parqonaut-store/` |
| `crates/paraclete-service/` | `crates/parqonaut-service/` |
| `python/paraclete_plugins/` | `python/parqonaut_plugins/` |
| `fixtures/` | `fixtures/` |
| `scripts/` | `scripts/` |

Not imported in Phase 1: `paraclete-cli` (HTTP client superseded by unified CLI), `paraclete-tui`, mkdocs site artifacts.

### From parqknife (transformation engine)

| Source path | PARQONAUT destination |
|-------------|----------------------|
| `src/` (library modules) | `crates/parqonaut-transform/src/` |
| `tests/integration/` | `crates/parqonaut-transform/tests/` |

Arrow 54 API fixes applied during import. The upstream `LICENSE` file contained AGPL v3 text and was **not** copied; see licensing section below.

### From streaming-parquet / maw (streaming engine)

| Source path | PARQONAUT destination |
|-------------|----------------------|
| `src/` | `crates/parqonaut-stream/src/` |
| `tests/` | `crates/parqonaut-stream/tests/` |
| `benches/` | `benches/stream-throughput/` |

Refactored from binary-only to library + CLI wrapper. Parquet writer completed for Phase 1.

## Licensing in this repository

| Artifact | Purpose |
|----------|---------|
| [`LICENSE`](../LICENSE) | MIT — default license for the combined PARQONAUT work |
| [`LICENSE-APACHE-2.0`](../LICENSE-APACHE-2.0) | Apache-2.0 text for `parqonaut-transform` dual-license option |
| [`NOTICE`](../NOTICE) | Copyright and attribution summary |
| [`THIRD_PARTY_LICENSES.md`](../THIRD_PARTY_LICENSES.md) | Factual record of predecessor licenses and import scope |

### parqknife licensing note

At import time, parqknife's `Cargo.toml` declared `MIT OR Apache-2.0`, but its repository `LICENSE` file contained GNU Affero GPL v3 text. PARQONAUT:

1. Retains `license = "MIT OR Apache-2.0"` on `crates/parqonaut-transform/Cargo.toml` (matching upstream crate metadata).
2. Does **not** include the upstream AGPL license file.
3. Provides [`LICENSE-APACHE-2.0`](../LICENSE-APACHE-2.0) for recipients who elect the Apache-2.0 option.

This documents what was observed upstream; it is not a legal determination of applicable terms. Consult [`THIRD_PARTY_LICENSES.md`](../THIRD_PARTY_LICENSES.md) for full detail.

## Git History

Import method: structured copy with SHA documentation (not a history-preserving merge). Useful history remains in the upstream repositories listed above.
