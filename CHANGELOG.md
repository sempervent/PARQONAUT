# Changelog

All notable changes to PARQONAUT are documented here.

## [0.1.0] - 2026-09-18

Initial unified release consolidating Paraclete, parqknife, and streaming-parquet (maw).

### Added

- Cargo workspace with Paraclete-derived scan chassis (`paraclete-*` crates)
- `parqonaut-transform` — parqknife-derived inspect/rewrite/filter pipeline (Apache Arrow 54)
- `parqonaut-stream` — maw-derived streaming CSV/Parquet conversion (arrow2)
- Unified `parqonaut` CLI with `scan`, `inspect`, `rewrite`, and `convert` commands
- End-to-end demo: CSV → Parquet → scan → rewrite → rescan (`just demo`)
- Integration tests covering all three engine families
- Fixtures, Python plugin contracts, and migration documentation
- GitHub Actions CI (fmt, clippy, test)
- Provenance and architecture ADRs

### Not yet implemented

- HTTP service CLI (`parqonaut server`) and TUI
- Plugin execution bridge
- parqknife partition, merge, split, S3 I/O, and spec-file workflows
- maw resumability, progress wiring, and full schema unification in the stream pipeline
- Arrow/Parquet dependency stack convergence across engines
- In-memory cross-engine pipelines without file handoff

[0.1.0]: https://github.com/sempervent/parqonaut/releases/tag/v0.1.0
