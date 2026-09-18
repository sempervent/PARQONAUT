# PARQONAUT Migration Analysis

Migration source HEAD commits (2026-09-17):

| Repository | Remote | HEAD SHA |
|------------|--------|----------|
| Paraclete | `sempervent/paraclete` | `4dd0316f76811370bdd38f7b3778beed7b71a1c6` |
| parqknife | `sempervent/parqknife` | `a179c9a8d5ddd0c2cbc055e631135470d63a20a7` |
| streaming-parquet (maw) | `sempervent/streaming-parquet` | `4de2dbe2f97bfe5a43cf53c94f05eb11e1bfa4ad` |

## Feature Matrix

| Capability | Paraclete | parqknife | maw | PARQONAUT destination |
|------------|-----------|-----------|-----|------------------------|
| **Forensic scan / inspect** | `ScanEngine::run` local pipeline; parquet/CSV/JSON shallow probes; rules; findings; validated reports | `inspect` command (schema, row groups, metadata, optional stats) | Parquet input reader (arrow2); no diagnostic rules | `parqonaut scan` via `paraclete-core`; `parqonaut inspect` via `parqonaut-transform` |
| **Evidence-backed findings** | Phase 1–3 rules, finding codes, report validation | None | None | `paraclete-types` + `paraclete-core` + `paraclete-report` |
| **SQLite / Postgres persistence** | Full store, migrations, auth tokens | None | State file/resume (partial) | `paraclete-store` (HTTP service path deferred in CLI) |
| **HTTP service + async jobs** | axum API, worker lease/recovery, OpenAPI | None | None | `paraclete-service` (`parqonaut server` subcommand) |
| **Scan history / diffs** | Run diff, paginated assets/findings | None | None | `paraclete-store` + `paraclete-cli` client patterns |
| **Plugin protocol** | Rust protocol + Python Pydantic contracts | None | None | `paraclete-plugin-protocol` + `python/paraclete_plugins` |
| **Parquet rewrite / recompression** | None | `rewrite` with projection, filter, compression, row-group size | None | `parqonaut transform rewrite` → `parqonaut-transform` |
| **Row-group resizing** | Detects suspicious row groups (findings) | `--row-group-size-mb` on rewrite | Config on writer (partial) | Transform engine |
| **Projection / filtering** | None | Pipeline transforms + nom filter DSL | Column whitelist/blacklist (CLI only, partial) | Transform engine |
| **Schema changes** | Dataset inference, schema split findings | `SchemaTransform` (cast stubbed) | Unified schema + type widening (partial) | Transform + stream (separate stacks) |
| **Partition / merge / split** | Hive layout detection | Stubs (warn only) | Rolling output (CLI only) | Deferred; stubs not exposed in Phase 1 CLI |
| **S3 I/O** | Not in core | Placeholder adapters | None | Deferred |
| **Transform spec files** | None | YAML/JSON spec types (unwired) | None | Deferred |
| **Streaming CSV input** | Shallow CSV probe only | None | `CsvReader` chunked batches | `parqonaut-stream` |
| **Streaming Parquet input** | Metadata inspect | Batch read via arrow 54 | `ParquetReader` via arrow2 | Both transform + stream engines |
| **CSV ↔ Parquet conversion** | None | None | Pipeline with bounded channel; CSV→CSV tested; CSV→Parquet needs writer completion | `parqonaut convert` → `parqonaut-stream` |
| **Schema unification / type widening** | Multi-dataset inference | None | `UnifiedSchema`, coercion module | Stream engine (Phase 1: minimal) |
| **Bounded-memory processing** | Bounded text probes | Batch streaming in rewrite | mpsc channel + batch readers | Stream engine |
| **Parallel readers** | Sequential local scan | `concurrency` flag (unused) | One task per file | Stream engine |
| **Resumability / state** | Job recovery in service | None | `StateManager` (unwired to pipeline) | Deferred |
| **Progress reporting** | Service metrics | indicatif (unused) | `ProgressTracker` (unwired) | Deferred |
| **Plan / dry-run** | Scan plan in types | `--dry-run` (unused) | `--plan`, `--dry-run` | Stream engine; scan via core tests |
| **Benchmarks** | None in repo | criterion declared, no benches | Placeholder throughput bench | `benches/` deferred |
| **Deterministic ordering** | Sorted asset paths, stable report IDs in tests | Sorted glob inputs | Reader order by discovery | Preserved in each engine |
| **CLI** | `paraclete` HTTP client | `parqknife` subcommands | `maw` positional args | `parqonaut` unified hierarchy |
| **TUI** | `paraclete-tui` over HTTP | None | None | Not migrated in Phase 1 |
| **CI** | Documented manual commands only | None | justfile only | `.github/workflows/ci.yml` |
| **Documentation / ADRs** | Extensive docs + ADRs | spec, recipes, PROGRESS | README | `docs/adr/`, README, provenance |

## Dependency Ecosystem Split

| Component | Arrow / Parquet stack | Notes |
|-----------|----------------------|-------|
| Paraclete (`paraclete-core`, etc.) | Apache `parquet` **53** | No direct `arrow` dependency; transitive via parquet |
| parqknife (`parqonaut-transform`) | Apache `arrow` + `parquet` **54** | Separate engine crate boundary |
| maw (`parqonaut-stream`) | `arrow2` **0.18** + `parquet2` **0.17** | Retained internally; not forced onto arrow 53/54 |

**Phase 1 decision:** three stacks coexist behind crate boundaries. Convergence deferred until measured (see ADR-0002).

## Git History Approach

Structured import with documented source SHAs (this file + `docs/provenance.md`). Original repositories remain untouched upstreams. No subtree merge into PARQONAUT history in Phase 1 — provenance is commit-SHA-based rather than mathematically complete ancestry.

## Phase 1 Wiring (minimum viable)

| Engine | CLI command | Underlying code |
|--------|-------------|-----------------|
| Scan | `parqonaut scan <path>` | `paraclete_core::ScanEngine::run` |
| Transform | `parqonaut rewrite <in> <out> --compression zstd` | `parqonaut_transform` rewrite path |
| Stream | `parqonaut convert <in.csv> -o <out.parquet>` | `parqonaut_stream` CSV→Parquet pipeline |

## Clippy exceptions (Phase 1)

- `parqonaut-transform`: `#![allow(dead_code, unused_imports, unused_variables)]` — inherited parqknife scaffolding (S3/spec/partition stubs) not yet wired.
- `parqonaut-stream` `pipeline` tests: targeted `needless_borrows_for_generic_args` allow on clap parse helpers.
- `paraclete-service` `auth_middleware`: `allow(clippy::result_large_err)` — axum `Result<Response, Response>` shape from Paraclete.

Blanket `#![allow(clippy::all)]` was removed for v0.1.0; trivial warnings were fixed in place.

## Known Gaps After Import

- parqknife shipped with Arrow 54 API mismatches; fixed during import.
- maw Parquet writer was a stub; minimal write path implemented for Phase 1.
- Paraclete CLI remains HTTP-only; local scan exposed via new `parqonaut scan`.
- Partition/merge/split, S3, spec files, TUI, full service workflow: deferred.
