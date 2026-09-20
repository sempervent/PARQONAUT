# Columnar convergence reconnaissance (v0.9.0)

Audit date: 2026-09-20. Baseline: `main` at v0.8.1 (`9904898`).

## Dependency snapshot (pre-migration)

| Crate | Arrow | Parquet | arrow2 | parquet2 | Notes |
| --- | --- | --- | --- | --- | --- |
| `paraclete-core` | — | **53** (workspace) | — | — | Scan/read metadata |
| `parqonaut-repair` | 54 | 54 | — | — | Rewrite via transform |
| `parqonaut-transform` | 54 | 54 | — | — | RecordBatch engine |
| `parqonaut-stream` | — | — | **0.18** | **0.17** | Chunk pipeline |
| `parqonaut-storage` | optional (fixtures) | optional | — | — | Not on hot path |
| `parqonaut-app` | transitive | transitive | — | — | Orchestrates repair/batch |
| `parqonaut-orchestrator` | — | — | — | — | No direct columnar |
| `parqonaut-workflow` | — | — | — | — | Transport-neutral contracts |

## Canonical target

- **CANONICAL_ARROW_VERSION:** 54.3.1
- **CANONICAL_PARQUET_VERSION:** 54.3.1
- Shared boundary: `crates/parqonaut-columnar` (`BatchStream`, `BatchSource`, `BatchSink`)

## Migration difficulty

| Area | Difficulty | Test surface |
| --- | --- | --- |
| Stream readers/writers | High | `parqonaut-stream` unit + e2e + resume + schema vectors |
| Schema unification | Medium | `fixtures/schema/compatibility-vectors.json` |
| Transform fusion | Medium | spec execute + integration tests |
| Scan parquet 53→54 | Low–medium | `paraclete-core` scan tests |
| Remote S3 matrix | High | FOGBANK + new pipeline fixtures |

## Performance / I/O

v0.8 transform spec may materialize intermediates; v0.9 must prove zero intermediate I/O for fully streamable graphs (see acceptance criteria).
