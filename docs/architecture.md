# PARQONAUT Architecture

PARQONAUT is a Rust-first toolkit for exploring, diagnosing, streaming, transforming, and repairing Parquet-oriented datasets.

## Three engines, one CLI

```text
                        ┌───────────────────┐
                        │     parqonaut     │
                        │        CLI        │
                        └─────────┬─────────┘
                                  │
                 ┌────────────────┼────────────────┐
                 │                │                │
                 ▼                ▼                ▼
          ┌────────────┐   ┌─────────────┐   ┌────────────┐
          │   SCAN     │   │  TRANSFORM  │   │   STREAM   │
          │ Paraclete  │   │ parqknife   │   │    maw     │
          │   core     │   │  lineage    │   │  lineage   │
          └────────────┘   └─────────────┘   └────────────┘
```

## Workspace layout

| Crate | Role |
|-------|------|
| `paraclete-types` | Durable scan/report contracts |
| `paraclete-core` | Local forensic scan engine |
| `paraclete-report` | Report rendering |
| `paraclete-store` | SQLite/Postgres persistence |
| `paraclete-service` | HTTP API + async jobs |
| `paraclete-plugin-protocol` | Plugin wire format |
| `parqonaut-transform` | Parquet rewrite/inspect (Arrow 54) |
| `parqonaut-stream` | Streaming CSV/Parquet (arrow2) |
| `parqonaut-cli` | Unified `parqonaut` binary |

Paraclete crate names are retained intentionally for provenance (see `docs/provenance.md`).

## Dependency boundaries

Three Arrow/Parquet stacks coexist in Phase 1:

- **Scan:** `parquet` 53 (Paraclete)
- **Transform:** `arrow`/`parquet` 54 (parqknife lineage)
- **Stream:** `arrow2`/`parquet2` (maw lineage)

Convergence is deferred; see ADR-0002.

## Phase 1 CLI

| Command | Engine |
|---------|--------|
| `parqonaut scan <path>` | `paraclete-core` |
| `parqonaut inspect <file>` | `parqonaut-transform` |
| `parqonaut rewrite <in> <out>` | `parqonaut-transform` |
| `parqonaut convert <inputs...> -o <out>` | `parqonaut-stream` |

HTTP service (`paraclete-service`) builds with the workspace but is not yet exposed as `parqonaut server` in Phase 1.
