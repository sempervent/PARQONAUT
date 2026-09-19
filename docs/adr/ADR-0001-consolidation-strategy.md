# ADR-0001: PARQONAUT consolidation strategy

## Status

Accepted (2026-09-17)

## Context

Three repositories (Paraclete, parqknife, streaming-parquet/maw) must become one product without a ground-up rewrite.

## Decision

1. Use **Paraclete's workspace** as the architectural chassis (types, scan engine, store, service, plugins).
2. Import **parqknife** as `parqonaut-transform` and **maw** as `parqonaut-stream` behind crate boundaries.
3. Expose a unified **`prqnt` CLI** that routes to real engine code — no placeholder commands.
4. Preserve **provenance** via documented source SHAs; do not mutate upstream repositories.
5. Use **structured copy** for Git import rather than blocking on perfect history merge.

## Consequences

- Paraclete crate names remain visible during staged migration.
- Phase 1 focuses on build + three working capability paths, not full feature parity.
