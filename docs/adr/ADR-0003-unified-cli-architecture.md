# ADR-0003: Unified CLI architecture

## Status

Accepted (2026-09-17)

## Context

Users need one tool (`prqnt`) for scan, transform, and stream operations inherited from three binaries (`paraclete`, `parqknife`, `maw`).

## Decision

1. Single binary: `prqnt` in `parqonaut-cli`.
2. Top-level subcommands map to engines:
   - `scan` → `paraclete_core::ScanEngine` (local, no HTTP required in Phase 1)
   - `inspect` / `rewrite` → `parqonaut_transform`
   - `convert` → `parqonaut_stream`
3. Do not expose stub commands (partition/merge/split/server) until implementations exist.
4. Paraclete HTTP client patterns remain available via `paraclete-service` for future `parqonaut server` / remote workflows.

## Consequences

- Local scan is newly exposed; Paraclete's HTTP-only CLI is not migrated verbatim.
- Command naming favors user-facing verbs over internal project names.
