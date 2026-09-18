# ADR-0005: Repair safety classification

## Status

Accepted (Phase 2)

## Context

Datasets must not be "fixed" blindly. Operators need explicit boundaries.

## Decision

Introduce `RepairSafety`:

| Class | Phase 2 behavior |
|-------|------------------|
| **Safe** | May appear in plans and auto-execute via `doctor --repair` / `repair` |
| **ReviewRequired** | May appear in plans; skipped unless future explicit authorization |
| **Destructive** | May be diagnosed; never auto-executed in Phase 2 |

Examples:

- **Safe:** recompress, merge compatible small files, resize row groups.
- **ReviewRequired:** schema alignment, casts, renames, repartition.
- **Destructive:** drop columns, filter rows, delete corrupt files.

## Consequences

- Executors filter on `RepairSafety::Safe` by default.
- ReviewRequired operations remain visible in human plan output.
