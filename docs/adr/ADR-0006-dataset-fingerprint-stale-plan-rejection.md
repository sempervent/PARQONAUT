# ADR-0006: Dataset fingerprint and stale-plan rejection

## Status

Accepted (Phase 2)

## Context

Repair plans must not run against changed datasets. Full column hashing is impractical at scale.

## Decision

Bind each plan to a **dataset fingerprint** (version 1):

- Sorted list of Parquet members with relative path, size, row count, row-group count, schema signature, compression labels.
- SHA-256 digest over canonical JSON of that list.

**Metadata only** — no column value hashing.

Before execution, recompute fingerprint; mismatch → fail with `dataset changed since plan generation`.
No casual `--force` bypass in Phase 2.

## Threat model

Detects: added/removed files, size changes, footer/metadata edits.
Does not detect: in-place row tampering with unchanged footer (out of scope for Phase 2).

## Consequences

- Operators must regenerate plans after intentional source changes.
- Fingerprint is stored in plan JSON for audit.
