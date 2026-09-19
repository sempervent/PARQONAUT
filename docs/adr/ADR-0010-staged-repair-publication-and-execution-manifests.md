# ADR-0010: Staged repair publication and execution manifests

## Status

Accepted

## Context

Partial repair failures must not masquerade as valid datasets. Operators need machine-readable audit receipts.

## Decision

Repairs write to `.parqonaut-staging-<execution-id>/` under the output parent, run verification, then promote to the final destination. Each successful repair emits `.parqonaut-manifest.json` with fingerprints, policy fingerprint, operation audit records, and optional verification report. `prqnt verify --manifest` checks output fingerprint against the manifest.

## Consequences

- Cross-filesystem rename atomicity is not claimed.
- Failed executions retain staging directories for inspection.
