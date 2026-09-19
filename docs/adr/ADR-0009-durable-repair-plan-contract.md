# ADR-0009: Durable repair-plan contract

## Status

Accepted

## Context

Repair plans must survive serialization, replay, CI diffing, and version upgrades.

## Decision

Plans use `schema_version: 1` with deterministic `plan_id`, embedded `policy_fingerprint`, and explicit `depends_on` edges. Canonical JSON strips volatile timestamps and UUIDs. Golden fixtures and contract tests guard the public shape. Unsupported `schema_version` values are rejected at load time.

## Consequences

- Breaking plan changes require incrementing `schema_version`.
- `prqnt plan diff` and `--canonical` support review workflows.
