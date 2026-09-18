# ADR-0008: Explicit authorization for review-required repairs

## Status

Accepted

## Context

Phase 2 refused all `ReviewRequired` operations. Phase 3 must allow human-authorized schema repairs with an auditable trail.

## Decision

Review-required operations execute only when the caller supplies `--authorize <operation_id>` (repeatable). Safe operations record `authorization_source = automatic-safe-policy`. Authorized review operations record `authorization_source = explicit-user`. No `--yes`, `--force`, or `--unsafe` flags authorize review-required work.

## Consequences

- CI and automation must enumerate operation IDs explicitly.
- Execution manifests record authorization metadata per operation.
