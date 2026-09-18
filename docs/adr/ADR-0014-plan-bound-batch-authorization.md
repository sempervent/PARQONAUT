# ADR-0014: Plan-bound batch authorization

## Status

Accepted

## Context

Execution must not proceed under a stale or substituted policy relative to the approved batch plan (same problem class as Phase 3 single-dataset authorization).

## Decision

A durable `BatchPlan` embeds per-dataset `RepairPlan` values plus resolved `output_path` entries. Run identity stores `batch_plan_id`, `config_fingerprint`, `plan_digest`, and `output_root`. Resume and verify re-validate these fields and per-dataset repair plan id, policy fingerprint, and source fingerprint when recorded. Mismatch yields typed `PlanIdentityMismatch` errors.

Plan id derivation reuses Phase 3 stable hashing conventions (`stable_hex_id`, canonical JSON payloads).

## Alternatives considered

- Trust on-disk config.toml at resume time: rejected.
- Manual operator override flags: deferred (would weaken audit trail).

## Consequences

- Plans are execution authorization artifacts; treat them like Phase 3 repair plans.
- Mutating sources or policies after planning requires replanning.
