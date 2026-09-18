# ADR-0011: Batch orchestration boundary

## Status

Accepted

## Context

Phase 3 repairs one dataset with durable plans and manifests. Operators need the same guarantees across many datasets without re-implementing scheduling, overlap safety, and recovery in shell scripts.

## Decision

Introduce `parqonaut-orchestrator` as the multi-dataset boundary. It owns batch configuration, planning, scheduling, journaling, and resume. It calls `parqonaut-repair` **in-process** only. The CLI exposes `parqonaut batch …` subcommands; no second parser or subprocess orchestration layer.

## Alternatives considered

- Shell-only fleet scripts: rejected (no durable resume, weak overlap guarantees).
- Subprocess-per-dataset CLI repair: rejected (higher overhead, weaker plan binding).

## Consequences

- Orchestrator version upgrades must stay compatible with embedded repair plans or reject stale authorization explicitly.
- Single-dataset workflows remain on Phase 3 commands unchanged.
