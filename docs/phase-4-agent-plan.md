# Phase 4 parallel agent plan

Baseline contract commit: **lead-owned** on `feat/fleet-orchestration` (see git log after `feat: define batch orchestration contracts`).

Terminology: **batch** = multi-dataset orchestration abstraction (ADR-0011).

## Shared contracts (frozen by lead)

| Type | Location | Notes |
|------|----------|-------|
| `BatchConfig` | `orchestrator/config.rs` | TOML `schema_version = 1` |
| `BatchPlan` | `orchestrator/plan.rs` | embeds `RepairPlan` per dataset |
| `DatasetState` | `orchestrator/state.rs` | validated transitions |
| `RunJournal` trait | `orchestrator/journal/mod.rs` | SQLite impl behind trait |
| `BatchRunId`, `DatasetId` | `orchestrator/ids.rs` | newtypes |
| `OrchestratorError` | `orchestrator/error.rs` | typed failure classes |

Library boundary: `parqonaut-orchestrator` calls `parqonaut-repair` in-process only.

## Agent ownership

### LEAD (integration)

- Branch: `feat/fleet-orchestration`
- Owns: merges, `Cargo.toml` workspace, `README.md`, contract changes, `docs/phase-4-agent-plan.md`
- Integration order: contracts → journal → batch plan → scheduler → CLI → fixtures/tests → docs

### SUBAGENT A — journal + state machine

- Branch/worktree: `phase4-journal` (from contract commit)
- Owns:
  - `crates/parqonaut-orchestrator/src/journal/**`
  - `crates/parqonaut-orchestrator/src/state.rs`
  - `crates/parqonaut-orchestrator/migrations/**`
  - journal/state unit tests
- Must NOT edit: scheduler, CLI, batch plan canonicalization
- Output: durable SQLite journal, migrations, state transition validation
- Depends on: frozen `DatasetState`, `RunJournal` trait

### SUBAGENT B — scheduler + execution

- Branch/worktree: `phase4-scheduler`
- Owns:
  - `crates/parqonaut-orchestrator/src/scheduler/**`
  - `crates/parqonaut-orchestrator/src/executor/**`
  - `crates/parqonaut-orchestrator/src/lock.rs`
  - `crates/parqonaut-orchestrator/src/overlap.rs`
- Must NOT edit: journal schema without lead approval
- Depends on: SUBAGENT A interfaces merged
- Output: bounded `--jobs`, failure isolation, retry hooks, cancellation

### SUBAGENT C — batch contracts + canonicalization

- Branch/worktree: `phase4-batch-plan`
- Owns:
  - `crates/parqonaut-orchestrator/src/config.rs` (extend parsing)
  - `crates/parqonaut-orchestrator/src/plan.rs` (generation)
  - `crates/parqonaut-orchestrator/src/plan_canonical.rs`
  - `fixtures/batch/**` golden JSON
  - contract tests in `crates/parqonaut-orchestrator/tests/golden_batch_plans.rs`
- Must NOT modify `parqonaut-repair` semantics
- Depends on: contract commit only
- Output: `batch plan`, determinism tests

### SUBAGENT D — CLI

- Branch/worktree: `phase4-cli`
- Owns:
  - `crates/parqonaut-cli/src/batch.rs`
  - `crates/parqonaut-cli/src/main.rs` (batch subcommands only)
- Must NOT embed orchestration logic (call library)
- Depends on: scheduler + journal APIs merged

### SUBAGENT E — SHIPWRECK fixtures + integration tests

- Branch/worktree: `phase4-shipwreck`
- Owns:
  - `fixtures/phase4/**`
  - `crates/parqonaut-orchestrator/src/bin/generate_phase4_fixtures.rs`
  - `crates/parqonaut-cli/tests/phase4_integration.rs`
  - failure injection (`cfg(test)` only)
- Depends on: documented public API (may stub until merge)

### SUBAGENT F — documentation (after behavior stabilizes)

- Owns: `docs/phase-4.md`, `docs/batch-execution.md`, `docs/resume-recovery.md`, `docs/concurrency-audit.md`, ADR-0011–0014
- Starts after scheduler + resume semantics land

## Parallelization graph

```text
LEAD contracts ──┬──► A journal (parallel with C)
                 └──► C batch plan (parallel with A)
A + C merged ──► B scheduler
B merged ──► D CLI
A+B+C ──► E integration tests (partial), then full after D
F docs last
```

## Serialized intentionally

- Scheduler (B) after journal (A): execution reads/writes run state
- CLI (D) after executor API stable
- Docs (F) after demos pass
