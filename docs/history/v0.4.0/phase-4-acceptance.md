# Phase 4 acceptance matrix

Verified against implementation on branch `feat/fleet-orchestration` after finalization audit (2026-09-18).

| Requirement | Status | Implementation evidence | Test evidence | Remaining issue |
|-------------|--------|-------------------------|---------------|-----------------|
| Phase 3 merged; v0.3.0 tagged | PASS | `17c1ff9` on `origin/main`, tag `v0.3.0` | N/A (precondition) | — |
| Dedicated Phase 4 branch | PASS | `feat/fleet-orchestration` | — | — |
| Safe parallel subagents (finalization audit) | PASS | `docs/phase-4-parallel-audit.md` | Read-only concurrent audits A–E | Implementation was lead-serialized; audits concurrent at finalization |
| One-dataset repair unchanged | PASS | `parqonaut-repair` untouched semantics | `just phase3-demo` | — |
| Versioned batch configuration | PASS | `config.rs` `schema_version = 1` | `batch_check_and_collision_rejection` | — |
| Durable batch plans | PASS | `plan.rs`, `BatchPlan::write_json` | `batch_plan_deterministic_serialization` | `plan_digest()` includes timestamps; use `batch_plan_id` for stability |
| Deterministic batch plans | PASS | `BatchPlanId::derive`, sorted dataset payload | `batch_plan_deterministic_serialization` | — |
| Durable execution journal | PASS | `journal/sqlite.rs`, migrations | `journal_migrations.rs` | — |
| Explicit validated state machine | PASS | `state.rs` `validate_transition` | state unit tests in `state.rs` | Journal upsert does not re-validate every write (P3) |
| Bounded concurrency (`--jobs`) | PASS | `scheduler/mod.rs`, `BatchRunOptions::validate` | `jobs_equivalence`, `scheduler.rs` | — |
| Concurrency never exceeds jobs | PASS | `Semaphore` + lazy scheduler | `executor_respects_jobs_bound_metric` | — |
| Failure isolation | PASS | per-dataset tasks, partial exit 2 | SHIPWRECK partial failures | Panicked task logged, batch continues (fixed) |
| Source fingerprint before execution | PASS | `executor/mod.rs` `verify_against` | `stale_source_rejected_at_execution` | — |
| Output destination isolation | PASS | `paths.rs` | collision test | — |
| Path overlap rejection | PASS | `overlap.rs`, `paths.rs` | `batch_check_and_collision_rejection` | Symlink/canonical edge cases documented as limitations |
| Dataset output locks | PASS | `lock.rs` | `concurrent_destination_lock_rejection` | Cross-run stale lock requires same `run_id` for takeover |
| Bounded recoverable retries | PASS | `executor/mod.rs` retry loop | Partial via interrupt/resume | Dedicated retry-exhaustion integration test deferred (P4) |
| Interruption leaves resumable state | PASS | `cancel.rs`, journal `cancelled_at` | `interrupt_resume_and_idempotent_second_resume` | — |
| `batch resume` | PASS | `run.rs` `resume_batch` | interrupt/resume integration | — |
| Successful datasets skipped on resume | PASS | executor skip `Succeeded` | interrupt/resume demo | — |
| Second resume no-op | PASS | `run_is_complete` | `interrupt_resume_and_idempotent_second_resume` | — |
| Aggregate status | PASS | `run.rs` `read_status` | `status_and_verify_from_persisted_journal` | — |
| Aggregate verify | PASS | `verify.rs` | `status_and_verify_from_persisted_journal` | — |
| Machine-readable report | PASS | `report.rs` → `report.json` | `just phase4-demo` | — |
| Plan-bound authorization | PASS | `auth.rs`, ADR-0014 | `stale_plan_rejected_on_resume` | Not cryptographic signing; structural digest binding |
| Destructive repairs blocked | PASS | Phase 3 repair engine | SHIPWRECK blocked-schema | — |
| `batch check` non-mutating | PASS | `check.rs` | `batch_check_and_collision_rejection` | — |
| Dry-run no repairs / no run dir | PASS | `execute_batch_plan` dry branch | `dry_run_has_no_persistent_side_effects` | Resume dry-run fixed to skip journal writes |
| SHIPWRECK fleet | PASS | `fixtures/phase4/shipwreck/` | integration + demos | — |
| Crash/interruption recovery tested | PASS | `--interrupt-after` | interrupt/resume tests | Real SIGINT test deferred (P4; uses cooperative injection) |
| jobs=1 vs jobs>1 semantic equivalence | PASS | lazy scheduler | `jobs_equivalence` | Compares terminal states, not byte-identical outputs |
| Scheduler instrumentation | PASS | `ConcurrencyMetrics` | scheduler/executor tests | — |
| Concurrency audit doc | PASS | `docs/phase-4-concurrency-audit.md` | — | — |
| Phase 1–3 CLI compatibility | PASS | existing commands in `main.rs` | `just phase3-demo` | — |
| No LLM/AI runtime | PASS | no new inference deps | — | — |
| `--jobs 0` rejected | PASS | `BatchRunOptions::validate` | `jobs_zero_rejected` | — |
| JSON automation output | PASS | `--json` on batch subcommands | various integration tests | Global error JSON envelope added; not all subcommands document exit codes yet (P3) |
| Exit codes partial failure | PASS | exit 2 on repair/resume partial | integration tests | Full exit-code matrix doc deferred (P3) |
| `batch plan --canonical` | DEFERRED WITH EXPLICIT APPROVAL | — | — | Not required for Phase 4 close; `batch_plan_id` provides stable identity |
| Orchestrator benchmark harness | DEFERRED WITH EXPLICIT APPROVAL | — | peak concurrency in reports | Spec §46 optional measurement |
| cfg(test) failure injection hooks | DEFERRED WITH EXPLICIT APPROVAL | `--interrupt-after` only | interrupt tests | Broader injection API deferred |
| README batch example | DEFERRED WITH EXPLICIT APPROVAL | — | `just phase4-demo` | Docs cover batch workflow |
