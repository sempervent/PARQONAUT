# Phase 5 parallel agent plan

Baseline: **lead-owned** on `feat/object-storage` from `v0.4.1` / `main` @ `480b031`.

Mission: first-class S3-compatible object storage with the same deterministic planning, authorization, repair, verification, failure-isolation, and recovery guarantees as local filesystem operation.

See [phase-5-storage-recon.md](phase-5-storage-recon.md) for existing code assessment.
See [storage-architecture.md](storage-architecture.md) for frozen interface contracts.

## Contract freeze

| Item | Value |
|------|-------|
| `PHASE5_CONTRACT_SHA` | `ebd305e9360eb196ddcc3e0da2bdf6eda51944a9` |
| Contract commit message | `feat: define phase 5 storage contracts` |
| Crate | `crates/parqonaut-storage/` |
| Contract version | `STORAGE_CONTRACT_VERSION = 1` |

### Lead-frozen public types (do not modify without lead approval)

| Type | File |
|------|------|
| `DatasetLocation`, `ObjectLocation`, `LocalLocation`, `S3Location` | `location.rs` |
| `ObjectMetadata` | `metadata.rs` |
| `StorageBackend`, `ByteRange`, `ListOptions`, `ListPage` | `backend.rs` |
| `StorageCapabilities` | `capabilities.rs` |
| `StorageError`, `RetryClass` | `error.rs` |
| `ConditionalCreate`, `ConditionalReplace` | `conditional.rs` |
| `ObjectReadStream`, `ObjectWriteStream` | `stream.rs` |
| `StorageMetrics`, `StorageMetricsCollector` | `metrics.rs` |
| `RedactUri`, `Redacted` | `redact.rs` |
| `storage_backend_contract` | `contract.rs` |
| `lib.rs` exports | `lib.rs` |

### Test-only / contract helpers (lead-owned)

| Type | File |
|------|------|
| `MemoryStorageBackend` | `memory.rs` |
| `NoopStorageBackend` | `noop.rs` |

## Agent ownership

### LEAD (integration)

| Field | Value |
|-------|-------|
| Branch | `feat/object-storage` |
| Starting SHA | `PHASE5_CONTRACT_SHA` (for integration work) |
| Owns | `lib.rs`, all contract files above, workspace `Cargo.toml`, cross-cutting integration, release |
| Forbidden | Subagents must not edit lead-owned contract files |
| Expected commits | integration commits, stub removal, release |

### SUBAGENT A — local backend

| Field | Value |
|-------|-------|
| Worktree | `.worktrees/phase5-local` |
| Branch | `phase5-storage-local` |
| Starting SHA | `PHASE5_CONTRACT_SHA` |
| Owns | `crates/parqonaut-storage/src/local.rs`, local backend tests |
| Read-only | all contract modules, `memory.rs`, `noop.rs` |
| Forbidden | `s3.rs`, `s3/`, `publication.rs`, `locking.rs`, repair, orchestrator, CLI |
| Expected commit | `feat: implement local storage backend` |
| Tests | `cargo test -p parqonaut-storage local`, contract suite against `LocalStorageBackend` |
| Integration order | 1 (first wave) |

### SUBAGENT B — S3 backend + MinIO primitives

| Field | Value |
|-------|-------|
| Worktree | `.worktrees/phase5-s3` |
| Branch | `phase5-storage-s3` |
| Starting SHA | `PHASE5_CONTRACT_SHA` |
| Owns | `crates/parqonaut-storage/src/s3.rs`, `crates/parqonaut-storage/src/s3/` |
| Read-only | all contract modules |
| Forbidden | `local.rs`, `publication.rs`, `locking.rs`, repair, orchestrator |
| Expected commit | `feat: implement s3 storage backend` |
| Tests | S3 unit tests, contract suite against MinIO |
| Integration order | 1 (first wave, parallel with A) |

### SUBAGENT C — inventory + fingerprints + Parquet range reads

| Field | Value |
|-------|-------|
| Worktree | `.worktrees/phase5-fingerprint` |
| Branch | `phase5-fingerprint-scan` |
| Starting SHA | `PHASE5_CONTRACT_SHA` |
| Owns | `inventory.rs`, `fingerprint.rs`, `parquet_range.rs` |
| Read-only | `StorageBackend` trait, contract modules |
| Forbidden | `local.rs`, `s3.rs`, AWS SDK, repair, orchestrator |
| Expected commit | `feat: add remote inventory fingerprints and parquet range reads` |
| Integration order | 1 (first wave, after contract; uses `MemoryStorageBackend` until A/B land) |

### SUBAGENT D — publication + locks

| Field | Value |
|-------|-------|
| Worktree | `.worktrees/phase5-publication` |
| Branch | `phase5-publication` |
| Starting SHA | `PHASE5_CONTRACT_SHA` |
| Owns | `publication.rs`, `locking.rs`, publication tests |
| Read-only | contract modules, `StorageBackend` |
| Forbidden | `local.rs`, `s3.rs`, AWS SDK, repair, orchestrator |
| Expected commit | `feat: add transactional object-store publication` |
| Integration order | 1 (first wave) |

### SUBAGENT E — FOGBANK / MinIO laboratory

| Field | Value |
|-------|-------|
| Worktree | `.worktrees/phase5-fogbank` |
| Branch | `phase5-fogbank` |
| Starting SHA | `PHASE5_CONTRACT_SHA` |
| Owns | `docker-compose.phase5.yml`, `fixtures/phase5/`, `scripts/phase5/`, integration test harness, Justfile snippets (propose if conflict) |
| Read-only | production storage implementation |
| Forbidden | modifying `local.rs`, `s3.rs` internals |
| Expected commit | `test: add fogbank object-storage laboratory` |
| Integration order | 1 (first wave; fixtures early, full tests after A/B) |

### SUBAGENT F — scan/diagnosis integration (second wave)

| Field | Value |
|-------|-------|
| Worktree | `.worktrees/phase5-scan` |
| Branch | `phase5-scan-integration` |
| Starting SHA | post first-wave integration SHA |
| Owns | storage-aware scan/plan/check/doctor adapters |
| Expected commit | `feat: make scanning and planning storage aware` |
| Integration order | 2 |

### SUBAGENT G — repair integration (second wave)

| Field | Value |
|-------|-------|
| Worktree | `.worktrees/phase5-repair` |
| Branch | `phase5-repair-integration` |
| Starting SHA | post first-wave integration SHA |
| Owns | remote/cross-backend repair adapters in `parqonaut-repair` |
| Expected commit | `feat: add remote and cross-backend repairs` |
| Integration order | 2 |

### SUBAGENT H — orchestrator/resume integration (second wave)

| Field | Value |
|-------|-------|
| Worktree | `.worktrees/phase5-orchestrator` |
| Branch | `phase5-orchestrator-integration` |
| Starting SHA | post first-wave integration SHA |
| Owns | `parqonaut-orchestrator` Phase 5 extensions |
| Expected commit | `feat: orchestrate remote and mixed-storage datasets` |
| Integration order | 2 |

### SUBAGENT I — remote chaos/fault tests (second wave)

| Field | Value |
|-------|-------|
| Worktree | `.worktrees/phase5-chaos` |
| Branch | `phase5-chaos-tests` |
| Starting SHA | post second-wave API freeze |
| Owns | test-only failure injection infrastructure |
| Expected commit | `test: add remote storage fault injection coverage` |
| Integration order | 2 (late) |

### SUBAGENT G-docs — documentation (final wave)

| Field | Value |
|-------|-------|
| Owns | `docs/phase-5.md`, `docs/s3.md`, `docs/remote-publication.md`, `docs/remote-recovery.md`, `docs/phase-5-security.md`, `docs/phase-5-acceptance.md`, ADRs 0015–0019, README, CHANGELOG |
| Integration order | 3 (after behavior stabilizes) |

## Parallelization graph

```text
LEAD contract freeze (PHASE5_CONTRACT_SHA)
    ├──► A local backend
    ├──► B S3 backend
    ├──► C inventory/fingerprint/parquet_range
    ├──► D publication/locking
    └──► E FOGBANK fixtures
First-wave integration + contract tests (local + MinIO)
    ├──► F scan/diagnosis
    ├──► G repair
    ├──► H orchestrator
    └──► I chaos tests
Final audits (5+ read-only) → docs → release v0.5.0
```

## Subagent deliverable format

Each agent returns:

```text
commit SHA
files changed
tests run
assumptions
known limitations
```

Lead reviews every diff before merge. No concurrent edits to lead-owned contract files.
Interface change requests must be returned as a proposed note; lead applies centrally.

## Acceptance reference

See Phase 5 specification acceptance criteria. Tracked in `docs/phase-5-acceptance.md`.
