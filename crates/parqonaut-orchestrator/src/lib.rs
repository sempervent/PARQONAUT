//! Deterministic multi-dataset batch orchestration.
//!
//! `parqonaut-repair` repairs one dataset; this crate coordinates many independent
//! repair jobs with bounded concurrency, durable journal state, and resumability.

#![forbid(unsafe_code)]

mod auth;
mod cancel;
mod check;
mod config;
mod error;
mod executor;
mod ids;
mod journal;
mod lock;
mod overlap;
mod paths;
mod plan;
mod report;
mod run;
mod scheduler;
mod state;
mod verify;

pub use auth::{
    assert_dataset_record_matches_plan, assert_datasets_match_plan, assert_plan_matches_identity,
};
pub use cancel::CancelFlag;
pub use check::{batch_check, ensure_output_root, BatchCheckReport};
pub use config::{BatchConfig, DatasetConfig};
pub use error::{FailureClass, OrchestratorError};
pub use executor::{
    BatchExecutor, BatchExecutorConfig, BatchRunOutcome, DatasetExecutionOutcome, DryRunReport,
    ExecutionMode, DEFAULT_MAX_RETRIES,
};
pub use ids::{BatchPlanId, DatasetId, RunId};
pub use journal::{
    DatasetRunRecord, RunIdentity, RunJournal, SqliteRunJournal, JOURNAL_SCHEMA_VERSION,
};
pub use lock::DatasetLock;
pub use overlap::{validate_batch_overlap, validate_batch_plan, BatchPathSpec};
pub use paths::{mappings_to_map, resolve_output_mappings, OutputMapping};
pub use plan::{build_batch_plan, BatchPlan, BatchSummary, DatasetPlan};
pub use report::{
    build_aggregate_report, BatchAggregateReport, BatchVerificationSummary, DatasetReportEntry,
    StateCounts, BATCH_REPORT_SCHEMA_VERSION,
};
pub use run::{
    check_config, execute_batch, execute_batch_plan, plan_batch, read_status, resume_batch,
    verify_run, write_plan, BatchExecutionResult, BatchRunOptions, BatchStatusReport, JOURNAL_FILE,
    PLAN_SNAPSHOT, REPORT_FILE, RUNS_DIR,
};
pub use scheduler::{BatchScheduler, ConcurrencyMetrics};
pub use state::{DatasetState, StateTransitionError};
pub use verify::{verify_batch, BatchVerifyReport, DatasetVerifyResult};

pub const BATCH_CONFIG_SCHEMA_VERSION: u32 = 1;
pub const BATCH_PLAN_SCHEMA_VERSION: u32 = 1;
