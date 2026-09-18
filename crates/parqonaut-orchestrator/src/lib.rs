//! Deterministic multi-dataset batch orchestration.
//!
//! `parqonaut-repair` repairs one dataset; this crate coordinates many independent
//! repair jobs with bounded concurrency, durable journal state, and resumability.

#![forbid(unsafe_code)]

mod config;
mod error;
mod executor;
mod ids;
mod journal;
mod lock;
mod overlap;
mod plan;
mod scheduler;
mod state;

pub use config::{BatchConfig, DatasetConfig};
pub use error::{FailureClass, OrchestratorError};
pub use executor::{
    BatchExecutor, BatchExecutorConfig, BatchRunOutcome, DatasetExecutionOutcome,
    DEFAULT_MAX_RETRIES,
};
pub use ids::{BatchPlanId, DatasetId, RunId};
pub use journal::{DatasetRunRecord, RunHeader, RunJournal, SqliteRunJournal, JOURNAL_SCHEMA_VERSION};
pub use lock::DatasetLock;
pub use overlap::{validate_batch_overlap, validate_batch_plan, BatchPathSpec};
pub use plan::{build_batch_plan, BatchPlan, BatchSummary, DatasetPlan};
pub use scheduler::{BatchScheduler, ConcurrencyMetrics};
pub use state::{DatasetState, StateTransitionError};

pub const BATCH_CONFIG_SCHEMA_VERSION: u32 = 1;
pub const BATCH_PLAN_SCHEMA_VERSION: u32 = 1;
