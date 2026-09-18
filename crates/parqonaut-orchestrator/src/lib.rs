//! Deterministic multi-dataset batch orchestration.
//!
//! `parqonaut-repair` repairs one dataset; this crate coordinates many independent
//! repair jobs with bounded concurrency, durable journal state, and resumability.

#![forbid(unsafe_code)]

mod config;
mod error;
mod ids;
mod journal;
mod plan;
mod state;

pub use config::{BatchConfig, DatasetConfig};
pub use error::{FailureClass, OrchestratorError};
pub use ids::{BatchPlanId, DatasetId, RunId};
pub use journal::{RunJournal, SqliteRunJournal, JOURNAL_SCHEMA_VERSION};
pub use plan::{build_batch_plan, BatchPlan, BatchSummary, DatasetPlan};
pub use state::{DatasetState, StateTransitionError};

pub const BATCH_CONFIG_SCHEMA_VERSION: u32 = 1;
pub const BATCH_PLAN_SCHEMA_VERSION: u32 = 1;
