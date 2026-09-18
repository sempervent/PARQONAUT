mod sqlite;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::FailureClass;
use crate::ids::{BatchPlanId, DatasetId, RunId};
use crate::state::DatasetState;

pub use sqlite::SqliteRunJournal;

pub const JOURNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetRunRecord {
    pub dataset_id: DatasetId,
    pub state: DatasetState,
    pub attempts: u32,
    pub source_fingerprint: Option<String>,
    pub output_fingerprint: Option<String>,
    pub error_class: Option<FailureClass>,
    pub error_message: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunHeader {
    pub run_id: RunId,
    pub batch_plan_id: BatchPlanId,
    pub batch_name: String,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Durable batch execution journal (SQLite-backed).
pub trait RunJournal: Send + Sync {
    fn run_header(&self) -> Result<Option<RunHeader>, crate::error::OrchestratorError>;
    fn list_datasets(&self) -> Result<Vec<DatasetRunRecord>, crate::error::OrchestratorError>;
    fn dataset(&self, id: &DatasetId) -> Result<Option<DatasetRunRecord>, crate::error::OrchestratorError>;
    fn upsert_dataset(&self, record: &DatasetRunRecord) -> Result<(), crate::error::OrchestratorError>;
    fn mark_run_completed(&self, at: DateTime<Utc>) -> Result<(), crate::error::OrchestratorError>;
}
