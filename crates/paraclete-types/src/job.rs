//! Async scan job identity and lifecycle (orchestration around the engine).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::RunId;

/// Stable identifier for one queued or completed scan job.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(transparent)]
pub struct JobId(pub Uuid);

impl JobId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Job lifecycle state persisted in SQLite and returned over HTTP.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    #[serde(rename = "canceled")]
    Canceled,
}

/// Machine-readable job failure class (parallel to HTTP `AppError` codes where applicable).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobErrorCode {
    InvalidRequest,
    TargetNotFound,
    ScanFailed,
    StoreError,
    InternalError,
    /// Worker died or lease expired before completion; job was requeued or failed per policy.
    WorkerLost,
    /// Stale job could not be requeued because `attempt_count` reached the configured maximum.
    JobRetriesExhausted,
}

/// Reference from a completed job to the persisted run (if any).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct JobResultRef {
    pub job_id: JobId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    pub status: JobStatus,
}
