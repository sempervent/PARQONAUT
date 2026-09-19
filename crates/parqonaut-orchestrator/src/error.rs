use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Recoverable,
    Permanent,
    Blocked,
    StaleSource,
    VerificationFailed,
    Cancelled,
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error("batch config invalid: {0}")]
    InvalidConfig(String),

    #[error("unsupported batch config schema version {found} (supported {supported})")]
    UnsupportedConfigVersion { found: u32, supported: u32 },

    #[error("unsupported batch plan schema version {found} (supported {supported})")]
    UnsupportedPlanVersion { found: u32, supported: u32 },

    #[error("duplicate dataset id: {0}")]
    DuplicateDatasetId(String),

    #[error("path overlap detected: {0}")]
    PathOverlap(String),

    #[error("dataset lock held: {0}")]
    LockHeld(String),

    #[error("journal error: {0}")]
    Journal(String),

    #[error("repair error: {0}")]
    Repair(#[from] parqonaut_repair::RepairError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("batch plan identity mismatch: {field} expected `{expected}` found `{found}`")]
    PlanIdentityMismatch { field: String, expected: String, found: String },

    #[error("run already completed; resume is a no-op")]
    RunAlreadyCompleted,

    #[error("batch run cancelled")]
    Cancelled,

    #[error("state transition error: {0}")]
    State(#[from] crate::state::StateTransitionError),

    #[error("storage error: {0}")]
    Storage(String),
}
