use thiserror::Error;

#[derive(Debug, Error)]
pub enum RepairError {
    #[error("input dataset is unreadable: {0}")]
    DatasetUnreadable(String),

    #[error(
        "dataset changed since plan generation (expected fingerprint {expected}, found {actual})"
    )]
    DatasetChanged { expected: String, actual: String },

    #[error("unsupported repair operation: {operation_id} ({action})")]
    UnsupportedOperation { operation_id: String, action: String },

    #[error("review-required repair not authorized: {operation_id}")]
    ReviewRequiredNotAuthorized { operation_id: String },

    #[error("destructive repair cannot be executed automatically: {operation_id}")]
    DestructiveNotAllowed { operation_id: String },

    #[error("output path already exists: {0}")]
    OutputExists(String),

    #[error("source and destination overlap: {0}")]
    SourceDestinationOverlap(String),

    #[error("precondition failed for operation {operation_id}: {detail}")]
    PreconditionFailed { operation_id: String, detail: String },

    #[error("verification invariant failed: {invariant} — {detail}")]
    VerificationInvariantFailed { invariant: String, detail: String },

    #[error("repair partially executed; staging retained at {staging}")]
    PartialExecution { staging: String },

    #[error("plan file invalid: {0}")]
    InvalidPlan(String),

    #[error("scan failed: {0}")]
    ScanFailed(String),

    #[error("transform failed: {0}")]
    TransformFailed(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}
