//! Transport-neutral application errors mapped at CLI/HTTP edges.

use parqonaut_core::CoreError;
use parqonaut_repair::RepairError;
use parqonaut_storage::error::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("location not allowed: {0}")]
    LocationNotAllowed(String),
    #[error("target not found: {0}")]
    TargetNotFound(String),
    #[error("scan failed: {0}")]
    ScanFailed(String),
    #[error("repair failed: {0}")]
    RepairFailed(String),
    #[error("batch failed: {0}")]
    BatchFailed(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("review authorization required")]
    ReviewRequired,
    #[error("blocked repair: {0}")]
    BlockedRepair(String),
    #[error("stale source: {0}")]
    StaleSource(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("plugin error: {0}")]
    PluginHost(String),
    #[error("plugin execution disabled on server")]
    PluginExecutionDisabled,
    #[error("plugin not allowed: {0}")]
    PluginNotAllowed(String),
    #[error("plugin not found: {0}")]
    PluginNotFound(String),
    #[error("plugin incompatible: {0}")]
    PluginIncompatible(String),
    #[error("stale plugin {name}: expected digest {expected}, current {actual}")]
    PluginStale { name: String, expected: String, actual: String },
    #[error("plugin execution cancelled")]
    PluginCancelled,
}

impl From<CoreError> for ApplicationError {
    fn from(e: CoreError) -> Self {
        match e {
            CoreError::MissingPath(p) => Self::TargetNotFound(p.to_string()),
            CoreError::Unsupported(m) => Self::InvalidRequest(m.to_string()),
            CoreError::PluginCancelled => Self::PluginCancelled,
            CoreError::PluginHost(m) => Self::PluginHost(m),
            other => Self::Internal(other.to_string()),
        }
    }
}

impl From<RepairError> for ApplicationError {
    fn from(e: RepairError) -> Self {
        match e {
            RepairError::ScanFailed(m) => Self::ScanFailed(m),
            RepairError::DatasetUnreadable(m) => Self::TargetNotFound(m),
            RepairError::DatasetChanged { expected, actual } => {
                Self::StaleSource(format!("expected {expected}, found {actual}"))
            }
            RepairError::ReviewRequiredNotAuthorized { .. } => Self::ReviewRequired,
            RepairError::DestructiveNotAllowed { operation_id } => {
                Self::BlockedRepair(format!("destructive operation `{operation_id}` is blocked"))
            }
            RepairError::Storage(m) => Self::InvalidRequest(m),
            other => Self::RepairFailed(other.to_string()),
        }
    }
}

impl From<StorageError> for ApplicationError {
    fn from(e: StorageError) -> Self {
        Self::InvalidRequest(e.to_string())
    }
}
