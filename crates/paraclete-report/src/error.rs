use thiserror::Error;

/// Errors emitted by report helpers.
#[derive(Debug, Error)]
pub enum ReportError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Validation(#[from] paraclete_types::ValidationError),
}
