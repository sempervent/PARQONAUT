use thiserror::Error;

/// Semantic client errors separating transport, auth, transient, and operation failures.
#[derive(Debug, Error)]
pub enum OperatorClientError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("transient error: {0}")]
    Transient(String),
    #[error("operation failed ({code}, status {status}): {message}")]
    Operation { status: u16, code: String, message: String, details: serde_json::Value },
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("invalid server response: {0}")]
    InvalidResponse(String),
}

impl OperatorClientError {
    pub fn category_label(&self) -> &'static str {
        match self {
            Self::Connection(_) => "connection_failure",
            Self::Unauthorized(_) => "authorization_required",
            Self::Forbidden(_) => "forbidden",
            Self::Transient(_) => "transient_error",
            Self::Operation { .. } => "operation_failure",
            Self::InvalidInput(_) => "invalid_input",
            Self::InvalidResponse(_) => "invalid_response",
        }
    }
}
