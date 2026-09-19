use thiserror::Error;

/// Whether an operation may be retried with backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryClass {
    NeverRetry,
    Retryable,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found: {location}")]
    NotFound { location: String },

    #[error("permission denied: {location}")]
    PermissionDenied { location: String },

    #[error("authentication failed")]
    Authentication,

    #[error("conflict: {message}")]
    Conflict { message: String },

    #[error("precondition failed: {message}")]
    PreconditionFailed { message: String },

    #[error("transient storage error: {message}")]
    Transient { message: String },

    #[error("storage unavailable: {message}")]
    Unavailable { message: String },

    #[error("invalid location: {message}")]
    InvalidLocation { message: String },

    #[error("unsupported capability: {capability}")]
    UnsupportedCapability { capability: String },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage error: {message}")]
    Other { message: String },
}

impl StorageError {
    pub fn retry_class(&self) -> RetryClass {
        match self {
            Self::Transient { .. } | Self::Unavailable { .. } => RetryClass::Retryable,
            Self::NotFound { .. }
            | Self::PermissionDenied { .. }
            | Self::Authentication
            | Self::Conflict { .. }
            | Self::PreconditionFailed { .. }
            | Self::InvalidLocation { .. }
            | Self::UnsupportedCapability { .. }
            | Self::Io(_)
            | Self::Other { .. } => RetryClass::NeverRetry,
        }
    }
}
