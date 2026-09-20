use thiserror::Error;

#[derive(Debug, Error)]
pub enum ColumnarError {
    #[error("schema error: {0}")]
    Schema(String),
    #[error("batch validation failed: {0}")]
    Validation(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("arrow error: {0}")]
    Arrow(String),
    #[error("pipeline cancelled")]
    Cancelled,
    #[error("{0}")]
    Other(String),
}
