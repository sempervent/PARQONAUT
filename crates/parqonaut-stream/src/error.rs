use thiserror::Error;

#[derive(Error, Debug)]
pub enum MawError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CSV parsing error: {0}")]
    Csv(#[from] csv::Error),

    #[error("Parquet error: {0}")]
    Parquet(String),

    #[error("Arrow error: {0}")]
    Arrow(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("State error: {0}")]
    State(String),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Walkdir error: {0}")]
    Walkdir(#[from] walkdir::Error),

    #[error("Glob error: {0}")]
    Glob(#[from] globwalk::GlobError),

    #[error("Join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub type Result<T> = std::result::Result<T, MawError>;

impl From<arrow::error::ArrowError> for MawError {
    fn from(value: arrow::error::ArrowError) -> Self {
        MawError::Arrow(value.to_string())
    }
}

impl From<parquet::errors::ParquetError> for MawError {
    fn from(value: parquet::errors::ParquetError) -> Self {
        MawError::Parquet(value.to_string())
    }
}
