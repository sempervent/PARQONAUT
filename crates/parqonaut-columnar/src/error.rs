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
    #[error("parquet error: {0}")]
    Parquet(String),
    #[error("{0}")]
    Other(String),
}

impl From<parquet::errors::ParquetError> for ColumnarError {
    fn from(value: parquet::errors::ParquetError) -> Self {
        Self::Parquet(value.to_string())
    }
}

impl From<arrow::error::ArrowError> for ColumnarError {
    fn from(value: arrow::error::ArrowError) -> Self {
        Self::Arrow(value.to_string())
    }
}
