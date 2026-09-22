use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransformError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Columnar error: {0}")]
    Columnar(#[from] parqonaut_columnar::ColumnarError),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Filter expression error: {0}")]
    FilterError(String),

    #[error("Spec parsing error: {0}")]
    SpecError(String),

    #[error("S3 error: {0}")]
    S3Error(String),

    #[error("Unsupported operation: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, TransformError>;
