use thiserror::Error;

/// Store-layer failures (SQLite, serialization, validation).
#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("report failed validation: {0}")]
    ReportValidation(String),
    #[error("run not found: {0}")]
    RunNotFound(uuid::Uuid),
    #[error("scan job not found: {0}")]
    JobNotFound(uuid::Uuid),
    #[error("auth token: {0}")]
    AuthToken(String),
    #[error("auth token not found: {0}")]
    AuthTokenNotFound(uuid::Uuid),
}
