//! Stable JSON API errors (`error.code`, `error.message`, `error.details`).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use paraclete_core::CoreError;
use paraclete_store::StoreError;
use paraclete_types::ValidationError;
use serde::Serialize;
#[allow(unused_imports)]
use serde_json::json;

/// Wire-level error codes (stable strings).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidRequest,
    InvalidJsonRequest,
    RunNotFound,
    JobNotFound,
    TokenNotFound,
    TargetNotFound,
    ScanFailed,
    StoreError,
    InternalError,
    Unauthorized,
    Forbidden,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ErrorEnvelope {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default)]
    pub details: serde_json::Value,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "error": {
        "code": "run_not_found",
        "message": "run not found",
        "details": {}
    }
}))]
pub struct ErrorBody {
    pub error: ErrorEnvelope,
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    InvalidRequest(String),
    #[error("{message}")]
    InvalidJsonRequest { status: StatusCode, message: String },
    #[error("run not found")]
    RunNotFound,
    #[error("job not found")]
    JobNotFound,
    #[error("token not found")]
    TokenNotFound,
    #[error("target not found")]
    TargetNotFound(String),
    #[error("scan failed: {0}")]
    ScanFailed(String),
    #[error("store error: {0}")]
    Store(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("{0}")]
    Unauthorized(String),
    #[error("{0}")]
    Forbidden(String),
}

impl AppError {
    pub fn code(&self) -> ErrorCode {
        match self {
            AppError::InvalidRequest(_) => ErrorCode::InvalidRequest,
            AppError::InvalidJsonRequest { .. } => ErrorCode::InvalidJsonRequest,
            AppError::RunNotFound => ErrorCode::RunNotFound,
            AppError::JobNotFound => ErrorCode::JobNotFound,
            AppError::TokenNotFound => ErrorCode::TokenNotFound,
            AppError::TargetNotFound(_) => ErrorCode::TargetNotFound,
            AppError::ScanFailed(_) => ErrorCode::ScanFailed,
            AppError::Store(_) => ErrorCode::StoreError,
            AppError::Internal(_) => ErrorCode::InternalError,
            AppError::Unauthorized(_) => ErrorCode::Unauthorized,
            AppError::Forbidden(_) => ErrorCode::Forbidden,
        }
    }

    pub fn status(&self) -> StatusCode {
        match self {
            AppError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            AppError::InvalidJsonRequest { status, .. } => *status,
            AppError::RunNotFound => StatusCode::NOT_FOUND,
            AppError::JobNotFound => StatusCode::NOT_FOUND,
            AppError::TokenNotFound => StatusCode::NOT_FOUND,
            AppError::TargetNotFound(_) => StatusCode::NOT_FOUND,
            AppError::ScanFailed(_) => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            AppError::Forbidden(_) => StatusCode::FORBIDDEN,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = ErrorBody {
            error: ErrorEnvelope {
                code: self.code(),
                message: self.to_string(),
                details: serde_json::Value::Object(Default::default()),
            },
        };
        (status, Json(body)).into_response()
    }
}

impl From<StoreError> for AppError {
    fn from(e: StoreError) -> Self {
        let kind = store_error_kind(&e);
        metrics::counter!("paraclete_store_errors_total", "kind" => kind).increment(1);
        match e {
            StoreError::RunNotFound(_) => AppError::RunNotFound,
            StoreError::JobNotFound(_) => AppError::JobNotFound,
            StoreError::AuthTokenNotFound(_) => AppError::TokenNotFound,
            other => AppError::Store(other.to_string()),
        }
    }
}

fn store_error_kind(e: &StoreError) -> &'static str {
    match e {
        StoreError::Migrate(_) => "migrate",
        StoreError::Sql(_) => "sql",
        StoreError::Json(_) => "json",
        StoreError::ReportValidation(_) => "report_validation",
        StoreError::RunNotFound(_) => "run_not_found",
        StoreError::JobNotFound(_) => "job_not_found",
        StoreError::AuthToken(_) => "auth_token",
        StoreError::AuthTokenNotFound(_) => "auth_token_not_found",
    }
}

impl From<CoreError> for AppError {
    fn from(e: CoreError) -> Self {
        match &e {
            CoreError::MissingPath(_) => AppError::TargetNotFound(e.to_string()),
            CoreError::Unsupported(_) => AppError::InvalidRequest(e.to_string()),
            CoreError::NonUtf8Path(_) => AppError::InvalidRequest(e.to_string()),
            CoreError::ReportValidation(msg) => AppError::InvalidRequest(msg.clone()),
            _ => AppError::ScanFailed(e.to_string()),
        }
    }
}

impl From<ValidationError> for AppError {
    fn from(e: ValidationError) -> Self {
        AppError::InvalidRequest(e.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        AppError::InvalidRequest(e.to_string())
    }
}
