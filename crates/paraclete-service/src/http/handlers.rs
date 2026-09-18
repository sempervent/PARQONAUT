//! Axum handlers — call [`crate::service::ParacleteService`] only.

use axum::body::Body;
use axum::extract::{Extension, Path, Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Response, StatusCode};
use axum::Json;
use paraclete_types::{AuthPrincipal, AuthTokenId, JobId, RunId};
use uuid::Uuid;

use crate::api_types::{
    AuthTokenCreateRequest, AuthTokenCreateResponse, AuthTokenListResponse,
    AuthTokenRotateResponse, AuthTokenSummaryView, DiffQuery, HealthResponse, JobListQuery,
    PageQuery, PagedAssetsResponse, PagedFindingsResponse, StartScanRequest, TargetRunsQuery,
    WhoAmIResponse,
};
use crate::error::AppError;
use crate::http::extract::ApiJson;
use crate::http::AppState;
use crate::observability::audit;

const KNOWN_TARGET_KINDS: &[&str] =
    &["local_file", "local_directory", "logical_dataset", "object_store_placeholder"];

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

/// Prometheus text exposition (unauthenticated; scrape locally or protect at the edge).
pub async fn prometheus_metrics(State(state): State<AppState>) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(state.prometheus.render()))
        .expect("metrics response")
}

/// Returns the authenticated principal (requires a valid bearer token).
pub async fn whoami(Extension(principal): Extension<AuthPrincipal>) -> Json<WhoAmIResponse> {
    Json(WhoAmIResponse {
        token_id: principal.token_id.0,
        label: principal.label,
        role: principal.role,
    })
}

/// Async scan: queue job and return 202 (same handler as `POST /api/v1/jobs/scans`).
pub async fn post_async_scan(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<StartScanRequest>,
) -> Result<(StatusCode, Json<crate::api_types::ScanJobSubmissionResponse>), AppError> {
    let out = state.service.submit_scan_job(body).await?;
    Ok((StatusCode::ACCEPTED, Json(out)))
}

/// Synchronous scan for dev/tests (`POST /api/v1/scans/sync`).
pub async fn post_scan_sync(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<StartScanRequest>,
) -> Result<(StatusCode, Json<crate::api_types::StartScanResponse>), AppError> {
    let out = state.service.start_scan_and_persist(body).await?;
    Ok((StatusCode::CREATED, Json(out)))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> Result<Json<crate::api_types::ScanJobView>, AppError> {
    let v = state.service.get_scan_job_view(JobId(job_id)).await?;
    Ok(Json(v))
}

pub async fn list_jobs(
    State(state): State<AppState>,
    Query(q): Query<JobListQuery>,
) -> Result<Json<crate::api_types::ScanJobListResponse>, AppError> {
    Ok(Json(state.service.list_scan_jobs(&q).await?))
}

pub async fn get_run(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
) -> Result<Json<crate::api_types::RunSummaryView>, AppError> {
    let v = state.service.get_run_summary_view(RunId(run_id)).await?;
    Ok(Json(v))
}

pub async fn get_report(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
) -> Result<Json<paraclete_types::ScanReport>, AppError> {
    Ok(Json(state.service.get_run_report(RunId(run_id)).await?))
}

pub async fn get_assets(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
    Query(q): Query<PageQuery>,
) -> Result<Json<PagedAssetsResponse>, AppError> {
    let (items, total) = state.service.list_assets_page(RunId(run_id), &q).await?;
    Ok(Json(PagedAssetsResponse {
        items,
        total,
        limit: q.limit.clamp(1, crate::api_types::MAX_PAGE_SIZE),
        offset: q.offset,
    }))
}

pub async fn get_findings(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
    Query(q): Query<PageQuery>,
) -> Result<Json<PagedFindingsResponse>, AppError> {
    let (items, total) = state.service.list_findings_page(RunId(run_id), &q).await?;
    Ok(Json(PagedFindingsResponse {
        items,
        total,
        limit: q.limit.clamp(1, crate::api_types::MAX_PAGE_SIZE),
        offset: q.offset,
    }))
}

pub async fn list_target_runs(
    State(state): State<AppState>,
    Path(target_kind): Path<String>,
    Query(q): Query<TargetRunsQuery>,
) -> Result<Json<Vec<paraclete_types::ScanRunListItem>>, AppError> {
    if !KNOWN_TARGET_KINDS.contains(&target_kind.as_str()) {
        return Err(AppError::TargetNotFound(format!("unknown target_kind `{target_kind}`")));
    }
    let key = q.normalized_key.trim();
    if key.is_empty() {
        return Err(AppError::InvalidRequest("normalized_key query parameter is required".into()));
    }
    let identity = paraclete_types::TargetIdentity { target_kind, normalized_key: key.to_string() };
    Ok(Json(state.service.list_runs_for_target(&identity, q.limit.clamp(1, 500)).await?))
}

pub async fn get_diff(
    State(state): State<AppState>,
    Query(q): Query<DiffQuery>,
) -> Result<Json<paraclete_types::RunDiff>, AppError> {
    let d = state.service.diff_runs(RunId(q.left_run_id), RunId(q.right_run_id)).await?;
    Ok(Json(d))
}

pub async fn post_admin_tokens(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<AuthTokenCreateRequest>,
) -> Result<(StatusCode, Json<AuthTokenCreateResponse>), AppError> {
    let out = state.service.admin_create_token(body).await?;
    Ok((StatusCode::CREATED, Json(out)))
}

pub async fn get_admin_tokens(
    State(state): State<AppState>,
) -> Result<Json<AuthTokenListResponse>, AppError> {
    Ok(Json(state.service.admin_list_tokens().await?))
}

pub async fn get_admin_token(
    State(state): State<AppState>,
    Path(token_id): Path<Uuid>,
) -> Result<Json<AuthTokenSummaryView>, AppError> {
    Ok(Json(state.service.admin_get_token(AuthTokenId(token_id)).await?))
}

pub async fn post_admin_token_disable(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthPrincipal>,
    Path(token_id): Path<Uuid>,
) -> Result<Json<AuthTokenSummaryView>, AppError> {
    let out = state.service.admin_disable_token(AuthTokenId(token_id)).await?;
    audit::token_disabled(token_id, actor.token_id.0);
    Ok(Json(out))
}

/// Rotate a token (admin): new secret once; previous token disabled in the same transaction.
pub async fn post_admin_token_rotate(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthPrincipal>,
    Path(token_id): Path<Uuid>,
) -> Result<(StatusCode, Json<AuthTokenRotateResponse>), AppError> {
    let out = state.service.admin_rotate_token(AuthTokenId(token_id)).await?;
    audit::token_rotated(token_id, out.token_id, actor.token_id.0);
    Ok((StatusCode::CREATED, Json(out)))
}
