//! Axum handlers — call [`crate::service::ParqonautService`] only.

use std::convert::Infallible;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Extension, Path, Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::Json;
use futures_util::stream::Stream;
use parqonaut_types::{AuthPrincipal, AuthTokenId, JobId, JobStatus, RunId};
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;

use crate::api_types::{
    AuthTokenCreateRequest, AuthTokenCreateResponse, AuthTokenListResponse,
    AuthTokenRotateResponse, AuthTokenSummaryView, DiffQuery, HealthResponse, JobEventListQuery,
    JobEventListResponse, JobListQuery, OverviewResponse, PageQuery, PagedAssetsResponse,
    PagedFindingsResponse, PluginCatalogEntryView, PluginCatalogListResponse, RunListQuery,
    RunListResponse, ScanJobSubmissionResponse, StartScanRequest, TargetRunsQuery, WhoAmIResponse,
};
use crate::application_http::{
    BatchCheckResponse, BatchConfigBody, BatchPlanResponse, BatchRepairJobBody, BatchResumeBody,
    BatchStatusResponse, BatchVerifyResponse, CheckResponse, DiagnoseResponse, JobCancelResponse,
    LocationPolicyBody, PlanBody, PlanResponse, RepairJobBody, VerifyBody, VerifyResponse,
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

pub async fn health_live() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

pub async fn health_ready(State(state): State<AppState>) -> Result<Json<HealthResponse>, AppError> {
    state.service.store().ping().await?;
    metrics::gauge!("parqonaut_workers_active").set(1.0);
    Ok(Json(HealthResponse { status: "ready" }))
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
pub async fn list_plugins(
    State(state): State<AppState>,
) -> Result<Json<PluginCatalogListResponse>, AppError> {
    Ok(Json(state.service.list_plugin_catalog()))
}

pub async fn get_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<PluginCatalogEntryView>, AppError> {
    Ok(Json(state.service.get_plugin_catalog_entry(&name)?))
}

fn extract_idempotency_key(headers: &HeaderMap) -> Result<Option<String>, AppError> {
    let Some(val) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let s = val
        .to_str()
        .map_err(|_| AppError::InvalidRequest("Idempotency-Key header must be valid ASCII".into()))?
        .trim();
    if s.is_empty() || s.len() > 128 || !s.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(AppError::InvalidRequest(
            "Idempotency-Key must be 1..=128 printable ASCII characters".into(),
        ));
    }
    Ok(Some(s.to_string()))
}

pub async fn post_async_scan(
    State(state): State<AppState>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<StartScanRequest>,
) -> Result<(StatusCode, Json<crate::api_types::ScanJobSubmissionResponse>), AppError> {
    let key = extract_idempotency_key(&headers)?;
    let out = state.service.submit_scan_job_with_idempotency(body, key.as_deref()).await?;
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

pub async fn get_overview(
    State(state): State<AppState>,
) -> Result<Json<OverviewResponse>, AppError> {
    Ok(Json(state.service.get_overview().await?))
}

pub async fn list_runs(
    State(state): State<AppState>,
    Query(q): Query<RunListQuery>,
) -> Result<Json<RunListResponse>, AppError> {
    Ok(Json(state.service.list_runs_page(&q).await?))
}

pub async fn get_job_events(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    Query(q): Query<JobEventListQuery>,
) -> Result<Json<JobEventListResponse>, AppError> {
    Ok(Json(state.service.list_job_events(JobId(job_id), &q).await?))
}

#[derive(serde::Deserialize)]
pub struct StreamEventsQuery {
    #[serde(default)]
    pub after_seq: i64,
}

pub async fn stream_job_events(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    headers: HeaderMap,
    Query(q): Query<StreamEventsQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, AppError> {
    let jid = JobId(job_id);
    let initial_view = state.service.get_scan_job_view(jid).await?;
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let initial_cursor = q.after_seq.max(last_event_id).max(0);
    let mut rx = state.service.events().subscribe();
    let (sse_tx, mut sse_rx) = tokio::sync::mpsc::channel::<Event>(32);
    let svc_bg = state.service.clone();
    tokio::spawn(async move {
        let mut cursor = initial_cursor;
        // 1. Replay persisted history strictly > cursor.
        loop {
            match svc_bg.store().list_job_events(jid, cursor, 200, None, None).await {
                Ok(batch) if batch.is_empty() => break,
                Ok(batch) => {
                    let count = batch.len();
                    for ev in batch {
                        if ev.seq <= cursor {
                            continue;
                        }
                        cursor = ev.seq;
                        if let Ok(frame) = Event::default()
                            .id(ev.seq.to_string())
                            .event("job_event")
                            .json_data(&ev)
                        {
                            if sse_tx.send(frame).await.is_err() {
                                return;
                            }
                        }
                    }
                    if count < 200 {
                        break;
                    }
                }
                Err(_) => return,
            }
        }

        let current_status =
            svc_bg.get_scan_job_view(jid).await.map(|v| v.status).unwrap_or(initial_view.status);
        if is_terminal_job_status(current_status) {
            let status_ev = Event::default().event("job_status").data(
                serde_json::json!({
                    "job_id": jid.0,
                    "status": current_status,
                    "last_seq": cursor,
                })
                .to_string(),
            );
            let _ = sse_tx.send(status_ev).await;
            return;
        }

        // 2. Tail live broadcast events with lag recovery and periodic terminal check.
        loop {
            match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Ok(ev)) => {
                    if ev.job_id != jid || ev.seq <= cursor {
                        continue;
                    }
                    cursor = ev.seq;
                    let terminal = ev.is_terminal;
                    if let Ok(frame) =
                        Event::default().id(ev.seq.to_string()).event("job_event").json_data(&ev)
                    {
                        if sse_tx.send(frame).await.is_err() {
                            return;
                        }
                    }
                    if terminal {
                        let status = svc_bg
                            .get_scan_job_view(jid)
                            .await
                            .map(|v| v.status)
                            .unwrap_or(JobStatus::Succeeded);
                        let status_ev = Event::default().event("job_status").data(
                            serde_json::json!({
                                "job_id": jid.0,
                                "status": status,
                                "last_seq": cursor,
                            })
                            .to_string(),
                        );
                        let _ = sse_tx.send(status_ev).await;
                        return;
                    }
                }
                Ok(Err(RecvError::Lagged(_))) | Err(_) => {
                    while let Ok(batch) =
                        svc_bg.store().list_job_events(jid, cursor, 200, None, None).await
                    {
                        if batch.is_empty() {
                            break;
                        }
                        for ev in batch {
                            if ev.seq <= cursor {
                                continue;
                            }
                            cursor = ev.seq;
                            if let Ok(frame) = Event::default()
                                .id(ev.seq.to_string())
                                .event("job_event")
                                .json_data(&ev)
                            {
                                if sse_tx.send(frame).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    if let Ok(view) = svc_bg.get_scan_job_view(jid).await {
                        if is_terminal_job_status(view.status) {
                            let status_ev = Event::default().event("job_status").data(
                                serde_json::json!({
                                    "job_id": jid.0,
                                    "status": view.status,
                                    "last_seq": cursor,
                                })
                                .to_string(),
                            );
                            let _ = sse_tx.send(status_ev).await;
                            return;
                        }
                    }
                }
                Ok(Err(RecvError::Closed)) => return,
            }
        }
    });

    let out_stream = futures_util::stream::poll_fn(move |cx| {
        sse_rx.poll_recv(cx).map(|opt| opt.map(Ok::<Event, Infallible>))
    });
    Ok(Sse::new(out_stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

fn is_terminal_job_status(s: JobStatus) -> bool {
    matches!(s, JobStatus::Succeeded | JobStatus::Failed | JobStatus::Canceled)
}

pub async fn get_job_batch(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> Result<Json<BatchStatusResponse>, AppError> {
    Ok(Json(state.service.get_job_batch_status(JobId(job_id)).await?))
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
) -> Result<Json<parqonaut_types::ScanReport>, AppError> {
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
) -> Result<Json<Vec<parqonaut_types::ScanRunListItem>>, AppError> {
    if !KNOWN_TARGET_KINDS.contains(&target_kind.as_str()) {
        return Err(AppError::TargetNotFound(format!("unknown target_kind `{target_kind}`")));
    }
    let key = q.normalized_key.trim();
    if key.is_empty() {
        return Err(AppError::InvalidRequest("normalized_key query parameter is required".into()));
    }
    let identity = parqonaut_types::TargetIdentity { target_kind, normalized_key: key.to_string() };
    Ok(Json(state.service.list_runs_for_target(&identity, q.limit.clamp(1, 500)).await?))
}

pub async fn get_diff(
    State(state): State<AppState>,
    Query(q): Query<DiffQuery>,
) -> Result<Json<parqonaut_types::RunDiff>, AppError> {
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
pub async fn post_diagnose(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<LocationPolicyBody>,
) -> Result<Json<DiagnoseResponse>, AppError> {
    Ok(Json(state.service.diagnose_sync(body).await?))
}

pub async fn post_plan(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<PlanBody>,
) -> Result<Json<PlanResponse>, AppError> {
    Ok(Json(state.service.plan_sync(body).await?))
}

pub async fn post_check(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<LocationPolicyBody>,
) -> Result<Json<CheckResponse>, AppError> {
    Ok(Json(state.service.check_sync(body).await?))
}

pub async fn post_verify(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<VerifyBody>,
) -> Result<Json<VerifyResponse>, AppError> {
    Ok(Json(state.service.verify_sync(body).await?))
}

pub async fn post_repair_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<RepairJobBody>,
) -> Result<(StatusCode, Json<ScanJobSubmissionResponse>), AppError> {
    let key = extract_idempotency_key(&headers)?;
    let out = state.service.submit_repair_job_with_idempotency(body, key.as_deref()).await?;
    Ok((StatusCode::ACCEPTED, Json(out)))
}

pub async fn post_batch_check(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<BatchConfigBody>,
) -> Result<Json<BatchCheckResponse>, AppError> {
    Ok(Json(state.service.batch_check_sync(body).await?))
}

pub async fn post_batch_plan(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<BatchConfigBody>,
) -> Result<Json<BatchPlanResponse>, AppError> {
    Ok(Json(state.service.batch_plan_sync(body).await?))
}

pub async fn post_batch_repair_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<BatchRepairJobBody>,
) -> Result<(StatusCode, Json<ScanJobSubmissionResponse>), AppError> {
    let key = extract_idempotency_key(&headers)?;
    let out = state.service.submit_batch_repair_job_with_idempotency(body, key.as_deref()).await?;
    Ok((StatusCode::ACCEPTED, Json(out)))
}

fn batch_run_dir(segment: &str) -> Result<camino::Utf8PathBuf, AppError> {
    let decoded = urlencoding::decode(segment)
        .map_err(|_| AppError::InvalidRequest("invalid batch run_dir encoding".into()))?;
    Ok(camino::Utf8PathBuf::from(decoded.as_ref()))
}

pub async fn get_batch_status(
    State(state): State<AppState>,
    Path(run_dir): Path<String>,
) -> Result<Json<BatchStatusResponse>, AppError> {
    Ok(Json(state.service.batch_status_sync(batch_run_dir(&run_dir)?).await?))
}

pub async fn post_batch_resume(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_dir): Path<String>,
    ApiJson(body): ApiJson<BatchResumeBody>,
) -> Result<(StatusCode, Json<ScanJobSubmissionResponse>), AppError> {
    let key = extract_idempotency_key(&headers)?;
    let out = state
        .service
        .submit_batch_resume_job_with_idempotency(batch_run_dir(&run_dir)?, body, key.as_deref())
        .await?;
    Ok((StatusCode::ACCEPTED, Json(out)))
}

pub async fn post_batch_verify(
    State(state): State<AppState>,
    Path(run_dir): Path<String>,
) -> Result<Json<BatchVerifyResponse>, AppError> {
    Ok(Json(state.service.batch_verify_sync(batch_run_dir(&run_dir)?).await?))
}

pub async fn post_job_cancel(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> Result<Json<JobCancelResponse>, AppError> {
    Ok(Json(state.service.cancel_job(JobId(job_id)).await?))
}

pub async fn post_admin_token_rotate(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthPrincipal>,
    Path(token_id): Path<Uuid>,
) -> Result<(StatusCode, Json<AuthTokenRotateResponse>), AppError> {
    let out = state.service.admin_rotate_token(AuthTokenId(token_id)).await?;
    audit::token_rotated(token_id, out.token_id, actor.token_id.0);
    Ok((StatusCode::CREATED, Json(out)))
}
