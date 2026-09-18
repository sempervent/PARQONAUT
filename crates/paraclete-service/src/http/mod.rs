//! HTTP server wiring (Axum). Handlers delegate to [`crate::service::ParacleteService`].

pub mod auth;
pub mod extract;
pub mod handlers;
pub mod http_layers;

use axum::http::StatusCode;
use axum::middleware::{from_fn, from_fn_with_state};
use axum::routing::{get, post};
use axum::{Json, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use tower_http::compression::CompressionLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::observability;
use crate::service::ParacleteService;
use crate::worker::spawn_scan_job_worker;

#[derive(Clone)]
pub struct AppState {
    pub service: ParacleteService,
    pub prometheus: PrometheusHandle,
}

/// Builds the full `/api/v1` router with compression and request tracing.
///
/// Starts an in-process background worker that drains the `scan_jobs` queue using the same
/// [`ParacleteService::execute_scan_and_persist`] path as synchronous scans.
///
/// All routes except [`handlers::health`] require `Authorization: Bearer <token>` and a sufficient role.
pub fn build_router(service: ParacleteService) -> Router {
    std::mem::drop(spawn_scan_job_worker(service.clone()));
    let prometheus = observability::metrics_handle();
    let state = AppState { service, prometheus };

    let protected = Router::new()
        .route("/openapi.json", get(openapi_json))
        .route("/scans", post(handlers::post_async_scan))
        .route("/scans/sync", post(handlers::post_scan_sync))
        .route("/jobs/scans", post(handlers::post_async_scan))
        .route("/jobs/{job_id}", get(handlers::get_job))
        .route("/jobs", get(handlers::list_jobs))
        .route("/runs/{run_id}", get(handlers::get_run))
        .route("/runs/{run_id}/report", get(handlers::get_report))
        .route("/runs/{run_id}/assets", get(handlers::get_assets))
        .route("/runs/{run_id}/findings", get(handlers::get_findings))
        .route("/targets/{target_kind}/runs", get(handlers::list_target_runs))
        .route("/diff", get(handlers::get_diff))
        .route("/whoami", get(handlers::whoami))
        .route("/admin/tokens", get(handlers::get_admin_tokens).post(handlers::post_admin_tokens))
        .route("/admin/tokens/{token_id}", get(handlers::get_admin_token))
        .route("/admin/tokens/{token_id}/rotate", post(handlers::post_admin_token_rotate))
        .route("/admin/tokens/{token_id}/disable", post(handlers::post_admin_token_disable))
        .layer(from_fn_with_state(state.clone(), auth::auth_middleware))
        .with_state(state.clone());

    Router::new()
        .route("/metrics", get(handlers::prometheus_metrics))
        .route("/api/v1/health", get(handlers::health))
        .nest("/api/v1", protected)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            std::time::Duration::from_secs(300),
        ))
        .layer(from_fn(http_layers::http_metrics_middleware))
        .layer(from_fn(http_layers::request_id_middleware))
        .with_state(state)
}

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(crate::openapi::openapi_spec())
}
