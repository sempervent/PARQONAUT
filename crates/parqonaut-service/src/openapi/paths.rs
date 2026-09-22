//! `utoipa::path` stubs — document the HTTP contract; real handlers live in `http::handlers`.
#![allow(dead_code)]

use crate::api_types::{
    AuthTokenCreateRequest, AuthTokenCreateResponse, AuthTokenListResponse,
    AuthTokenRotateResponse, AuthTokenSummaryView, DiffQuery, HealthResponse, JobListQuery,
    PageQuery, PagedAssetsResponse, PagedFindingsResponse, PluginCatalogEntryView,
    PluginCatalogListResponse, RunSummaryView, ScanJobListResponse, ScanJobSubmissionResponse,
    ScanJobView, StartScanRequest, StartScanResponse, TargetRunsQuery, WhoAmIResponse,
};
use crate::application_http::{
    BatchCheckResponse, BatchConfigBody, BatchPlanResponse, BatchRepairJobBody, BatchResumeBody,
    BatchStatusResponse, BatchVerifyResponse, CheckResponse, DiagnoseResponse, JobCancelResponse,
    LocationPolicyBody, PlanBody, PlanResponse, RepairJobBody, VerifyBody, VerifyResponse,
};
use crate::error::ErrorBody;
use parqonaut_types::{RunDiff, ScanReport, ScanRunListItem};

#[utoipa::path(
    get,
    path = "/metrics",
    tag = "meta",
    responses(
        (status = 200, description = "Prometheus text exposition (`text/plain`; unauthenticated — protect at the edge in production)")
    )
)]
pub fn get_metrics() {}

#[utoipa::path(
    get,
    path = "/api/v1/health",
    tag = "health",
    responses(
        (status = 200, description = "Process is reachable", body = HealthResponse)
    )
)]
pub fn get_health() {}

#[utoipa::path(
    get,
    path = "/api/v1/openapi.json",
    tag = "meta",
    responses(
        (status = 200, description = "OpenAPI 3.1 JSON document (`application/json`)"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_openapi_json() {}

#[utoipa::path(
    get,
    path = "/api/v1/whoami",
    tag = "health",
    responses(
        (status = 200, description = "Authenticated token identity", body = WhoAmIResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_whoami() {}

#[utoipa::path(
    get,
    path = "/api/v1/plugins",
    tag = "plugins",
    responses(
        (status = 200, description = "Server allowlisted plugin catalog", body = PluginCatalogListResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn list_plugins() {}

#[utoipa::path(
    get,
    path = "/api/v1/plugins/{name}",
    tag = "plugins",
    params(("name" = String, Path, description = "Plugin name")),
    responses(
        (status = 200, description = "Allowlisted plugin entry", body = PluginCatalogEntryView),
        (status = 404, description = "Plugin not found or not allowlisted", body = ErrorBody),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_plugin_by_name() {}

#[utoipa::path(
    post,
    path = "/api/v1/scans",
    tag = "jobs",
    request_body(content = StartScanRequest, description = "Scan target, profile, and options"),
    responses(
        (status = 202, description = "Async scan job accepted", body = ScanJobSubmissionResponse),
        (status = 400, description = "Invalid JSON request body (`invalid_json_request`; e.g. syntax, body buffer)", body = ErrorBody),
        (status = 415, description = "Missing or non-JSON `Content-Type` (`invalid_json_request`)", body = ErrorBody),
        (status = 422, description = "JSON value did not match the expected request shape (`invalid_json_request`)", body = ErrorBody),
        (status = 413, description = "Request body exceeded configured limit while buffering (`invalid_json_request`)", body = ErrorBody),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 403, description = "Insufficient role (operator required)", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_scans_async() {}

#[utoipa::path(
    post,
    path = "/api/v1/jobs/scans",
    tag = "jobs",
    request_body(content = StartScanRequest, description = "Same payload as `POST /api/v1/scans`"),
    responses(
        (status = 202, description = "Async scan job accepted", body = ScanJobSubmissionResponse),
        (status = 400, description = "Invalid JSON request body (`invalid_json_request`; e.g. syntax, body buffer)", body = ErrorBody),
        (status = 415, description = "Missing or non-JSON `Content-Type` (`invalid_json_request`)", body = ErrorBody),
        (status = 422, description = "JSON value did not match the expected request shape (`invalid_json_request`)", body = ErrorBody),
        (status = 413, description = "Request body exceeded configured limit while buffering (`invalid_json_request`)", body = ErrorBody),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 403, description = "Insufficient role (operator required)", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_jobs_scans() {}

#[utoipa::path(
    post,
    path = "/api/v1/scans/sync",
    tag = "jobs",
    request_body(content = StartScanRequest, description = "Scan target, profile, and options"),
    responses(
        (status = 201, description = "Run persisted synchronously (dev/tests)", body = StartScanResponse),
        (status = 400, description = "Invalid JSON request body (`invalid_json_request`; e.g. syntax, body buffer)", body = ErrorBody),
        (status = 415, description = "Missing or non-JSON `Content-Type` (`invalid_json_request`)", body = ErrorBody),
        (status = 422, description = "Invalid JSON shape (`invalid_json_request`) or scan engine failure (`scan_failed`)", body = ErrorBody),
        (status = 413, description = "Request body exceeded configured limit while buffering (`invalid_json_request`)", body = ErrorBody),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 403, description = "Insufficient role (operator required)", body = ErrorBody),
        (status = 500, description = "Store or internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_scans_sync() {}

#[utoipa::path(
    get,
    path = "/api/v1/jobs/{job_id}",
    tag = "jobs",
    params(
        ("job_id" = Uuid, Path, description = "Scan job id")
    ),
    responses(
        (status = 200, description = "Job status and outcome", body = ScanJobView),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 404, description = "Unknown job id", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_job_by_id() {}

#[utoipa::path(
    get,
    path = "/api/v1/jobs",
    tag = "jobs",
    params(JobListQuery),
    responses(
        (status = 200, description = "Recent jobs (newest first)", body = ScanJobListResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn list_jobs() {}

#[utoipa::path(
    get,
    path = "/api/v1/runs/{run_id}",
    tag = "runs",
    params(
        ("run_id" = Uuid, Path, description = "Persisted run id")
    ),
    responses(
        (status = 200, description = "Run header and summary (no full report blob)", body = RunSummaryView),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 404, description = "Unknown run id", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_run_by_id() {}

#[utoipa::path(
    get,
    path = "/api/v1/runs/{run_id}/report",
    tag = "runs",
    params(
        ("run_id" = Uuid, Path, description = "Persisted run id")
    ),
    responses(
        (status = 200, description = "Full canonical `ScanReport` JSON", body = ScanReport),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 404, description = "Unknown run id", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_run_report() {}

#[utoipa::path(
    get,
    path = "/api/v1/runs/{run_id}/assets",
    tag = "runs",
    params(
        ("run_id" = Uuid, Path, description = "Persisted run id"),
        PageQuery
    ),
    responses(
        (status = 200, description = "Paginated asset projections", body = PagedAssetsResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 404, description = "Unknown run id", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_run_assets() {}

#[utoipa::path(
    get,
    path = "/api/v1/runs/{run_id}/findings",
    tag = "runs",
    params(
        ("run_id" = Uuid, Path, description = "Persisted run id"),
        PageQuery
    ),
    responses(
        (status = 200, description = "Paginated finding projections", body = PagedFindingsResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 404, description = "Unknown run id", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_run_findings() {}

#[utoipa::path(
    get,
    path = "/api/v1/targets/{target_kind}/runs",
    tag = "runs",
    params(
        ("target_kind" = String, Path, description = "Target discriminator (`local_file`, `local_directory`, …)"),
        TargetRunsQuery
    ),
    responses(
        (status = 200, description = "Runs for the given target identity", body = Vec<ScanRunListItem>),
        (status = 400, description = "Missing `normalized_key` or invalid filter", body = ErrorBody),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 404, description = "Unknown `target_kind`", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn list_target_runs() {}

#[utoipa::path(
    get,
    path = "/api/v1/diff",
    tag = "runs",
    params(DiffQuery),
    responses(
        (status = 200, description = "Deterministic diff between two materialized reports", body = RunDiff),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 404, description = "One or both runs not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_diff() {}

#[utoipa::path(
    get,
    path = "/api/v1/admin/tokens",
    tag = "admin",
    responses(
        (status = 200, description = "All API tokens (metadata only; no secrets)", body = AuthTokenListResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 403, description = "Admin role required", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_admin_tokens() {}

#[utoipa::path(
    post,
    path = "/api/v1/admin/tokens",
    tag = "admin",
    request_body(content = AuthTokenCreateRequest, description = "Label, role, optional note"),
    responses(
        (status = 201, description = "Token created; secret returned once", body = AuthTokenCreateResponse),
        (status = 400, description = "Invalid JSON request body (`invalid_json_request`; e.g. syntax, body buffer)", body = ErrorBody),
        (status = 415, description = "Missing or non-JSON `Content-Type` (`invalid_json_request`)", body = ErrorBody),
        (status = 422, description = "JSON value did not match the expected request shape (`invalid_json_request`)", body = ErrorBody),
        (status = 413, description = "Request body exceeded configured limit while buffering (`invalid_json_request`)", body = ErrorBody),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 403, description = "Admin role required", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_admin_tokens() {}

#[utoipa::path(
    get,
    path = "/api/v1/admin/tokens/{token_id}",
    tag = "admin",
    params(
        ("token_id" = Uuid, Path, description = "Token row id")
    ),
    responses(
        (status = 200, description = "Token metadata (no secret)", body = AuthTokenSummaryView),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 403, description = "Admin role required", body = ErrorBody),
        (status = 404, description = "Unknown token id", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn get_admin_token_by_id() {}

#[utoipa::path(
    post,
    path = "/api/v1/admin/tokens/{token_id}/rotate",
    tag = "admin",
    params(
        ("token_id" = Uuid, Path, description = "Active token id to rotate (disabled; new token replaces it)")
    ),
    responses(
        (status = 201, description = "New token minted; previous secret invalidated; `token_secret` shown once", body = AuthTokenRotateResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 403, description = "Admin role required", body = ErrorBody),
        (status = 404, description = "Unknown or already-disabled token id", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_admin_token_rotate() {}

#[utoipa::path(
    post,
    path = "/api/v1/admin/tokens/{token_id}/disable",
    tag = "admin",
    params(
        ("token_id" = Uuid, Path, description = "Token row id")
    ),
    responses(
        (status = 200, description = "Updated token metadata (`status=disabled`)", body = AuthTokenSummaryView),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 403, description = "Admin role required", body = ErrorBody),
        (status = 404, description = "Unknown token id", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_admin_token_disable() {}

#[utoipa::path(
    get,
    path = "/api/v1/metrics",
    tag = "meta",
    responses(
        (status = 200, description = "Prometheus text exposition (authenticated alias of `/metrics`)")
    ),
    security(("bearerAuth" = []))
)]
pub fn get_api_metrics() {}

#[utoipa::path(
    post,
    path = "/api/v1/diagnose",
    tag = "repair",
    request_body(content = LocationPolicyBody),
    responses(
        (status = 200, description = "Diagnosis report", body = DiagnoseResponse),
        (status = 401, body = ErrorBody),
        (status = 403, body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_diagnose() {}

#[utoipa::path(
    post,
    path = "/api/v1/plans",
    tag = "repair",
    request_body(content = PlanBody),
    responses(
        (status = 200, description = "Repair plan", body = PlanResponse),
        (status = 401, body = ErrorBody),
        (status = 403, body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_plans() {}

#[utoipa::path(
    post,
    path = "/api/v1/checks",
    tag = "repair",
    request_body(content = LocationPolicyBody),
    responses(
        (status = 200, description = "Check report", body = CheckResponse),
        (status = 401, body = ErrorBody),
        (status = 403, body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_checks() {}

#[utoipa::path(
    post,
    path = "/api/v1/repairs",
    tag = "repair",
    request_body(content = RepairJobBody),
    responses(
        (status = 202, description = "Repair job accepted", body = ScanJobSubmissionResponse),
        (status = 401, body = ErrorBody),
        (status = 403, body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_repairs() {}

#[utoipa::path(
    post,
    path = "/api/v1/verifications",
    tag = "repair",
    request_body(content = VerifyBody),
    responses(
        (status = 200, description = "Verification report", body = VerifyResponse),
        (status = 401, body = ErrorBody),
        (status = 403, body = ErrorBody)
    ),
    security(("bearerAuth" = []))
)]
pub fn post_verifications() {}

#[utoipa::path(
    post,
    path = "/api/v1/batches/check",
    tag = "batch",
    request_body(content = BatchConfigBody),
    responses((status = 200, body = BatchCheckResponse)),
    security(("bearerAuth" = []))
)]
pub fn post_batch_check() {}

#[utoipa::path(
    post,
    path = "/api/v1/batches/plans",
    tag = "batch",
    request_body(content = BatchConfigBody),
    responses((status = 200, body = BatchPlanResponse)),
    security(("bearerAuth" = []))
)]
pub fn post_batch_plans() {}

#[utoipa::path(
    post,
    path = "/api/v1/batches/repairs",
    tag = "batch",
    request_body(content = BatchRepairJobBody),
    responses((status = 202, body = ScanJobSubmissionResponse)),
    security(("bearerAuth" = []))
)]
pub fn post_batch_repairs() {}

#[utoipa::path(
    get,
    path = "/api/v1/batches/{run_dir}",
    tag = "batch",
    params(("run_dir" = String, Path)),
    responses((status = 200, body = BatchStatusResponse)),
    security(("bearerAuth" = []))
)]
pub fn get_batch_status() {}

#[utoipa::path(
    post,
    path = "/api/v1/batches/{run_dir}/resume",
    tag = "batch",
    request_body(content = BatchResumeBody),
    responses((status = 202, body = ScanJobSubmissionResponse)),
    security(("bearerAuth" = []))
)]
pub fn post_batch_resume() {}

#[utoipa::path(
    post,
    path = "/api/v1/batches/{run_dir}/verify",
    tag = "batch",
    responses((status = 200, body = BatchVerifyResponse)),
    security(("bearerAuth" = []))
)]
pub fn post_batch_verify() {}

#[utoipa::path(
    post,
    path = "/api/v1/jobs/{job_id}/cancel",
    tag = "jobs",
    responses((status = 200, body = JobCancelResponse)),
    security(("bearerAuth" = []))
)]
pub fn post_job_cancel() {}
