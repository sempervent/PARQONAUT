//! HTTP-facing DTOs (transport shapes, not storage rows).

use chrono::{DateTime, Utc};
use paraclete_types::{
    AuthRole, AuthTokenStatus, JobErrorCode, JobStatus, RedactionPolicy, RunOutcome, ScanOptions,
    ScanProfile, ScanSummary, ScanTarget,
};
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use serde_json::json;
use uuid::Uuid;

/// Maximum `limit` for paginated asset/finding endpoints.
pub const MAX_PAGE_SIZE: u32 = 500;
/// Default page size when `limit` is omitted.
pub const DEFAULT_PAGE_SIZE: u32 = 50;

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct HealthResponse {
    #[schema(example = "ok", value_type = String)]
    pub status: &'static str,
}

/// Response for `GET /api/v1/whoami` (authenticated).
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct WhoAmIResponse {
    pub token_id: Uuid,
    pub label: String,
    pub role: AuthRole,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct StartScanRequest {
    pub target: ScanTarget,
    pub profile: ScanProfile,
    #[serde(default)]
    pub options: ScanOptions,
    #[serde(default)]
    pub scan_id: Option<Uuid>,
    /// When omitted, [`paraclete_types::RedactionPolicy::transport_safe_persist`] is applied before persistence.
    pub redaction: Option<RedactionPolicy>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct StartScanResponse {
    pub run_id: Uuid,
    pub target_kind: String,
    pub normalized_target_key: String,
    pub run_outcome: RunOutcome,
    pub summary: ScanSummary,
    /// Relative URL to fetch the canonical stored report.
    pub report_url: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "run_id": "660e8400-e29b-41d4-a716-446655440001",
    "request_scan_id": "770e8400-e29b-41d4-a716-446655440002",
    "target_kind": "local_file",
    "normalized_target_key": "/data/sample.parquet",
    "started_at": "2026-04-15T12:00:01Z",
    "completed_at": "2026-04-15T12:00:05Z",
    "run_outcome": "completed",
    "engine_revision": "paraclete-test",
    "contract_schema_version": "0.5.0",
    "report_format_version": "0.5.0",
    "report_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "summary": {
        "discovered_assets": 1,
        "files_scanned": 1,
        "inspected_assets": 1,
        "failed_inspection_assets": 0,
        "skipped_inspection_assets": 0,
        "dataset_member_assets": 0,
        "dataset_count": 0,
        "partial_inspection": false,
        "scan_truncated": false,
        "findings_total": 0,
        "findings_by_severity": {}
    }
}))]
pub struct RunSummaryView {
    pub run_id: Uuid,
    pub request_scan_id: Uuid,
    pub target_kind: String,
    pub normalized_target_key: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub run_outcome: RunOutcome,
    pub engine_revision: Option<String>,
    pub contract_schema_version: String,
    pub report_format_version: String,
    pub report_sha256: String,
    pub summary: ScanSummary,
}

#[derive(Debug, Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
#[into_params(parameter_in = Query)]
pub struct PageQuery {
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
    pub inspection_status: Option<String>,
    pub severity: Option<String>,
    pub code: Option<String>,
}

fn default_limit() -> u32 {
    DEFAULT_PAGE_SIZE
}

impl PageQuery {
    pub fn clamped(&self) -> (u64, u64) {
        let lim = self.limit.clamp(1, MAX_PAGE_SIZE) as u64;
        let off = self.offset as u64;
        (lim, off)
    }
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "items": [{
        "path": "/data/sample.parquet",
        "format": "parquet",
        "inspection_status": "inspected",
        "failure_kind": null,
        "failure_message": null,
        "dataset_id": null
    }],
    "total": 1,
    "limit": 50,
    "offset": 0
}))]
pub struct PagedAssetsResponse {
    pub items: Vec<paraclete_store::StoredAssetRow>,
    pub total: u64,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "items": [{
        "fingerprint": "a1b2c3",
        "code": "system.format.unknown",
        "severity": "low",
        "category": "format",
        "asset_path": "/data/sample.parquet",
        "dataset_id": null
    }],
    "total": 1,
    "limit": 50,
    "offset": 0
}))]
pub struct PagedFindingsResponse {
    pub items: Vec<paraclete_store::StoredFindingRow>,
    pub total: u64,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
#[into_params(parameter_in = Query)]
pub struct DiffQuery {
    pub left_run_id: Uuid,
    pub right_run_id: Uuid,
}

#[derive(Debug, Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
#[into_params(parameter_in = Query)]
pub struct TargetRunsQuery {
    pub normalized_key: String,
    #[serde(default = "default_list_limit")]
    pub limit: i64,
}

fn default_list_limit() -> i64 {
    50
}

/// Response for `POST /api/v1/jobs/scans` and async `POST /api/v1/scans` (202 Accepted).
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "queued",
    "submitted_at": "2026-04-15T12:00:00Z",
    "target_kind": "local_file",
    "normalized_target_key": "/data/sample.parquet",
    "job_url": "/api/v1/jobs/550e8400-e29b-41d4-a716-446655440000"
}))]
pub struct ScanJobSubmissionResponse {
    pub job_id: Uuid,
    pub status: JobStatus,
    pub submitted_at: DateTime<Utc>,
    pub target_kind: String,
    pub normalized_target_key: String,
    pub job_url: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct JobFailureBody {
    pub code: JobErrorCode,
    pub message: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "succeeded",
    "submitted_at": "2026-04-15T12:00:00Z",
    "started_at": "2026-04-15T12:00:01Z",
    "completed_at": "2026-04-15T12:00:05Z",
    "target_kind": "local_file",
    "normalized_target_key": "/data/sample.parquet",
    "attempt_count": 1,
    "run_id": "660e8400-e29b-41d4-a716-446655440001",
    "failure": null,
    "job_url": "/api/v1/jobs/550e8400-e29b-41d4-a716-446655440000"
}))]
pub struct ScanJobView {
    pub job_id: Uuid,
    pub status: JobStatus,
    pub submitted_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub target_kind: String,
    pub normalized_target_key: String,
    /// Monotonic claim count for this job (each dequeue from `queued` increments).
    pub attempt_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leased_until: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<JobFailureBody>,
    pub job_url: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ScanJobListResponse {
    pub items: Vec<ScanJobView>,
    pub total: u64,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
#[into_params(parameter_in = Query)]
pub struct JobListQuery {
    pub status: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

// --- Token administration (Phase 11; admin role only) ---

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct AuthTokenCreateRequest {
    pub label: String,
    pub role: AuthRole,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "token_id": "880e8400-e29b-41d4-a716-446655440003",
    "label": "ci-deploy",
    "role": "operator",
    "created_at": "2026-04-15T12:00:00Z",
    "token_secret": "plct_live_xxxxxxxxxxxxxxxx",
    "token_prefix": "plct_live_",
    "note": null
}))]
pub struct AuthTokenCreateResponse {
    pub token_id: Uuid,
    pub label: String,
    pub role: AuthRole,
    pub created_at: DateTime<Utc>,
    /// Shown exactly once; never stored in plaintext.
    pub token_secret: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AuthTokenSummaryView {
    pub token_id: Uuid,
    pub label: String,
    pub role: AuthRole,
    pub status: AuthTokenStatus,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Last successful bearer authentication (updated on each verified request).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<DateTime<Utc>>,
    /// If this row was rotated out, the new token id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replaced_by_token_id: Option<Uuid>,
}

/// Response for `POST /api/v1/admin/tokens/{token_id}/rotate` (admin only; new secret once).
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "token_id": "880e8400-e29b-41d4-a716-446655440099",
    "previous_token_id": "880e8400-e29b-41d4-a716-446655440001",
    "label": "ci-deploy",
    "role": "operator",
    "created_at": "2026-04-15T12:00:00Z",
    "token_secret": "plc_xxxxxxxx",
    "token_prefix": "plc_xxxxxx",
    "note": null,
    "previous_disabled_at": "2026-04-15T12:00:00Z"
}))]
pub struct AuthTokenRotateResponse {
    pub token_id: Uuid,
    pub previous_token_id: Uuid,
    pub label: String,
    pub role: AuthRole,
    pub created_at: DateTime<Utc>,
    /// New bearer secret; shown once (only a hash is stored).
    pub token_secret: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// When the previous token row was disabled (same transaction as minting the new token).
    pub previous_disabled_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AuthTokenListResponse {
    pub items: Vec<AuthTokenSummaryView>,
}
