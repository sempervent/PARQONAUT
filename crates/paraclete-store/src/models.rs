//! Shared store row types (SQLite and Postgres).

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use paraclete_types::{
    DataFormat, FailureKind, InspectionStatus, RunId, RunOutcome, ScanSummary, TargetIdentity,
};
use uuid::Uuid;

/// One row from `scan_jobs` (async orchestration; request payload is JSON text).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ScanJobRow {
    pub job_id: String,
    pub submitted_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub status: String,
    pub target_kind: String,
    pub normalized_target_key: String,
    pub request_json: String,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
    pub run_id: Option<String>,
    pub worker_id: Option<String>,
    pub attempt_count: i64,
    pub heartbeat_at: Option<String>,
    pub leased_until: Option<String>,
    pub recovery_note: Option<String>,
}

/// Result of stale-job recovery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StaleRecoveryStats {
    pub requeued: u64,
    pub failed_retries_exhausted: u64,
}

/// One row from `scan_assets` (projection; reload full `ScanReport` for complete `AssetRecord`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StoredAssetRow {
    #[schema(value_type = String)]
    pub path: Utf8PathBuf,
    pub format: DataFormat,
    pub inspection_status: InspectionStatus,
    pub failure_kind: Option<FailureKind>,
    pub failure_message: Option<String>,
    pub dataset_id: Option<String>,
}

/// Run header + summary + blob digest (no `report_json`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunPublicMeta {
    pub run_id: RunId,
    pub request_scan_id: Uuid,
    pub target_identity: TargetIdentity,
    pub target_json: serde_json::Value,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub run_outcome: RunOutcome,
    pub engine_revision: Option<String>,
    pub contract_schema_version: String,
    pub report_format_version: String,
    pub report_sha256: String,
    pub summary: ScanSummary,
}

/// One row from `scan_findings` (projection; not a full [`paraclete_types::Finding`]).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StoredFindingRow {
    pub fingerprint: String,
    pub code: String,
    pub severity: String,
    pub category: String,
    pub asset_path: Option<String>,
    pub dataset_id: Option<String>,
}
