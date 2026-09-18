//! Scan report envelope, summaries, and metadata.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    contract_schema_version, report_format_version, AssetRecord, ContractSchemaVersion, DataFormat,
    Dataset, Finding, ReportFormatVersion, ScanRequest,
};

/// Aggregate statistics for a completed scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ScanSummary {
    /// Total assets discovered during resolution (equals `files_scanned` for local scans).
    pub discovered_assets: u64,
    /// Legacy alias for `discovered_assets` (kept for stable JSON field names).
    pub files_scanned: u64,
    /// Assets that completed a format-specific inspection pass.
    pub inspected_assets: u64,
    /// Assets where inspection was attempted and failed.
    pub failed_inspection_assets: u64,
    /// Assets where no format-specific probe ran (for example unknown extension).
    pub skipped_inspection_assets: u64,
    /// Count of assets listed as members of at least one inferred Parquet dataset.
    pub dataset_member_assets: u64,
    /// Logical Parquet datasets inferred for this scan.
    pub dataset_count: u64,
    /// True when truncation or one or more inspection failures occurred.
    pub partial_inspection: bool,
    /// True when resolution stopped early because `max_files` was reached.
    pub scan_truncated: bool,
    pub findings_total: u64,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub findings_by_severity: std::collections::BTreeMap<String, u64>,
}

/// High-level view of a single dataset within the report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DatasetSummary {
    pub dataset_id: String,
    pub file_count: u64,
    pub dominant_format: DataFormat,
}

/// Per-format counts for quick triage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FormatSummary {
    pub format: DataFormat,
    pub file_count: u64,
}

/// Provenance and versioning for a serialized report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ReportMetadata {
    pub scan_id: Uuid,
    pub generated_at: DateTime<Utc>,
    pub contract_schema: ContractSchemaVersion,
    pub report_format: ReportFormatVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_revision: Option<String>,
}

/// Top-level scan output suitable for archival and downstream rendering.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ScanReport {
    pub request: ScanRequest,
    pub metadata: ReportMetadata,
    pub summary: ScanSummary,
    /// Per-asset inspection outcomes (execution truth); reconcile with `summary`.
    pub assets: Vec<AssetRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<Dataset>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dataset_summaries: Vec<DatasetSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub format_summaries: Vec<FormatSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<Finding>,
}

impl ScanReport {
    /// Builds a minimal report for tests and golden fixtures.
    pub fn minimal_example() -> Self {
        let request = ScanRequest::new(
            crate::ScanTarget::LocalDirectory { path: camino::Utf8PathBuf::from("fixtures/csv") },
            crate::ScanProfile::Quick,
        );
        Self {
            request: request.clone(),
            metadata: ReportMetadata {
                scan_id: request.scan_id,
                generated_at: Utc::now(),
                contract_schema: contract_schema_version(),
                report_format: report_format_version(),
                engine_revision: Some("phase-4-scaffold".into()),
            },
            summary: ScanSummary {
                discovered_assets: 0,
                files_scanned: 0,
                inspected_assets: 0,
                failed_inspection_assets: 0,
                skipped_inspection_assets: 0,
                dataset_member_assets: 0,
                dataset_count: 0,
                partial_inspection: false,
                scan_truncated: false,
                findings_total: 0,
                findings_by_severity: std::collections::BTreeMap::new(),
            },
            assets: Vec::new(),
            datasets: Vec::new(),
            dataset_summaries: Vec::new(),
            format_summaries: Vec::new(),
            findings: Vec::new(),
        }
    }
}
