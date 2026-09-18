//! Scan run identity, outcomes, persistence-oriented projections, and run diffs.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ScanReport, ScanSummary, ScanTarget};

/// Stable identifier for one persisted scan execution (distinct from `ScanRequest.scan_id`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(transparent)]
pub struct RunId(pub Uuid);

impl RunId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// High-level completion status for a stored run.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    /// Scan finished and `ScanReport.summary.partial_inspection` is false.
    Completed,
    /// Scan finished but failures or truncation were recorded (`partial_inspection` true).
    CompletedPartial,
}

/// Normalized identity for listing history of the “same” target across runs (local paths only in Phase 4).
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
pub struct TargetIdentity {
    /// Stable discriminator, for example `local_file`, `local_directory`, `logical_dataset`.
    pub target_kind: String,
    /// Canonical UTF-8 path or logical key used for equality (no symlinks resolved here).
    pub normalized_key: String,
}

impl TargetIdentity {
    /// Builds a best-effort identity from a [`ScanTarget`] (local paths normalized as given).
    pub fn from_scan_target(target: &ScanTarget) -> Self {
        match target {
            ScanTarget::LocalFile { path } => {
                Self { target_kind: "local_file".into(), normalized_key: path.as_str().to_string() }
            }
            ScanTarget::LocalDirectory { path } => Self {
                target_kind: "local_directory".into(),
                normalized_key: path.as_str().to_string(),
            },
            ScanTarget::LogicalDataset { inner } => Self {
                target_kind: "logical_dataset".into(),
                normalized_key: inner.dataset_id.clone(),
            },
            ScanTarget::ObjectStorePlaceholder { inner } => Self {
                target_kind: "object_store_placeholder".into(),
                normalized_key: inner.uri.as_str().to_string(),
            },
        }
    }
}

/// Pointer to a persisted canonical report (library boundary; not HTTP).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct StoredReportRef {
    pub run_id: RunId,
    /// Hex-encoded SHA-256 of the canonical JSON bytes stored for this run (after redaction).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
}

/// Metadata for a persisted scan run (row-shaped for storage and APIs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct ScanRun {
    pub run_id: RunId,
    pub request_scan_id: Uuid,
    pub target_identity: TargetIdentity,
    /// Original `ScanTarget` serialized to JSON for faithful reconstruction.
    pub target_json: serde_json::Value,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub run_outcome: RunOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_revision: Option<String>,
    pub contract_schema_version: String,
    pub report_format_version: String,
}

/// Lightweight row for listing runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ScanRunListItem {
    pub run_id: RunId,
    pub request_scan_id: Uuid,
    pub completed_at: DateTime<Utc>,
    pub run_outcome: RunOutcome,
    pub target_identity: TargetIdentity,
}

/// First-class diff between two materialized reports (deterministic ordering).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct RunDiff {
    pub run_a: RunId,
    pub run_b: RunId,
    pub findings: FindingDelta,
    pub datasets_added: Vec<String>,
    pub datasets_removed: Vec<String>,
    pub summary_delta: SummaryDelta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, utoipa::ToSchema)]
pub struct SummaryDelta {
    pub discovered_assets_delta: i64,
    pub inspected_assets_delta: i64,
    pub failed_inspection_assets_delta: i64,
    pub skipped_inspection_assets_delta: i64,
    pub dataset_member_assets_delta: i64,
    pub dataset_count_delta: i64,
    pub findings_total_delta: i64,
}

/// Per-finding fingerprint change (digest strings).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FindingDelta {
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

/// Policy applied before persisting blobs/projections (Phase 4 baseline; expand later).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct RedactionPolicy {
    /// Remove `AssetRecord.inspection_hints` before persistence.
    pub strip_probe_inspection_hints: bool,
    /// Remove `AssetRecord.probe` before persistence.
    pub strip_probe_metadata: bool,
    /// Remove `Finding.evidence[].payload` (summaries and locations retained).
    pub strip_finding_evidence_payloads: bool,
    /// Truncate `failure_message` on assets to this many UTF-8 chars (if set).
    pub truncate_failure_messages_to: Option<usize>,
}

impl RedactionPolicy {
    /// Defaults for HTTP-driven persistence: strip heavy probe and evidence payloads, cap failure text.
    pub fn transport_safe_persist() -> Self {
        Self {
            strip_probe_inspection_hints: true,
            strip_probe_metadata: true,
            strip_finding_evidence_payloads: true,
            truncate_failure_messages_to: Some(8192),
        }
    }
}

/// Retention hints (placeholder for TTL / export; storage honors redaction first).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct RetentionPolicy {
    /// When true, storage may omit non-indexed large blobs in future phases (no effect in SQLite v1).
    pub prefer_minimal_projections: bool,
}

/// Computes a deterministic diff between two reports (ignores timestamps in metadata).
pub fn diff_reports(run_a: RunId, run_b: RunId, a: &ScanReport, b: &ScanReport) -> RunDiff {
    let fa = finding_fingerprints(a);
    let fb = finding_fingerprints(b);
    let findings_added: Vec<String> = fb.difference(&fa).cloned().collect();
    let findings_removed: Vec<String> = fa.difference(&fb).cloned().collect();

    let da = dataset_ids(a);
    let db = dataset_ids(b);
    let datasets_added: Vec<String> = db.difference(&da).cloned().collect();
    let datasets_removed: Vec<String> = da.difference(&db).cloned().collect();

    RunDiff {
        run_a,
        run_b,
        findings: FindingDelta { added: findings_added, removed: findings_removed },
        datasets_added,
        datasets_removed,
        summary_delta: diff_summaries(&a.summary, &b.summary),
    }
}

fn finding_fingerprints(report: &ScanReport) -> BTreeSet<String> {
    report
        .findings
        .iter()
        .map(|f| {
            f.fingerprint
                .as_ref()
                .map(|fp| fp.digest.clone())
                .unwrap_or_else(|| format!("fallback:{}", f.id))
        })
        .collect()
}

fn dataset_ids(report: &ScanReport) -> BTreeSet<String> {
    report.datasets.iter().map(|d| d.dataset_id.clone()).collect()
}

fn diff_summaries(a: &ScanSummary, b: &ScanSummary) -> SummaryDelta {
    SummaryDelta {
        discovered_assets_delta: i64::try_from(b.discovered_assets).unwrap_or(0)
            - i64::try_from(a.discovered_assets).unwrap_or(0),
        inspected_assets_delta: i64::try_from(b.inspected_assets).unwrap_or(0)
            - i64::try_from(a.inspected_assets).unwrap_or(0),
        failed_inspection_assets_delta: i64::try_from(b.failed_inspection_assets).unwrap_or(0)
            - i64::try_from(a.failed_inspection_assets).unwrap_or(0),
        skipped_inspection_assets_delta: i64::try_from(b.skipped_inspection_assets).unwrap_or(0)
            - i64::try_from(a.skipped_inspection_assets).unwrap_or(0),
        dataset_member_assets_delta: i64::try_from(b.dataset_member_assets).unwrap_or(0)
            - i64::try_from(a.dataset_member_assets).unwrap_or(0),
        dataset_count_delta: i64::try_from(b.dataset_count).unwrap_or(0)
            - i64::try_from(a.dataset_count).unwrap_or(0),
        findings_total_delta: i64::try_from(b.findings_total).unwrap_or(0)
            - i64::try_from(a.findings_total).unwrap_or(0),
    }
}

/// Applies redaction to a copy of the report before persistence.
pub fn apply_redaction_policy(mut report: ScanReport, policy: &RedactionPolicy) -> ScanReport {
    for a in &mut report.assets {
        if policy.strip_probe_inspection_hints {
            a.inspection_hints = None;
        }
        if policy.strip_probe_metadata {
            a.probe = None;
        }
        if let Some(max) = policy.truncate_failure_messages_to {
            if let Some(ref mut msg) = a.failure_message {
                let t: String = msg.chars().take(max).collect();
                *msg = t;
            }
        }
    }
    if policy.strip_finding_evidence_payloads {
        for f in &mut report.findings {
            for ev in &mut f.evidence {
                ev.payload = None;
            }
        }
    }
    report
}
