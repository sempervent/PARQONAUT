//! Durable, serializable domain contracts shared across Paraclete crates.
//!
//! This crate intentionally stays dependency-light and stable-minded: types here
//! should serialize cleanly to JSON and evolve with explicit versioning rather than
//! implicit behavior changes.

#![forbid(unsafe_code)]

mod assets;
mod auth;
mod dataset;
mod failure;
mod finding;
pub mod finding_code;
mod fingerprint;
mod format;
mod job;
mod job_recovery;
mod report;
mod run;
mod scan;
mod scan_plan;
mod target;
mod validation;
mod version;

pub use assets::{AssetRecord, InspectionStatus, ParseConfidence, ProbeDepth, ProbeMetadata};
pub use auth::{AuthPrincipal, AuthRole, AuthTokenId, AuthTokenStatus};
pub use dataset::{
    ColumnProfile, Dataset, DatasetFile, FieldDefinition, GroupingKind, PartitionLayout,
    PartitionScheme, PartitionSegment, SchemaSnapshot,
};
pub use failure::FailureKind;
pub use finding::{
    Evidence, EvidenceKind, EvidenceLocationRef, EvidenceReference, Finding, FindingCategory,
    FindingFingerprint, FindingLocation, FindingSeverity, FingerprintAlgorithm, Recommendation,
};
pub use finding_code::{system, FindingCode, FindingCodeError};
pub use format::{DataFormat, FormatSupportTier};
pub use job::{JobErrorCode, JobId, JobResultRef, JobStatus};
pub use job_recovery::JobRecoveryPolicy;
pub use report::{DatasetSummary, FormatSummary, ReportMetadata, ScanReport, ScanSummary};
pub use run::{
    apply_redaction_policy, diff_reports, FindingDelta, RedactionPolicy, RetentionPolicy, RunDiff,
    RunId, RunOutcome, ScanRun, ScanRunListItem, StoredReportRef, SummaryDelta, TargetIdentity,
};
pub use scan::{scan_profile_id, ScanMode, ScanOptions, ScanProfile, ScanRequest};
pub use scan_plan::{ResolvedAsset, ScanPlan};
pub use target::{
    LogicalDatasetTarget, ObjectStoreTargetPlaceholder, ScanTarget, TargetKind, TargetReference,
};
pub use validation::{validate_finding, validate_report, ValidationError};
pub use version::{
    contract_schema_version, report_format_version, ContractSchemaVersion, ReportFormatVersion,
};
