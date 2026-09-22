//! Lightweight validation helpers for invariants not captured by types alone.

use thiserror::Error;

use crate::{Finding, InspectionStatus, ScanReport};

/// Validation failures for domain objects.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("finding summary must not be empty")]
    FindingEmptySummary,
    #[error("finding detail must not be empty")]
    FindingEmptyDetail,
    #[error("report metadata scan_id must match request.scan_id")]
    ReportScanIdMismatch,
    #[error("report summary.findings_total must match findings.len()")]
    ReportFindingCountMismatch,
    #[error("report summary.dataset_count must match datasets.len()")]
    ReportDatasetCountMismatch,
    #[error("report summary.partial_inspection must align with failures and truncation flags")]
    ReportPartialInspectionMismatch,
    #[error("report summary.discovered_assets must match assets.len() and files_scanned")]
    ReportDiscoveryCountMismatch,
    #[error("report summary inspection counters must match asset records")]
    ReportAssetInspectionMismatch,
    #[error("report summary.dataset_member_assets must match assets with dataset_id")]
    ReportDatasetMemberCountMismatch,
    #[error("dataset member path missing from assets or dataset_id mismatch")]
    ReportDatasetMembershipMismatch,
    #[error("asset record fields inconsistent with inspection_status")]
    ReportAssetRecordInconsistent,
}

/// Validates textual invariants for a [`Finding`].
pub fn validate_finding(finding: &Finding) -> Result<(), ValidationError> {
    if finding.summary.trim().is_empty() {
        return Err(ValidationError::FindingEmptySummary);
    }
    if finding.detail.trim().is_empty() {
        return Err(ValidationError::FindingEmptyDetail);
    }
    Ok(())
}

/// Validates cross-field invariants for a [`ScanReport`].
pub fn validate_report(report: &ScanReport) -> Result<(), ValidationError> {
    if report.metadata.scan_id != report.request.scan_id {
        return Err(ValidationError::ReportScanIdMismatch);
    }
    if report.summary.findings_total != report.findings.len() as u64 {
        return Err(ValidationError::ReportFindingCountMismatch);
    }
    if report.summary.dataset_count != report.datasets.len() as u64 {
        return Err(ValidationError::ReportDatasetCountMismatch);
    }
    let expect_partial =
        report.summary.failed_inspection_assets > 0 || report.summary.scan_truncated;
    if report.summary.partial_inspection != expect_partial {
        return Err(ValidationError::ReportPartialInspectionMismatch);
    }

    let n = report.assets.len() as u64;
    if report.summary.discovered_assets != n
        || report.summary.files_scanned != n
        || report.summary.discovered_assets != report.summary.files_scanned
    {
        return Err(ValidationError::ReportDiscoveryCountMismatch);
    }

    let mut inspected = 0u64;
    let mut failed = 0u64;
    let mut skipped = 0u64;
    let mut dataset_members = 0u64;
    for a in &report.assets {
        match a.inspection_status {
            InspectionStatus::Inspected => inspected += 1,
            InspectionStatus::Failed => failed += 1,
            InspectionStatus::Skipped => skipped += 1,
        }
        if a.dataset_id.is_some() {
            dataset_members += 1;
        }
        match a.inspection_status {
            InspectionStatus::Failed => {
                if a.failure_kind.is_none() {
                    return Err(ValidationError::ReportAssetRecordInconsistent);
                }
                let msg = a.failure_message.as_deref().unwrap_or("").trim();
                if msg.is_empty() {
                    return Err(ValidationError::ReportAssetRecordInconsistent);
                }
            }
            InspectionStatus::Inspected => {
                if a.failure_message.is_some() || a.failure_kind.is_some() {
                    return Err(ValidationError::ReportAssetRecordInconsistent);
                }
            }
            InspectionStatus::Skipped => {
                if a.failure_message.is_some()
                    || a.failure_kind.is_some()
                    || a.probe.is_some()
                    || a.dataset_id.is_some()
                {
                    return Err(ValidationError::ReportAssetRecordInconsistent);
                }
            }
        }
    }
    if inspected != report.summary.inspected_assets
        || failed != report.summary.failed_inspection_assets
        || skipped != report.summary.skipped_inspection_assets
        || dataset_members != report.summary.dataset_member_assets
    {
        return Err(ValidationError::ReportAssetInspectionMismatch);
    }
    if inspected + failed + skipped != report.summary.discovered_assets {
        return Err(ValidationError::ReportAssetInspectionMismatch);
    }

    let mut membership: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    for a in &report.assets {
        if let Some(ds) = &a.dataset_id {
            membership.insert(a.path.as_str().to_string(), ds.clone());
        }
    }
    for ds in &report.datasets {
        for f in &ds.files {
            let id = f.path.as_str();
            match membership.get(id) {
                Some(x) if x == &ds.dataset_id => {}
                _ => return Err(ValidationError::ReportDatasetMembershipMismatch),
            }
        }
    }

    Ok(())
}
