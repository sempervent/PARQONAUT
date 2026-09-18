use camino::Utf8Path;
use paraclete_core::ScanEngine;
use paraclete_types::{system, ScanProfile, ScanReport, ScanRequest, ScanTarget};

use crate::diagnose::diagnose;
use crate::error::RepairError;
use crate::inventory::DatasetInventory;
use crate::manifest::ExecutionManifest;
use crate::policy::RepairPolicy;
use crate::safety::RepairSafety;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationOutcome {
    Verified,
    VerifiedWithWarnings,
    Failed,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InvariantResult {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerificationReport {
    pub before_scan_id: uuid::Uuid,
    pub after_scan_id: uuid::Uuid,
    pub invariant_results: Vec<InvariantResult>,
    pub resolved_finding_codes: Vec<String>,
    pub remaining_finding_codes: Vec<String>,
    pub new_finding_codes: Vec<String>,
    pub blocked_finding_codes: Vec<String>,
    pub outcome: VerificationOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_plan_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_match: Option<bool>,
}

pub fn verify_repair(
    before: &ScanReport,
    after: &ScanReport,
    before_root: &Utf8Path,
    after_root: &Utf8Path,
    policy: &RepairPolicy,
    manifest: Option<&ExecutionManifest>,
) -> Result<VerificationReport, RepairError> {
    let before_inv = DatasetInventory::from_scan_report(before_root, before)?;
    let after_inv = DatasetInventory::from_scan_report(after_root, after)?;

    let mut invariants = Vec::new();

    let row_ok = before_inv.total_rows == after_inv.total_rows;
    invariants.push(InvariantResult {
        name: "row_count".into(),
        passed: row_ok,
        detail: format!("before={} after={}", before_inv.total_rows, after_inv.total_rows),
    });

    let schema_ok = before_inv.schema_signatures.len() >= after_inv.schema_signatures.len()
        || after_inv.schema_signatures.len() <= 1;
    invariants.push(InvariantResult {
        name: "schema_preservation".into(),
        passed: schema_ok,
        detail: format!(
            "before_signatures={} after_signatures={}",
            before_inv.schema_signatures.len(),
            after_inv.schema_signatures.len()
        ),
    });

    invariants.push(InvariantResult {
        name: "output_readability".into(),
        passed: !after_inv.parquet_files.is_empty(),
        detail: format!("after_files={}", after_inv.parquet_files.len()),
    });

    if let Some(m) = manifest {
        let current = crate::fingerprint::compute_fingerprint_from_scan(after_root, after)?;
        let fp_match = current.digest == m.output_fingerprint.digest;
        invariants.push(InvariantResult {
            name: "manifest_fingerprint".into(),
            passed: fp_match,
            detail: format!("recorded={} current={}", m.output_fingerprint.digest, current.digest),
        });
    }

    let before_dx = diagnose(before_root, before, policy)?;
    let after_dx = diagnose(after_root, after, policy)?;

    let repair_codes = [
        system::REPAIR_EXCESSIVE_SMALL_FILES,
        system::REPAIR_INEFFICIENT_ROW_GROUPS,
        system::REPAIR_INCONSISTENT_COMPRESSION,
        system::REPAIR_SCHEMA_DRIFT,
        system::REPAIR_MISSING_STATISTICS,
        system::REPAIR_OVERSIZED_FILE,
        system::REPAIR_SCHEMA_CONFLICT,
    ];

    let before_set: std::collections::BTreeSet<_> =
        before_dx.findings.iter().map(|f| f.code.as_str().to_string()).collect();
    let after_set: std::collections::BTreeSet<_> =
        after_dx.findings.iter().map(|f| f.code.as_str().to_string()).collect();

    let resolved: Vec<String> = before_set
        .difference(&after_set)
        .filter(|c| repair_codes.contains(&c.as_str()))
        .cloned()
        .collect();
    let remaining: Vec<String> = after_set
        .intersection(&before_set)
        .filter(|c| repair_codes.contains(&c.as_str()))
        .cloned()
        .collect();
    let new_findings: Vec<String> = after_set.difference(&before_set).cloned().collect();
    let mut blocked: Vec<String> =
        after_set.iter().filter(|c| *c == system::REPAIR_SCHEMA_CONFLICT).cloned().collect();
    if let Some(m) = manifest {
        for op in &m.operations {
            if op.safety == RepairSafety::Blocked && !op.executed {
                blocked.push(format!("blocked:{}", op.operation_id));
            }
        }
        blocked.sort();
        blocked.dedup();
    }

    // Scan may discover benign format/metadata codes in repair output (manifest sidecars, etc.).
    let has_regression =
        new_findings.iter().any(|c| c.as_str() == system::FORMAT_PARQUET_READ_FAILED);

    let outcome = if !row_ok || has_regression {
        VerificationOutcome::Failed
    } else if !remaining.is_empty() || !blocked.is_empty() || !new_findings.is_empty() {
        VerificationOutcome::VerifiedWithWarnings
    } else {
        VerificationOutcome::Verified
    };

    Ok(VerificationReport {
        before_scan_id: before.metadata.scan_id,
        after_scan_id: after.metadata.scan_id,
        invariant_results: invariants,
        resolved_finding_codes: resolved,
        remaining_finding_codes: remaining,
        new_finding_codes: new_findings,
        blocked_finding_codes: blocked,
        outcome,
        manifest_plan_id: manifest.map(|m| m.plan_id.clone()),
        fingerprint_match: manifest.map(|m| {
            crate::fingerprint::compute_fingerprint_from_scan(after_root, after)
                .map(|fp| fp.digest == m.output_fingerprint.digest)
                .unwrap_or(false)
        }),
    })
}

pub fn scan_directory(path: &Utf8Path) -> Result<ScanReport, RepairError> {
    let target = if path.is_file() {
        ScanTarget::LocalFile { path: path.to_path_buf() }
    } else {
        ScanTarget::LocalDirectory { path: path.to_path_buf() }
    };
    let request = ScanRequest::new(target, ScanProfile::Standard);
    ScanEngine::run(&request).map_err(|e| RepairError::ScanFailed(e.to_string()))
}
