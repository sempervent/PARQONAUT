use camino::Utf8Path;
use parqonaut_repair::{
    scan_directory, verify_repair, EffectivePolicy, ExecutionManifest, VerificationOutcome,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::OrchestratorError;
use crate::journal::DatasetRunRecord;
use crate::plan::BatchPlan;
use crate::state::DatasetState;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetVerifyResult {
    pub dataset_id: String,
    pub outcome: VerificationOutcome,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchVerifyReport {
    pub ok: bool,
    pub datasets: Vec<DatasetVerifyResult>,
    pub passed: u32,
    pub failed: u32,
    pub skipped: u32,
}

pub fn verify_batch(
    plan: &BatchPlan,
    records: &[DatasetRunRecord],
) -> Result<BatchVerifyReport, OrchestratorError> {
    plan.validate_version()?;
    let mut datasets = Vec::new();
    let mut passed = 0u32;
    let mut failed = 0u32;
    let mut skipped = 0u32;

    for ds in &plan.datasets {
        let record = records.iter().find(|r| r.dataset_id == ds.dataset_id).ok_or_else(|| {
            OrchestratorError::InvalidConfig(format!(
                "missing journal record for dataset `{}`",
                ds.dataset_id.0
            ))
        })?;

        if record.state != DatasetState::Succeeded {
            skipped += 1;
            datasets.push(DatasetVerifyResult {
                dataset_id: ds.dataset_id.0.clone(),
                outcome: VerificationOutcome::Failed,
                message: format!("dataset state is {:?}, expected succeeded", record.state),
            });
            continue;
        }

        let source = Utf8Path::new(&ds.source_path);
        let output = Utf8Path::new(&ds.output_path);
        let policy = EffectivePolicy::from_toml(None)
            .map_err(|e| OrchestratorError::InvalidConfig(e.to_string()))?;
        let before_scan = scan_directory(source)?;
        let after_scan = scan_directory(output)?;
        let manifest_path = output.join(".parqonaut-manifest.json");
        let manifest = if manifest_path.exists() {
            Some(ExecutionManifest::read_json(&manifest_path)?)
        } else {
            None
        };
        let report = verify_repair(
            &before_scan,
            &after_scan,
            source,
            output,
            &policy.repair,
            manifest.as_ref(),
        )?;
        let ok = matches!(
            report.outcome,
            VerificationOutcome::Verified | VerificationOutcome::VerifiedWithWarnings
        );
        if ok {
            passed += 1;
        } else {
            failed += 1;
        }
        datasets.push(DatasetVerifyResult {
            dataset_id: ds.dataset_id.0.clone(),
            outcome: report.outcome,
            message: format!(
                "resolved={} remaining={} new={}",
                report.resolved_finding_codes.len(),
                report.remaining_finding_codes.len(),
                report.new_finding_codes.len()
            ),
        });
    }

    Ok(BatchVerifyReport { ok: failed == 0, datasets, passed, failed, skipped })
}

pub fn verify_results_map(report: &BatchVerifyReport) -> HashMap<String, DatasetVerifyResult> {
    report.datasets.iter().map(|d| (d.dataset_id.clone(), d.clone())).collect()
}
