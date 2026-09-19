use camino::Utf8Path;
use parqonaut_repair::{
    location_root, scan_dataset, scan_directory, verify_repair, EffectivePolicy, ExecutionManifest,
    RepairBackend, ScanReport, VerificationOutcome,
};
use parqonaut_storage::location::DatasetLocation;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::OrchestratorError;
use crate::ids::RunId;
use crate::journal::DatasetRunRecord;
use crate::plan::BatchPlan;
use crate::state::DatasetState;
use crate::storage::{remote_output_committed, BatchStorageRuntime, block_on_async};

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
    verify_batch_with_run(plan, records, None)
}

pub fn verify_batch_with_run(
    plan: &BatchPlan,
    records: &[DatasetRunRecord],
    batch_run_id: Option<&RunId>,
) -> Result<BatchVerifyReport, OrchestratorError> {
    let storage = BatchStorageRuntime::new(plan.max_storage_requests);
    block_on_async(verify_batch_async(plan, records, &storage, batch_run_id))
}

async fn verify_batch_async(
    plan: &BatchPlan,
    records: &[DatasetRunRecord],
    storage: &BatchStorageRuntime,
    batch_run_id: Option<&RunId>,
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

        let policy = EffectivePolicy::from_toml(None)
            .map_err(|e| OrchestratorError::InvalidConfig(e.to_string()))?;

        let uses_remote =
            ds.source_path.starts_with("s3://") || ds.output_path.starts_with("s3://");

        let (before_scan, after_scan, source_root, output_root, manifest) = if uses_remote {
            let source = DatasetLocation::parse(&ds.source_path).map_err(storage_loc_err)?;
            let output = DatasetLocation::parse(&ds.output_path).map_err(storage_loc_err)?;
            let before = scan_location(storage, &source).await?;
            let after = scan_location(storage, &output).await?;
            let manifest = local_manifest_if_present(&output);
            (
                before,
                after,
                location_root(&source),
                location_root(&output),
                manifest,
            )
        } else {
            let source = Utf8Path::new(&ds.source_path);
            let output = Utf8Path::new(&ds.output_path);
            let before = scan_directory(source)?;
            let after = scan_directory(output)?;
            let manifest_path = output.join(".parqonaut-manifest.json");
            let manifest = if manifest_path.exists() {
                Some(ExecutionManifest::read_json(&manifest_path)?)
            } else {
                None
            };
            (before, after, source.to_path_buf(), output.to_path_buf(), manifest)
        };

        let report = verify_repair(
            &before_scan,
            &after_scan,
            &source_root,
            &output_root,
            &policy.repair,
            manifest.as_ref(),
        )?;

        let mut ok = matches!(
            report.outcome,
            VerificationOutcome::Verified | VerificationOutcome::VerifiedWithWarnings
        );

        if ok && ds.output_path.starts_with("s3://") {
            let output = DatasetLocation::parse(&ds.output_path).map_err(storage_loc_err)?;
            let run_id = batch_run_id.map(|r| r.0.as_str()).unwrap_or("batch-run");
            if !remote_output_committed(storage, &output, run_id).await? {
                ok = false;
            }
        }

        if ok {
            passed += 1;
        } else {
            failed += 1;
        }
        datasets.push(DatasetVerifyResult {
            dataset_id: ds.dataset_id.0.clone(),
            outcome: if ok { report.outcome } else { VerificationOutcome::Failed },
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

fn local_manifest_if_present(output: &DatasetLocation) -> Option<ExecutionManifest> {
    let DatasetLocation::Local(local) = output else {
        return None;
    };
    let manifest_path = local.path.join(".parqonaut-manifest.json");
    ExecutionManifest::read_json(&manifest_path).ok()
}

async fn scan_location(
    storage: &BatchStorageRuntime,
    location: &DatasetLocation,
) -> Result<ScanReport, OrchestratorError> {
    match storage.repair_backend_for(location) {
        RepairBackend::Local(b) => scan_dataset(location, &b).await.map_err(Into::into),
        RepairBackend::Memory(b) => scan_dataset(location, &b).await.map_err(Into::into),
        #[cfg(feature = "s3")]
        RepairBackend::S3(b) => scan_dataset(location, b.as_ref()).await.map_err(Into::into),
    }
}

fn storage_loc_err(e: parqonaut_storage::error::StorageError) -> OrchestratorError {
    OrchestratorError::InvalidConfig(e.to_string())
}

pub fn verify_results_map(report: &BatchVerifyReport) -> HashMap<String, DatasetVerifyResult> {
    report.datasets.iter().map(|d| (d.dataset_id.clone(), d.clone())).collect()
}
