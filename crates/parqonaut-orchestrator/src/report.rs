use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::FailureClass;
use crate::journal::{DatasetRunRecord, RunIdentity};
use crate::plan::BatchPlan;
use crate::state::DatasetState;
use crate::verify::DatasetVerifyResult;

pub const BATCH_REPORT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchAggregateReport {
    pub schema_version: u32,
    pub run_id: String,
    pub batch_plan_id: String,
    pub config_fingerprint: String,
    pub plan_digest: String,
    pub output_root: String,
    pub parqonaut_version: String,
    pub executor_version: String,
    pub max_concurrency: u32,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub dataset_count: usize,
    pub state_counts: StateCounts,
    pub datasets: Vec<DatasetReportEntry>,
    pub verification: Option<BatchVerificationSummary>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StateCounts {
    pub pending: u32,
    pub running: u32,
    pub succeeded: u32,
    pub failed_recoverable: u32,
    pub failed_permanent: u32,
    pub blocked: u32,
    pub cancelled: u32,
    pub stale_source: u32,
    pub verification_failed: u32,
    pub other: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetReportEntry {
    pub dataset_id: String,
    pub output_path: String,
    pub state: DatasetState,
    pub attempts: u32,
    pub repair_plan_id: String,
    pub policy_fingerprint: String,
    pub source_fingerprint: Option<String>,
    pub error_class: Option<FailureClass>,
    pub error_message: Option<String>,
    pub manifest_path: Option<String>,
    pub verification: Option<DatasetVerifyResult>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchVerificationSummary {
    pub ok: bool,
    pub passed: u32,
    pub failed: u32,
    pub skipped: u32,
}

pub fn build_aggregate_report(
    plan: &BatchPlan,
    identity: &RunIdentity,
    records: &[DatasetRunRecord],
    verification: Option<BatchVerificationSummary>,
    verify_by_dataset: std::collections::HashMap<String, DatasetVerifyResult>,
) -> BatchAggregateReport {
    let state_counts = count_states(records);
    let datasets = records
        .iter()
        .map(|r| DatasetReportEntry {
            dataset_id: r.dataset_id.0.clone(),
            output_path: r.output_path.clone(),
            state: r.state,
            attempts: r.attempts,
            repair_plan_id: r.repair_plan_id.clone(),
            policy_fingerprint: r.policy_fingerprint.clone(),
            source_fingerprint: r.source_fingerprint.clone(),
            error_class: r.error_class,
            error_message: r.error_message.clone(),
            manifest_path: if r.state == DatasetState::Succeeded {
                Some(format!("{}/.parqonaut-manifest.json", r.output_path.trim_end_matches('/')))
            } else {
                None
            },
            verification: verify_by_dataset.get(&r.dataset_id.0).cloned(),
        })
        .collect();

    BatchAggregateReport {
        schema_version: BATCH_REPORT_SCHEMA_VERSION,
        run_id: identity.run_id.0.clone(),
        batch_plan_id: identity.batch_plan_id.0.clone(),
        config_fingerprint: identity.config_fingerprint.clone(),
        plan_digest: identity.plan_digest.clone(),
        output_root: identity.output_root.clone(),
        parqonaut_version: plan.parqonaut_version.clone(),
        executor_version: plan.parqonaut_version.clone(),
        max_concurrency: plan.max_concurrency,
        started_at: identity.started_at,
        completed_at: identity.completed_at,
        cancelled_at: identity.cancelled_at,
        dataset_count: plan.datasets.len(),
        state_counts,
        datasets,
        verification,
    }
}

fn count_states(records: &[DatasetRunRecord]) -> StateCounts {
    let mut counts = StateCounts::default();
    for r in records {
        match r.state {
            DatasetState::Pending | DatasetState::PlanningValidated => counts.pending += 1,
            DatasetState::Running | DatasetState::RepairComplete | DatasetState::Verifying => {
                counts.running += 1
            }
            DatasetState::Succeeded => counts.succeeded += 1,
            DatasetState::FailedRecoverable => counts.failed_recoverable += 1,
            DatasetState::FailedPermanent => counts.failed_permanent += 1,
            DatasetState::Blocked => counts.blocked += 1,
            DatasetState::Cancelled => counts.cancelled += 1,
            DatasetState::StaleSource => counts.stale_source += 1,
            DatasetState::VerificationFailed => counts.verification_failed += 1,
        }
    }
    counts
}

impl BatchAggregateReport {
    pub fn write_json(
        &self,
        path: &camino::Utf8Path,
    ) -> Result<(), crate::error::OrchestratorError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent.as_std_path())?;
        }
        std::fs::write(path.as_std_path(), serde_json::to_string_pretty(self)?)?;
        Ok(())
    }

    pub fn exit_code(&self) -> i32 {
        if self.cancelled_at.is_some() {
            return 130;
        }
        if self.state_counts.succeeded == self.dataset_count as u32 {
            return 0;
        }
        if self.state_counts.succeeded > 0 {
            return 2;
        }
        1
    }
}
