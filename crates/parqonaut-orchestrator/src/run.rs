use std::fs;
use std::sync::Arc;

use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use parqonaut_repair::{RepairExecutor, PARQONAUT_VERSION};
use parqonaut_storage::location::DatasetLocation;
use serde::{Deserialize, Serialize};

use crate::auth::{assert_datasets_match_plan, assert_plan_matches_identity};
use crate::cancel::CancelFlag;
use crate::check::{batch_check, ensure_output_root, ensure_run_root, BatchCheckReport};
use crate::error::OrchestratorError;
use crate::executor::{
    BatchExecutor, BatchExecutorConfig, BatchRunOutcome, DryRunReport, ExecutionMode,
};
use crate::ids::RunId;
use crate::journal::{DatasetRunRecord, RunIdentity, RunJournal, SqliteRunJournal};
use crate::plan::{build_batch_plan, BatchPlan};
use crate::report::{
    build_aggregate_report, BatchVerificationSummary, BATCH_REPORT_SCHEMA_VERSION,
};
use crate::verify::{verify_batch, verify_batch_with_run, verify_results_map, BatchVerifyReport};

pub const RUNS_DIR: &str = ".parqonaut/runs";
pub const JOURNAL_FILE: &str = "journal.sqlite";
pub const PLAN_SNAPSHOT: &str = "batch-plan.json";
pub const REPORT_FILE: &str = "report.json";

#[derive(Debug, Clone)]
pub struct BatchRunOptions {
    pub dry_run: bool,
    pub jobs: Option<u32>,
    pub cancel: CancelFlag,
    pub interrupt_after_completed: Option<usize>,
}

impl Default for BatchRunOptions {
    fn default() -> Self {
        Self {
            dry_run: false,
            jobs: None,
            cancel: CancelFlag::new(),
            interrupt_after_completed: None,
        }
    }
}

impl BatchRunOptions {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if self.jobs == Some(0) {
            return Err(OrchestratorError::InvalidConfig(
                "jobs must be >= 1 when specified".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchStatusReport {
    pub run_id: String,
    pub batch_plan_id: String,
    pub batch_name: String,
    pub output_root: String,
    pub config_fingerprint: String,
    pub plan_digest: String,
    pub started_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub cancelled_at: Option<String>,
    pub state_counts: crate::report::StateCounts,
    pub datasets: Vec<DatasetRunRecord>,
    pub resumable: bool,
    pub completed: bool,
}

pub fn run_directory(run_root: &Utf8Path, run_id: &RunId) -> Utf8PathBuf {
    run_root.join(RUNS_DIR).join(&run_id.0)
}

pub fn check_config(config_path: &Utf8Path) -> BatchCheckReport {
    batch_check(config_path)
}

pub fn plan_batch(config_path: &Utf8Path) -> Result<BatchPlan, OrchestratorError> {
    let config = crate::config::BatchConfig::from_toml_path(config_path)?;
    build_batch_plan(&config, config_path)
}

pub fn write_plan(plan: &BatchPlan, path: &Utf8Path) -> Result<(), OrchestratorError> {
    plan.write_json(path)
}

pub async fn execute_batch(
    plan_path: &Utf8Path,
    options: BatchRunOptions,
) -> Result<BatchExecutionResult, OrchestratorError> {
    let plan = BatchPlan::from_json(&fs::read(plan_path.as_std_path())?)?;
    execute_batch_plan(plan, options).await
}

pub async fn resume_batch(
    run_dir: &Utf8Path,
    options: BatchRunOptions,
) -> Result<BatchExecutionResult, OrchestratorError> {
    options.validate()?;
    let plan_path = run_dir.join(PLAN_SNAPSHOT);
    let plan = BatchPlan::from_json(&fs::read(plan_path.as_std_path())?)?;
    let journal_path = run_dir.join(JOURNAL_FILE);
    let journal = SqliteRunJournal::open_existing_sync(&journal_path)?;
    let identity = journal
        .run_identity()?
        .ok_or_else(|| OrchestratorError::Journal("missing run identity".into()))?;
    assert_plan_matches_identity(&plan, &identity)?;
    let records = journal.list_datasets()?;
    assert_datasets_match_plan(&plan, &records)?;

    if options.dry_run {
        let executor =
            BatchExecutor::new(RepairExecutor::default(), dry_run_config(&plan, &options));
        let dry = executor.dry_run(&plan).await?;
        return Ok(BatchExecutionResult {
            run_id: identity.run_id.clone(),
            run_dir: run_dir.to_path_buf(),
            outcome: None,
            dry_run: Some(dry),
            already_completed: false,
        });
    }

    if run_is_complete(&plan, &records) {
        if identity.completed_at.is_none() {
            journal.mark_run_completed(Utc::now())?;
        }
        return Ok(BatchExecutionResult {
            run_id: identity.run_id.clone(),
            run_dir: run_dir.to_path_buf(),
            outcome: None,
            dry_run: None,
            already_completed: true,
        });
    }

    execute_with_journal(plan, identity.run_id.clone(), run_dir.to_path_buf(), options, true).await
}

pub async fn execute_batch_plan(
    plan: BatchPlan,
    options: BatchRunOptions,
) -> Result<BatchExecutionResult, OrchestratorError> {
    options.validate()?;
    if options.dry_run {
        let executor =
            BatchExecutor::new(RepairExecutor::default(), dry_run_config(&plan, &options));
        let dry = executor.dry_run(&plan).await?;
        return Ok(BatchExecutionResult {
            run_id: RunId::new(),
            run_dir: Utf8PathBuf::from("."),
            outcome: None,
            dry_run: Some(dry),
            already_completed: false,
        });
    }

    let run_root = plan.effective_run_root();
    ensure_run_root(&run_root)?;
    if let Ok(output_root) = DatasetLocation::parse(&plan.output_root) {
        ensure_output_root(&output_root)?;
    }
    let run_id = RunId::new();
    let run_dir = run_directory(&run_root, &run_id);
    fs::create_dir_all(run_dir.as_std_path())?;
    plan.write_json(&run_dir.join(PLAN_SNAPSHOT))?;
    execute_with_journal(plan, run_id, run_dir, options, false).await
}

async fn execute_with_journal(
    plan: BatchPlan,
    run_id: RunId,
    run_dir: Utf8PathBuf,
    options: BatchRunOptions,
    resume: bool,
) -> Result<BatchExecutionResult, OrchestratorError> {
    let now = Utc::now();
    let identity = RunIdentity {
        run_id: run_id.clone(),
        batch_plan_id: plan.batch_plan_id.clone(),
        config_fingerprint: plan.config_fingerprint.clone(),
        batch_name: plan.batch_name.clone(),
        output_root: plan.output_root.clone(),
        parqonaut_version: PARQONAUT_VERSION.to_string(),
        plan_digest: plan.plan_digest(),
        started_at: now,
        updated_at: now,
        completed_at: None,
        cancelled_at: None,
    };

    let journal_path = run_dir.join(JOURNAL_FILE);
    let journal: Arc<dyn RunJournal> = if resume {
        Arc::new(SqliteRunJournal::open_existing_sync(&journal_path)?)
    } else {
        Arc::new(SqliteRunJournal::open(&journal_path, &identity).await?)
    };

    if resume {
        let stored = journal
            .run_identity()?
            .ok_or_else(|| OrchestratorError::Journal("missing run identity".into()))?;
        assert_plan_matches_identity(&plan, &stored)?;
        let records = journal.list_datasets()?;
        assert_datasets_match_plan(&plan, &records)?;
        recover_journal_states(&journal)?;
    }

    let exec_config = BatchExecutorConfig {
        interrupt_after_completed: options.interrupt_after_completed,
        max_storage_requests: plan.max_storage_requests,
        ..BatchExecutorConfig::default()
    };

    let executor = BatchExecutor::new(
        RepairExecutor { verify_before_publish: true, ..RepairExecutor::default() },
        exec_config,
    );

    let mode = if resume { ExecutionMode::Resume } else { ExecutionMode::Fresh };

    let effective_jobs = options.jobs.unwrap_or(plan.max_concurrency);
    let mut plan = plan;
    plan.max_concurrency = effective_jobs;

    let outcome = executor
        .run(&plan, &run_id, journal.clone(), mode, options.cancel.clone(), options.dry_run)
        .await?;

    let records = journal.list_datasets()?;
    if options.cancel.is_cancelled() {
        journal.mark_run_cancelled(Utc::now())?;
    } else if run_is_complete(&plan, &records) {
        journal.mark_run_completed(Utc::now())?;
    }

    let verify = verify_batch(&plan, &records).ok();
    let verify_map = verify.as_ref().map(verify_results_map).unwrap_or_default();
    let identity = journal.run_identity()?.unwrap_or(identity);
    let verification_summary = verify.as_ref().map(|v| BatchVerificationSummary {
        ok: v.ok,
        passed: v.passed,
        failed: v.failed,
        skipped: v.skipped,
    });
    let report =
        build_aggregate_report(&plan, &identity, &records, verification_summary, verify_map);
    report.write_json(&run_dir.join(REPORT_FILE))?;

    Ok(BatchExecutionResult {
        run_id,
        run_dir,
        outcome: Some(outcome),
        dry_run: None,
        already_completed: false,
    })
}

fn recover_journal_states(journal: &Arc<dyn RunJournal>) -> Result<(), OrchestratorError> {
    for mut record in journal.list_datasets()? {
        let from = record.state;
        let recovered = from.recover_after_crash();
        if recovered != from {
            record.state = recovered;
            record.error_class = Some(crate::error::FailureClass::Recoverable);
            record.error_message = Some(format!("recovered after interruption from {from:?}"));
            record.updated_at = Utc::now();
            journal.upsert_dataset(&record)?;
        }
    }
    Ok(())
}

fn run_is_complete(plan: &BatchPlan, records: &[DatasetRunRecord]) -> bool {
    plan.datasets.iter().all(|ds| {
        records
            .iter()
            .find(|r| r.dataset_id == ds.dataset_id)
            .map(|r| r.state.is_terminal())
            .unwrap_or(false)
    })
}

pub fn read_status(run_dir: &Utf8Path) -> Result<BatchStatusReport, OrchestratorError> {
    let journal_path = run_dir.join(JOURNAL_FILE);
    let journal = SqliteRunJournal::open_existing_sync(&journal_path)?;
    let identity = journal
        .run_identity()?
        .ok_or_else(|| OrchestratorError::Journal("missing run identity".into()))?;
    let datasets = journal.list_datasets()?;
    let plan = BatchPlan::from_json(&fs::read(run_dir.join(PLAN_SNAPSHOT).as_std_path())?)?;
    let state_counts = crate::report::build_aggregate_report(
        &placeholder_plan(&identity),
        &identity,
        &datasets,
        None,
        Default::default(),
    )
    .state_counts;
    let completed = run_is_complete(&plan, &datasets);
    let resumable = !completed
        && (identity.cancelled_at.is_some()
            || datasets.len() < plan.datasets.len()
            || datasets.iter().any(|d| {
                d.state.is_runnable()
                    || d.state == crate::state::DatasetState::FailedRecoverable
                    || d.state == crate::state::DatasetState::Cancelled
            }));
    Ok(BatchStatusReport {
        run_id: identity.run_id.0,
        batch_plan_id: identity.batch_plan_id.0,
        batch_name: identity.batch_name,
        output_root: identity.output_root,
        config_fingerprint: identity.config_fingerprint,
        plan_digest: identity.plan_digest,
        started_at: identity.started_at.to_rfc3339(),
        updated_at: identity.updated_at.to_rfc3339(),
        completed_at: identity.completed_at.map(|t| t.to_rfc3339()),
        cancelled_at: identity.cancelled_at.map(|t| t.to_rfc3339()),
        state_counts,
        datasets,
        resumable,
        completed,
    })
}

pub fn verify_run(run_dir: &Utf8Path) -> Result<BatchVerifyReport, OrchestratorError> {
    let plan = BatchPlan::from_json(&fs::read(run_dir.join(PLAN_SNAPSHOT).as_std_path())?)?;
    let journal = SqliteRunJournal::open_existing_sync(&run_dir.join(JOURNAL_FILE))?;
    let identity = journal
        .run_identity()?
        .ok_or_else(|| OrchestratorError::Journal("missing run identity".into()))?;
    assert_plan_matches_identity(&plan, &identity)?;
    let records = journal.list_datasets()?;
    assert_datasets_match_plan(&plan, &records)?;
    verify_batch_with_run(&plan, &records, Some(&identity.run_id))
}

fn placeholder_plan(identity: &RunIdentity) -> BatchPlan {
    BatchPlan {
        schema_version: BATCH_REPORT_SCHEMA_VERSION,
        batch_plan_id: identity.batch_plan_id.clone(),
        config_fingerprint: identity.config_fingerprint.clone(),
        batch_name: identity.batch_name.clone(),
        max_concurrency: 1,
        max_storage_requests: 8,
        output_root: identity.output_root.clone(),
        run_root: identity.output_root.clone(),
        parqonaut_version: identity.parqonaut_version.clone(),
        datasets: vec![],
    }
}

fn dry_run_config(plan: &BatchPlan, options: &BatchRunOptions) -> BatchExecutorConfig {
    BatchExecutorConfig {
        interrupt_after_completed: options.interrupt_after_completed,
        max_storage_requests: plan.max_storage_requests,
        ..BatchExecutorConfig::default()
    }
}

#[derive(Debug, Clone)]
pub struct BatchExecutionResult {
    pub run_id: RunId,
    pub run_dir: Utf8PathBuf,
    pub outcome: Option<BatchRunOutcome>,
    pub dry_run: Option<DryRunReport>,
    pub already_completed: bool,
}
