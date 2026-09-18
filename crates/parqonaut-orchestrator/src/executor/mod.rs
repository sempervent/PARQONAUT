#![allow(clippy::too_many_arguments)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use camino::Utf8Path;
use chrono::Utc;
use parqonaut_repair::{
    compute_dataset_fingerprint, scan_directory, DatasetInventory, RepairAuthorization,
    RepairError, RepairExecutor,
};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use crate::cancel::CancelFlag;
use crate::error::{FailureClass, OrchestratorError};
use crate::ids::{DatasetId, RunId};
use crate::journal::{DatasetRunRecord, RunJournal};
use crate::lock::DatasetLock;
use crate::overlap::validate_batch_plan;
use crate::plan::{BatchPlan, DatasetPlan};
use crate::scheduler::{BatchScheduler, ConcurrencyMetrics};
use crate::state::DatasetState;

pub const DEFAULT_MAX_RETRIES: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Fresh,
    Resume,
    DryRun,
}

#[derive(Debug, Clone)]
pub struct BatchExecutorConfig {
    pub max_retries: u32,
    pub interrupt_after_completed: Option<usize>,
}

impl Default for BatchExecutorConfig {
    fn default() -> Self {
        Self { max_retries: DEFAULT_MAX_RETRIES, interrupt_after_completed: None }
    }
}

pub struct BatchExecutor {
    pub config: BatchExecutorConfig,
    pub repair: RepairExecutor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetExecutionOutcome {
    pub dataset_id: DatasetId,
    pub state: DatasetState,
    pub attempts: u32,
    pub error_class: Option<FailureClass>,
    pub error_message: Option<String>,
    pub skipped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRunOutcome {
    pub peak_concurrent_datasets: usize,
    pub datasets: Vec<DatasetExecutionOutcome>,
    pub cancelled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DryRunDatasetPreview {
    pub dataset_id: String,
    pub source_path: String,
    pub output_path: String,
    pub action: String,
    pub repair_plan_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DryRunReport {
    pub batch_plan_id: String,
    pub dataset_count: usize,
    pub max_concurrency: u32,
    pub datasets: Vec<DryRunDatasetPreview>,
    pub creates_journal: bool,
}

impl BatchExecutor {
    pub fn new(repair: RepairExecutor, config: BatchExecutorConfig) -> Self {
        Self { repair, config }
    }

    pub async fn dry_run(&self, plan: &BatchPlan) -> Result<DryRunReport, OrchestratorError> {
        plan.validate_version()?;
        validate_batch_plan(plan)?;
        let datasets = plan
            .datasets
            .iter()
            .map(|ds| DryRunDatasetPreview {
                dataset_id: ds.dataset_id.0.clone(),
                source_path: ds.source_path.clone(),
                output_path: ds.output_path.clone(),
                action: "repair".into(),
                repair_plan_id: ds.repair_plan.plan_id.clone(),
            })
            .collect();
        Ok(DryRunReport {
            batch_plan_id: plan.batch_plan_id.0.clone(),
            dataset_count: plan.datasets.len(),
            max_concurrency: plan.max_concurrency,
            datasets,
            creates_journal: false,
        })
    }

    #[instrument(skip(self, plan, journal), fields(run_id = %run_id.0, datasets = plan.datasets.len()))]
    pub async fn run(
        &self,
        plan: &BatchPlan,
        run_id: &RunId,
        journal: Arc<dyn RunJournal>,
        mode: ExecutionMode,
        cancel: CancelFlag,
        dry_run: bool,
    ) -> Result<BatchRunOutcome, OrchestratorError> {
        if dry_run {
            return Ok(BatchRunOutcome {
                peak_concurrent_datasets: 0,
                datasets: vec![],
                cancelled: false,
            });
        }

        plan.validate_version()?;
        validate_batch_plan(plan)?;

        let existing = journal.list_datasets()?;
        let scheduler = BatchScheduler::new(plan.max_concurrency);
        let metrics = Arc::new(ConcurrencyMetrics::default());
        let run_id = run_id.clone();
        let config = self.config.clone();
        let repair = clone_repair_executor(&self.repair);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for dataset in &plan.datasets {
            if cancel.is_cancelled() {
                break;
            }
            let prior = existing.iter().find(|r| r.dataset_id == dataset.dataset_id);
            if mode == ExecutionMode::Resume {
                if let Some(record) = prior {
                    if record.state == DatasetState::Succeeded {
                        continue;
                    }
                    if !record.state.is_runnable() && record.state.is_terminal() {
                        continue;
                    }
                }
            }

            let dataset = dataset.clone();
            let run_id = run_id.clone();
            let journal = Arc::clone(&journal);
            let config = config.clone();
            let repair = clone_repair_executor(&repair);
            let cancel = cancel.clone();
            let completed_counter = Arc::clone(&completed_counter);
            let prior_state = prior.map(|r| r.state);

            tasks.push(move || async move {
                if cancel.is_cancelled() {
                    return skipped_outcome(&dataset.dataset_id, prior_state);
                }
                execute_dataset_with_retries(
                    &dataset,
                    &run_id,
                    journal,
                    &repair,
                    &config,
                    prior_state,
                    cancel,
                    completed_counter,
                )
                .await
            });
        }

        let mut dataset_outcomes =
            scheduler.run_bounded_cancellable(tasks, metrics.clone(), cancel.clone()).await;

        for ds in &plan.datasets {
            if !dataset_outcomes.iter().any(|o| o.dataset_id == ds.dataset_id) {
                if let Some(record) = existing.iter().find(|r| r.dataset_id == ds.dataset_id) {
                    if record.state == DatasetState::Succeeded {
                        dataset_outcomes.push(DatasetExecutionOutcome {
                            dataset_id: ds.dataset_id.clone(),
                            state: DatasetState::Succeeded,
                            attempts: record.attempts,
                            error_class: None,
                            error_message: None,
                            skipped: true,
                        });
                    } else if record.state.is_terminal() {
                        dataset_outcomes.push(DatasetExecutionOutcome {
                            dataset_id: ds.dataset_id.clone(),
                            state: record.state,
                            attempts: record.attempts,
                            error_class: record.error_class,
                            error_message: record.error_message.clone(),
                            skipped: true,
                        });
                    }
                }
            }
        }

        Ok(BatchRunOutcome {
            peak_concurrent_datasets: metrics.peak_concurrent(),
            datasets: dataset_outcomes,
            cancelled: cancel.is_cancelled(),
        })
    }
}

fn skipped_outcome(id: &DatasetId, prior: Option<DatasetState>) -> DatasetExecutionOutcome {
    DatasetExecutionOutcome {
        dataset_id: id.clone(),
        state: prior.unwrap_or(DatasetState::Cancelled),
        attempts: 0,
        error_class: if prior.is_none() { Some(FailureClass::Cancelled) } else { None },
        error_message: if prior.is_none() {
            Some("batch run cancelled before start".into())
        } else {
            None
        },
        skipped: true,
    }
}

fn clone_repair_executor(repair: &RepairExecutor) -> RepairExecutor {
    RepairExecutor {
        authorization: repair.authorization.clone(),
        verify_before_publish: repair.verify_before_publish,
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_dataset_with_retries(
    dataset: &DatasetPlan,
    run_id: &RunId,
    journal: Arc<dyn RunJournal>,
    repair: &RepairExecutor,
    config: &BatchExecutorConfig,
    prior_state: Option<DatasetState>,
    cancel: CancelFlag,
    completed_counter: Arc<AtomicUsize>,
) -> DatasetExecutionOutcome {
    let dataset_id = dataset.dataset_id.clone();
    let mut attempts = journal.dataset(&dataset_id).ok().flatten().map(|r| r.attempts).unwrap_or(0);
    let max_attempts = config.max_retries.saturating_add(1);

    loop {
        if cancel.is_cancelled() {
            upsert_record(
                &journal,
                dataset,
                DatasetState::Cancelled,
                attempts,
                None,
                Some(FailureClass::Cancelled),
                Some("batch run cancelled".into()),
                None,
                Some(Utc::now()),
            );
            return DatasetExecutionOutcome {
                dataset_id,
                state: DatasetState::Cancelled,
                attempts,
                error_class: Some(FailureClass::Cancelled),
                error_message: Some("batch run cancelled".into()),
                skipped: false,
            };
        }

        attempts += 1;
        let started = Utc::now();
        if prior_state != Some(DatasetState::Succeeded) {
            upsert_record(
                &journal,
                dataset,
                DatasetState::PlanningValidated,
                attempts,
                None,
                None,
                None,
                Some(started),
                None,
            );
        }

        match execute_dataset_once(dataset, run_id, journal.clone(), repair, attempts, started)
            .await
        {
            Ok(outcome) => {
                info!(dataset = %dataset.dataset_id.0, attempts, "dataset execution finished");
                if outcome.state == DatasetState::Succeeded {
                    let done = completed_counter.fetch_add(1, Ordering::SeqCst) + 1;
                    if config.interrupt_after_completed == Some(done) {
                        cancel.cancel();
                    }
                }
                return outcome;
            }
            Err(retryable)
                if retryable.class == FailureClass::Recoverable && attempts < max_attempts =>
            {
                warn!(
                    dataset = %dataset.dataset_id.0,
                    attempts,
                    max_attempts,
                    error = %retryable.message,
                    "retrying recoverable dataset failure"
                );
                upsert_record(
                    &journal,
                    dataset,
                    DatasetState::FailedRecoverable,
                    attempts,
                    None,
                    Some(retryable.class),
                    Some(retryable.message.clone()),
                    None,
                    None,
                );
            }
            Err(final_failure) => {
                return DatasetExecutionOutcome {
                    dataset_id: dataset.dataset_id.clone(),
                    state: failure_state(final_failure.class),
                    attempts,
                    error_class: Some(final_failure.class),
                    error_message: Some(final_failure.message),
                    skipped: false,
                };
            }
        }
    }
}

struct DatasetFailure {
    class: FailureClass,
    message: String,
}

#[allow(clippy::too_many_arguments)]
async fn execute_dataset_once(
    dataset: &DatasetPlan,
    run_id: &RunId,
    journal: Arc<dyn RunJournal>,
    repair: &RepairExecutor,
    attempts: u32,
    started: chrono::DateTime<Utc>,
) -> Result<DatasetExecutionOutcome, DatasetFailure> {
    let output = Utf8Path::new(&dataset.output_path);
    let _lock = match DatasetLock::acquire(output, run_id) {
        Ok(lock) => lock,
        Err(err) => {
            let message = err.to_string();
            let class = FailureClass::Permanent;
            upsert_record(
                &journal,
                dataset,
                failure_state(class),
                attempts,
                None,
                Some(class),
                Some(message.clone()),
                Some(started),
                Some(Utc::now()),
            );
            return Err(DatasetFailure { class, message });
        }
    };

    let source = Utf8Path::new(&dataset.source_path);
    let scan = match scan_directory(source) {
        Ok(scan) => scan,
        Err(err) => {
            return fail_dataset(journal, dataset, attempts, started, &err);
        }
    };

    let current_fp = match DatasetInventory::from_scan_report(source, &scan)
        .and_then(|inventory| compute_dataset_fingerprint(source, &inventory))
    {
        Ok(fp) => fp,
        Err(err) => {
            return fail_dataset(journal, dataset, attempts, started, &err);
        }
    };

    if let Err(err) = dataset.repair_plan.dataset_fingerprint.verify_against(&current_fp) {
        let class = FailureClass::StaleSource;
        let message = err.to_string();
        upsert_record(
            &journal,
            dataset,
            DatasetState::StaleSource,
            attempts,
            Some(current_fp.digest.clone()),
            Some(class),
            Some(message.clone()),
            Some(started),
            Some(Utc::now()),
        );
        return Err(DatasetFailure { class, message });
    }

    upsert_record(
        &journal,
        dataset,
        DatasetState::Running,
        attempts,
        Some(current_fp.digest.clone()),
        None,
        None,
        Some(started),
        None,
    );

    let mut executor = clone_repair_executor(repair);
    executor.authorization = RepairAuthorization::from_ids(dataset.authorize.clone());
    let repair_plan = dataset.repair_plan.clone();
    let source_digest = repair_plan.dataset_fingerprint.digest.clone();
    let output_path = output.to_path_buf();
    let dataset_id = dataset.dataset_id.clone();
    let repair_plan_for_exec = repair_plan.clone();

    let execution = tokio::task::spawn_blocking(move || {
        executor.execute(&repair_plan_for_exec, &output_path, &scan)
    })
    .await
    .map_err(|err| DatasetFailure {
        class: FailureClass::Permanent,
        message: format!("dataset task join error: {err}"),
    })?;

    match execution {
        Ok(report) => {
            let completed = Utc::now();
            upsert_record(
                &journal,
                &DatasetPlan {
                    dataset_id: dataset_id.clone(),
                    source_path: dataset.source_path.clone(),
                    output_path: dataset.output_path.clone(),
                    repair_plan: repair_plan.clone(),
                    authorize: dataset.authorize.clone(),
                },
                DatasetState::Succeeded,
                attempts,
                Some(source_digest),
                None,
                None,
                Some(started),
                Some(completed),
            );
            info!(
                dataset = %dataset_id.0,
                output = %report.output_path,
                "dataset repair succeeded"
            );
            Ok(DatasetExecutionOutcome {
                dataset_id,
                state: DatasetState::Succeeded,
                attempts,
                error_class: None,
                error_message: None,
                skipped: false,
            })
        }
        Err(err) => fail_dataset(journal, dataset, attempts, started, &err),
    }
}

fn fail_dataset(
    journal: Arc<dyn RunJournal>,
    dataset: &DatasetPlan,
    attempts: u32,
    started: chrono::DateTime<Utc>,
    err: &RepairError,
) -> Result<DatasetExecutionOutcome, DatasetFailure> {
    let class = classify_repair_error(err);
    let message = err.to_string();
    upsert_record(
        &journal,
        dataset,
        failure_state(class),
        attempts,
        None,
        Some(class),
        Some(message.clone()),
        Some(started),
        Some(Utc::now()),
    );
    Err(DatasetFailure { class, message })
}

fn classify_repair_error(err: &RepairError) -> FailureClass {
    match err {
        RepairError::DatasetChanged { .. } => FailureClass::StaleSource,
        RepairError::ReviewRequiredNotAuthorized { .. }
        | RepairError::DestructiveNotAllowed { .. } => FailureClass::Blocked,
        RepairError::VerificationInvariantFailed { .. } => FailureClass::VerificationFailed,
        RepairError::PartialExecution { .. }
        | RepairError::Io(_)
        | RepairError::TransformFailed(_) => FailureClass::Recoverable,
        RepairError::OutputExists(_)
        | RepairError::SourceDestinationOverlap(_)
        | RepairError::UnsupportedOperation { .. }
        | RepairError::PreconditionFailed { .. }
        | RepairError::DatasetUnreadable(_)
        | RepairError::ScanFailed(_)
        | RepairError::InvalidPlan(_)
        | RepairError::UnsupportedPlanVersion { .. }
        | RepairError::Json(_) => FailureClass::Permanent,
    }
}

fn failure_state(class: FailureClass) -> DatasetState {
    match class {
        FailureClass::Recoverable => DatasetState::FailedRecoverable,
        FailureClass::Permanent => DatasetState::FailedPermanent,
        FailureClass::Blocked => DatasetState::Blocked,
        FailureClass::StaleSource => DatasetState::StaleSource,
        FailureClass::VerificationFailed => DatasetState::VerificationFailed,
        FailureClass::Cancelled => DatasetState::Cancelled,
    }
}

#[allow(clippy::too_many_arguments)]
fn upsert_record(
    journal: &Arc<dyn RunJournal>,
    dataset: &DatasetPlan,
    state: DatasetState,
    attempts: u32,
    source_fingerprint: Option<String>,
    error_class: Option<FailureClass>,
    error_message: Option<String>,
    started_at: Option<chrono::DateTime<Utc>>,
    completed_at: Option<chrono::DateTime<Utc>>,
) {
    let now = Utc::now();
    let record = DatasetRunRecord {
        dataset_id: dataset.dataset_id.clone(),
        output_path: dataset.output_path.clone(),
        repair_plan_id: dataset.repair_plan.plan_id.clone(),
        policy_fingerprint: dataset.repair_plan.policy_fingerprint.clone(),
        state,
        attempts,
        source_fingerprint,
        output_fingerprint: None,
        error_class,
        error_message,
        started_at,
        updated_at: now,
        completed_at,
    };
    if let Err(err) =
        run_journal_sync(Arc::clone(journal), move |journal| journal.upsert_dataset(&record))
    {
        warn!(dataset = %dataset.dataset_id.0, %err, "failed to persist dataset journal record");
    }
}

fn run_journal_sync<T>(
    journal: Arc<dyn RunJournal>,
    op: impl FnOnce(&dyn RunJournal) -> Result<T, OrchestratorError> + Send + 'static,
) -> Result<T, OrchestratorError>
where
    T: Send + 'static,
{
    std::thread::spawn(move || op(journal.as_ref()))
        .join()
        .map_err(|_| OrchestratorError::Journal("journal worker thread panicked".into()))?
}
