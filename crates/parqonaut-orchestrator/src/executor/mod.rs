use std::collections::HashMap;
use std::sync::Arc;

use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use parqonaut_repair::{
    compute_dataset_fingerprint, scan_directory, DatasetInventory, RepairAuthorization,
    RepairError, RepairExecutor,
};
use tracing::{info, instrument, warn};

use crate::error::{FailureClass, OrchestratorError};
use crate::ids::{DatasetId, RunId};
use crate::journal::{DatasetRunRecord, RunJournal};
use crate::lock::DatasetLock;
use crate::overlap::validate_batch_plan;
use crate::plan::{BatchPlan, DatasetPlan};
use crate::scheduler::{BatchScheduler, ConcurrencyMetrics};
use crate::state::DatasetState;

pub const DEFAULT_MAX_RETRIES: u32 = 2;

#[derive(Debug, Clone)]
pub struct BatchExecutorConfig {
    pub max_retries: u32,
}

impl Default for BatchExecutorConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}

pub struct BatchExecutor {
    pub config: BatchExecutorConfig,
    pub repair: RepairExecutor,
}

#[derive(Debug, Clone)]
pub struct DatasetExecutionOutcome {
    pub dataset_id: DatasetId,
    pub state: DatasetState,
    pub attempts: u32,
    pub error_class: Option<FailureClass>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BatchRunOutcome {
    pub peak_concurrent_datasets: usize,
    pub datasets: Vec<DatasetExecutionOutcome>,
}

impl BatchExecutor {
    pub fn new(repair: RepairExecutor, config: BatchExecutorConfig) -> Self {
        Self { repair, config }
    }

    #[instrument(skip(self, plan, outputs, journal), fields(run_id = %run_id.0, datasets = plan.datasets.len()))]
    pub async fn run(
        &self,
        plan: &BatchPlan,
        outputs: &HashMap<DatasetId, Utf8PathBuf>,
        run_id: &RunId,
        journal: Arc<dyn RunJournal>,
    ) -> Result<BatchRunOutcome, OrchestratorError> {
        plan.validate_version()?;
        validate_batch_plan(plan, outputs)?;

        let scheduler = BatchScheduler::new(plan.max_concurrency);
        let metrics = Arc::new(ConcurrencyMetrics::default());
        let run_id = run_id.clone();
        let config = self.config.clone();
        let repair = clone_repair_executor(&self.repair);

        let tasks: Vec<_> = plan
            .datasets
            .iter()
            .map(|dataset| {
                let dataset = dataset.clone();
                let output = outputs.get(&dataset.dataset_id).expect("validated").clone();
                let run_id = run_id.clone();
                let journal = Arc::clone(&journal);
                let config = config.clone();
                let repair = clone_repair_executor(&repair);
                move || {
                    async move {
                        execute_dataset_with_retries(
                            &dataset,
                            &output,
                            &run_id,
                            journal,
                            &repair,
                            &config,
                        )
                        .await
                    }
                }
            })
            .collect();

        let dataset_outcomes = scheduler.run_bounded(tasks, metrics.clone()).await;
        let all_terminal = dataset_outcomes.iter().all(|o| o.state.is_terminal());

        if all_terminal {
            run_journal_sync(journal.clone(), |journal| journal.mark_run_completed(Utc::now()))?;
        }

        Ok(BatchRunOutcome {
            peak_concurrent_datasets: metrics.peak_concurrent(),
            datasets: dataset_outcomes,
        })
    }
}

fn clone_repair_executor(repair: &RepairExecutor) -> RepairExecutor {
    RepairExecutor {
        authorization: repair.authorization.clone(),
        verify_before_publish: repair.verify_before_publish,
    }
}

async fn execute_dataset_with_retries(
    dataset: &DatasetPlan,
    output: &Utf8Path,
    run_id: &RunId,
    journal: Arc<dyn RunJournal>,
    repair: &RepairExecutor,
    config: &BatchExecutorConfig,
) -> DatasetExecutionOutcome {
    let dataset_id = dataset.dataset_id.clone();
    let mut attempts = run_journal_sync(journal.clone(), move |journal| journal.dataset(&dataset_id))
        .ok()
        .flatten()
        .map(|r| r.attempts)
        .unwrap_or(0);
    let max_attempts = config.max_retries.saturating_add(1);

    loop {
        attempts += 1;
        let started = Utc::now();
        upsert_record(
            &journal,
            &dataset.dataset_id,
            DatasetState::PlanningValidated,
            attempts,
            None,
            None,
            None,
            Some(started),
            None,
        );

        match execute_dataset_once(
            dataset,
            output,
            run_id,
            journal.clone(),
            repair,
            attempts,
            started,
        )
        .await
        {
            Ok(outcome) => {
                info!(dataset = %dataset.dataset_id.0, attempts, "dataset execution finished");
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
                    &dataset.dataset_id,
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
                };
            }
        }
    }
}

struct DatasetFailure {
    class: FailureClass,
    message: String,
}

async fn execute_dataset_once(
    dataset: &DatasetPlan,
    output: &Utf8Path,
    run_id: &RunId,
    journal: Arc<dyn RunJournal>,
    repair: &RepairExecutor,
    attempts: u32,
    started: chrono::DateTime<Utc>,
) -> Result<DatasetExecutionOutcome, DatasetFailure> {
    let _lock = match DatasetLock::acquire(output, run_id) {
        Ok(lock) => lock,
        Err(err) => {
            let message = err.to_string();
            let class = FailureClass::Permanent;
            upsert_record(
                &journal,
                &dataset.dataset_id,
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

    if let Err(err) = dataset
        .repair_plan
        .dataset_fingerprint
        .verify_against(&current_fp)
    {
        let class = FailureClass::StaleSource;
        let message = err.to_string();
        upsert_record(
            &journal,
            &dataset.dataset_id,
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
        &dataset.dataset_id,
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
    let plan = dataset.repair_plan.clone();
    let source_digest = plan.dataset_fingerprint.digest.clone();
    let output = output.to_path_buf();
    let dataset_id = dataset.dataset_id.clone();

    let execution = tokio::task::spawn_blocking(move || executor.execute(&plan, &output, &scan))
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
                &dataset_id,
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
        &dataset.dataset_id,
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
        RepairError::ReviewRequiredNotAuthorized { .. } | RepairError::DestructiveNotAllowed { .. } => {
            FailureClass::Blocked
        }
        RepairError::VerificationInvariantFailed { .. } => FailureClass::VerificationFailed,
        RepairError::PartialExecution { .. } | RepairError::Io(_) | RepairError::TransformFailed(_) => {
            FailureClass::Recoverable
        }
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

fn upsert_record(
    journal: &Arc<dyn RunJournal>,
    dataset_id: &DatasetId,
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
        dataset_id: dataset_id.clone(),
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
    if let Err(err) = run_journal_sync(Arc::clone(journal), move |journal| journal.upsert_dataset(&record)) {
        warn!(dataset = %dataset_id.0, %err, "failed to persist dataset journal record");
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
