//! Batch orchestration use cases (synchronous control plane + durable job payloads).

use std::sync::Arc;

use camino::Utf8Path;
use parqonaut_orchestrator::{
    check_config, execute_batch_with_progress, plan_batch, read_status, resume_batch_with_progress,
    verify_run, BatchRunOptions, CancelFlag,
};
use parqonaut_workflow::{NoOpProgressObserver, ProgressObserver};

use crate::contracts::{
    BatchCheckRequest, BatchPlanRequest, BatchPlanResult, BatchRepairRequest, BatchRepairResult,
    BatchResumeRequest, BatchStatusRequest, BatchStatusResult, BatchVerifyRequest,
    BatchVerifyResult,
};
use crate::error::ApplicationError;

pub fn batch_check(req: &BatchCheckRequest) -> parqonaut_orchestrator::BatchCheckReport {
    check_config(&req.config_path)
}

pub fn batch_plan(req: &BatchPlanRequest) -> Result<BatchPlanResult, ApplicationError> {
    let plan =
        plan_batch(&req.config_path).map_err(|e| ApplicationError::BatchFailed(e.to_string()))?;
    Ok(BatchPlanResult { plan })
}

pub async fn batch_repair(
    req: &BatchRepairRequest,
    cancel: CancelFlag,
) -> Result<BatchRepairResult, ApplicationError> {
    batch_repair_with_progress(req, cancel, Arc::new(NoOpProgressObserver)).await
}

pub async fn batch_repair_with_progress(
    req: &BatchRepairRequest,
    cancel: CancelFlag,
    progress: Arc<dyn ProgressObserver>,
) -> Result<BatchRepairResult, ApplicationError> {
    let options = BatchRunOptions {
        dry_run: req.dry_run,
        jobs: req.jobs,
        cancel,
        interrupt_after_completed: req.interrupt_after_completed,
    };
    let execution = execute_batch_with_progress(&req.plan_path, options, progress)
        .await
        .map_err(|e| ApplicationError::BatchFailed(e.to_string()))?;
    Ok(BatchRepairResult { execution })
}

pub fn batch_status(req: &BatchStatusRequest) -> Result<BatchStatusResult, ApplicationError> {
    let status =
        read_status(&req.run_dir).map_err(|e| ApplicationError::BatchFailed(e.to_string()))?;
    Ok(BatchStatusResult { status })
}

pub async fn batch_resume(
    req: &BatchResumeRequest,
    cancel: CancelFlag,
) -> Result<BatchRepairResult, ApplicationError> {
    batch_resume_with_progress(req, cancel, Arc::new(NoOpProgressObserver)).await
}

pub async fn batch_resume_with_progress(
    req: &BatchResumeRequest,
    cancel: CancelFlag,
    progress: Arc<dyn ProgressObserver>,
) -> Result<BatchRepairResult, ApplicationError> {
    let options = BatchRunOptions {
        dry_run: req.dry_run,
        jobs: req.jobs,
        cancel,
        interrupt_after_completed: None,
    };
    let execution = resume_batch_with_progress(&req.run_dir, options, progress)
        .await
        .map_err(|e| ApplicationError::BatchFailed(e.to_string()))?;
    Ok(BatchRepairResult { execution })
}

pub fn batch_verify(req: &BatchVerifyRequest) -> Result<BatchVerifyResult, ApplicationError> {
    let report =
        verify_run(&req.run_dir).map_err(|e| ApplicationError::BatchFailed(e.to_string()))?;
    Ok(BatchVerifyResult { report })
}

pub fn validate_batch_config_path(path: &Utf8Path) -> Result<(), ApplicationError> {
    if !path.exists() {
        return Err(ApplicationError::InvalidRequest(format!("batch config not found: {path}")));
    }
    Ok(())
}
