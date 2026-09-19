//! Transport-neutral PARQONAUT application boundary.

#![forbid(unsafe_code)]

pub mod batch;
pub mod contracts;
pub mod error;
pub mod location;
pub mod policy;
pub mod repair;
pub mod scan;

pub use contracts::{
    BatchCheckRequest, BatchPlanRequest, BatchPlanResult, BatchRepairRequest, BatchRepairResult,
    BatchResumeRequest, BatchStatusRequest, BatchStatusResult, BatchVerifyRequest,
    BatchVerifyResult, CheckRequest, CheckResult, DiagnoseRequest, DiagnoseResult,
    DurableJobPayload, JobResultReference, PlanRequest, PlanResult, RepairRequest, RepairResult,
    ScanRequest, ScanResult, VerifyRequest, VerifyResult, APP_REQUEST_SCHEMA_VERSION,
};
pub use error::ApplicationError;
pub use policy::StoragePolicy;

use scan::AppScanRequest;

/// Shared application facade holding server location policy.
#[derive(Debug, Clone)]
pub struct ParqonautApp {
    policy: StoragePolicy,
}

impl ParqonautApp {
    #[must_use]
    pub fn new(policy: StoragePolicy) -> Self {
        Self { policy }
    }

    #[must_use]
    pub fn cli() -> Self {
        Self::new(StoragePolicy::cli_unrestricted_local())
    }

    #[must_use]
    pub fn policy(&self) -> &StoragePolicy {
        &self.policy
    }

    pub async fn scan(&self, req: ScanRequest) -> Result<ScanResult, ApplicationError> {
        let app_req = AppScanRequest { location: req.location, profile: req.profile };
        let report = scan::run_scan(&app_req, &self.policy).await?;
        Ok(ScanResult { report })
    }

    pub async fn diagnose(&self, req: DiagnoseRequest) -> Result<DiagnoseResult, ApplicationError> {
        repair::diagnose(&req, &self.policy).await
    }

    pub async fn plan(&self, req: PlanRequest) -> Result<PlanResult, ApplicationError> {
        repair::plan(&req, &self.policy).await
    }

    pub async fn check(&self, req: CheckRequest) -> Result<CheckResult, ApplicationError> {
        repair::check(&req, &self.policy).await
    }

    pub async fn repair(&self, req: RepairRequest) -> Result<RepairResult, ApplicationError> {
        repair::repair(&req, &self.policy).await
    }

    pub async fn verify(&self, req: VerifyRequest) -> Result<VerifyResult, ApplicationError> {
        repair::verify(&req, &self.policy).await
    }

    pub fn batch_check(&self, req: BatchCheckRequest) -> parqonaut_orchestrator::BatchCheckReport {
        batch::batch_check(&req)
    }

    pub fn batch_plan(&self, req: BatchPlanRequest) -> Result<BatchPlanResult, ApplicationError> {
        batch::validate_batch_config_path(&req.config_path)?;
        batch::batch_plan(&req)
    }

    pub async fn batch_repair(
        &self,
        req: BatchRepairRequest,
        cancel: parqonaut_orchestrator::CancelFlag,
    ) -> Result<BatchRepairResult, ApplicationError> {
        batch::batch_repair(&req, cancel).await
    }

    pub fn batch_status(
        &self,
        req: BatchStatusRequest,
    ) -> Result<BatchStatusResult, ApplicationError> {
        batch::batch_status(&req)
    }

    pub async fn batch_resume(
        &self,
        req: BatchResumeRequest,
        cancel: parqonaut_orchestrator::CancelFlag,
    ) -> Result<BatchRepairResult, ApplicationError> {
        batch::batch_resume(&req, cancel).await
    }

    pub fn batch_verify(
        &self,
        req: BatchVerifyRequest,
    ) -> Result<BatchVerifyResult, ApplicationError> {
        batch::batch_verify(&req)
    }
}
