//! Repair workflow use cases shared by CLI and HTTP.

use crate::contracts::{
    CheckRequest, CheckResult, DiagnoseRequest, DiagnoseResult, PlanRequest, PlanResult,
    RepairRequest, RepairResult, VerifyRequest, VerifyResult,
};
use crate::error::ApplicationError;
use crate::policy::StoragePolicy;
use parqonaut_repair::{
    backend_for_location, diagnose_location, evaluate_check_for_location,
    generate_plan_for_location, location_root, requires_storage_execution, verify_repair,
    ExecutionManifest, RepairAuthorization, RepairExecutor,
};

pub async fn diagnose(
    req: &DiagnoseRequest,
    policy: &StoragePolicy,
) -> Result<DiagnoseResult, ApplicationError> {
    policy.validate_dataset(&req.location)?;
    let backend = backend_for_location(&req.location).await?;
    let report = diagnose_location(&req.location, &backend, &req.policy).await?;
    Ok(DiagnoseResult { report })
}

pub async fn plan(
    req: &PlanRequest,
    policy: &StoragePolicy,
) -> Result<PlanResult, ApplicationError> {
    policy.validate_dataset(&req.location)?;
    let backend = backend_for_location(&req.location).await?;
    let plan =
        generate_plan_for_location(&req.location, &backend, &req.policy, req.target_schema.clone())
            .await?;
    Ok(PlanResult { plan })
}

pub async fn check(
    req: &CheckRequest,
    policy: &StoragePolicy,
) -> Result<CheckResult, ApplicationError> {
    policy.validate_dataset(&req.location)?;
    let backend = backend_for_location(&req.location).await?;
    let report = evaluate_check_for_location(&req.location, &backend, &req.policy).await?;
    Ok(CheckResult { report })
}

pub async fn repair(
    req: &RepairRequest,
    policy: &StoragePolicy,
) -> Result<RepairResult, ApplicationError> {
    policy.validate_dataset(&req.source)?;
    policy.validate_dataset(&req.output)?;
    let source_backend = backend_for_location(&req.source).await?;
    let scan = source_backend.scan(&req.source).await?;
    let executor = RepairExecutor {
        authorization: RepairAuthorization::from_ids(req.authorize.clone()),
        verify_before_publish: true,
    };
    let run_id = uuid::Uuid::new_v4().to_string();
    let execution = if requires_storage_execution(&req.source, &req.output) {
        let output_backend = backend_for_location(&req.output).await?;
        source_backend
            .execute_plan(&executor, &req.plan, &req.output, &scan, &output_backend, &run_id)
            .await?
    } else {
        let out = location_root(&req.output);
        executor.execute(&req.plan, &out, &scan)?
    };
    Ok(RepairResult { execution })
}

pub async fn verify(
    req: &VerifyRequest,
    policy: &StoragePolicy,
) -> Result<VerifyResult, ApplicationError> {
    policy.validate_dataset(&req.before)?;
    let before_root = location_root(&req.before);
    let policy_repair = &req.policy.repair;
    let backend = backend_for_location(&req.before).await?;
    let before_scan = backend.scan(&req.before).await?;
    let after_scan = parqonaut_repair::scan_directory(&req.after_local_path)?;
    let manifest_obj = match &req.manifest_path {
        Some(p) => Some(ExecutionManifest::read_json(p)?),
        None => None,
    };
    let report = verify_repair(
        &before_scan,
        &after_scan,
        &before_root,
        &req.after_local_path,
        policy_repair,
        manifest_obj.as_ref(),
    )?;
    Ok(VerifyResult { report })
}
