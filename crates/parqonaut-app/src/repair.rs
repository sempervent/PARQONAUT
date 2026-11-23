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
    repair_with_progress(req, policy, &parqonaut_workflow::NoOpProgressObserver).await
}

pub async fn repair_with_progress(
    req: &RepairRequest,
    policy: &StoragePolicy,
    progress: &dyn parqonaut_workflow::ProgressObserver,
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
            .execute_plan_with_progress(
                &executor,
                &req.plan,
                &req.output,
                &scan,
                &output_backend,
                &run_id,
                progress,
            )
            .await?
    } else {
        let out = location_root(&req.output);
        executor.execute_with_progress(&req.plan, &out, &scan, progress)?
    };
    Ok(RepairResult { execution })
}

pub async fn verify(
    req: &VerifyRequest,
    policy: &StoragePolicy,
) -> Result<VerifyResult, ApplicationError> {
    policy.validate_dataset(&req.before)?;
    let after_loc =
        parqonaut_storage::location::DatasetLocation::parse(req.after_local_path.as_str())
            .map_err(|e| ApplicationError::InvalidRequest(e.to_string()))?;
    policy.validate_dataset(&after_loc)?;
    let before_root = location_root(&req.before);
    let after_root = location_root(&after_loc);
    let policy_repair = &req.policy.repair;
    let backend = backend_for_location(&req.before).await?;
    let before_scan = backend.scan(&req.before).await?;
    let after_scan = match &after_loc {
        parqonaut_storage::location::DatasetLocation::Local(local) => {
            parqonaut_repair::scan_directory(&local.path)?
        }
        parqonaut_storage::location::DatasetLocation::S3(_) => {
            let after_backend = backend_for_location(&after_loc).await?;
            after_backend.scan(&after_loc).await?
        }
    };
    let manifest_obj = match &req.manifest_path {
        Some(p) => Some(ExecutionManifest::read_json(p)?),
        None => None,
    };
    let report = verify_repair(
        &before_scan,
        &after_scan,
        &before_root,
        &after_root,
        policy_repair,
        manifest_obj.as_ref(),
    )?;
    Ok(VerifyResult { report })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::APP_REQUEST_SCHEMA_VERSION;
    use camino::Utf8PathBuf;
    use parqonaut_repair::EffectivePolicy;
    use parqonaut_storage::location::DatasetLocation;

    #[tokio::test]
    async fn verify_validates_after_location_against_storage_policy() {
        let base =
            std::env::temp_dir().join(format!("parqonaut-app-verify-{}", uuid::Uuid::new_v4()));
        let allowed = Utf8PathBuf::from_path_buf(base.join("allowed")).unwrap();
        let disallowed = Utf8PathBuf::from_path_buf(base.join("disallowed")).unwrap();
        std::fs::create_dir_all(&allowed).unwrap();
        std::fs::create_dir_all(&disallowed).unwrap();

        let policy = StoragePolicy {
            allow_unrestricted_local: false,
            allowed_local_roots: vec![allowed.clone()],
            allowed_s3_buckets: vec!["allowed-bucket".into()],
            allowed_s3_prefixes: Vec::new(),
        };

        let req_local = VerifyRequest {
            schema_version: APP_REQUEST_SCHEMA_VERSION,
            before: DatasetLocation::parse(allowed.as_str()).unwrap(),
            after_local_path: disallowed,
            manifest_path: None,
            policy: EffectivePolicy::default(),
        };
        let err_local = verify(&req_local, &policy).await.unwrap_err();
        assert!(matches!(err_local, ApplicationError::LocationNotAllowed(_)), "{err_local:?}");

        let req_s3 = VerifyRequest {
            schema_version: APP_REQUEST_SCHEMA_VERSION,
            before: DatasetLocation::parse(allowed.as_str()).unwrap(),
            after_local_path: Utf8PathBuf::from("s3://forbidden-bucket/out/"),
            manifest_path: None,
            policy: EffectivePolicy::default(),
        };
        let err_s3 = verify(&req_s3, &policy).await.unwrap_err();
        assert!(matches!(err_s3, ApplicationError::LocationNotAllowed(_)), "{err_s3:?}");

        let _ = std::fs::remove_dir_all(&base);
    }
}
