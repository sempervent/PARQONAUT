use crate::error::OrchestratorError;
use crate::journal::{DatasetRunRecord, RunIdentity};
use crate::plan::{BatchPlan, DatasetPlan};
use crate::state::DatasetState;

/// Verify that an on-disk batch plan matches persisted run authorization.
pub fn assert_plan_matches_identity(
    plan: &BatchPlan,
    identity: &RunIdentity,
) -> Result<(), OrchestratorError> {
    plan.validate_version()?;
    mismatch("batch_plan_id", &identity.batch_plan_id.0, &plan.batch_plan_id.0)?;
    mismatch("config_fingerprint", &identity.config_fingerprint, &plan.config_fingerprint)?;
    mismatch("output_root", &identity.output_root, &plan.output_root)?;
    mismatch("plan_digest", &identity.plan_digest, &plan.plan_digest())?;
    Ok(())
}

/// Verify journal dataset records still correspond to the authorized plan.
pub fn assert_datasets_match_plan(
    plan: &BatchPlan,
    records: &[DatasetRunRecord],
) -> Result<(), OrchestratorError> {
    for ds in &plan.datasets {
        let Some(record) = records.iter().find(|r| r.dataset_id == ds.dataset_id) else {
            continue;
        };
        assert_dataset_record_matches_plan(ds, record)?;
    }
    Ok(())
}

pub fn assert_dataset_record_matches_plan(
    ds: &DatasetPlan,
    record: &DatasetRunRecord,
) -> Result<(), OrchestratorError> {
    mismatch("output_path", &record.output_path, &ds.output_path)?;
    mismatch("repair_plan_id", &record.repair_plan_id, &ds.repair_plan.plan_id)?;
    mismatch("policy_fingerprint", &record.policy_fingerprint, &ds.repair_plan.policy_fingerprint)?;
    // Only succeeded rows bind the plan-time digest; in-flight/recoverable rows store
    // the last observed scan fingerprint and are revalidated before repair executes.
    if record.state == DatasetState::Succeeded {
        if let Some(source_fp) = record.source_fingerprint.as_deref().filter(|fp| !fp.is_empty()) {
            mismatch("source_fingerprint", source_fp, &ds.repair_plan.dataset_fingerprint.digest)?;
        }
    }
    Ok(())
}

fn mismatch(field: &str, expected: &str, found: &str) -> Result<(), OrchestratorError> {
    if expected == found {
        Ok(())
    } else {
        Err(OrchestratorError::PlanIdentityMismatch {
            field: field.into(),
            expected: expected.into(),
            found: found.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{BatchPlanId, RunId};
    use chrono::Utc;

    #[test]
    fn rejects_plan_digest_mismatch() {
        let plan = BatchPlan {
            schema_version: 1,
            batch_plan_id: BatchPlanId("bp".into()),
            config_fingerprint: "cfg".into(),
            batch_name: "t".into(),
            max_concurrency: 1,
            max_storage_requests: 8,
            output_root: "/out".into(),
            run_root: "/out".into(),
            parqonaut_version: "0.3.0".into(),
            datasets: vec![],
        };
        let identity = RunIdentity {
            run_id: RunId::new(),
            batch_plan_id: plan.batch_plan_id.clone(),
            config_fingerprint: plan.config_fingerprint.clone(),
            batch_name: plan.batch_name.clone(),
            output_root: plan.output_root.clone(),
            parqonaut_version: plan.parqonaut_version.clone(),
            plan_digest: "stale".into(),
            started_at: Utc::now(),
            updated_at: Utc::now(),
            completed_at: None,
            cancelled_at: None,
        };
        let err = assert_plan_matches_identity(&plan, &identity).unwrap_err();
        assert!(matches!(err, OrchestratorError::PlanIdentityMismatch { .. }));
    }
}
