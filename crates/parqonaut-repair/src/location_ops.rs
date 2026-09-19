//! Storage-aware entry points for diagnose, plan, and check.

use parqonaut_storage::location::DatasetLocation;

use crate::check::{evaluate_check, CheckReport};
use crate::diagnose::{diagnose, DiagnosisReport};
use crate::error::RepairError;
use crate::plan::generate_plan;
use crate::schema::FieldDescriptor;
use crate::schema_policy::EffectivePolicy;
use crate::storage_scan::{location_root, RepairBackend};

/// Scan, then diagnose a dataset at `location`.
pub async fn diagnose_location(
    location: &DatasetLocation,
    backend: &RepairBackend,
    policy: &EffectivePolicy,
) -> Result<DiagnosisReport, RepairError> {
    let scan = backend.scan(location).await?;
    let root = location_root(location);
    diagnose(&root, &scan, &policy.repair)
}

/// Scan, then generate a repair plan for a dataset at `location`.
pub async fn generate_plan_for_location(
    location: &DatasetLocation,
    backend: &RepairBackend,
    policy: &EffectivePolicy,
    target_schema: Option<Vec<FieldDescriptor>>,
) -> Result<crate::RepairPlan, RepairError> {
    let scan = backend.scan(location).await?;
    let root = location_root(location);
    generate_plan(&root, &scan, policy, target_schema)
}

/// Scan, then evaluate CI policy compliance for a dataset at `location`.
pub async fn evaluate_check_for_location(
    location: &DatasetLocation,
    backend: &RepairBackend,
    policy: &EffectivePolicy,
) -> Result<CheckReport, RepairError> {
    let scan = backend.scan(location).await?;
    let root = location_root(location);
    evaluate_check(&root, &scan, policy)
}
