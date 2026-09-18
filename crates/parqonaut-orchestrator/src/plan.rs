use camino::Utf8Path;
use serde::{Deserialize, Serialize};

use parqonaut_repair::{
    generate_plan, scan_directory, RepairPlan, PARQONAUT_VERSION, PLAN_SCHEMA_VERSION,
};

use crate::config::BatchConfig;
use crate::error::OrchestratorError;
use crate::ids::{BatchPlanId, DatasetId};
use crate::BATCH_PLAN_SCHEMA_VERSION;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchPlan {
    pub schema_version: u32,
    pub batch_plan_id: BatchPlanId,
    pub config_fingerprint: String,
    pub batch_name: String,
    pub max_concurrency: u32,
    pub parqonaut_version: String,
    pub datasets: Vec<DatasetPlan>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetPlan {
    pub dataset_id: DatasetId,
    pub source_path: String,
    pub repair_plan: RepairPlan,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorize: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchSummary {
    pub dataset_count: usize,
    pub safe_operations: usize,
    pub review_required_operations: usize,
    pub blocked_operations: usize,
}

pub fn build_batch_plan(
    config: &BatchConfig,
    config_path: &Utf8Path,
) -> Result<BatchPlan, OrchestratorError> {
    config.validate()?;
    let base = config_path.parent().unwrap_or_else(|| Utf8Path::new("."));
    let mut datasets = Vec::new();
    for ds in &config.datasets {
        let root = BatchConfig::canonicalize_path(base, &ds.path)?;
        let policy = config.resolve_policy(ds)?;
        let target = config.resolve_target_schema(ds)?;
        let scan = scan_directory(&root)?;
        let repair_plan = generate_plan(&root, &scan, &policy, target)?;
        datasets.push(DatasetPlan {
            dataset_id: ds.id(),
            source_path: root.as_str().to_string(),
            repair_plan,
            authorize: ds.authorize.clone(),
        });
    }

    let config_fp = config.config_fingerprint();
    let payload = serde_json::json!({
        "config_fingerprint": config_fp,
        "datasets": datasets.iter().map(|d| (&d.dataset_id.0, d.repair_plan.plan_id.clone())).collect::<Vec<_>>(),
    });
    let batch_plan_id = BatchPlanId::derive(&payload);

    Ok(BatchPlan {
        schema_version: BATCH_PLAN_SCHEMA_VERSION,
        batch_plan_id,
        config_fingerprint: config_fp,
        batch_name: config.batch.name.clone(),
        max_concurrency: config.batch.max_concurrency,
        parqonaut_version: PARQONAUT_VERSION.to_string(),
        datasets,
    })
}

impl BatchPlan {
    pub fn validate_version(&self) -> Result<(), OrchestratorError> {
        if self.schema_version != BATCH_PLAN_SCHEMA_VERSION {
            return Err(OrchestratorError::UnsupportedPlanVersion {
                found: self.schema_version,
                supported: BATCH_PLAN_SCHEMA_VERSION,
            });
        }
        for ds in &self.datasets {
            if ds.repair_plan.schema_version != PLAN_SCHEMA_VERSION {
                return Err(OrchestratorError::UnsupportedPlanVersion {
                    found: ds.repair_plan.schema_version,
                    supported: PLAN_SCHEMA_VERSION,
                });
            }
        }
        Ok(())
    }

    pub fn summary(&self) -> BatchSummary {
        let mut safe = 0usize;
        let mut review = 0usize;
        let mut blocked = 0usize;
        for ds in &self.datasets {
            for op in &ds.repair_plan.operations {
                match op.safety {
                    parqonaut_repair::RepairSafety::Safe => safe += 1,
                    parqonaut_repair::RepairSafety::ReviewRequired => review += 1,
                    parqonaut_repair::RepairSafety::Blocked => blocked += 1,
                    parqonaut_repair::RepairSafety::Destructive => {}
                }
            }
        }
        BatchSummary {
            dataset_count: self.datasets.len(),
            safe_operations: safe,
            review_required_operations: review,
            blocked_operations: blocked,
        }
    }
}
