use std::fs;

use camino::Utf8Path;
use serde::{Deserialize, Serialize};

use chrono::Utc;
use parqonaut_repair::{
    generate_plan, scan_directory, stable_hex_id, DatasetFingerprint, EffectivePolicy, RepairPlan,
    PARQONAUT_VERSION, PLAN_SCHEMA_VERSION,
};
use uuid::Uuid;

use crate::config::BatchConfig;
use crate::error::OrchestratorError;
use crate::ids::{BatchPlanId, DatasetId};
use crate::paths::resolve_output_mappings;
use crate::BATCH_PLAN_SCHEMA_VERSION;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchPlan {
    pub schema_version: u32,
    pub batch_plan_id: BatchPlanId,
    pub config_fingerprint: String,
    pub batch_name: String,
    pub max_concurrency: u32,
    pub output_root: String,
    pub parqonaut_version: String,
    pub datasets: Vec<DatasetPlan>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetPlan {
    pub dataset_id: DatasetId,
    pub source_path: String,
    pub output_path: String,
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
    let mappings = resolve_output_mappings(config, base)?;
    let output_root = config.require_output_root()?;
    let output_root_resolved =
        if output_root.is_absolute() { output_root.to_path_buf() } else { base.join(output_root) };

    let mut datasets = Vec::new();
    for (ds_cfg, mapping) in config.datasets.iter().zip(mappings.iter()) {
        let policy = config.resolve_policy(base, ds_cfg)?;
        let target = config.resolve_target_schema(base, ds_cfg)?;
        let repair_plan = match scan_directory(&mapping.source)
            .and_then(|scan| generate_plan(&mapping.source, &scan, &policy, target.clone()))
        {
            Ok(plan) => plan,
            Err(_) => unscannable_dataset_plan(&mapping.source, &policy, target)?,
        };
        datasets.push(DatasetPlan {
            dataset_id: mapping.dataset_id.clone(),
            source_path: mapping.source.as_str().to_string(),
            output_path: mapping.output.as_str().to_string(),
            repair_plan,
            authorize: ds_cfg.authorize.clone(),
        });
    }

    let config_fp = config.config_fingerprint();
    let payload = serde_json::json!({
        "config_fingerprint": config_fp,
        "output_root": output_root_resolved.as_str(),
        "datasets": datasets.iter().map(|d| serde_json::json!({
            "id": d.dataset_id.0,
            "output": d.output_path,
            "plan_id": d.repair_plan.plan_id,
            "source_fp": d.repair_plan.dataset_fingerprint.digest,
            "policy_fp": d.repair_plan.policy_fingerprint,
        })).collect::<Vec<_>>(),
    });
    let batch_plan_id = BatchPlanId::derive(&payload);

    Ok(BatchPlan {
        schema_version: BATCH_PLAN_SCHEMA_VERSION,
        batch_plan_id,
        config_fingerprint: config_fp,
        batch_name: config.batch.name.clone(),
        max_concurrency: config.batch.max_concurrency,
        output_root: output_root_resolved.as_str().to_string(),
        parqonaut_version: PARQONAUT_VERSION.to_string(),
        datasets,
    })
}

fn unscannable_dataset_plan(
    root: &Utf8Path,
    policy: &EffectivePolicy,
    target_schema: Option<Vec<parqonaut_repair::FieldDescriptor>>,
) -> Result<RepairPlan, OrchestratorError> {
    let digest = stable_hex_id("unscannable-dataset", root.as_str());
    let policy_fp = policy.fingerprint();
    let plan_id = stable_hex_id("plan", &format!("{digest}:{policy_fp}"));
    Ok(RepairPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        plan_id,
        parqonaut_version: PARQONAUT_VERSION.to_string(),
        dataset_root: root.as_str().to_string(),
        dataset_fingerprint: DatasetFingerprint {
            version: 1,
            root: root.as_str().to_string(),
            entries: vec![],
            digest,
        },
        policy_fingerprint: policy_fp,
        generated_at: Utc::now(),
        source_scan_id: Uuid::nil(),
        policy: policy.clone(),
        operations: vec![],
        expected_outcomes: vec![],
        diagnosis_findings: vec![],
        schema_diff: None,
        schema_conflicts: None,
        target_schema,
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

    pub fn plan_digest(&self) -> String {
        stable_hex_id("batch-plan-body", &serde_json::to_string(self).unwrap_or_default())
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, OrchestratorError> {
        let plan: Self = serde_json::from_slice(bytes)?;
        plan.validate_version()?;
        Ok(plan)
    }

    pub fn to_json_pretty(&self) -> Result<String, OrchestratorError> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub fn write_json(&self, path: &Utf8Path) -> Result<(), OrchestratorError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent.as_std_path())?;
        }
        fs::write(path.as_std_path(), self.to_json_pretty()?)?;
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

    pub fn outputs_map(&self) -> std::collections::HashMap<DatasetId, camino::Utf8PathBuf> {
        self.datasets
            .iter()
            .map(|d| (d.dataset_id.clone(), camino::Utf8PathBuf::from(&d.output_path)))
            .collect()
    }
}
