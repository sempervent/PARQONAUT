use std::fs;

use camino::Utf8Path;
use serde::{Deserialize, Serialize};

use chrono::Utc;
use parqonaut_repair::{
    generate_plan, generate_plan_for_location, location_root, scan_directory, stable_hex_id,
    DatasetFingerprint, EffectivePolicy, RepairPlan, PARQONAUT_VERSION, PLAN_SCHEMA_VERSION,
};
use parqonaut_storage::location::DatasetLocation;
use uuid::Uuid;

use crate::config::BatchConfig;
use crate::error::OrchestratorError;
use crate::ids::{BatchPlanId, DatasetId};
use crate::location::location_display;
use crate::paths::{resolve_output_mappings, resolve_run_root};
use crate::storage::{block_on_async, default_max_storage_requests, BatchStorageRuntime};
use crate::BATCH_PLAN_SCHEMA_VERSION;

fn default_plan_storage_requests() -> u32 {
    default_max_storage_requests()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchPlan {
    pub schema_version: u32,
    pub batch_plan_id: BatchPlanId,
    pub config_fingerprint: String,
    pub batch_name: String,
    pub max_concurrency: u32,
    #[serde(default = "default_plan_storage_requests")]
    pub max_storage_requests: u32,
    /// Resolved output root URI (local path or `s3://`).
    pub output_root: String,
    /// Local directory for run journals and plan snapshots.
    #[serde(default)]
    pub run_root: String,
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
    let storage = BatchStorageRuntime::new(config.max_storage_requests());
    block_on_async(build_batch_plan_async(config, config_path, &storage))
}

pub async fn build_batch_plan_async(
    config: &BatchConfig,
    config_path: &Utf8Path,
    storage: &BatchStorageRuntime,
) -> Result<BatchPlan, OrchestratorError> {
    config.validate()?;
    let base = config_path.parent().unwrap_or_else(|| Utf8Path::new("."));
    let mappings = resolve_output_mappings(config, base)?;
    let output_root = config.require_output_root()?.resolve(base)?;
    let run_root = resolve_run_root(config, base)?;

    let mut datasets = Vec::new();
    for (ds_cfg, mapping) in config.datasets.iter().zip(mappings.iter()) {
        let policy = config.resolve_policy(base, ds_cfg)?;
        let target = config.resolve_target_schema(base, ds_cfg)?;
        let repair_plan = plan_dataset(storage, &mapping.source, &policy, target).await?;
        datasets.push(DatasetPlan {
            dataset_id: mapping.dataset_id.clone(),
            source_path: mapping.source_display(),
            output_path: mapping.output_display(),
            repair_plan,
            authorize: ds_cfg.authorize.clone(),
        });
    }

    let config_fp = config.config_fingerprint();
    let payload = serde_json::json!({
        "config_fingerprint": config_fp,
        "output_root": location_display(&output_root),
        "run_root": run_root.as_str(),
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
        max_storage_requests: config.max_storage_requests(),
        output_root: location_display(&output_root),
        run_root: run_root.to_string(),
        parqonaut_version: PARQONAUT_VERSION.to_string(),
        datasets,
    })
}

async fn plan_dataset(
    storage: &BatchStorageRuntime,
    source: &DatasetLocation,
    policy: &EffectivePolicy,
    target_schema: Option<Vec<parqonaut_repair::FieldDescriptor>>,
) -> Result<RepairPlan, OrchestratorError> {
    match source {
        DatasetLocation::Local(local) => {
            match scan_directory(&local.path)
                .and_then(|scan| generate_plan(&local.path, &scan, policy, target_schema.clone()))
            {
                Ok(plan) => Ok(plan),
                Err(_) => unscannable_dataset_plan(source, policy, target_schema),
            }
        }
        DatasetLocation::S3(_) => {
            let backend = storage.repair_backend_for(source);
            match generate_plan_for_location(source, &backend, policy, target_schema.clone()).await
            {
                Ok(plan) => Ok(plan),
                Err(_) => unscannable_dataset_plan(source, policy, target_schema),
            }
        }
    }
}

fn unscannable_dataset_plan(
    root: &DatasetLocation,
    policy: &EffectivePolicy,
    target_schema: Option<Vec<parqonaut_repair::FieldDescriptor>>,
) -> Result<RepairPlan, OrchestratorError> {
    let root = location_root(root);
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

    pub fn dataset_locations(
        &self,
    ) -> Result<Vec<(DatasetId, DatasetLocation, DatasetLocation)>, OrchestratorError> {
        self.datasets
            .iter()
            .map(|ds| {
                Ok((
                    ds.dataset_id.clone(),
                    DatasetLocation::parse(&ds.source_path)
                        .map_err(|e| OrchestratorError::InvalidConfig(e.to_string()))?,
                    DatasetLocation::parse(&ds.output_path)
                        .map_err(|e| OrchestratorError::InvalidConfig(e.to_string()))?,
                ))
            })
            .collect()
    }

    pub fn effective_run_root(&self) -> camino::Utf8PathBuf {
        if self.run_root.is_empty() {
            camino::Utf8PathBuf::from(&self.output_root)
        } else {
            camino::Utf8PathBuf::from(&self.run_root)
        }
    }
}
