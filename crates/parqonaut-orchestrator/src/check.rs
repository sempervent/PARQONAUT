use std::fs;

use camino::Utf8Path;
use parqonaut_storage::location::DatasetLocation;
use serde::{Deserialize, Serialize};

use crate::config::BatchConfig;
use crate::error::OrchestratorError;
use crate::location::location_display;
use crate::paths::{resolve_output_mappings, resolve_run_root};
use crate::plan::build_batch_plan_async;
use crate::storage::{
    block_on_async, dataset_exists, dataset_is_directory_like, BatchStorageRuntime,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchCheckReport {
    pub ok: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub dataset_count: usize,
    pub output_root: Option<String>,
    pub config_fingerprint: Option<String>,
}

impl BatchCheckReport {
    pub fn success(config: &BatchConfig, output_root: &str) -> Self {
        Self {
            ok: true,
            errors: vec![],
            warnings: vec![],
            dataset_count: config.datasets.len(),
            output_root: Some(output_root.to_string()),
            config_fingerprint: Some(config.config_fingerprint()),
        }
    }

    pub fn failure(errors: Vec<String>) -> Self {
        Self {
            ok: false,
            errors,
            warnings: vec![],
            dataset_count: 0,
            output_root: None,
            config_fingerprint: None,
        }
    }
}

/// Fast, side-effect-free validation of batch configuration.
pub fn batch_check(config_path: &Utf8Path) -> BatchCheckReport {
    match batch_check_inner(config_path) {
        Ok(report) => report,
        Err(err) => BatchCheckReport::failure(vec![err.to_string()]),
    }
}

fn batch_check_inner(config_path: &Utf8Path) -> Result<BatchCheckReport, OrchestratorError> {
    block_on_async(batch_check_async(config_path))
}

async fn batch_check_async(config_path: &Utf8Path) -> Result<BatchCheckReport, OrchestratorError> {
    let config = BatchConfig::from_toml_path(config_path)?;
    let base = config_path.parent().unwrap_or_else(|| Utf8Path::new("."));
    let storage = BatchStorageRuntime::new(config.max_storage_requests());
    let mappings = resolve_output_mappings(&config, base)?;
    let output_root = config.require_output_root()?.resolve(base)?;
    let run_root = resolve_run_root(&config, base)?;

    let mut warnings = Vec::new();
    for (ds, mapping) in config.datasets.iter().zip(mappings.iter()) {
        if !dataset_exists(&storage, &mapping.source).await? {
            return Ok(BatchCheckReport::failure(vec![format!(
                "dataset `{}`: source `{}` does not exist",
                ds.id,
                mapping.source_display()
            )]));
        }
        if !dataset_is_directory_like(&storage, &mapping.source).await? {
            warnings.push(format!(
                "dataset `{}`: source `{}` is not a directory-like dataset prefix",
                ds.id,
                mapping.source_display()
            ));
        }
        if let Some(policy) = &ds.policy {
            let policy_path = BatchConfig::resolve_path(base, policy);
            if !policy_path.exists() {
                return Ok(BatchCheckReport::failure(vec![format!(
                    "dataset `{}`: policy `{}` not found",
                    ds.id, policy_path
                )]));
            }
        }
        if let Some(schema) = &ds.target_schema {
            let schema_path = BatchConfig::resolve_path(base, schema);
            if !schema_path.exists() {
                return Ok(BatchCheckReport::failure(vec![format!(
                    "dataset `{}`: target_schema `{}` not found",
                    ds.id, schema_path
                )]));
            }
        }
        let _ = config.resolve_policy(base, ds)?;
    }

    if let Err(err) = build_batch_plan_async(&config, config_path, &storage).await {
        return Ok(match err {
            OrchestratorError::Repair(e) => {
                BatchCheckReport::failure(vec![format!("repair planning failed: {e}")])
            }
            other => BatchCheckReport::failure(vec![other.to_string()]),
        });
    }

    match &output_root {
        DatasetLocation::Local(local) => {
            if !local.path.exists() {
                warnings.push(format!(
                    "output_root `{}` does not exist yet (will be created on execution)",
                    local.path
                ));
            } else if !local.path.as_std_path().is_dir() {
                return Ok(BatchCheckReport::failure(vec![format!(
                    "output_root `{}` exists but is not a directory",
                    local.path
                )]));
            }
        }
        DatasetLocation::S3(_) => {
            warnings.push(format!(
                "output_root `{}` is remote; run journals will be stored under `{}`",
                location_display(&output_root),
                run_root
            ));
        }
    }

    let mut report = BatchCheckReport::success(&config, location_display(&output_root).as_str());
    report.warnings = warnings;
    Ok(report)
}

/// Ensure local output/run parent directories can be created (used before repair, not check).
pub fn ensure_output_root(output_root: &DatasetLocation) -> Result<(), OrchestratorError> {
    if let DatasetLocation::Local(local) = output_root {
        fs::create_dir_all(local.path.as_std_path())?;
    }
    Ok(())
}

/// Ensure the local run root exists for journal/plan storage.
pub fn ensure_run_root(run_root: &Utf8Path) -> Result<(), OrchestratorError> {
    fs::create_dir_all(run_root.as_std_path())?;
    Ok(())
}
