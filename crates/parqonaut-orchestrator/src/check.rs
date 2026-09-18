use std::fs;

use camino::Utf8Path;
use serde::{Deserialize, Serialize};

use crate::config::BatchConfig;
use crate::error::OrchestratorError;
use crate::paths::resolve_output_mappings;
use crate::plan::build_batch_plan;

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
    let config = BatchConfig::from_toml_path(config_path)?;
    let base = config_path.parent().unwrap_or_else(|| Utf8Path::new("."));
    let mappings = resolve_output_mappings(&config, base)?;
    let output_root = config.require_output_root()?;
    let output_root_resolved =
        if output_root.is_absolute() { output_root.to_path_buf() } else { base.join(output_root) };

    let mut warnings = Vec::new();
    for (ds, mapping) in config.datasets.iter().zip(mappings.iter()) {
        if !mapping.source.exists() {
            return Ok(BatchCheckReport::failure(vec![format!(
                "dataset `{}`: source `{}` does not exist",
                ds.id, mapping.source
            )]));
        }
        if !mapping.source.is_dir() {
            warnings.push(format!(
                "dataset `{}`: source `{}` is not a directory",
                ds.id, mapping.source
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

    // Planning dry-run proves policy/plan generation without mutating datasets.
    if let Err(err) = build_batch_plan(&config, config_path) {
        return Ok(match err {
            OrchestratorError::Repair(e) => {
                BatchCheckReport::failure(vec![format!("repair planning failed: {e}")])
            }
            other => BatchCheckReport::failure(vec![other.to_string()]),
        });
    }

    if !output_root_resolved.exists() {
        warnings.push(format!(
            "output_root `{}` does not exist yet (will be created on execution)",
            output_root_resolved
        ));
    } else if !output_root_resolved.is_dir() {
        return Ok(BatchCheckReport::failure(vec![format!(
            "output_root `{}` exists but is not a directory",
            output_root_resolved
        )]));
    }

    let mut report = BatchCheckReport::success(&config, output_root_resolved.as_str());
    report.warnings = warnings;
    Ok(report)
}

/// Ensure output parent directories can be created (used before repair, not check).
pub fn ensure_output_root(output_root: &Utf8Path) -> Result<(), OrchestratorError> {
    fs::create_dir_all(output_root.as_std_path())?;
    Ok(())
}
