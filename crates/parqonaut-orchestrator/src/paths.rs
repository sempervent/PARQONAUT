use std::collections::HashMap;

use camino::{Utf8Path, Utf8PathBuf};

use crate::config::{BatchConfig, DatasetConfig};
use crate::error::OrchestratorError;
use crate::ids::DatasetId;
use crate::overlap::{validate_batch_overlap, BatchPathSpec};

/// Resolved source/output paths for every dataset in a batch configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputMapping {
    pub dataset_id: DatasetId,
    pub source: Utf8PathBuf,
    pub output: Utf8PathBuf,
}

impl OutputMapping {
    pub fn specs(mappings: &[OutputMapping]) -> Vec<BatchPathSpec> {
        mappings
            .iter()
            .map(|m| BatchPathSpec {
                dataset_id: m.dataset_id.clone(),
                source: m.source.clone(),
                output: m.output.clone(),
            })
            .collect()
    }
}

/// Derive deterministic per-dataset output paths from batch configuration.
///
/// Convention: `{output_root}/{dataset.output or dataset.id}` unless `output` is set.
pub fn resolve_output_mappings(
    config: &BatchConfig,
    config_base: &Utf8Path,
) -> Result<Vec<OutputMapping>, OrchestratorError> {
    let output_root = config.require_output_root()?;
    let output_root = resolve_under_base(config_base, output_root)?;
    let mut mappings = Vec::with_capacity(config.datasets.len());
    for ds in &config.datasets {
        let source = BatchConfig::canonicalize_path(config_base, &ds.path)?;
        let rel = ds.output_segment();
        if rel.is_empty() || rel.contains("..") {
            return Err(OrchestratorError::InvalidConfig(format!(
                "dataset `{}` has invalid output mapping `{rel}`",
                ds.id
            )));
        }
        let output = output_root.join(rel);
        mappings.push(OutputMapping { dataset_id: ds.id(), source, output });
    }
    let mut seen_outputs = std::collections::HashSet::new();
    for mapping in &mappings {
        if !seen_outputs.insert(mapping.output.clone()) {
            return Err(OrchestratorError::PathOverlap(format!(
                "datasets `{}` and others target the same output `{}`",
                mapping.dataset_id.0, mapping.output
            )));
        }
    }
    validate_batch_overlap(&OutputMapping::specs(&mappings))?;
    Ok(mappings)
}

pub fn mappings_to_map(mappings: &[OutputMapping]) -> HashMap<DatasetId, Utf8PathBuf> {
    mappings.iter().map(|m| (m.dataset_id.clone(), m.output.clone())).collect()
}

fn resolve_under_base(base: &Utf8Path, path: &Utf8Path) -> Result<Utf8PathBuf, OrchestratorError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(base.join(path))
    }
}

impl BatchConfig {
    pub fn require_output_root(&self) -> Result<&Utf8Path, OrchestratorError> {
        self.batch.output_root.as_deref().ok_or_else(|| {
            OrchestratorError::InvalidConfig(
                "batch.output_root is required for planning and execution".into(),
            )
        })
    }
}

impl DatasetConfig {
    pub fn output_segment(&self) -> &str {
        self.output.as_ref().map(|p| p.as_str()).unwrap_or(self.id.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BatchConfig, BatchSection, DatasetConfig};
    use std::fs;
    use tempfile::TempDir;

    fn cfg(base: &Utf8Path, ids: &[(&str, Option<&str>)]) -> BatchConfig {
        BatchConfig {
            schema_version: 1,
            batch: BatchSection {
                name: "t".into(),
                max_concurrency: 2,
                output_root: Some(Utf8PathBuf::from("out")),
            },
            datasets: ids
                .iter()
                .map(|(id, output)| DatasetConfig {
                    id: (*id).into(),
                    path: base.join("src").join(id),
                    policy: None,
                    target_schema: None,
                    authorize: vec![],
                    output: output.map(Utf8PathBuf::from),
                })
                .collect(),
        }
    }

    #[test]
    fn deterministic_output_mapping() {
        let tmp = TempDir::new().unwrap();
        let base = Utf8PathBuf::from(tmp.path().to_str().unwrap());
        fs::create_dir_all(base.join("src/a")).unwrap();
        fs::create_dir_all(base.join("src/b")).unwrap();
        let maps =
            resolve_output_mappings(&cfg(&base, &[("a", None), ("b", Some("custom-b"))]), &base)
                .unwrap();
        assert_eq!(maps[0].output, base.join("out/a"));
        assert_eq!(maps[1].output, base.join("out/custom-b"));
    }

    #[test]
    fn rejects_duplicate_output_targets() {
        let tmp = TempDir::new().unwrap();
        let base = Utf8PathBuf::from(tmp.path().to_str().unwrap());
        fs::create_dir_all(base.join("src/a")).unwrap();
        fs::create_dir_all(base.join("src/b")).unwrap();
        let err = resolve_output_mappings(&cfg(&base, &[("a", None), ("b", Some("a"))]), &base)
            .unwrap_err();
        assert!(matches!(err, OrchestratorError::PathOverlap(_)));
    }
}
