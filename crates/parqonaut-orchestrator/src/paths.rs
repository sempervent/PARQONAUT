use std::collections::{HashMap, HashSet};

use camino::Utf8Path;
use parqonaut_storage::location::DatasetLocation;

use crate::config::{BatchConfig, DatasetConfig};
use crate::error::OrchestratorError;
use crate::ids::DatasetId;
use crate::location::{join_output_segment, location_display, parse_output_override};
use crate::overlap::{validate_batch_overlap, BatchPathSpec};

/// Resolved source/output locations for every dataset in a batch configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputMapping {
    pub dataset_id: DatasetId,
    pub source: DatasetLocation,
    pub output: DatasetLocation,
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

    pub fn source_display(&self) -> String {
        location_display(&self.source)
    }

    pub fn output_display(&self) -> String {
        location_display(&self.output)
    }
}

/// Derive deterministic per-dataset output locations from batch configuration.
pub fn resolve_output_mappings(
    config: &BatchConfig,
    config_base: &Utf8Path,
) -> Result<Vec<OutputMapping>, OrchestratorError> {
    let output_root = config.require_output_root()?.resolve(config_base)?;
    let mut mappings = Vec::with_capacity(config.datasets.len());
    for ds in &config.datasets {
        let source = ds.path.canonicalize_local(config_base)?;
        let output = resolve_dataset_output(config_base, &output_root, ds)?;
        mappings.push(OutputMapping { dataset_id: ds.id(), source, output });
    }
    let mut seen_outputs = HashSet::new();
    for mapping in &mappings {
        let key = location_display(&mapping.output);
        if !seen_outputs.insert(key) {
            return Err(OrchestratorError::PathOverlap(format!(
                "datasets `{}` and others target the same output `{}`",
                mapping.dataset_id.0,
                mapping.output_display()
            )));
        }
    }
    validate_batch_overlap(&OutputMapping::specs(&mappings))?;
    Ok(mappings)
}

fn resolve_dataset_output(
    config_base: &Utf8Path,
    output_root: &DatasetLocation,
    ds: &DatasetConfig,
) -> Result<DatasetLocation, OrchestratorError> {
    match &ds.output {
        None => join_output_segment(output_root, ds.id.as_str()),
        Some(raw) => {
            let text = raw.as_str();
            if text.contains("://") || raw.is_absolute() {
                parse_output_override(text, config_base)
            } else {
                join_output_segment(output_root, text)
            }
        }
    }
}

/// Local directory for durable run journals and plan snapshots.
pub fn resolve_run_root(
    config: &BatchConfig,
    config_base: &Utf8Path,
) -> Result<camino::Utf8PathBuf, OrchestratorError> {
    if let Some(run_root) = &config.batch.run_root {
        return Ok(BatchConfig::resolve_path(config_base, run_root));
    }
    let output_root = config.require_output_root()?.resolve(config_base)?;
    match output_root {
        DatasetLocation::Local(local) => Ok(local.path),
        DatasetLocation::S3(_) => Ok(config_base.join(".parqonaut").join("runs")),
    }
}

pub fn mappings_to_map(mappings: &[OutputMapping]) -> HashMap<DatasetId, DatasetLocation> {
    mappings.iter().map(|m| (m.dataset_id.clone(), m.output.clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BatchConfig, BatchSection, DatasetConfig};
    use crate::location::ConfigLocation;
    use std::fs;
    use tempfile::TempDir;

    fn cfg(base: &camino::Utf8PathBuf, ids: &[(&str, Option<&str>)]) -> BatchConfig {
        BatchConfig {
            schema_version: 1,
            batch: BatchSection {
                name: "t".into(),
                max_concurrency: 2,
                output_root: Some(ConfigLocation(DatasetLocation::parse("out").expect("parse"))),
                run_root: None,
                max_storage_requests: 8,
            },
            datasets: ids
                .iter()
                .map(|(id, output)| DatasetConfig {
                    id: (*id).into(),
                    path: ConfigLocation(
                        DatasetLocation::parse(base.join("src").join(id).as_str()).unwrap(),
                    ),
                    policy: None,
                    target_schema: None,
                    authorize: vec![],
                    output: output.map(camino::Utf8PathBuf::from),
                })
                .collect(),
        }
    }

    #[test]
    fn deterministic_output_mapping() {
        let tmp = TempDir::new().unwrap();
        let base = camino::Utf8PathBuf::from(tmp.path().to_str().unwrap());
        fs::create_dir_all(base.join("src/a")).unwrap();
        fs::create_dir_all(base.join("src/b")).unwrap();
        let maps =
            resolve_output_mappings(&cfg(&base, &[("a", None), ("b", Some("custom-b"))]), &base)
                .unwrap();
        match &maps[0].output {
            DatasetLocation::Local(l) => assert!(l.path.as_str().ends_with("out/a")),
            _ => panic!("expected local output"),
        }
        match &maps[1].output {
            DatasetLocation::Local(l) => assert!(l.path.as_str().ends_with("out/custom-b")),
            _ => panic!("expected local output"),
        }
    }

    #[test]
    fn rejects_duplicate_output_targets() {
        let tmp = TempDir::new().unwrap();
        let base = camino::Utf8PathBuf::from(tmp.path().to_str().unwrap());
        fs::create_dir_all(base.join("src/a")).unwrap();
        fs::create_dir_all(base.join("src/b")).unwrap();
        let err = resolve_output_mappings(&cfg(&base, &[("a", None), ("b", Some("a"))]), &base)
            .unwrap_err();
        assert!(matches!(err, OrchestratorError::PathOverlap(_)));
    }

    #[test]
    fn run_root_defaults_local_under_config_for_remote_output_root() {
        let cfg = BatchConfig {
            schema_version: 1,
            batch: BatchSection {
                name: "remote".into(),
                max_concurrency: 1,
                output_root: Some(ConfigLocation(
                    DatasetLocation::parse("s3://bucket/batch/").unwrap(),
                )),
                run_root: None,
                max_storage_requests: 4,
            },
            datasets: vec![],
        };
        let base = camino::Utf8PathBuf::from("/cfg");
        let run_root = resolve_run_root(&cfg, &base).unwrap();
        assert_eq!(run_root, camino::Utf8PathBuf::from("/cfg/.parqonaut/runs"));
    }
}
