use camino::{Utf8Path, Utf8PathBuf};

use crate::error::OrchestratorError;
use crate::ids::DatasetId;
use crate::plan::BatchPlan;

/// Source and output paths for one dataset in a batch run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchPathSpec {
    pub dataset_id: DatasetId,
    pub source: Utf8PathBuf,
    pub output: Utf8PathBuf,
}

/// Returns true when two paths refer to the same location or one is nested inside the other.
pub fn paths_overlap(a: &Utf8Path, b: &Utf8Path) -> bool {
    let a = normalize_path(a);
    let b = normalize_path(b);
    a.starts_with(&b) || b.starts_with(&a)
}

fn normalize_path(path: &Utf8Path) -> Utf8PathBuf {
    std::fs::canonicalize(path.as_std_path())
        .ok()
        .and_then(|p| p.try_into().ok())
        .unwrap_or_else(|| path.to_path_buf())
}

/// Reject batch plans whose dataset source/output paths would collide.
pub fn validate_batch_overlap(specs: &[BatchPathSpec]) -> Result<(), OrchestratorError> {
    for spec in specs {
        if paths_overlap(&spec.source, &spec.output) {
            return Err(OrchestratorError::PathOverlap(format!(
                "dataset `{}`: source `{}` overlaps output `{}`",
                spec.dataset_id.0, spec.source, spec.output
            )));
        }
    }

    for i in 0..specs.len() {
        for j in (i + 1)..specs.len() {
            let left = &specs[i];
            let right = &specs[j];
            if let Some(detail) = cross_overlap_detail(left, right) {
                return Err(OrchestratorError::PathOverlap(detail));
            }
        }
    }
    Ok(())
}

fn cross_overlap_detail(left: &BatchPathSpec, right: &BatchPathSpec) -> Option<String> {
    let pairs = [
        ("source", &left.source, "source", &right.source),
        ("source", &left.source, "output", &right.output),
        ("output", &left.output, "source", &right.source),
        ("output", &left.output, "output", &right.output),
    ];
    for (left_kind, left_path, right_kind, right_path) in pairs {
        if paths_overlap(left_path, right_path) {
            return Some(format!(
                "datasets `{}` and `{}`: {} `{}` overlaps {} `{}`",
                left.dataset_id.0, right.dataset_id.0, left_kind, left_path, right_kind, right_path
            ));
        }
    }
    None
}

/// Validate path safety for a batch plan using embedded output paths.
pub fn validate_batch_plan(plan: &BatchPlan) -> Result<(), OrchestratorError> {
    let specs: Vec<BatchPathSpec> = plan
        .datasets
        .iter()
        .map(|ds| BatchPathSpec {
            dataset_id: ds.dataset_id.clone(),
            source: Utf8PathBuf::from(&ds.source_path),
            output: Utf8PathBuf::from(&ds.output_path),
        })
        .collect();
    validate_batch_overlap(&specs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::DatasetId;

    #[test]
    fn rejects_source_output_overlap() {
        let root = Utf8PathBuf::from("/data/project");
        let err = validate_batch_overlap(&[BatchPathSpec {
            dataset_id: DatasetId("a".into()),
            source: root.clone(),
            output: root.join("nested"),
        }])
        .unwrap_err();
        assert!(matches!(err, OrchestratorError::PathOverlap(_)));
    }

    #[test]
    fn rejects_cross_dataset_output_overlap() {
        let err = validate_batch_overlap(&[
            BatchPathSpec {
                dataset_id: DatasetId("a".into()),
                source: Utf8PathBuf::from("/data/a/src"),
                output: Utf8PathBuf::from("/data/shared/out"),
            },
            BatchPathSpec {
                dataset_id: DatasetId("b".into()),
                source: Utf8PathBuf::from("/data/b/src"),
                output: Utf8PathBuf::from("/data/shared/out/nested"),
            },
        ])
        .unwrap_err();
        assert!(matches!(err, OrchestratorError::PathOverlap(_)));
    }

    #[test]
    fn accepts_disjoint_paths() {
        validate_batch_overlap(&[
            BatchPathSpec {
                dataset_id: DatasetId("a".into()),
                source: Utf8PathBuf::from("/data/a/src"),
                output: Utf8PathBuf::from("/data/a/out"),
            },
            BatchPathSpec {
                dataset_id: DatasetId("b".into()),
                source: Utf8PathBuf::from("/data/b/src"),
                output: Utf8PathBuf::from("/data/b/out"),
            },
        ])
        .unwrap();
    }
}
