use parqonaut_repair::locations_overlap;
use parqonaut_storage::location::DatasetLocation;

use crate::error::OrchestratorError;
use crate::ids::DatasetId;
use crate::location::location_display;
use crate::plan::BatchPlan;

/// Source and output locations for one dataset in a batch run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchPathSpec {
    pub dataset_id: DatasetId,
    pub source: DatasetLocation,
    pub output: DatasetLocation,
}

/// Reject batch plans whose dataset source/output locations would collide.
pub fn validate_batch_overlap(specs: &[BatchPathSpec]) -> Result<(), OrchestratorError> {
    for spec in specs {
        if locations_overlap(&spec.source, &spec.output) {
            return Err(OrchestratorError::PathOverlap(format!(
                "dataset `{}`: source `{}` overlaps output `{}`",
                spec.dataset_id.0,
                location_display(&spec.source),
                location_display(&spec.output)
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
        if locations_overlap(left_path, right_path) {
            return Some(format!(
                "datasets `{}` and `{}`: {} `{}` overlaps {} `{}`",
                left.dataset_id.0,
                right.dataset_id.0,
                left_kind,
                location_display(left_path),
                right_kind,
                location_display(right_path)
            ));
        }
    }
    None
}

/// Validate path safety for a batch plan using embedded output paths.
pub fn validate_batch_plan(plan: &BatchPlan) -> Result<(), OrchestratorError> {
    let specs: Vec<BatchPathSpec> = plan
        .dataset_locations()?
        .into_iter()
        .map(|(dataset_id, source, output)| BatchPathSpec { dataset_id, source, output })
        .collect();
    validate_batch_overlap(&specs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parqonaut_storage::location::S3Location;

    #[test]
    fn rejects_source_output_overlap() {
        let root = DatasetLocation::parse("/data/project").unwrap();
        let err = validate_batch_overlap(&[BatchPathSpec {
            dataset_id: DatasetId("a".into()),
            source: root.clone(),
            output: DatasetLocation::Local(parqonaut_storage::location::LocalLocation {
                path: "/data/project/nested".into(),
            }),
        }])
        .unwrap_err();
        assert!(matches!(err, OrchestratorError::PathOverlap(_)));
    }

    #[test]
    fn rejects_cross_dataset_s3_output_overlap() {
        let err = validate_batch_overlap(&[
            BatchPathSpec {
                dataset_id: DatasetId("a".into()),
                source: DatasetLocation::parse("s3://b/data/a/src").unwrap(),
                output: DatasetLocation::S3(S3Location {
                    bucket: "shared".into(),
                    prefix: "out".into(),
                }),
            },
            BatchPathSpec {
                dataset_id: DatasetId("b".into()),
                source: DatasetLocation::parse("s3://b/data/b/src").unwrap(),
                output: DatasetLocation::S3(S3Location {
                    bucket: "shared".into(),
                    prefix: "out/nested".into(),
                }),
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
                source: DatasetLocation::parse("/data/a/src").unwrap(),
                output: DatasetLocation::parse("/data/a/out").unwrap(),
            },
            BatchPathSpec {
                dataset_id: DatasetId("b".into()),
                source: DatasetLocation::parse("/data/b/src").unwrap(),
                output: DatasetLocation::parse("/data/b/out").unwrap(),
            },
        ])
        .unwrap();
    }
}
