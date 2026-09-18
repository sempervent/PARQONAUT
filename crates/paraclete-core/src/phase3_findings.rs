//! Phase 3 findings: bounded probes, grouping ambiguity, skipped/partial inspection aggregates.

use camino::Utf8PathBuf;
use paraclete_types::{
    system, Evidence, EvidenceKind, EvidenceLocationRef, Finding, FindingCategory, FindingCode,
    FindingLocation, FindingSeverity,
};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::dataset_infer::AnchorInferenceNote;

fn code(s: &str) -> FindingCode {
    FindingCode::try_new(s).expect("static finding code")
}

fn finalize(mut f: Finding) -> Finding {
    f.fingerprint = Some(f.compute_fingerprint());
    f
}

pub fn text_probe_bounded_finding(
    path: &Utf8PathBuf,
    hints: &Value,
    probe: &paraclete_types::ProbeMetadata,
) -> Finding {
    finalize(Finding {
        id: Uuid::new_v4(),
        code: code(system::FORMAT_TEXT_PROBE_BOUNDED),
        severity: FindingSeverity::Info,
        category: FindingCategory::Format,
        summary: "Bounded shallow text probe completed".into(),
        detail: "Second-class text formats are only sampled within configured byte limits; see probe metadata for coverage and confidence.".into(),
        evidence: vec![Evidence {
            id: Uuid::new_v4(),
            kind: EvidenceKind::FileSlice,
            summary: "Probe bounds and hints".into(),
            location_ref: Some(EvidenceLocationRef {
                path: Some(path.as_str().to_string()),
                row_group_index: None,
                partition: Vec::new(),
            }),
            payload: Some(json!({
                "hints": hints,
                "probe": probe,
            })),
            references: Vec::new(),
        }],
        locations: vec![FindingLocation {
            file: Some(path.clone()),
            columns: Vec::new(),
            partition: Vec::new(),
            row_group: None,
        }],
        recommendations: Vec::new(),
        fingerprint: None,
        attributes: Default::default(),
    })
}

pub fn inspection_partial_finding(path: &Utf8PathBuf, reason: &str) -> Finding {
    finalize(Finding {
        id: Uuid::new_v4(),
        code: code(system::ASSET_INSPECTION_PARTIAL),
        severity: FindingSeverity::Low,
        category: FindingCategory::Coverage,
        summary: "Inspection was only partial for this asset".into(),
        detail: reason.to_string(),
        evidence: vec![Evidence {
            id: Uuid::new_v4(),
            kind: EvidenceKind::FileSlice,
            summary: "Partial inspection context".into(),
            location_ref: Some(EvidenceLocationRef {
                path: Some(path.as_str().to_string()),
                row_group_index: None,
                partition: Vec::new(),
            }),
            payload: Some(json!({ "reason": reason })),
            references: Vec::new(),
        }],
        locations: vec![FindingLocation {
            file: Some(path.clone()),
            columns: Vec::new(),
            partition: Vec::new(),
            row_group: None,
        }],
        recommendations: Vec::new(),
        fingerprint: None,
        attributes: Default::default(),
    })
}

pub fn inspection_skipped_aggregate_finding(skipped: u64, sample_paths: &[Utf8PathBuf]) -> Finding {
    let sample: Vec<String> = sample_paths.iter().take(8).map(|p| p.to_string()).collect();
    finalize(Finding {
        id: Uuid::new_v4(),
        code: code(system::ASSET_INSPECTION_SKIPPED),
        severity: FindingSeverity::Info,
        category: FindingCategory::Coverage,
        summary: format!("{skipped} asset(s) skipped format-specific inspection"),
        detail: "Unknown or unsupported extensions do not receive Parquet footer or shallow text probes in this engine revision.".into(),
        evidence: vec![Evidence {
            id: Uuid::new_v4(),
            kind: EvidenceKind::FileSlice,
            summary: "Skipped count and sample paths".into(),
            location_ref: None,
            payload: Some(json!({ "skipped_assets": skipped, "sample_paths": sample })),
            references: Vec::new(),
        }],
        locations: vec![FindingLocation {
            file: None,
            columns: Vec::new(),
            partition: Vec::new(),
            row_group: None,
        }],
        recommendations: Vec::new(),
        fingerprint: None,
        attributes: Default::default(),
    })
}

pub fn grouping_ambiguous_from_notes(notes: &[AnchorInferenceNote]) -> Vec<Finding> {
    let mut out = Vec::new();
    for n in notes {
        match n {
            AnchorInferenceNote::SchemaSplit { anchor, dataset_ids } => {
                out.push(finalize(Finding {
                    id: Uuid::new_v4(),
                    code: code(system::DATASET_GROUPING_AMBIGUOUS),
                    severity: FindingSeverity::Medium,
                    category: FindingCategory::Partitioning,
                    summary: "Parquet grouping under one path anchor was ambiguous".into(),
                    detail: "Multiple readable Parquet schemas were detected under the same anchor; datasets were split by schema signature.".into(),
                    evidence: vec![Evidence {
                        id: Uuid::new_v4(),
                        kind: EvidenceKind::PartitionLayout,
                        summary: "Anchor and resulting dataset ids".into(),
                        location_ref: None,
                        payload: Some(json!({
                            "anchor": anchor,
                            "resolution": "schema_split",
                            "dataset_ids": dataset_ids,
                        })),
                        references: Vec::new(),
                    }],
                    locations: vec![FindingLocation {
                        file: None,
                        columns: Vec::new(),
                        partition: Vec::new(),
                        row_group: None,
                    }],
                    recommendations: Vec::new(),
                    fingerprint: None,
                    attributes: Default::default(),
                }));
            }
            AnchorInferenceNote::MixedSchemaPartPattern { anchor, dataset_id } => {
                out.push(finalize(Finding {
                    id: Uuid::new_v4(),
                    code: code(system::DATASET_GROUPING_AMBIGUOUS),
                    severity: FindingSeverity::Low,
                    category: FindingCategory::Partitioning,
                    summary: "Mixed Parquet schemas co-located under part-file naming".into(),
                    detail: "Schemas differ but filenames resemble Parquet shard parts; the engine kept a single dataset—verify writer intent.".into(),
                    evidence: vec![Evidence {
                        id: Uuid::new_v4(),
                        kind: EvidenceKind::PartitionLayout,
                        summary: "Anchor and dataset".into(),
                        location_ref: None,
                        payload: Some(json!({
                            "anchor": anchor,
                            "resolution": "part_file_pattern_merge",
                            "dataset_id": dataset_id,
                        })),
                        references: Vec::new(),
                    }],
                    locations: vec![FindingLocation {
                        file: None,
                        columns: Vec::new(),
                        partition: Vec::new(),
                        row_group: None,
                    }],
                    recommendations: Vec::new(),
                    fingerprint: None,
                    attributes: Default::default(),
                }));
            }
        }
    }
    out
}
