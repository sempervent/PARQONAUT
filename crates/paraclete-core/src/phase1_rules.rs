//! Phase 1 built-in rules (namespaced finding codes).

use std::collections::{BTreeSet, HashSet};

use camino::Utf8PathBuf;
use paraclete_types::{
    system, Dataset, Evidence, EvidenceKind, EvidenceLocationRef, Finding, FindingCategory,
    FindingCode, FindingLocation, FindingSeverity, ResolvedAsset, ScanPlan,
};
use serde_json::json;
use uuid::Uuid;

use crate::parquet_inspect::ParquetInspection;

/// Files smaller than this (bytes) emit `system.metadata.file_too_small` for Parquet assets.
pub const FILE_TOO_SMALL_BYTES: u64 = 4096;

/// Row groups with at most this many rows are suspicious when the file is large enough.
pub const ROW_GROUP_SMALL_ROWS: i64 = 48;

/// Minimum total rows before row-group fragmentation heuristics apply.
pub const ROW_GROUP_RULE_MIN_TOTAL_ROWS: i64 = 200;

/// Minimum row groups before fragmentation heuristics apply.
pub const ROW_GROUP_RULE_MIN_GROUPS: usize = 4;

fn code(s: &str) -> FindingCode {
    FindingCode::try_new(s).expect("static finding code")
}

fn finalize(mut f: Finding) -> Finding {
    f.fingerprint = Some(f.compute_fingerprint());
    f
}

/// Evaluates Phase 1 rules against resolved assets and Parquet inspections.
pub fn evaluate_phase1_rules(
    plan: &ScanPlan,
    assets: &[ResolvedAsset],
    inspections: &[ParquetInspection],
    datasets: &[Dataset],
) -> Vec<Finding> {
    let mut out = Vec::new();

    let unknown: Vec<&ResolvedAsset> =
        assets.iter().filter(|a| a.format == paraclete_types::DataFormat::Unknown).collect();
    if !unknown.is_empty() {
        let ev: Vec<Evidence> = unknown
            .iter()
            .map(|a| Evidence {
                id: Uuid::new_v4(),
                kind: EvidenceKind::FileSlice,
                summary: format!("Unknown format for {}", a.path),
                location_ref: Some(EvidenceLocationRef {
                    path: Some(a.path.as_str().to_string()),
                    row_group_index: None,
                    partition: Vec::new(),
                }),
                payload: Some(json!({"size_bytes": a.size_bytes})),
                references: Vec::new(),
            })
            .collect();
        out.push(finalize(Finding {
            id: Uuid::new_v4(),
            code: code(system::FORMAT_UNKNOWN),
            severity: FindingSeverity::Medium,
            category: FindingCategory::Format,
            summary: "One or more files use an unknown format".into(),
            detail: "Extension and naming did not map to a supported tabular format.".into(),
            evidence: ev,
            locations: unknown
                .iter()
                .map(|a| FindingLocation {
                    file: Some(a.path.clone()),
                    columns: Vec::new(),
                    partition: Vec::new(),
                    row_group: None,
                })
                .collect(),
            recommendations: Vec::new(),
            fingerprint: None,
            attributes: Default::default(),
        }));
    }

    let mut format_set = HashSet::new();
    for a in assets {
        match a.format {
            paraclete_types::DataFormat::Unknown => {}
            other => {
                format_set.insert(other);
            }
        }
    }
    if format_set.len() >= 2 {
        out.push(finalize(Finding {
            id: Uuid::new_v4(),
            code: code(system::FORMAT_MIXED_DATASET),
            severity: FindingSeverity::Medium,
            category: FindingCategory::Format,
            summary: "Directory contains multiple tabular formats".into(),
            detail: "Mixed formats reduce shared diagnostics depth; isolate Parquet datasets where possible.".into(),
            evidence: vec![Evidence {
                id: Uuid::new_v4(),
                kind: EvidenceKind::PartitionLayout,
                summary: "Detected format mixture under scan root".into(),
                location_ref: Some(EvidenceLocationRef {
                    path: Some(plan.root.as_str().to_string()),
                    row_group_index: None,
                    partition: Vec::new(),
                }),
                payload: Some({
                    let mut labels: Vec<&'static str> =
                        format_set.iter().map(|f| format_label(f)).collect();
                    labels.sort();
                    json!({ "formats": labels })
                }),
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

    for dataset in datasets {
        let partition_sets = crate::dataset_infer::partition_key_sets_for_dataset(dataset);
        let has_nonempty = partition_sets.iter().any(|(_, k)| !k.is_empty());
        let has_empty = partition_sets.iter().any(|(_, k)| k.is_empty());
        let mut sigs: BTreeSet<Vec<String>> = BTreeSet::new();
        for (_, keys) in &partition_sets {
            if !keys.is_empty() {
                let mut v: Vec<String> = keys.iter().cloned().collect();
                v.sort();
                sigs.insert(v);
            }
        }
        let inconsistent = (has_nonempty && has_empty) || sigs.len() > 1;
        if inconsistent && !partition_sets.is_empty() {
            let mut ev: Vec<Evidence> = partition_sets
                .iter()
                .map(|(p, keys)| Evidence {
                    id: Uuid::new_v4(),
                    kind: EvidenceKind::PartitionLayout,
                    summary: format!("Partition keys for {}", p),
                    location_ref: Some(EvidenceLocationRef {
                        path: Some(p.as_str().to_string()),
                        row_group_index: None,
                        partition: Vec::new(),
                    }),
                    payload: Some(json!({
                        "dataset_id": &dataset.dataset_id,
                        "keys": keys.iter().collect::<Vec<_>>(),
                    })),
                    references: Vec::new(),
                })
                .collect();
            ev.insert(
                0,
                Evidence {
                    id: Uuid::new_v4(),
                    kind: EvidenceKind::PartitionLayout,
                    summary: "Affected dataset".into(),
                    location_ref: None,
                    payload: Some(json!({ "dataset_id": &dataset.dataset_id })),
                    references: Vec::new(),
                },
            );
            out.push(finalize(Finding {
                id: Uuid::new_v4(),
                code: code(system::PARTITION_INCONSISTENT_KEYS),
                severity: FindingSeverity::High,
                category: FindingCategory::Partitioning,
                summary: "Hive-style partition keys are inconsistent across files".into(),
                detail: format!(
                    "Dataset `{}` mixes partition key sets or combines partitioned and unpartitioned paths.",
                    dataset.dataset_id
                ),
                evidence: ev,
                locations: partition_sets
                    .iter()
                    .map(|(p, _)| FindingLocation {
                        file: Some(p.clone()),
                        columns: Vec::new(),
                        partition: Vec::new(),
                        row_group: None,
                    })
                    .collect(),
                recommendations: Vec::new(),
                fingerprint: None,
                attributes: Default::default(),
            }));
        }
    }

    for a in assets {
        if a.format == paraclete_types::DataFormat::Parquet && a.size_bytes < FILE_TOO_SMALL_BYTES {
            out.push(finalize(Finding {
                id: Uuid::new_v4(),
                code: code(system::METADATA_FILE_TOO_SMALL),
                severity: FindingSeverity::Low,
                category: FindingCategory::Metadata,
                summary: format!("Parquet file is very small ({} bytes)", a.size_bytes),
                detail: format!(
                    "Files under {} bytes are often incomplete, truncated, or unsuitable for reliable metadata heuristics.",
                    FILE_TOO_SMALL_BYTES
                ),
                evidence: vec![Evidence {
                    id: Uuid::new_v4(),
                    kind: EvidenceKind::ParquetFooter,
                    summary: "File size on disk".into(),
                    location_ref: Some(EvidenceLocationRef {
                        path: Some(a.path.as_str().to_string()),
                        row_group_index: None,
                        partition: Vec::new(),
                    }),
                    payload: Some(json!({"size_bytes": a.size_bytes})),
                    references: Vec::new(),
                }],
                locations: vec![FindingLocation {
                    file: Some(a.path.clone()),
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

    for insp in inspections {
        if insp.num_row_groups >= ROW_GROUP_RULE_MIN_GROUPS
            && insp.num_rows >= ROW_GROUP_RULE_MIN_TOTAL_ROWS
        {
            if let Some(min_rows) = insp.row_groups.iter().map(|r| r.num_rows).min() {
                if min_rows < ROW_GROUP_SMALL_ROWS {
                    out.push(finalize(Finding {
                        id: Uuid::new_v4(),
                        code: code(system::METADATA_ROW_GROUP_SUSPICIOUSLY_SMALL),
                        severity: FindingSeverity::Medium,
                        category: FindingCategory::Metadata,
                        summary: "Parquet row groups look suspiciously small".into(),
                        detail: format!(
                            "At least one row group has fewer than {ROW_GROUP_SMALL_ROWS} rows while the file has {} rows across {} row groups, suggesting fragmentation or suboptimal write patterns.",
                            insp.num_rows, insp.num_row_groups
                        ),
                        evidence: vec![Evidence {
                            id: Uuid::new_v4(),
                            kind: EvidenceKind::ParquetFooter,
                            summary: "Row group row counts from footer".into(),
                            location_ref: Some(EvidenceLocationRef {
                                path: Some(insp.path.clone()),
                                row_group_index: None,
                                partition: Vec::new(),
                            }),
                            payload: Some(json!({
                                "num_rows": insp.num_rows,
                                "num_row_groups": insp.num_row_groups,
                                "min_row_group_rows": min_rows,
                            })),
                            references: Vec::new(),
                        }],
                        locations: vec![FindingLocation {
                            file: Some(Utf8PathBuf::from(insp.path.as_str())),
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
    }

    out
}

fn format_label(f: &paraclete_types::DataFormat) -> &'static str {
    match f {
        paraclete_types::DataFormat::Parquet => "parquet",
        paraclete_types::DataFormat::Csv => "csv",
        paraclete_types::DataFormat::Json => "json",
        paraclete_types::DataFormat::Ndjson => "ndjson",
        paraclete_types::DataFormat::Unknown => "unknown",
    }
}
