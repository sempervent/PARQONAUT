//! Phase 2 structural findings (truncation, read failures, shallow text, dataset inventory).

use camino::Utf8PathBuf;
use paraclete_types::{
    system, Evidence, EvidenceKind, EvidenceLocationRef, Finding, FindingCategory, FindingCode,
    FindingLocation, FindingSeverity,
};
use serde_json::json;
use uuid::Uuid;

use crate::CoreError;

fn code(s: &str) -> FindingCode {
    FindingCode::try_new(s).expect("static finding code")
}

fn finalize(mut f: Finding) -> Finding {
    f.fingerprint = Some(f.compute_fingerprint());
    f
}

pub fn scan_truncated_finding(root: &str, max_files: u64) -> Finding {
    finalize(Finding {
        id: Uuid::new_v4(),
        code: code(system::TARGET_SCAN_TRUNCATED),
        severity: FindingSeverity::Medium,
        category: FindingCategory::Target,
        summary: "Scan resolution was truncated by max_files".into(),
        detail: format!(
            "The local resolver stopped after reaching the configured max_files cap ({max_files}); some paths may be missing from this report."
        ),
        evidence: vec![Evidence {
            id: Uuid::new_v4(),
            kind: EvidenceKind::FileSlice,
            summary: "Truncation context".into(),
            location_ref: Some(EvidenceLocationRef {
                path: Some(root.to_string()),
                row_group_index: None,
                partition: Vec::new(),
            }),
            payload: Some(json!({ "max_files": max_files })),
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

pub fn parquet_read_failed_finding(path: &Utf8PathBuf, err: &CoreError) -> Finding {
    finalize(Finding {
        id: Uuid::new_v4(),
        code: code(system::FORMAT_PARQUET_READ_FAILED),
        severity: FindingSeverity::High,
        category: FindingCategory::Format,
        summary: "Parquet metadata could not be read".into(),
        detail: format!(
            "The engine could not deserialize Parquet footer metadata for this path: {err}"
        ),
        evidence: vec![Evidence {
            id: Uuid::new_v4(),
            kind: EvidenceKind::ParquetFooter,
            summary: "Read or parse failure".into(),
            location_ref: Some(EvidenceLocationRef {
                path: Some(path.as_str().to_string()),
                row_group_index: None,
                partition: Vec::new(),
            }),
            payload: Some(json!({ "error": err.to_string() })),
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

pub fn multiple_datasets_finding(root: &str, count: usize) -> Finding {
    finalize(Finding {
        id: Uuid::new_v4(),
        code: code(system::DATASET_MULTIPLE_DETECTED),
        severity: FindingSeverity::Info,
        category: FindingCategory::Coverage,
        summary: "Multiple logical Parquet datasets detected under one scan root".into(),
        detail: "Parquet files were grouped into more than one dataset anchor; treat each dataset summary independently.".into(),
        evidence: vec![Evidence {
            id: Uuid::new_v4(),
            kind: EvidenceKind::PartitionLayout,
            summary: "Dataset count".into(),
            location_ref: Some(EvidenceLocationRef {
                path: Some(root.to_string()),
                row_group_index: None,
                partition: Vec::new(),
            }),
            payload: Some(json!({ "dataset_count": count })),
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

pub fn unpartitioned_collection_finding(dataset_id: &str, file_count: usize) -> Finding {
    finalize(Finding {
        id: Uuid::new_v4(),
        code: code(system::PARTITION_UNPARTITIONED_COLLECTION),
        severity: FindingSeverity::Info,
        category: FindingCategory::Partitioning,
        summary: "Parquet files form a collection without Hive-style partition directories".into(),
        detail: "Multiple readable Parquet files share a dataset anchor but no Hive `key=value` path segments were observed.".into(),
        evidence: vec![Evidence {
            id: Uuid::new_v4(),
            kind: EvidenceKind::PartitionLayout,
            summary: "Co-located Parquet files".into(),
            location_ref: None,
            payload: Some(json!({ "dataset_id": dataset_id, "file_count": file_count })),
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
