use camino::Utf8Path;
use parqonaut_types::{
    system, Evidence, EvidenceKind, EvidenceLocationRef, Finding, FindingCategory, FindingCode,
    FindingLocation, FindingSeverity, ScanReport,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::error::RepairError;
use crate::inventory::DatasetInventory;
use crate::policy::RepairPolicy;

fn code(s: &str) -> FindingCode {
    FindingCode::try_new(s).expect("static finding code")
}

fn finalize(mut f: Finding) -> Finding {
    f.fingerprint = Some(f.compute_fingerprint());
    f
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosisReport {
    pub scan_id: Uuid,
    pub findings: Vec<Finding>,
    pub total_rows: i64,
    pub parquet_file_count: usize,
}

/// Enriches a scan report with repair-oriented findings backed by inventory evidence.
pub fn diagnose(
    root: &Utf8Path,
    scan: &ScanReport,
    policy: &RepairPolicy,
) -> Result<DiagnosisReport, RepairError> {
    let inventory = DatasetInventory::from_scan_report(root, scan)?;
    let mut findings = scan.findings.clone();

    if inventory.parquet_files.is_empty() {
        return Ok(DiagnosisReport {
            scan_id: scan.metadata.scan_id,
            findings,
            total_rows: 0,
            parquet_file_count: 0,
        });
    }

    let threshold = policy.small_file_threshold_bytes();
    let small = inventory.small_files(threshold);
    if small.len() >= policy.min_files_for_merge {
        let paths: Vec<String> = small.iter().map(|f| f.path.as_str().to_string()).collect();
        let sizes: Vec<u64> = small.iter().map(|f| f.size_bytes).collect();
        let median = crate::inventory::median_u64(&sizes);
        findings.push(finalize(Finding {
            id: Uuid::new_v4(),
            code: code(system::REPAIR_EXCESSIVE_SMALL_FILES),
            severity: FindingSeverity::Medium,
            category: FindingCategory::Metadata,
            summary: format!(
                "Dataset contains {} pathologically small Parquet files",
                small.len()
            ),
            detail: format!(
                "{} files are under {} MiB (median {} KiB). Consolidation may improve scan and read efficiency.",
                small.len(),
                policy.small_file_threshold_mb,
                median / 1024
            ),
            evidence: vec![Evidence {
                id: Uuid::new_v4(),
                kind: EvidenceKind::ParquetFooter,
                summary: "Small file inventory".into(),
                location_ref: Some(EvidenceLocationRef {
                    path: Some(root.as_str().to_string()),
                    row_group_index: None,
                    partition: Vec::new(),
                }),
                payload: Some(json!({
                    "file_count": small.len(),
                    "median_size_bytes": median,
                    "threshold_bytes": threshold,
                    "paths_sample": paths.iter().take(5).collect::<Vec<_>>(),
                })),
                references: Vec::new(),
            }],
            locations: small
                .iter()
                .take(10)
                .map(|f| FindingLocation {
                    file: Some(f.path.clone()),
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

    let target_rg = policy.target_row_group_bytes();
    let threshold = policy.small_file_threshold_bytes();
    let mut inefficient_files: Vec<&crate::inventory::ParquetFileMeta> = Vec::new();
    for f in &inventory.parquet_files {
        let tiny_groups = f.num_row_groups >= 8 && f.median_row_group_bytes < target_rg / 64;
        let huge_groups =
            f.median_row_group_bytes > target_rg * 2 && f.num_row_groups <= 2 && f.num_rows > 1000;
        let shard_single_group = inventory.parquet_files.len() >= policy.min_files_for_merge
            && f.num_row_groups == 1
            && f.size_bytes < threshold
            && f.num_rows > 0
            && f.num_rows <= 5000;
        if tiny_groups || huge_groups || shard_single_group {
            inefficient_files.push(f);
        }
    }
    if !inefficient_files.is_empty() {
        let median_rg = crate::inventory::median_u64(
            &inefficient_files.iter().map(|f| f.median_row_group_bytes).collect::<Vec<_>>(),
        );
        findings.push(finalize(Finding {
            id: Uuid::new_v4(),
            code: code(system::REPAIR_INEFFICIENT_ROW_GROUPS),
            severity: FindingSeverity::Medium,
            category: FindingCategory::Metadata,
            summary: "Parquet row groups are inefficient for this dataset".into(),
            detail: format!(
                "{} file(s) have row groups far from the {} MiB target (median compressed RG ~ {} MiB).",
                inefficient_files.len(),
                policy.target_row_group_mb,
                median_rg / (1024 * 1024)
            ),
            evidence: vec![Evidence {
                id: Uuid::new_v4(),
                kind: EvidenceKind::ParquetFooter,
                summary: "Row group size distribution".into(),
                location_ref: None,
                payload: Some(json!({
                    "affected_files": inefficient_files.len(),
                    "median_row_group_bytes": median_rg,
                    "target_bytes": target_rg,
                })),
                references: Vec::new(),
            }],
            locations: inefficient_files
                .iter()
                .take(10)
                .map(|f| FindingLocation {
                    file: Some(f.path.clone()),
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

    let codecs = inventory.all_compression_codecs();
    if codecs.len() >= policy.min_codecs_for_inconsistency {
        findings.push(finalize(Finding {
            id: Uuid::new_v4(),
            code: code(system::REPAIR_INCONSISTENT_COMPRESSION),
            severity: FindingSeverity::Low,
            category: FindingCategory::Metadata,
            summary: "Dataset uses mixed Parquet compression codecs".into(),
            detail: format!(
                "Observed codecs: {}. Normalizing to `{}` is a representation change only when schemas and rows are preserved.",
                codecs.join(", "),
                policy.default_compression
            ),
            evidence: vec![Evidence {
                id: Uuid::new_v4(),
                kind: EvidenceKind::ParquetFooter,
                summary: "Codec inventory".into(),
                location_ref: None,
                payload: Some(json!({ "codecs": codecs })),
                references: Vec::new(),
            }],
            locations: Vec::new(),
            recommendations: Vec::new(),
            fingerprint: None,
            attributes: Default::default(),
        }));
    }

    if inventory.schema_signatures.len() > 1 {
        let sigs: Vec<serde_json::Value> = inventory
            .schema_signatures
            .iter()
            .map(|(sig, paths)| {
                json!({
                    "schema_signature": sig,
                    "file_count": paths.len(),
                    "sample_path": paths.first().map(|p| p.as_str()),
                })
            })
            .collect();
        findings.push(finalize(Finding {
            id: Uuid::new_v4(),
            code: code(system::REPAIR_SCHEMA_DRIFT),
            severity: FindingSeverity::High,
            category: FindingCategory::Metadata,
            summary: "Schema drift detected across dataset files".into(),
            detail: format!(
                "{} distinct schema signatures under `{}`. Alignment requires explicit review.",
                inventory.schema_signatures.len(),
                root
            ),
            evidence: vec![Evidence {
                id: Uuid::new_v4(),
                kind: EvidenceKind::SchemaFragment,
                summary: "Schema signature set".into(),
                location_ref: None,
                payload: Some(json!({ "signatures": sigs })),
                references: Vec::new(),
            }],
            locations: Vec::new(),
            recommendations: Vec::new(),
            fingerprint: None,
            attributes: Default::default(),
        }));
    }

    findings.sort_by(|a, b| a.code.as_str().cmp(b.code.as_str()));

    Ok(DiagnosisReport {
        scan_id: scan.metadata.scan_id,
        findings,
        total_rows: inventory.total_rows,
        parquet_file_count: inventory.parquet_files.len(),
    })
}
