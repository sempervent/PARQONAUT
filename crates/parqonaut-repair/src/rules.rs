use camino::Utf8Path;
use paraclete_types::{system, Finding, ScanReport};

use crate::action::{EvidenceRef, ExpectedOutcome, Precondition, RepairAction};
use crate::error::RepairError;
use crate::fingerprint::relativize;
use crate::inventory::DatasetInventory;
use crate::safety::RepairSafety;
use crate::schema_policy::EffectivePolicy;
use crate::stable_id::{canonical_json, stable_hex_id};
use serde_json::json;

#[derive(Debug, Clone)]
pub struct ProposedOperation {
    pub operation_id: String,
    pub finding_ids: Vec<String>,
    pub safety: RepairSafety,
    pub action: RepairAction,
    pub rationale: String,
    pub evidence: Vec<EvidenceRef>,
    pub preconditions: Vec<Precondition>,
    pub expected_outcomes: Vec<ExpectedOutcome>,
}

pub fn apply_repair_rules(
    root: &Utf8Path,
    scan: &ScanReport,
    diagnosis_findings: &[Finding],
    inventory: &DatasetInventory,
    policy: &EffectivePolicy,
) -> Result<Vec<ProposedOperation>, RepairError> {
    let mut ops = Vec::new();

    for finding in diagnosis_findings {
        match finding.code.as_str() {
            system::REPAIR_EXCESSIVE_SMALL_FILES => {
                let threshold = policy.repair.small_file_threshold_bytes();
                let small: Vec<_> = inventory.small_files(threshold);
                if small.len() < policy.repair.min_files_for_merge {
                    continue;
                }
                let mut key_counts: std::collections::BTreeMap<String, u64> =
                    std::collections::BTreeMap::new();
                for f in &small {
                    let key = crate::inventory::DatasetInventory::structural_schema_key(&f.fields);
                    *key_counts.entry(key).or_default() += 1;
                }
                let dominant_key =
                    key_counts.iter().max_by_key(|(_, count)| *count).map(|(key, _)| key.clone());
                let input_paths: Vec<String> = small
                    .iter()
                    .filter(|f| f.size_bytes <= policy.files.max_file_bytes())
                    .filter(|f| match dominant_key.as_ref() {
                        None => true,
                        Some(key) => {
                            crate::inventory::DatasetInventory::structural_schema_key(&f.fields)
                                == *key
                        }
                    })
                    .filter_map(|f| relativize(root, &f.path).ok())
                    .collect();
                if input_paths.len() < policy.repair.min_files_for_merge {
                    continue;
                }
                let action = RepairAction::MergeSmallFiles {
                    target_bytes: policy.repair.merge_target_bytes(),
                    input_paths: input_paths.clone(),
                };
                ops.push(build_op(
                    "merge",
                    finding,
                    RepairSafety::Safe,
                    action,
                    format!(
                        "Merge {} small files (median {} KiB) toward ~{} MiB targets",
                        small.len(),
                        inventory.median_file_size() / 1024,
                        policy.repair.merge_target_mb
                    ),
                    vec![Precondition {
                        kind: "schema_compatible".into(),
                        description: "All merged files share compatible schema signatures".into(),
                    }],
                    vec![ExpectedOutcome {
                        metric: "file_count".into(),
                        before: json!(inventory.parquet_files.len()),
                        after_expected: json!("fewer"),
                    }],
                ));
            }
            system::REPAIR_INEFFICIENT_ROW_GROUPS => {
                let paths: Vec<String> = inventory
                    .parquet_files
                    .iter()
                    .filter_map(|f| relativize(root, &f.path).ok())
                    .collect();
                let action = RepairAction::ResizeRowGroups {
                    target_bytes: policy.repair.target_row_group_bytes(),
                    target_paths: paths,
                };
                ops.push(build_op(
                    "rowgroups",
                    finding,
                    RepairSafety::Safe,
                    action,
                    format!(
                        "Normalize row groups toward {} MiB target",
                        policy.repair.target_row_group_mb
                    ),
                    vec![Precondition {
                        kind: "readable_parquet".into(),
                        description: "Target files must be readable Parquet".into(),
                    }],
                    vec![ExpectedOutcome {
                        metric: "row_group_size".into(),
                        before: json!(inventory
                            .parquet_files
                            .first()
                            .map(|f| f.median_row_group_bytes)),
                        after_expected: json!(policy.repair.target_row_group_bytes()),
                    }],
                ));
            }
            system::REPAIR_INCONSISTENT_COMPRESSION => {
                let paths: Vec<String> = inventory
                    .parquet_files
                    .iter()
                    .filter_map(|f| relativize(root, &f.path).ok())
                    .collect();
                let action = RepairAction::Recompress {
                    codec: policy.repair.default_compression.clone(),
                    target_paths: paths,
                };
                ops.push(build_op(
                    "compress",
                    finding,
                    RepairSafety::Safe,
                    action,
                    format!(
                        "Normalize compression to `{}` per repair policy",
                        policy.repair.default_compression
                    ),
                    vec![Precondition {
                        kind: "lossless_rewrite".into(),
                        description: "Recompression preserves logical values".into(),
                    }],
                    vec![ExpectedOutcome {
                        metric: "compression_codec".into(),
                        before: json!(inventory.all_compression_codecs()),
                        after_expected: json!([policy.repair.default_compression.to_uppercase()]),
                    }],
                ));
            }
            _ => {}
        }
    }

    let no_stats: Vec<_> = inventory.parquet_files.iter().filter(|f| !f.has_statistics).collect();
    if !no_stats.is_empty() {
        let paths: Vec<String> =
            no_stats.iter().filter_map(|f| relativize(root, &f.path).ok()).collect();
        ops.push(ProposedOperation {
            operation_id: stable_hex_id("stats", &canonical_json(&json!({"paths": paths}))),
            finding_ids: vec!["missing-statistics".into()],
            safety: RepairSafety::Safe,
            action: RepairAction::RebuildStatistics { target_paths: paths },
            rationale: format!(
                "Rebuild statistics for {} file(s) missing column stats",
                no_stats.len()
            ),
            evidence: vec![],
            preconditions: vec![],
            expected_outcomes: vec![],
        });
    }

    for f in &inventory.parquet_files {
        if f.size_bytes > policy.files.max_file_bytes() {
            if let Ok(rel) = relativize(root, &f.path) {
                ops.push(ProposedOperation {
                    operation_id: stable_hex_id(
                        "split",
                        &canonical_json(&json!({"path": rel, "size": f.size_bytes})),
                    ),
                    finding_ids: vec![format!("oversized-{rel}")],
                    safety: RepairSafety::Safe,
                    action: RepairAction::SplitLargeFile {
                        target_bytes: policy.files.split_target_bytes(),
                        input_path: rel,
                    },
                    rationale: format!(
                        "Split oversized file {} ({} MiB > {} MiB max)",
                        f.path,
                        f.size_bytes / (1024 * 1024),
                        policy.files.max_file_mb
                    ),
                    evidence: vec![],
                    preconditions: vec![],
                    expected_outcomes: vec![],
                });
            }
        }
    }

    // Also map existing scan finding for tiny row groups if diagnosis did not already fire.
    let has_rg_op = ops.iter().any(|o| matches!(o.action, RepairAction::ResizeRowGroups { .. }));
    if !has_rg_op {
        for finding in &scan.findings {
            if finding.code.as_str() == system::METADATA_ROW_GROUP_SUSPICIOUSLY_SMALL {
                let paths: Vec<String> = inventory
                    .parquet_files
                    .iter()
                    .filter_map(|f| relativize(root, &f.path).ok())
                    .collect();
                let action = RepairAction::ResizeRowGroups {
                    target_bytes: policy.repair.target_row_group_bytes(),
                    target_paths: paths,
                };
                ops.push(build_op(
                    "rowgroups-scan",
                    finding,
                    RepairSafety::Safe,
                    action,
                    "Normalize suspiciously small row groups detected during scan".into(),
                    vec![],
                    vec![],
                ));
            }
        }
    }

    ops.sort_by(|a, b| a.operation_id.cmp(&b.operation_id));
    Ok(ops)
}

fn build_op(
    prefix: &str,
    finding: &Finding,
    safety: RepairSafety,
    action: RepairAction,
    rationale: String,
    preconditions: Vec<Precondition>,
    expected_outcomes: Vec<ExpectedOutcome>,
) -> ProposedOperation {
    let content = json!({
        "code": finding.code.as_str(),
        "action": action,
    });
    let operation_id = stable_hex_id(prefix, &canonical_json(&content));
    ProposedOperation {
        operation_id,
        finding_ids: vec![finding.code.as_str().to_string()],
        safety,
        action,
        rationale,
        evidence: vec![EvidenceRef {
            finding_id: finding.id.to_string(),
            evidence_summary: finding.summary.clone(),
            payload: finding.evidence.first().and_then(|e| e.payload.clone()),
        }],
        preconditions,
        expected_outcomes,
    }
}
