use camino::Utf8Path;
use paraclete_types::{system, Finding, ScanReport};

use crate::action::{EvidenceRef, ExpectedOutcome, Precondition, RepairAction};
use crate::error::RepairError;
use crate::fingerprint::relativize;
use crate::inventory::DatasetInventory;
use crate::policy::RepairPolicy;
use crate::safety::RepairSafety;
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
    policy: &RepairPolicy,
) -> Result<Vec<ProposedOperation>, RepairError> {
    let mut ops = Vec::new();

    for finding in diagnosis_findings {
        match finding.code.as_str() {
            system::REPAIR_EXCESSIVE_SMALL_FILES => {
                let threshold = policy.small_file_threshold_bytes();
                let small: Vec<_> = inventory.small_files(threshold);
                if small.len() < policy.min_files_for_merge {
                    continue;
                }
                let dominant_sig = inventory
                    .schema_signatures
                    .iter()
                    .max_by_key(|(_, paths)| paths.len())
                    .map(|(sig, _)| sig.clone());
                let input_paths: Vec<String> = small
                    .iter()
                    .filter(|f| match dominant_sig.as_ref() {
                        None => true,
                        Some(sig) => &f.schema_signature == sig,
                    })
                    .filter_map(|f| relativize(root, &f.path).ok())
                    .collect();
                if input_paths.len() < policy.min_files_for_merge {
                    continue;
                }
                let action = RepairAction::MergeSmallFiles {
                    target_bytes: policy.merge_target_bytes(),
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
                        policy.merge_target_mb
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
                    target_bytes: policy.target_row_group_bytes(),
                    target_paths: paths,
                };
                ops.push(build_op(
                    "rowgroups",
                    finding,
                    RepairSafety::Safe,
                    action,
                    format!(
                        "Normalize row groups toward {} MiB target",
                        policy.target_row_group_mb
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
                        after_expected: json!(policy.target_row_group_bytes()),
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
                    codec: policy.default_compression.clone(),
                    target_paths: paths,
                };
                ops.push(build_op(
                    "compress",
                    finding,
                    RepairSafety::Safe,
                    action,
                    format!(
                        "Normalize compression to `{}` per repair policy",
                        policy.default_compression
                    ),
                    vec![Precondition {
                        kind: "lossless_rewrite".into(),
                        description: "Recompression preserves logical values".into(),
                    }],
                    vec![ExpectedOutcome {
                        metric: "compression_codec".into(),
                        before: json!(inventory.all_compression_codecs()),
                        after_expected: json!([policy.default_compression.to_uppercase()]),
                    }],
                ));
            }
            system::REPAIR_SCHEMA_DRIFT => {
                if inventory.schema_signatures.len() < 2 {
                    continue;
                }
                let (sig_a, paths_a) = inventory.schema_signatures.iter().next().unwrap();
                let (sig_b, paths_b) = inventory.schema_signatures.iter().nth(1).unwrap();
                let action = RepairAction::AlignSchema {
                    field: "mixed_fields".into(),
                    from_type: sig_a.clone(),
                    to_type: sig_b.clone(),
                };
                ops.push(build_op(
                    "schema",
                    finding,
                    RepairSafety::ReviewRequired,
                    action,
                    format!(
                        "Align schema between {} and {} file groups ({} vs {} files)",
                        paths_a.len(),
                        paths_b.len(),
                        truncate_sig(sig_a),
                        truncate_sig(sig_b)
                    ),
                    vec![Precondition {
                        kind: "explicit_authorization".into(),
                        description: "Schema alignment requires --authorize-review flag".into(),
                    }],
                    vec![ExpectedOutcome {
                        metric: "schema_signatures".into(),
                        before: json!(inventory.schema_signatures.len()),
                        after_expected: json!(1),
                    }],
                ));
            }
            _ => {}
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
                    target_bytes: policy.target_row_group_bytes(),
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

fn truncate_sig(sig: &str) -> String {
    if sig.len() > 16 {
        format!("{}…", &sig[..16])
    } else {
        sig.to_string()
    }
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
    let finding_id = finding
        .fingerprint
        .as_ref()
        .map(|fp| fp.digest.clone())
        .unwrap_or_else(|| finding.id.to_string());
    let content = json!({
        "finding_id": finding_id,
        "code": finding.code.as_str(),
        "action": action,
    });
    let operation_id = stable_hex_id(prefix, &canonical_json(&content));
    ProposedOperation {
        operation_id,
        finding_ids: vec![finding_id],
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
