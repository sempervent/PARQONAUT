use camino::Utf8Path;

use crate::action::{EvidenceRef, ExpectedOutcome, Precondition, RepairAction};
use crate::inventory::DatasetInventory;
use crate::rules::ProposedOperation;
use crate::safety::RepairSafety;
use crate::schema::compatibility::Compatibility;
use crate::schema::types::{FieldDescriptor, SchemaDiff, SchemaDifferenceKind};
use crate::schema_policy::SchemaPolicy;
use crate::stable_id::{canonical_json, stable_hex_id};
use serde_json::json;

fn schema_signature_has_column(signature: &str, column: &str) -> bool {
    signature.split('\u{001f}').any(|part| part.starts_with(&format!("{column}=")))
}

/// Explicit rename map entries from policy → ReviewRequired rename operations.
pub fn apply_rename_rules(
    inventory: &DatasetInventory,
    policy: &SchemaPolicy,
) -> Result<Vec<ProposedOperation>, crate::error::RepairError> {
    let mut ops = Vec::new();
    for (from, to) in &policy.rename {
        if from == to {
            continue;
        }
        let mut affected = 0usize;
        for file in &inventory.parquet_files {
            if schema_signature_has_column(&file.schema_signature, from) {
                affected += 1;
            }
        }
        if affected == 0 {
            continue;
        }
        let content = json!({"from": from, "to": to});
        let operation_id = stable_hex_id("schema-rename", &canonical_json(&content));
        ops.push(ProposedOperation {
            operation_id,
            finding_ids: vec![format!("schema-rename-{from}")],
            safety: RepairSafety::ReviewRequired,
            action: RepairAction::RenameColumn { from: from.clone(), to: to.clone() },
            rationale: format!("Explicit rename map: `{from}` → `{to}` in {affected} file(s)"),
            evidence: vec![EvidenceRef {
                finding_id: from.clone(),
                evidence_summary: "Explicit schema rename policy".into(),
                payload: Some(json!({"from": from, "to": to, "affected_files": affected})),
            }],
            preconditions: vec![Precondition {
                kind: "explicit_authorization".into(),
                description: "Requires --authorize <operation_id>".into(),
            }],
            expected_outcomes: vec![ExpectedOutcome {
                metric: "schema_field".into(),
                before: json!({"name": from}),
                after_expected: json!({"name": to}),
            }],
        });
    }
    Ok(ops)
}

pub fn apply_schema_rules(
    root: &Utf8Path,
    inventory: &DatasetInventory,
    diff: &SchemaDiff,
    canonical: &[FieldDescriptor],
    policy: &SchemaPolicy,
) -> Result<Vec<ProposedOperation>, crate::error::RepairError> {
    let _ = (root, inventory, policy);
    let mut ops = Vec::new();

    for field_diff in &diff.fields {
        if field_diff.difference == SchemaDifferenceKind::Identical {
            continue;
        }

        let (safety, prefix) = match field_diff.compatibility {
            Compatibility::Lossless => (RepairSafety::ReviewRequired, "schema-widen"),
            Compatibility::ConditionallyLossless => (RepairSafety::ReviewRequired, "schema-cond"),
            Compatibility::PotentiallyLossy | Compatibility::Unsupported => {
                continue;
            }
        };

        let left = field_diff.left.as_ref().expect("left field");
        let fallback = canonical.iter().find(|f| f.name == left.name).cloned().unwrap_or_default();
        let right = field_diff.right.as_ref().unwrap_or(&fallback);

        let action = if left.physical_type != right.physical_type {
            RepairAction::CastColumn {
                column: field_diff.path.display(),
                from_type: left.physical_type.clone(),
                to_type: right.physical_type.clone(),
            }
        } else if left.nullable != right.nullable {
            RepairAction::CastColumn {
                column: field_diff.path.display(),
                from_type: format!("nullable={}", left.nullable),
                to_type: format!("nullable={}", right.nullable),
            }
        } else if policy.allow_column_reorder {
            RepairAction::AlignSchema {
                field: field_diff.path.display(),
                from_type: left.physical_type.clone(),
                to_type: right.physical_type.clone(),
            }
        } else {
            continue;
        };

        let content = json!({"field": field_diff.path.display(), "action": action});
        let operation_id = stable_hex_id(prefix, &canonical_json(&content));

        ops.push(ProposedOperation {
            operation_id,
            finding_ids: vec![format!("schema-diff-{}", field_diff.path.display())],
            safety,
            action,
            rationale: format!(
                "Harmonize column `{}`: {:?} → {:?} ({:?}, {:?})",
                field_diff.path.display(),
                left,
                right,
                field_diff.difference,
                field_diff.compatibility
            ),
            evidence: vec![EvidenceRef {
                finding_id: field_diff.path.display(),
                evidence_summary: "Structured schema diff".into(),
                payload: Some(json!(field_diff)),
            }],
            preconditions: vec![Precondition {
                kind: "explicit_authorization".into(),
                description: "Requires --authorize <operation_id>".into(),
            }],
            expected_outcomes: vec![ExpectedOutcome {
                metric: "schema_field".into(),
                before: json!(left),
                after_expected: json!(right),
            }],
        });
    }

    Ok(ops)
}
