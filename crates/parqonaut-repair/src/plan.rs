use camino::Utf8Path;
use chrono::{DateTime, Utc};
use parqonaut_types::{Finding, ScanReport};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::action::{EvidenceRef, ExpectedOutcome, Precondition, RepairAction};
use crate::deps::assign_dependencies;
use crate::diagnose::diagnose;
use crate::error::RepairError;
use crate::fingerprint::{compute_dataset_fingerprint, DatasetFingerprint};
use crate::inventory::DatasetInventory;
use crate::rules::apply_repair_rules;
use crate::safety::RepairSafety;
use crate::schema::canonical::UnresolvableSchemaConflict;
use crate::schema::types::{FieldDescriptor, SchemaDiff};
use crate::schema::{resolve_canonical_schema, views_from_inventory};
use crate::schema_policy::EffectivePolicy;
use crate::schema_rules::{apply_rename_rules, apply_schema_rules};
use crate::stable_id::{canonical_json, stable_hex_id};

pub const PLAN_SCHEMA_VERSION: u32 = 1;
pub const PARQONAUT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairOperation {
    pub operation_id: String,
    pub finding_ids: Vec<String>,
    pub safety: RepairSafety,
    pub action: RepairAction,
    pub rationale: String,
    pub evidence: Vec<EvidenceRef>,
    pub preconditions: Vec<Precondition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairPlan {
    pub schema_version: u32,
    pub plan_id: String,
    pub parqonaut_version: String,
    pub dataset_root: String,
    pub dataset_fingerprint: DatasetFingerprint,
    pub policy_fingerprint: String,
    pub generated_at: DateTime<Utc>,
    pub source_scan_id: Uuid,
    pub policy: EffectivePolicy,
    pub operations: Vec<RepairOperation>,
    pub expected_outcomes: Vec<ExpectedOutcome>,
    pub diagnosis_findings: Vec<Finding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_diff: Option<SchemaDiff>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_conflicts: Option<Vec<UnresolvableSchemaConflict>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_schema: Option<Vec<FieldDescriptor>>,
}

impl RepairPlan {
    pub fn validate_version(&self) -> Result<(), RepairError> {
        if self.schema_version != PLAN_SCHEMA_VERSION {
            return Err(RepairError::UnsupportedPlanVersion {
                found: self.schema_version,
                supported: PLAN_SCHEMA_VERSION,
            });
        }
        Ok(())
    }

    pub fn safe_operations(&self) -> impl Iterator<Item = &RepairOperation> {
        self.operations.iter().filter(|o| o.safety == RepairSafety::Safe)
    }
}

pub fn generate_plan(
    root: &Utf8Path,
    scan: &ScanReport,
    policy: &EffectivePolicy,
    target_schema: Option<Vec<FieldDescriptor>>,
) -> Result<RepairPlan, RepairError> {
    let inventory = DatasetInventory::from_scan_report(root, scan)?;
    let diagnosis = diagnose(root, scan, &policy.repair)?;
    let mut proposed = apply_repair_rules(root, scan, &diagnosis.findings, &inventory, policy)?;

    let views = views_from_inventory(&inventory);
    let explicit = target_schema.as_deref();
    let resolution = resolve_canonical_schema(&views, explicit, &policy.schema);
    let canonical_fields =
        resolution.target_schema.as_deref().unwrap_or(&resolution.diff.canonical_schema);

    proposed.extend(apply_rename_rules(&inventory, &policy.schema)?);
    proposed.extend(apply_schema_rules(
        root,
        &inventory,
        &resolution.diff,
        canonical_fields,
        &policy.schema,
    )?);

    if !resolution.conflicts.is_empty() {
        proposed.extend(apply_schema_rules_blocked(&resolution.conflicts));
    }

    let schema_diff = Some(resolution.diff);
    let schema_conflicts =
        if resolution.conflicts.is_empty() { None } else { Some(resolution.conflicts) };
    let resolved_target = resolution.target_schema;

    let fingerprint = compute_dataset_fingerprint(root, &inventory)?;
    let policy_fp = policy.fingerprint();

    let mut expected_outcomes = Vec::new();
    let mut operations: Vec<RepairOperation> = proposed
        .into_iter()
        .map(|p| {
            expected_outcomes.extend(p.expected_outcomes.clone());
            RepairOperation {
                operation_id: p.operation_id,
                finding_ids: p.finding_ids,
                safety: p.safety,
                action: p.action,
                rationale: p.rationale,
                evidence: p.evidence,
                preconditions: p.preconditions,
                depends_on: Vec::new(),
            }
        })
        .collect();

    assign_dependencies(&mut operations);

    let plan_content = serde_json::json!({
        "fingerprint": fingerprint.digest,
        "policy_fingerprint": policy_fp,
        "operations": operations.iter().map(|o| (&o.operation_id, &o.action, &o.safety)).collect::<Vec<_>>(),
        "target_schema": resolved_target,
    });
    let plan_id = stable_hex_id("plan", &canonical_json(&plan_content));

    Ok(RepairPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        plan_id,
        parqonaut_version: PARQONAUT_VERSION.to_string(),
        dataset_root: root.as_str().to_string(),
        dataset_fingerprint: fingerprint,
        policy_fingerprint: policy_fp,
        generated_at: Utc::now(),
        source_scan_id: scan.metadata.scan_id,
        policy: policy.clone(),
        operations,
        expected_outcomes,
        diagnosis_findings: diagnosis.findings,
        schema_diff,
        schema_conflicts,
        target_schema: resolved_target,
    })
}

fn apply_schema_rules_blocked(
    conflicts: &[UnresolvableSchemaConflict],
) -> Vec<crate::rules::ProposedOperation> {
    conflicts
        .iter()
        .enumerate()
        .map(|(i, c)| crate::rules::ProposedOperation {
            operation_id: format!("blocked-schema-{i:04}"),
            finding_ids: vec![format!("schema-conflict-{}", c.field)],
            safety: RepairSafety::Blocked,
            action: RepairAction::AlignSchema {
                field: c.field.clone(),
                from_type: c.variants.first().map(|v| v.physical_type.clone()).unwrap_or_default(),
                to_type: c.variants.get(1).map(|v| v.physical_type.clone()).unwrap_or_default(),
            },
            rationale: format!(
                "Blocked: unresolvable schema conflict on `{}` — {}",
                c.field, c.reason
            ),
            evidence: vec![],
            preconditions: vec![],
            expected_outcomes: vec![],
        })
        .collect()
}

impl RepairPlan {
    pub fn from_json(bytes: &[u8]) -> Result<Self, RepairError> {
        let plan: Self =
            serde_json::from_slice(bytes).map_err(|e| RepairError::InvalidPlan(e.to_string()))?;
        plan.validate_version()?;
        Ok(plan)
    }

    pub fn to_json_pretty(&self) -> Result<String, RepairError> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}
