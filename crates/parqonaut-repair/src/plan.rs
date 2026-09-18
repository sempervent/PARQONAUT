use camino::Utf8Path;
use chrono::{DateTime, Utc};
use paraclete_types::{Finding, ScanReport};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::action::{EvidenceRef, ExpectedOutcome, Precondition, RepairAction};
use crate::diagnose::diagnose;
use crate::error::RepairError;
use crate::fingerprint::{compute_dataset_fingerprint, DatasetFingerprint};
use crate::inventory::DatasetInventory;
use crate::policy::RepairPolicy;
use crate::rules::apply_repair_rules;
use crate::safety::RepairSafety;
use crate::stable_id::{canonical_json, stable_hex_id};

pub const PLAN_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairOperation {
    pub operation_id: String,
    pub finding_ids: Vec<String>,
    pub safety: RepairSafety,
    pub action: RepairAction,
    pub rationale: String,
    pub evidence: Vec<EvidenceRef>,
    pub preconditions: Vec<Precondition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairPlan {
    pub schema_version: u32,
    pub plan_id: String,
    pub dataset_root: String,
    pub dataset_fingerprint: DatasetFingerprint,
    pub generated_at: DateTime<Utc>,
    pub source_scan_id: Uuid,
    pub policy: RepairPolicy,
    pub operations: Vec<RepairOperation>,
    pub expected_outcomes: Vec<ExpectedOutcome>,
    pub diagnosis_findings: Vec<Finding>,
}

impl RepairPlan {
    pub fn safe_operations(&self) -> impl Iterator<Item = &RepairOperation> {
        self.operations.iter().filter(|o| o.safety == RepairSafety::Safe)
    }

    pub fn auto_executable_operations(&self) -> impl Iterator<Item = &RepairOperation> {
        self.operations.iter().filter(|o| o.safety.is_auto_executable())
    }
}

pub fn generate_plan(
    root: &Utf8Path,
    scan: &ScanReport,
    policy: &RepairPolicy,
) -> Result<RepairPlan, RepairError> {
    let inventory = DatasetInventory::from_scan_report(root, scan)?;
    let diagnosis = diagnose(root, scan, policy)?;
    let proposed = apply_repair_rules(root, scan, &diagnosis.findings, &inventory, policy)?;
    let fingerprint = compute_dataset_fingerprint(root, &inventory)?;

    let mut expected_outcomes = Vec::new();
    let operations: Vec<RepairOperation> = proposed
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
            }
        })
        .collect();

    let plan_content = serde_json::json!({
        "fingerprint": fingerprint.digest,
        "operations": operations.iter().map(|o| (&o.operation_id, &o.action)).collect::<Vec<_>>(),
        "policy": policy,
    });
    let plan_id = stable_hex_id("plan", &canonical_json(&plan_content));

    Ok(RepairPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        plan_id,
        dataset_root: root.as_str().to_string(),
        dataset_fingerprint: fingerprint,
        generated_at: Utc::now(),
        source_scan_id: scan.metadata.scan_id,
        policy: policy.clone(),
        operations,
        expected_outcomes,
        diagnosis_findings: diagnosis.findings,
    })
}

impl RepairPlan {
    pub fn from_json(bytes: &[u8]) -> Result<Self, RepairError> {
        serde_json::from_slice(bytes).map_err(|e| RepairError::InvalidPlan(e.to_string()))
    }

    pub fn to_json_pretty(&self) -> Result<String, RepairError> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}
