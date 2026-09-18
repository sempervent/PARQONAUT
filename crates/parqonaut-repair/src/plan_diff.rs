use serde::{Deserialize, Serialize};

use crate::plan::RepairPlan;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanDiff {
    pub operations_added: Vec<String>,
    pub operations_removed: Vec<String>,
    pub operations_changed: Vec<String>,
    pub policy_changed: bool,
    pub dataset_fingerprint_changed: bool,
    pub authorization_class_changed: Vec<String>,
}

pub fn diff_plans(left: &RepairPlan, right: &RepairPlan) -> PlanDiff {
    let left_ops: std::collections::BTreeMap<_, _> =
        left.operations.iter().map(|o| (&o.operation_id, o)).collect();
    let right_ops: std::collections::BTreeMap<_, _> =
        right.operations.iter().map(|o| (&o.operation_id, o)).collect();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    let mut auth_changed = Vec::new();

    for id in right_ops.keys() {
        if !left_ops.contains_key(id) {
            added.push((*id).clone());
        }
    }
    for id in left_ops.keys() {
        if !right_ops.contains_key(id) {
            removed.push((*id).clone());
        }
    }
    for (id, l) in &left_ops {
        if let Some(r) = right_ops.get(id) {
            if l.action != r.action || l.safety != r.safety {
                changed.push((*id).clone());
            }
            if l.safety != r.safety {
                auth_changed.push((*id).clone());
            }
        }
    }

    PlanDiff {
        operations_added: added,
        operations_removed: removed,
        operations_changed: changed,
        policy_changed: left.policy_fingerprint != right.policy_fingerprint,
        dataset_fingerprint_changed: left.dataset_fingerprint.digest
            != right.dataset_fingerprint.digest,
        authorization_class_changed: auth_changed,
    }
}
