use std::collections::{BTreeMap, BTreeSet};

use crate::action::RepairAction;
use crate::error::RepairError;
use crate::plan::RepairOperation;

/// Assign deterministic `depends_on` edges based on action kind constraints.
pub fn assign_dependencies(ops: &mut [RepairOperation]) {
    let order = |action: &RepairAction| -> u8 {
        match action {
            RepairAction::CastColumn { .. }
            | RepairAction::AlignSchema { .. }
            | RepairAction::RenameColumn { .. } => 0,
            RepairAction::SplitLargeFile { .. } => 1,
            RepairAction::ResizeRowGroups { .. } => 2,
            RepairAction::Recompress { .. } => 3,
            RepairAction::RebuildStatistics { .. } => 4,
            RepairAction::MergeSmallFiles { .. } => 5,
            RepairAction::Repartition { .. } => 6,
        }
    };

    ops.sort_by(|a, b| {
        order(&a.action).cmp(&order(&b.action)).then(a.operation_id.cmp(&b.operation_id))
    });

    for i in 0..ops.len() {
        let current_order = order(&ops[i].action);
        ops[i].depends_on = ops[..i]
            .iter()
            .filter(|prev| order(&prev.action) < current_order)
            .map(|prev| prev.operation_id.clone())
            .collect();
    }
}

/// Topological sort; returns error on cycles.
pub fn sort_by_dependencies(ops: &[RepairOperation]) -> Result<Vec<&RepairOperation>, RepairError> {
    let ids: BTreeSet<_> = ops.iter().map(|o| o.operation_id.as_str()).collect();
    let mut indegree: BTreeMap<&str, usize> = BTreeMap::new();
    let mut dependents: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for op in ops {
        indegree.insert(op.operation_id.as_str(), op.depends_on.len());
        for dep in &op.depends_on {
            if !ids.contains(dep.as_str()) {
                return Err(RepairError::PreconditionFailed {
                    operation_id: op.operation_id.clone(),
                    detail: format!("unknown dependency {dep}"),
                });
            }
            dependents.entry(dep.as_str()).or_default().push(op.operation_id.as_str());
        }
    }

    let mut ready: Vec<_> = indegree.iter().filter(|(_, d)| **d == 0).map(|(id, _)| *id).collect();
    ready.sort();

    let mut sorted = Vec::new();
    let op_map: BTreeMap<_, _> = ops.iter().map(|o| (o.operation_id.as_str(), o)).collect();

    while let Some(id) = ready.first().copied() {
        ready.remove(0);
        sorted.push(op_map[id]);
        if let Some(children) = dependents.get(id) {
            for child in children {
                let entry = indegree.get_mut(child).expect("child indegree");
                *entry -= 1;
                if *entry == 0 {
                    ready.push(child);
                }
            }
            ready.sort();
        }
    }

    if sorted.len() != ops.len() {
        return Err(RepairError::InvalidPlan("operation dependency cycle detected".into()));
    }

    Ok(sorted)
}
