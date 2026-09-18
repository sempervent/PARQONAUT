use crate::{RepairPlan, RepairSafety};

/// Human-readable repair plan summary (derived from structured state).
pub fn format_plan_human(plan: &RepairPlan) -> String {
    let mut out = String::new();
    out.push_str(&format!("Dataset: {}\n\n", plan.dataset_root));

    let safe: Vec<_> = plan.operations.iter().filter(|o| o.safety == RepairSafety::Safe).collect();
    let review: Vec<_> =
        plan.operations.iter().filter(|o| o.safety == RepairSafety::ReviewRequired).collect();
    let destructive: Vec<_> =
        plan.operations.iter().filter(|o| o.safety == RepairSafety::Destructive).collect();

    if safe.is_empty() && review.is_empty() && destructive.is_empty() {
        out.push_str("No repair proposals.\n");
        return out;
    }

    if !safe.is_empty() {
        out.push_str(&format!("{} finding(s) can be repaired safely.\n\n", safe.len()));
    } else {
        out.push_str("No safe automatic repairs proposed.\n\n");
    }

    if !safe.is_empty() {
        out.push_str("SAFE\n");
        for op in &safe {
            out.push_str(&format!("  [{}] {}\n", op.operation_id, op.rationale));
            if let Some(code) = op.finding_ids.first() {
                out.push_str(&format!("         Finding fingerprint: {code}\n"));
            }
        }
        out.push('\n');
    }

    if !review.is_empty() {
        out.push_str("REVIEW REQUIRED\n");
        for op in &review {
            out.push_str(&format!("  [{}] {}\n", op.operation_id, op.rationale));
        }
        out.push('\n');
    }

    if !destructive.is_empty() {
        out.push_str("DESTRUCTIVE (not auto-executed)\n");
        for op in &destructive {
            out.push_str(&format!("  [{}] {}\n", op.operation_id, op.rationale));
        }
        out.push('\n');
    }

    out.push_str(&format!("Automatic repair will execute {} operation(s) only.\n", safe.len()));
    out
}
