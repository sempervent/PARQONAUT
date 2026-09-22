use camino::Utf8Path;
use parqonaut_types::ScanReport;
use serde::Serialize;

use crate::diagnose::diagnose;
use crate::plan::generate_plan;
use crate::safety::RepairSafety;
use crate::schema_policy::EffectivePolicy;

/// CI-oriented exit codes for `prqnt check`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CheckExitCode {
    Compliant = 0,
    RepairableIssues = 2,
    ReviewRequired = 3,
    UnresolvableOrPolicyViolation = 4,
    OperationalFailure = 5,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CheckReport {
    #[serde(with = "check_exit_code")]
    pub exit_code: CheckExitCode,
    pub safe_operations: usize,
    pub review_required_operations: usize,
    pub blocked_operations: usize,
    pub destructive_operations: usize,
    pub unresolvable_schema_conflicts: usize,
    pub summary: String,
}

mod check_exit_code {
    use super::CheckExitCode;
    use serde::Serializer;

    pub fn serialize<S: Serializer>(code: &CheckExitCode, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_i32(*code as i32)
    }
}

pub fn evaluate_check(
    root: &Utf8Path,
    scan: &ScanReport,
    policy: &EffectivePolicy,
) -> Result<CheckReport, crate::error::RepairError> {
    let ci = policy.ci.clone().unwrap_or_default();
    let _diagnosis = diagnose(root, scan, &policy.repair)?;
    let plan = generate_plan(root, scan, policy, None)?;

    let safe = plan.operations.iter().filter(|o| o.safety == RepairSafety::Safe).count();
    let review =
        plan.operations.iter().filter(|o| o.safety == RepairSafety::ReviewRequired).count();
    let blocked = plan.operations.iter().filter(|o| o.safety == RepairSafety::Blocked).count();
    let destructive =
        plan.operations.iter().filter(|o| o.safety == RepairSafety::Destructive).count();
    let unresolvable = plan.schema_conflicts.as_ref().map(|c| c.len()).unwrap_or(0);

    let mut exit = CheckExitCode::Compliant;
    let mut reasons = Vec::new();

    if unresolvable > 0 && ci.fail_on_unresolvable_schema {
        exit = CheckExitCode::UnresolvableOrPolicyViolation;
        reasons.push(format!("{unresolvable} unresolvable schema conflict(s)"));
    }
    if review > 0 && ci.fail_on_review_required {
        if exit == CheckExitCode::Compliant {
            exit = CheckExitCode::ReviewRequired;
        }
        reasons.push(format!("{review} review-required operation(s)"));
    }
    if safe > 0 && ci.fail_on_safe_findings {
        if exit == CheckExitCode::Compliant {
            exit = CheckExitCode::RepairableIssues;
        }
        reasons.push(format!("{safe} safe repair operation(s) available"));
    }
    if destructive > 0 && ci.fail_on_destructive {
        exit = CheckExitCode::UnresolvableOrPolicyViolation;
        reasons.push(format!("{destructive} destructive proposal(s)"));
    }

    let summary = if reasons.is_empty() {
        "dataset compliant with CI policy".into()
    } else {
        reasons.join("; ")
    };

    Ok(CheckReport {
        exit_code: exit,
        safe_operations: safe,
        review_required_operations: review,
        blocked_operations: blocked,
        destructive_operations: destructive,
        unresolvable_schema_conflicts: unresolvable,
        summary,
    })
}
