//! Shared transport-neutral console state, RBAC gating, and repair plan review model.

use std::collections::BTreeSet;

use parqonaut_orchestrator::BatchStatusReport;
use parqonaut_repair::{RepairPlan, RepairSafety};
use parqonaut_service::api_types::{
    OverviewResponse, PluginCatalogListResponse, ScanJobView, StoredAssetRow, StoredFindingRow,
    WhoAmIResponse,
};
use parqonaut_types::{AuthRole, JobEvent, JobStatus, ScanRunListItem};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsoleTab {
    Overview,
    Jobs,
    Findings,
    RepairReview,
    Batches,
    Activity,
}

impl ConsoleTab {
    pub const ALL: [Self; 6] = [
        Self::Overview,
        Self::Jobs,
        Self::Findings,
        Self::RepairReview,
        Self::Batches,
        Self::Activity,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Jobs => "Jobs",
            Self::Findings => "Findings",
            Self::RepairReview => "Repair",
            Self::Batches => "Batches",
            Self::Activity => "Activity",
        }
    }

    pub fn next(self) -> Self {
        let idx = Self::ALL.iter().position(|t| *t == self).unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    pub fn prev(self) -> Self {
        let idx = Self::ALL.iter().position(|t| *t == self).unwrap_or(0);
        Self::ALL[(idx + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected { base_url: String, principal: WhoAmIResponseSnapshot },
    Unauthorized(String),
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhoAmIResponseSnapshot {
    pub label: String,
    pub role: AuthRole,
}

impl From<&WhoAmIResponse> for WhoAmIResponseSnapshot {
    fn from(w: &WhoAmIResponse) -> Self {
        Self { label: w.label.clone(), role: w.role }
    }
}

/// Client-side UI capability flags derived from the authenticated role (server remains authoritative).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PermittedActions {
    pub can_read: bool,
    pub can_mutate: bool,
    pub can_admin: bool,
}

impl PermittedActions {
    pub fn from_role(role: Option<AuthRole>) -> Self {
        match role {
            None => Self::default(),
            Some(AuthRole::Reader) => Self { can_read: true, can_mutate: false, can_admin: false },
            Some(AuthRole::Operator) => Self { can_read: true, can_mutate: true, can_admin: false },
            Some(AuthRole::Admin) => Self { can_read: true, can_mutate: true, can_admin: true },
        }
    }
}

/// Interactive repair plan review model enforcing explicit per-ID opt-in for ReviewRequired ops.
#[derive(Debug, Clone)]
pub struct RepairReviewState {
    pub source: String,
    pub output: String,
    pub plan: RepairPlan,
    pub authorized_ids: BTreeSet<String>,
}

impl RepairReviewState {
    pub fn new(source: impl Into<String>, output: impl Into<String>, plan: RepairPlan) -> Self {
        Self { source: source.into(), output: output.into(), plan, authorized_ids: BTreeSet::new() }
    }

    /// Toggles explicit authorization for a `ReviewRequired` operation.
    /// Never permits authorizing `Blocked` or `Destructive` operations.
    pub fn toggle_operation(&mut self, operation_id: &str) -> Result<bool, &'static str> {
        let Some(op) = self.plan.operations.iter().find(|o| o.operation_id == operation_id) else {
            return Err("unknown operation_id");
        };
        match op.safety {
            RepairSafety::ReviewRequired => {
                if self.authorized_ids.remove(operation_id) {
                    Ok(false)
                } else {
                    self.authorized_ids.insert(operation_id.to_string());
                    Ok(true)
                }
            }
            RepairSafety::Safe => Err("safe operations are authorized automatically by policy"),
            RepairSafety::Blocked | RepairSafety::Destructive => {
                Err("blocked/destructive operations cannot be authorized")
            }
        }
    }

    pub fn authorized_list(&self) -> Vec<String> {
        self.authorized_ids.iter().cloned().collect()
    }

    pub fn counts_by_safety(&self) -> (usize, usize, usize) {
        let mut safe = 0usize;
        let mut review = 0usize;
        let mut blocked = 0usize;
        for op in &self.plan.operations {
            match op.safety {
                RepairSafety::Safe => safe += 1,
                RepairSafety::ReviewRequired => review += 1,
                RepairSafety::Blocked | RepairSafety::Destructive => blocked += 1,
            }
        }
        (safe, review, blocked)
    }

    pub fn has_unselected_review_required(&self) -> bool {
        self.plan.operations.iter().any(|op| {
            op.safety == RepairSafety::ReviewRequired
                && !self.authorized_ids.contains(&op.operation_id)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ConsoleModel {
    pub connection: ConnectionState,
    pub tab: ConsoleTab,
    pub overview: Option<OverviewResponse>,
    pub jobs: Vec<ScanJobView>,
    pub selected_job_events: Vec<JobEvent>,
    pub selected_batch_status: Option<BatchStatusReport>,
    pub runs: Vec<ScanRunListItem>,
    pub findings: Vec<StoredFindingRow>,
    pub assets: Vec<StoredAssetRow>,
    pub plugins: Option<PluginCatalogListResponse>,
    pub repair_review: Option<RepairReviewState>,
}

impl Default for ConsoleModel {
    fn default() -> Self {
        Self {
            connection: ConnectionState::Disconnected,
            tab: ConsoleTab::Overview,
            overview: None,
            jobs: Vec::new(),
            selected_job_events: Vec::new(),
            selected_batch_status: None,
            runs: Vec::new(),
            findings: Vec::new(),
            assets: Vec::new(),
            plugins: None,
            repair_review: None,
        }
    }
}

impl ConsoleModel {
    pub fn permitted_actions(&self) -> PermittedActions {
        match &self.connection {
            ConnectionState::Connected { principal, .. } => {
                PermittedActions::from_role(Some(principal.role))
            }
            _ => PermittedActions::default(),
        }
    }

    pub fn can_cancel_job(job: &ScanJobView, role: Option<AuthRole>) -> bool {
        PermittedActions::from_role(role).can_mutate
            && matches!(job.status, JobStatus::Queued | JobStatus::Running)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parqonaut_repair::{
        DatasetFingerprint, EffectivePolicy, RepairAction, RepairOperation, RepairSafety,
        PARQONAUT_VERSION, PLAN_SCHEMA_VERSION,
    };

    fn sample_plan() -> RepairPlan {
        RepairPlan {
            schema_version: PLAN_SCHEMA_VERSION,
            plan_id: "plan-1".into(),
            parqonaut_version: PARQONAUT_VERSION.into(),
            dataset_root: "/tmp/src".into(),
            dataset_fingerprint: DatasetFingerprint {
                version: 1,
                root: "/tmp/src".into(),
                entries: vec![],
                digest: "a".repeat(64),
            },
            policy_fingerprint: "b".repeat(64),
            generated_at: chrono::Utc::now(),
            source_scan_id: uuid::Uuid::nil(),
            policy: EffectivePolicy::default(),
            operations: vec![
                RepairOperation {
                    operation_id: "R001".into(),
                    finding_ids: vec![],
                    safety: RepairSafety::Safe,
                    action: RepairAction::RebuildStatistics { target_paths: vec![] },
                    rationale: "safe stats".into(),
                    evidence: vec![],
                    preconditions: vec![],
                    depends_on: vec![],
                },
                RepairOperation {
                    operation_id: "R002".into(),
                    finding_ids: vec![],
                    safety: RepairSafety::ReviewRequired,
                    action: RepairAction::CastColumn {
                        column: "id".into(),
                        from_type: "int32".into(),
                        to_type: "int64".into(),
                    },
                    rationale: "widen id".into(),
                    evidence: vec![],
                    preconditions: vec![],
                    depends_on: vec!["R001".into()],
                },
                RepairOperation {
                    operation_id: "R003".into(),
                    finding_ids: vec![],
                    safety: RepairSafety::Blocked,
                    action: RepairAction::RenameColumn { from: "a".into(), to: "b".into() },
                    rationale: "blocked rename".into(),
                    evidence: vec![],
                    preconditions: vec![],
                    depends_on: vec![],
                },
            ],
            expected_outcomes: vec![],
            diagnosis_findings: vec![],
            schema_diff: None,
            schema_conflicts: None,
            target_schema: None,
        }
    }

    #[test]
    fn repair_review_requires_explicit_opt_in_and_locks_blocked() {
        let mut review = RepairReviewState::new("/tmp/src", "/tmp/out", sample_plan());
        assert!(review.authorized_list().is_empty());
        assert!(review.has_unselected_review_required());
        assert_eq!(review.counts_by_safety(), (1, 1, 1));

        assert!(review.toggle_operation("R001").is_err());
        assert!(review.toggle_operation("R003").is_err());
        assert_eq!(review.toggle_operation("R002"), Ok(true));
        assert_eq!(review.authorized_list(), vec!["R002".to_string()]);
        assert!(!review.has_unselected_review_required());
        assert_eq!(review.toggle_operation("R002"), Ok(false));
        assert!(review.authorized_list().is_empty());
    }

    #[test]
    fn rbac_permitted_actions_match_roles() {
        assert!(!PermittedActions::from_role(Some(AuthRole::Reader)).can_mutate);
        assert!(PermittedActions::from_role(Some(AuthRole::Operator)).can_mutate);
        assert!(!PermittedActions::from_role(Some(AuthRole::Operator)).can_admin);
        assert!(PermittedActions::from_role(Some(AuthRole::Admin)).can_admin);
    }
}
