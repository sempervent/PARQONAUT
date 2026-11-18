//! Pure TUI state reducer and key-event routing (testable without a real TTY).

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use parqonaut_client::{
    ConnectionState, ConsoleModel, ConsoleTab, OperatorClientError, PermittedActions,
    RepairReviewState, SseEventItem,
};
use parqonaut_service::api_types::ScanJobView;
use parqonaut_types::{JobEvent, JobId, JobKind, JobStatus, RunId};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialogMode {
    None,
    Help,
    FilterInput { buffer: String },
    TokenPrompt { input: String },
    ScanPrompt { path: String, profile: String, plugins: String, field: usize },
    PlanPrompt { location: String, policy_path: String, output: String, field: usize },
    ConfirmCancel { job_id: Uuid, target: String },
    ConfirmRepairSubmit,
    ConfirmBatchResume { run_dir: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TuiAction {
    None,
    Quit,
    RefreshAll,
    InspectJob(JobId),
    InspectRun(RunId),
    InspectBatchJob(JobId),
    GeneratePlan { location: String, policy_path: String, output: String },
    SubmitScan { path: String, profile: String, plugins: Vec<String> },
    SubmitRepair,
    CancelJob(JobId),
    ResumeBatch(String),
    ConnectToken(String),
}

pub struct TuiApp {
    pub model: ConsoleModel,
    pub dialog: DialogMode,
    pub filter_query: String,
    pub selected_job_idx: usize,
    pub selected_run_idx: usize,
    pub selected_finding_idx: usize,
    pub selected_repair_op_idx: usize,
    pub selected_batch_idx: usize,
    pub selected_activity_idx: usize,
    pub active_job_id: Option<JobId>,
    pub active_run_id: Option<RunId>,
    pub status_banner: String,
    pub error_banner: Option<String>,
    pub spinner_tick: usize,
}

impl TuiApp {
    pub fn new(needs_token: bool) -> Self {
        let dialog = if needs_token {
            DialogMode::TokenPrompt { input: String::new() }
        } else {
            DialogMode::None
        };
        Self {
            model: ConsoleModel::default(),
            dialog,
            filter_query: String::new(),
            selected_job_idx: 0,
            selected_run_idx: 0,
            selected_finding_idx: 0,
            selected_repair_op_idx: 0,
            selected_batch_idx: 0,
            selected_activity_idx: 0,
            active_job_id: None,
            active_run_id: None,
            status_banner: "Connecting to PARQONAUT server...".into(),
            error_banner: None,
            spinner_tick: 0,
        }
    }

    pub fn permitted(&self) -> PermittedActions {
        self.model.permitted_actions()
    }

    pub fn filtered_jobs(&self) -> Vec<&ScanJobView> {
        let q = self.filter_query.trim().to_ascii_lowercase();
        self.model
            .jobs
            .iter()
            .filter(|j| {
                if q.is_empty() {
                    return true;
                }
                j.normalized_target_key.to_ascii_lowercase().contains(&q)
                    || j.job_kind.as_str().contains(&q)
                    || format!("{:?}", j.status).to_ascii_lowercase().contains(&q)
                    || j.job_id.to_string().contains(&q)
            })
            .collect()
    }

    pub fn batch_jobs(&self) -> Vec<&ScanJobView> {
        self.filtered_jobs()
            .into_iter()
            .filter(|j| matches!(j.job_kind, JobKind::BatchRepair | JobKind::BatchResume))
            .collect()
    }

    pub fn filtered_findings(&self) -> Vec<&parqonaut_service::api_types::StoredFindingRow> {
        let q = self.filter_query.trim().to_ascii_lowercase();
        self.model
            .findings
            .iter()
            .filter(|f| {
                if q.is_empty() {
                    return true;
                }
                f.code.to_ascii_lowercase().contains(&q)
                    || f.severity.to_ascii_lowercase().contains(&q)
                    || f.asset_path.as_deref().unwrap_or("").to_ascii_lowercase().contains(&q)
            })
            .collect()
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> TuiAction {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return TuiAction::Quit;
        }

        // Route keys when a dialog / text-entry modal is active.
        match &mut self.dialog {
            DialogMode::Help => {
                if matches!(
                    key.code,
                    KeyCode::Esc | KeyCode::Char('?') | KeyCode::Enter | KeyCode::Char('q')
                ) {
                    self.dialog = DialogMode::None;
                }
                return TuiAction::None;
            }
            DialogMode::FilterInput { buffer } => {
                match key.code {
                    KeyCode::Esc => {
                        self.filter_query.clear();
                        self.dialog = DialogMode::None;
                    }
                    KeyCode::Enter => {
                        self.filter_query = buffer.trim().to_string();
                        self.selected_job_idx = 0;
                        self.selected_finding_idx = 0;
                        self.selected_batch_idx = 0;
                        self.dialog = DialogMode::None;
                    }
                    KeyCode::Backspace => {
                        buffer.pop();
                    }
                    KeyCode::Char(c) => buffer.push(c),
                    _ => {}
                }
                return TuiAction::None;
            }
            DialogMode::TokenPrompt { input } => {
                match key.code {
                    KeyCode::Esc => return TuiAction::Quit,
                    KeyCode::Enter => {
                        let tok = input.trim().to_string();
                        if !tok.is_empty() {
                            self.dialog = DialogMode::None;
                            return TuiAction::ConnectToken(tok);
                        }
                    }
                    KeyCode::Backspace => {
                        input.pop();
                    }
                    KeyCode::Char(c) => input.push(c),
                    _ => {}
                }
                return TuiAction::None;
            }
            DialogMode::ScanPrompt { path, profile, plugins, field } => {
                match key.code {
                    KeyCode::Esc => self.dialog = DialogMode::None,
                    KeyCode::Tab | KeyCode::Down => *field = (*field + 1) % 3,
                    KeyCode::BackTab | KeyCode::Up => *field = (*field + 2) % 3,
                    KeyCode::Enter => {
                        let p = path.trim().to_string();
                        if !p.is_empty() {
                            let prof = if profile.trim().is_empty() {
                                "standard".into()
                            } else {
                                profile.trim().to_string()
                            };
                            let plugs: Vec<String> = plugins
                                .split(',')
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect();
                            self.dialog = DialogMode::None;
                            return TuiAction::SubmitScan {
                                path: p,
                                profile: prof,
                                plugins: plugs,
                            };
                        }
                    }
                    KeyCode::Backspace => {
                        let target = match *field {
                            0 => path,
                            1 => profile,
                            _ => plugins,
                        };
                        target.pop();
                    }
                    KeyCode::Char(c) => {
                        let target = match *field {
                            0 => path,
                            1 => profile,
                            _ => plugins,
                        };
                        target.push(c);
                    }
                    _ => {}
                }
                return TuiAction::None;
            }
            DialogMode::PlanPrompt { location, policy_path, output, field } => {
                match key.code {
                    KeyCode::Esc => self.dialog = DialogMode::None,
                    KeyCode::Tab | KeyCode::Down => *field = (*field + 1) % 3,
                    KeyCode::BackTab | KeyCode::Up => *field = (*field + 2) % 3,
                    KeyCode::Enter => {
                        let loc = location.trim().to_string();
                        let out = output.trim().to_string();
                        let pol = policy_path.trim().to_string();
                        if !loc.is_empty() && !out.is_empty() {
                            self.dialog = DialogMode::None;
                            return TuiAction::GeneratePlan {
                                location: loc,
                                policy_path: pol,
                                output: out,
                            };
                        }
                    }
                    KeyCode::Backspace => {
                        let target = match *field {
                            0 => location,
                            1 => policy_path,
                            _ => output,
                        };
                        target.pop();
                    }
                    KeyCode::Char(c) => {
                        let target = match *field {
                            0 => location,
                            1 => policy_path,
                            _ => output,
                        };
                        target.push(c);
                    }
                    _ => {}
                }
                return TuiAction::None;
            }
            DialogMode::ConfirmCancel { job_id, .. } => {
                let id = *job_id;
                match key.code {
                    KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                        self.dialog = DialogMode::None;
                        return TuiAction::CancelJob(JobId(id));
                    }
                    KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
                        self.dialog = DialogMode::None;
                    }
                    _ => {}
                }
                return TuiAction::None;
            }
            DialogMode::ConfirmRepairSubmit => {
                match key.code {
                    KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                        self.dialog = DialogMode::None;
                        return TuiAction::SubmitRepair;
                    }
                    KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
                        self.dialog = DialogMode::None;
                    }
                    _ => {}
                }
                return TuiAction::None;
            }
            DialogMode::ConfirmBatchResume { run_dir } => {
                let rd = run_dir.clone();
                match key.code {
                    KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                        self.dialog = DialogMode::None;
                        return TuiAction::ResumeBatch(rd);
                    }
                    KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
                        self.dialog = DialogMode::None;
                    }
                    _ => {}
                }
                return TuiAction::None;
            }
            DialogMode::None => {}
        }

        // Standard non-modal navigation & actions.
        match key.code {
            KeyCode::Char('q') => TuiAction::Quit,
            KeyCode::Char('?') => {
                self.dialog = DialogMode::Help;
                TuiAction::None
            }
            KeyCode::Tab => {
                self.model.tab = self.model.tab.next();
                TuiAction::None
            }
            KeyCode::BackTab => {
                self.model.tab = self.model.tab.prev();
                TuiAction::None
            }
            KeyCode::Char('/') => {
                self.dialog = DialogMode::FilterInput { buffer: self.filter_query.clone() };
                TuiAction::None
            }
            KeyCode::Esc => {
                if !self.filter_query.is_empty() {
                    self.filter_query.clear();
                    self.status_banner = "Filter cleared".into();
                } else {
                    self.error_banner = None;
                }
                TuiAction::None
            }
            KeyCode::Char('r') => TuiAction::RefreshAll,
            KeyCode::Char('j') | KeyCode::Down => {
                self.move_selection(1);
                TuiAction::None
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.move_selection(-1);
                TuiAction::None
            }
            KeyCode::Enter => self.activate_selection(),
            KeyCode::Char('n') => {
                if !self.permitted().can_mutate {
                    self.error_banner =
                        Some("Operator or Admin role required to submit jobs".into());
                    return TuiAction::None;
                }
                self.dialog = DialogMode::ScanPrompt {
                    path: String::new(),
                    profile: "standard".into(),
                    plugins: String::new(),
                    field: 0,
                };
                TuiAction::None
            }
            KeyCode::Char('c') if self.model.tab == ConsoleTab::Jobs => {
                if !self.permitted().can_mutate {
                    self.error_banner =
                        Some("Operator or Admin role required to cancel jobs".into());
                    return TuiAction::None;
                }
                if let Some(job) = self.filtered_jobs().get(self.selected_job_idx).copied() {
                    if matches!(job.status, JobStatus::Queued | JobStatus::Running) {
                        self.dialog = DialogMode::ConfirmCancel {
                            job_id: job.job_id,
                            target: job.normalized_target_key.clone(),
                        };
                    } else {
                        self.error_banner = Some(format!(
                            "Job {} is already terminal ({:?})",
                            job.job_id, job.status
                        ));
                    }
                }
                TuiAction::None
            }
            KeyCode::Char('[') if self.model.tab == ConsoleTab::Findings => {
                if !self.model.runs.is_empty() {
                    self.selected_run_idx = self.selected_run_idx.saturating_sub(1);
                    if let Some(r) = self.model.runs.get(self.selected_run_idx) {
                        self.active_run_id = Some(r.run_id);
                        return TuiAction::InspectRun(r.run_id);
                    }
                }
                TuiAction::None
            }
            KeyCode::Char(']') if self.model.tab == ConsoleTab::Findings => {
                if self.selected_run_idx + 1 < self.model.runs.len() {
                    self.selected_run_idx += 1;
                    if let Some(r) = self.model.runs.get(self.selected_run_idx) {
                        self.active_run_id = Some(r.run_id);
                        return TuiAction::InspectRun(r.run_id);
                    }
                }
                TuiAction::None
            }
            KeyCode::Char('g') if self.model.tab == ConsoleTab::RepairReview => {
                if !self.permitted().can_read {
                    return TuiAction::None;
                }
                self.dialog = DialogMode::PlanPrompt {
                    location: String::new(),
                    policy_path: String::new(),
                    output: String::new(),
                    field: 0,
                };
                TuiAction::None
            }
            KeyCode::Char(' ') if self.model.tab == ConsoleTab::RepairReview => {
                if let Some(review) = self.model.repair_review.as_mut() {
                    if let Some(op) = review.plan.operations.get(self.selected_repair_op_idx) {
                        let id = op.operation_id.clone();
                        match review.toggle_operation(&id) {
                            Ok(selected) => {
                                self.error_banner = None;
                                self.status_banner = if selected {
                                    format!("Authorized ReviewRequired op {id}")
                                } else {
                                    format!("Removed authorization for {id}")
                                };
                            }
                            Err(reason) => {
                                self.error_banner = Some(reason.to_string());
                            }
                        }
                    }
                }
                TuiAction::None
            }
            KeyCode::Char('s') if self.model.tab == ConsoleTab::RepairReview => {
                if !self.permitted().can_mutate {
                    self.error_banner =
                        Some("Operator or Admin role required to submit repair jobs".into());
                    return TuiAction::None;
                }
                if self.model.repair_review.is_some() {
                    self.dialog = DialogMode::ConfirmRepairSubmit;
                } else {
                    self.error_banner = Some("Generate a repair plan first (`g`)".into());
                }
                TuiAction::None
            }
            KeyCode::Char('R') if self.model.tab == ConsoleTab::Batches => {
                if !self.permitted().can_mutate {
                    self.error_banner =
                        Some("Operator or Admin role required to resume batches".into());
                    return TuiAction::None;
                }
                if let Some(status) = &self.model.selected_batch_status {
                    if status.resumable {
                        if let Some(job) = self.batch_jobs().get(self.selected_batch_idx) {
                            if let Some(rd) = job
                                .result_ref
                                .as_ref()
                                .and_then(|v| v.get("run_dir"))
                                .and_then(|v| v.as_str())
                            {
                                self.dialog =
                                    DialogMode::ConfirmBatchResume { run_dir: rd.to_string() };
                            }
                        }
                    } else {
                        self.error_banner = Some("Selected batch run is not resumable".into());
                    }
                }
                TuiAction::None
            }
            _ => TuiAction::None,
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let step = |idx: usize, len: usize| -> usize {
            if len == 0 {
                0
            } else {
                (idx as isize + delta).clamp(0, (len - 1) as isize) as usize
            }
        };
        match self.model.tab {
            ConsoleTab::Overview => {}
            ConsoleTab::Jobs => {
                let len = self.filtered_jobs().len();
                self.selected_job_idx = step(self.selected_job_idx, len);
            }
            ConsoleTab::Findings => {
                let len = self.filtered_findings().len();
                self.selected_finding_idx = step(self.selected_finding_idx, len);
            }
            ConsoleTab::RepairReview => {
                let len =
                    self.model.repair_review.as_ref().map(|r| r.plan.operations.len()).unwrap_or(0);
                self.selected_repair_op_idx = step(self.selected_repair_op_idx, len);
            }
            ConsoleTab::Batches => {
                let len = self.batch_jobs().len();
                self.selected_batch_idx = step(self.selected_batch_idx, len);
            }
            ConsoleTab::Activity => {
                let len = self.model.selected_job_events.len();
                self.selected_activity_idx = step(self.selected_activity_idx, len);
            }
        }
    }

    fn activate_selection(&mut self) -> TuiAction {
        match self.model.tab {
            ConsoleTab::Jobs => {
                if let Some(job) = self.filtered_jobs().get(self.selected_job_idx) {
                    let jid = JobId(job.job_id);
                    self.active_job_id = Some(jid);
                    return TuiAction::InspectJob(jid);
                }
            }
            ConsoleTab::Batches => {
                if let Some(job) = self.batch_jobs().get(self.selected_batch_idx) {
                    let jid = JobId(job.job_id);
                    self.active_job_id = Some(jid);
                    return TuiAction::InspectBatchJob(jid);
                }
            }
            ConsoleTab::Findings => {
                if let Some(r) = self.model.runs.get(self.selected_run_idx) {
                    self.active_run_id = Some(r.run_id);
                    return TuiAction::InspectRun(r.run_id);
                }
            }
            _ => {}
        }
        TuiAction::None
    }

    pub fn apply_sse_item(&mut self, item: SseEventItem) {
        match item {
            SseEventItem::JobEvent(ev) => self.push_job_event(*ev),
            SseEventItem::JobStatus { job_id, status, last_seq } => {
                if let Some(j) = self.model.jobs.iter_mut().find(|j| j.job_id == job_id) {
                    j.status = status;
                }
                self.status_banner =
                    format!("Job {} reached {:?} (seq {})", job_id, status, last_seq);
            }
        }
    }

    pub fn push_job_event(&mut self, ev: JobEvent) {
        if self
            .model
            .selected_job_events
            .iter()
            .any(|existing| existing.seq == ev.seq && existing.job_id == ev.job_id)
        {
            return;
        }
        self.model.selected_job_events.push(ev);
        self.model.selected_job_events.sort_by_key(|e| e.seq);
    }

    pub fn set_error(&mut self, err: &OperatorClientError) {
        match err {
            OperatorClientError::Unauthorized(m) => {
                self.model.connection = ConnectionState::Unauthorized(m.clone());
                self.error_banner = Some(format!("[auth] {m}"));
                self.dialog = DialogMode::TokenPrompt { input: String::new() };
            }
            OperatorClientError::Connection(m) => {
                self.model.connection = ConnectionState::Error(m.clone());
                self.error_banner = Some(format!("[disconnected] {m}"));
            }
            OperatorClientError::Forbidden(m) => {
                self.error_banner = Some(format!("[forbidden] {m}"));
            }
            OperatorClientError::Transient(m) => {
                self.error_banner = Some(format!("[transient] {m}"));
            }
            OperatorClientError::Operation { code, message, .. } => {
                self.error_banner = Some(format!("[{code}] {message}"));
            }
            other => {
                self.error_banner = Some(other.to_string());
            }
        }
    }

    pub fn set_repair_review(
        &mut self,
        source: String,
        output: String,
        plan: parqonaut_repair::RepairPlan,
    ) {
        self.model.repair_review = Some(RepairReviewState::new(source, output, plan));
        self.selected_repair_op_idx = 0;
        self.error_banner = None;
        self.status_banner =
            "Plan loaded. Toggle ReviewRequired ops with Space; press `s` to review & submit."
                .into();
    }
}
