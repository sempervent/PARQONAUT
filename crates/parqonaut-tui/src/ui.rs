//! Ratatui rendering for all 6 operator console views and confirmation modals.

use parqonaut_client::{ConnectionState, ConsoleTab};
use parqonaut_repair::RepairSafety;
use parqonaut_types::JobStatus;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table, Tabs, Wrap,
};
use ratatui::Frame;

use crate::app::{DialogMode, TuiApp};

const SPINNER: [&str; 4] = ["|", "/", "-", "\\"];

pub fn render(frame: &mut Frame<'_>, app: &TuiApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(2),
        ])
        .split(frame.area());

    render_header(frame, chunks[0], app);
    render_tabs(frame, chunks[1], app);
    match app.model.tab {
        ConsoleTab::Overview => render_overview(frame, chunks[2], app),
        ConsoleTab::Jobs => render_jobs(frame, chunks[2], app),
        ConsoleTab::Findings => render_findings(frame, chunks[2], app),
        ConsoleTab::RepairReview => render_repair(frame, chunks[2], app),
        ConsoleTab::Batches => render_batches(frame, chunks[2], app),
        ConsoleTab::Activity => render_activity(frame, chunks[2], app),
    }
    render_footer(frame, chunks[3], app);
    render_dialog(frame, frame.area(), app);
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let (conn_text, conn_style) = match &app.model.connection {
        ConnectionState::Connected { base_url, principal } => (
            format!("CONNECTED {base_url}  |  {} ({:?})", principal.label, principal.role),
            Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
        ),
        ConnectionState::Connecting => ("CONNECTING...".into(), Style::default().fg(Color::Yellow)),
        ConnectionState::Unauthorized(m) => (
            format!("UNAUTHORIZED: {m}"),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        ConnectionState::Error(m) => (
            format!("DISCONNECTED: {m}"),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        ConnectionState::Disconnected => ("DISCONNECTED".into(), Style::default().fg(Color::Gray)),
    };
    let filter_hint = if app.filter_query.is_empty() {
        String::new()
    } else {
        format!("  |  filter: \"{}\"", app.filter_query)
    };
    let p = Paragraph::new(Line::from(vec![
        Span::styled(
            "PARQONAUT v0.11  ",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ),
        Span::styled(conn_text, conn_style),
        Span::raw(filter_hint),
    ]))
    .block(Block::default().borders(Borders::ALL).title("Operator Console"));
    frame.render_widget(p, area);
}

fn render_tabs(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let titles: Vec<Line<'_>> = ConsoleTab::ALL.iter().map(|t| Line::from(t.label())).collect();
    let idx = ConsoleTab::ALL.iter().position(|t| *t == app.model.tab).unwrap_or(0);
    let tabs = Tabs::new(titles)
        .select(idx)
        .block(Block::default().borders(Borders::ALL).title("Views (Tab / Shift-Tab)"))
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
    frame.render_widget(tabs, area);
}

fn render_overview(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(42), Constraint::Percentage(58)])
        .split(area);

    let left_text = if let Some(ov) = &app.model.overview {
        let q = ov.job_counts.get("queued").copied().unwrap_or(0);
        let r = ov.job_counts.get("running").copied().unwrap_or(0);
        let s = ov.job_counts.get("succeeded").copied().unwrap_or(0);
        let f = ov.job_counts.get("failed").copied().unwrap_or(0);
        let c = ov.job_counts.get("canceled").copied().unwrap_or(0);
        vec![
            Line::from(format!("Service status : {}", ov.status)),
            Line::from(format!("Queued jobs    : {q}")),
            Line::from(format!("Active jobs    : {r}")),
            Line::from(format!("Succeeded jobs : {s}")),
            Line::from(format!("Failed jobs    : {f}")),
            Line::from(format!("Canceled jobs  : {c}")),
            Line::from(""),
            Line::from(format!(
                "Plugins        : enabled={} ({} allowlisted)",
                ov.plugins_enabled, ov.allowed_plugins_count
            )),
            Line::from(format!("Persisted runs : {} recent", ov.recent_runs.len())),
        ]
    } else {
        vec![Line::from("No overview loaded yet. Press `r` to refresh.")]
    };
    frame.render_widget(
        Paragraph::new(left_text)
            .block(Block::default().borders(Borders::ALL).title("Service & Queue Summary"))
            .wrap(Wrap { trim: true }),
        cols[0],
    );

    let activity_items: Vec<ListItem<'_>> = if app.model.selected_job_events.is_empty() {
        app.model
            .jobs
            .iter()
            .take(12)
            .map(|j| {
                ListItem::new(format!(
                    "{:?}  {:?}  {}",
                    j.job_kind, j.status, j.normalized_target_key
                ))
            })
            .collect()
    } else {
        app.model
            .selected_job_events
            .iter()
            .rev()
            .take(15)
            .map(|e| {
                ListItem::new(format!(
                    "#{} [{}] {} — {}",
                    e.seq,
                    e.stage.as_str(),
                    e.kind.as_str(),
                    e.message.as_deref().unwrap_or("")
                ))
            })
            .collect()
    };
    frame.render_widget(
        List::new(activity_items)
            .block(Block::default().borders(Borders::ALL).title("Recent Activity")),
        cols[1],
    );
}

fn render_jobs(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    let jobs = app.filtered_jobs();
    let spin = SPINNER[app.spinner_tick % SPINNER.len()];
    let rows: Vec<Row<'_>> = jobs
        .iter()
        .enumerate()
        .map(|(i, j)| {
            let duration = match (j.started_at, j.completed_at) {
                (Some(s), Some(c)) => format!("{}ms", (c - s).num_milliseconds().max(0)),
                (Some(s), None) => {
                    format!("{spin} {}s", (chrono::Utc::now() - s).num_seconds().max(0))
                }
                _ => "-".into(),
            };
            let style = if i == app.selected_job_idx {
                Style::default().bg(Color::DarkGray).add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(j.job_kind.as_str().to_string()),
                Cell::from(format!("{:?}", j.status)),
                Cell::from(duration),
                Cell::from(j.normalized_target_key.clone()),
            ])
            .style(style)
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Min(15),
        ],
    )
    .header(
        Row::new(["Kind", "Status", "Duration", "Target"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default().borders(Borders::ALL).title("Jobs (Enter=inspect, c=cancel, n=new scan)"),
    );
    frame.render_widget(table, cols[0]);

    let detail_lines: Vec<Line<'_>> = if let Some(job) = jobs.get(app.selected_job_idx) {
        let mut lines = vec![
            Line::from(format!("Job ID  : {}", job.job_id)),
            Line::from(format!("Kind    : {:?}", job.job_kind)),
            Line::from(format!("Status  : {:?} (attempts={})", job.status, job.attempt_count)),
            Line::from(format!("Target  : {}", job.normalized_target_key)),
            Line::from(format!(
                "Progress: {}",
                format_latest_progress(&app.model.selected_job_events, job.status, spin)
            )),
        ];
        if let Some(fail) = &job.failure {
            lines.push(Line::from(Span::styled(
                format!("Failure : {:?} — {}", fail.code, fail.message),
                Style::default().fg(Color::Red),
            )));
        }
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Event Timeline:",
            Style::default().add_modifier(Modifier::BOLD),
        )));
        for ev in app.model.selected_job_events.iter().rev().take(12) {
            lines.push(Line::from(format!(
                "  #{} [{}] {}: {}",
                ev.seq,
                ev.stage.as_str(),
                ev.kind.as_str(),
                ev.message.as_deref().unwrap_or("")
            )));
        }
        lines
    } else {
        vec![Line::from("No matching jobs.")]
    };

    frame.render_widget(
        Paragraph::new(detail_lines)
            .block(Block::default().borders(Borders::ALL).title("Job Detail & Events"))
            .wrap(Wrap { trim: true }),
        cols[1],
    );
}

fn format_latest_progress(
    events: &[parqonaut_types::JobEvent],
    status: JobStatus,
    spin: &str,
) -> String {
    if let Some(last) = events.last() {
        if let Some(frac) = last.metrics.completion_fraction() {
            let pct = (frac * 100.0).round() as u64;
            return format!(
                "{pct}% ({}/{})",
                last.metrics.datasets_done.max(last.metrics.files_done),
                last.metrics.datasets_total.max(last.metrics.files_total)
            );
        }
        if matches!(status, JobStatus::Running | JobStatus::Queued) {
            return format!("{spin} indeterminate [{}]", last.stage.as_str());
        }
        return format!("completed [{}]", last.stage.as_str());
    }
    if matches!(status, JobStatus::Running | JobStatus::Queued) {
        format!("{spin} indeterminate")
    } else {
        format!("{status:?}")
    }
}

fn render_findings(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(6)])
        .split(area);

    let run_label = if let Some(run) = app.model.runs.get(app.selected_run_idx) {
        format!(
            "Run {}/{}: {} ({}) — `[` / `]` to switch runs",
            app.selected_run_idx + 1,
            app.model.runs.len(),
            run.run_id.0,
            run.target_identity.normalized_key
        )
    } else {
        "No runs found (`r` to refresh)".into()
    };
    frame.render_widget(
        Paragraph::new(run_label)
            .block(Block::default().borders(Borders::ALL).title("Selected Run")),
        chunks[0],
    );

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(chunks[1]);

    let findings = app.filtered_findings();
    let rows: Vec<Row<'_>> = findings
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let style = if i == app.selected_finding_idx {
                Style::default().bg(Color::DarkGray).add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(f.severity.clone()),
                Cell::from(f.code.clone()),
                Cell::from(f.asset_path.clone().unwrap_or_else(|| "-".into())),
            ])
            .style(style)
        })
        .collect();
    let table =
        Table::new(rows, [Constraint::Length(10), Constraint::Length(28), Constraint::Min(15)])
            .header(
                Row::new(["Severity", "Code", "Asset"])
                    .style(Style::default().add_modifier(Modifier::BOLD)),
            )
            .block(Block::default().borders(Borders::ALL).title("Findings (`/` to filter)"));
    frame.render_widget(table, cols[0]);

    let detail = if let Some(f) = findings.get(app.selected_finding_idx) {
        vec![
            Line::from(format!("Code       : {}", f.code)),
            Line::from(format!("Severity   : {}", f.severity)),
            Line::from(format!("Category   : {}", f.category)),
            Line::from(format!("Fingerprint: {}", f.fingerprint)),
            Line::from(format!("Asset      : {}", f.asset_path.as_deref().unwrap_or("-"))),
            Line::from(format!("Dataset    : {}", f.dataset_id.as_deref().unwrap_or("-"))),
            Line::from(format!("Run assets : {} loaded", app.model.assets.len())),
        ]
    } else {
        vec![Line::from("No findings match current filter.")]
    };
    frame.render_widget(
        Paragraph::new(detail)
            .block(Block::default().borders(Borders::ALL).title("Finding Detail"))
            .wrap(Wrap { trim: true }),
        cols[1],
    );
}

fn render_repair(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let Some(review) = &app.model.repair_review else {
        frame.render_widget(
            Paragraph::new("No repair plan loaded. Press `g` to generate a plan from a dataset.")
                .block(Block::default().borders(Borders::ALL).title("Repair Plan Review")),
            area,
        );
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(6)])
        .split(area);

    let (safe, rev, blocked) = review.counts_by_safety();
    let hdr = vec![
        Line::from(format!(
            "Plan: {}  |  Source: {}  ->  Output: {}",
            review.plan.plan_id, review.source, review.output
        )),
        Line::from(format!(
            "Dataset FP: {}  |  Policy FP: {}",
            review.plan.dataset_fingerprint.digest, review.plan.policy_fingerprint
        )),
        Line::from(format!(
            "Ops: safe={safe}, review_required={rev} (selected={}), blocked={blocked}  |  Space=toggle, s=submit",
            review.authorized_ids.len()
        )),
    ];
    frame.render_widget(
        Paragraph::new(hdr)
            .block(Block::default().borders(Borders::ALL).title("Repair Plan Identity"))
            .wrap(Wrap { trim: true }),
        chunks[0],
    );

    let rows: Vec<Row<'_>> = review
        .plan
        .operations
        .iter()
        .enumerate()
        .map(|(i, op)| {
            let auth_mark = match op.safety {
                RepairSafety::Safe => "[auto]",
                RepairSafety::ReviewRequired => {
                    if review.authorized_ids.contains(&op.operation_id) {
                        "[X]"
                    } else {
                        "[ ]"
                    }
                }
                RepairSafety::Blocked | RepairSafety::Destructive => "[LOCK]",
            };
            let style = if i == app.selected_repair_op_idx {
                Style::default().bg(Color::DarkGray).add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(auth_mark),
                Cell::from(op.operation_id.clone()),
                Cell::from(format!("{:?}", op.safety)),
                Cell::from(op.depends_on.join(",")),
                Cell::from(op.rationale.clone()),
            ])
            .style(style)
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(7),
            Constraint::Length(10),
            Constraint::Length(16),
            Constraint::Length(14),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new(["Auth", "Op ID", "Safety", "Depends", "Rationale"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default().borders(Borders::ALL).title("Operations (Space=authorize ReviewRequired)"),
    );
    frame.render_widget(table, chunks[1]);
}

fn render_batches(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    let batches = app.batch_jobs();
    let rows: Vec<Row<'_>> = batches
        .iter()
        .enumerate()
        .map(|(i, j)| {
            let style = if i == app.selected_batch_idx {
                Style::default().bg(Color::DarkGray).add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(format!("{:?}", j.status)),
                Cell::from(j.normalized_target_key.clone()),
            ])
            .style(style)
        })
        .collect();
    frame.render_widget(
        Table::new(rows, [Constraint::Length(12), Constraint::Min(15)])
            .header(
                Row::new(["Status", "Batch Plan / Run"])
                    .style(Style::default().add_modifier(Modifier::BOLD)),
            )
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Batch Jobs (Enter=inspect, R=resume)"),
            ),
        cols[0],
    );

    let detail: Vec<Line<'_>> = if let Some(st) = &app.model.selected_batch_status {
        let mut lines = vec![
            Line::from(format!("Batch: {} (run_id={})", st.batch_name, st.run_id)),
            Line::from(format!(
                "Succeeded={} Failed(Rec)={} Failed(Perm)={} Blocked={} Running={} Cancelled={}",
                st.state_counts.succeeded,
                st.state_counts.failed_recoverable,
                st.state_counts.failed_permanent,
                st.state_counts.blocked,
                st.state_counts.running,
                st.state_counts.cancelled
            )),
            Line::from(format!("Resumable={} Completed={}", st.resumable, st.completed)),
            Line::from(""),
        ];
        for ds in st.datasets.iter().take(20) {
            lines.push(Line::from(format!(
                "  {} [{:?}] attempts={} {}",
                ds.dataset_id.0,
                ds.state,
                ds.attempts,
                ds.error_message.as_deref().unwrap_or("")
            )));
        }
        lines
    } else {
        vec![Line::from("Select a batch job and press Enter to load journal status.")]
    };
    frame.render_widget(
        Paragraph::new(detail)
            .block(Block::default().borders(Borders::ALL).title("Batch Journal Status"))
            .wrap(Wrap { trim: true }),
        cols[1],
    );
}

fn render_activity(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let rows: Vec<Row<'_>> = app
        .model
        .selected_job_events
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let style = if i == app.selected_activity_idx {
                Style::default().bg(Color::DarkGray)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(e.seq.to_string()),
                Cell::from(e.occurred_at.format("%H:%M:%S").to_string()),
                Cell::from(e.stage.as_str().to_string()),
                Cell::from(e.kind.as_str().to_string()),
                Cell::from(e.message.as_deref().unwrap_or("").to_string()),
            ])
            .style(style)
        })
        .collect();
    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Length(6),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Length(20),
                Constraint::Min(20),
            ],
        )
        .header(
            Row::new(["Seq", "Time", "Stage", "Kind", "Message"])
                .style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .block(Block::default().borders(Borders::ALL).title("Job Event Timeline")),
        area,
    );
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let (msg, style) = if let Some(err) = &app.error_banner {
        (err.clone(), Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
    } else {
        (
            format!("{}  |  ? help  j/k move  / filter  r refresh  q quit", app.status_banner),
            Style::default().fg(Color::Gray),
        )
    };
    frame.render_widget(Paragraph::new(msg).style(style), area);
}

fn render_dialog(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    match &app.dialog {
        DialogMode::None => {}
        DialogMode::Help => {
            let popup = centered_rect(70, 70, area);
            frame.render_widget(Clear, popup);
            let lines = vec![
                Line::from("Tab / Shift-Tab : Switch console view"),
                Line::from("j / k / Up/Down : Move selection"),
                Line::from("Enter           : Inspect job / run / batch"),
                Line::from("/               : Filter jobs or findings"),
                Line::from("r               : Refresh data from server"),
                Line::from("n               : Submit new scan job (Operator/Admin)"),
                Line::from("c               : Cancel active job (Jobs view)"),
                Line::from("g / Space / s   : Generate / toggle / submit repair (Repair view)"),
                Line::from("[ / ]           : Previous / next run (Findings view)"),
                Line::from("R               : Resume batch run (Batches view)"),
                Line::from("Esc             : Close modal or clear filter"),
                Line::from("q               : Quit TUI"),
            ];
            frame.render_widget(
                Paragraph::new(lines)
                    .block(Block::default().borders(Borders::ALL).title("Keyboard Help (Esc)"))
                    .wrap(Wrap { trim: true }),
                popup,
            );
        }
        DialogMode::FilterInput { buffer } => {
            let popup = centered_rect(60, 20, area);
            frame.render_widget(Clear, popup);
            frame.render_widget(
                Paragraph::new(format!("Query: {buffer}_")).block(
                    Block::default().borders(Borders::ALL).title("Filter (Enter apply, Esc clear)"),
                ),
                popup,
            );
        }
        DialogMode::TokenPrompt { input } => {
            let popup = centered_rect(65, 24, area);
            frame.render_widget(Clear, popup);
            let masked = "*".repeat(input.len());
            frame.render_widget(
                Paragraph::new(format!(
                    "Enter bearer token (never passed via CLI flags):\n> {masked}_"
                ))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Authenticate (Enter connect, Esc quit)"),
                ),
                popup,
            );
        }
        DialogMode::ScanPrompt { path, profile, plugins, field } => {
            let popup = centered_rect(70, 36, area);
            frame.render_widget(Clear, popup);
            let marker = |idx: usize| if *field == idx { ">" } else { " " };
            let lines = vec![
                Line::from(format!("{} Path/URI : {path}", marker(0))),
                Line::from(format!("{} Profile  : {profile}", marker(1))),
                Line::from(format!(
                    "{} Plugins  : {plugins} (comma-separated, optional)",
                    marker(2)
                )),
                Line::from(""),
                Line::from("Tab switches field, Enter submits scan job, Esc cancels."),
            ];
            frame.render_widget(
                Paragraph::new(lines)
                    .block(Block::default().borders(Borders::ALL).title("Submit Scan Job")),
                popup,
            );
        }
        DialogMode::PlanPrompt { location, policy_path, output, field } => {
            let popup = centered_rect(72, 36, area);
            frame.render_widget(Clear, popup);
            let marker = |idx: usize| if *field == idx { ">" } else { " " };
            let lines = vec![
                Line::from(format!("{} Source dataset : {location}", marker(0))),
                Line::from(format!("{} Policy TOML    : {policy_path} (optional)", marker(1))),
                Line::from(format!("{} Repair output  : {output}", marker(2))),
                Line::from(""),
                Line::from("Tab switches field, Enter generates repair plan, Esc cancels."),
            ];
            frame.render_widget(
                Paragraph::new(lines)
                    .block(Block::default().borders(Borders::ALL).title("Generate Repair Plan")),
                popup,
            );
        }
        DialogMode::ConfirmCancel { job_id, target } => {
            let popup = centered_rect(60, 26, area);
            frame.render_widget(Clear, popup);
            frame.render_widget(
                Paragraph::new(format!(
                    "Cancel active job {job_id}?\nTarget: {target}\n\nPress `y` to confirm or `n` / Esc to abort."
                ))
                .block(Block::default().borders(Borders::ALL).title("Confirm Job Cancel"))
                .wrap(Wrap { trim: true }),
                popup,
            );
        }
        DialogMode::ConfirmRepairSubmit => {
            let popup = centered_rect(72, 42, area);
            frame.render_widget(Clear, popup);
            let text = if let Some(r) = &app.model.repair_review {
                let auth = r.authorized_list();
                format!(
                    "Submit repair job?\n\nSource : {}\nOutput : {}\nPlan ID: {}\nExplicitly authorized ReviewRequired IDs: {}\n\nPress `y` to submit (with Idempotency-Key) or `n`/Esc to abort.",
                    r.source,
                    r.output,
                    r.plan.plan_id,
                    if auth.is_empty() { "<none>".into() } else { auth.join(", ") }
                )
            } else {
                "No repair plan selected.".into()
            };
            frame.render_widget(
                Paragraph::new(text)
                    .block(
                        Block::default().borders(Borders::ALL).title("Confirm Repair Submission"),
                    )
                    .wrap(Wrap { trim: true }),
                popup,
            );
        }
        DialogMode::ConfirmBatchResume { run_dir } => {
            let popup = centered_rect(65, 26, area);
            frame.render_widget(Clear, popup);
            frame.render_widget(
                Paragraph::new(format!(
                    "Resume batch run?\nRun dir: {run_dir}\n\nPress `y` to confirm or `n`/Esc to abort."
                ))
                .block(Block::default().borders(Borders::ALL).title("Confirm Batch Resume"))
                .wrap(Wrap { trim: true }),
                popup,
            );
        }
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
