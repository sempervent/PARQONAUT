//! Async runtime loop connecting `TuiApp` with `OperatorClient` and live SSE streams.

use std::time::Duration;

use crossterm::event::{self, Event};
use parqonaut_client::{
    generate_idempotency_key, ConnectionState, OperatorClient, SseEventItem, WhoAmIResponseSnapshot,
};
use parqonaut_service::api_types::{
    JobEventListQuery, JobListQuery, PageQuery, RunListQuery, StartScanRequest,
};
use parqonaut_service::application_http::{BatchResumeBody, PlanBody, RepairJobBody};
use parqonaut_types::{JobId, RunId, ScanProfile, ScanTarget};
use tokio::sync::mpsc;

use crate::app::{TuiAction, TuiApp};
use crate::terminal::enter_tui_terminal;
use crate::ui::render;

#[derive(Debug, Clone)]
pub struct TuiLaunchConfig {
    pub server_url: String,
    pub token: Option<String>,
}

pub async fn run_tui(config: TuiLaunchConfig) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OperatorClient::new(&config.server_url, config.token.clone())?;
    let mut app = TuiApp::new(!client.has_token());
    let (mut terminal, _guard) = enter_tui_terminal()?;

    let (sse_tx, mut sse_rx) = mpsc::channel::<SseEventItem>(64);
    let mut active_sse_task: Option<tokio::task::JoinHandle<()>> = None;

    if client.has_token() {
        refresh_all(&client, &mut app).await;
    }

    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        while let Ok(item) = sse_rx.try_recv() {
            app.apply_sse_item(item);
        }

        terminal.draw(|f| render(f, &app))?;

        tokio::select! {
            _ = tick.tick() => {
                app.spinner_tick = app.spinner_tick.wrapping_add(1);
            }
            maybe_item = sse_rx.recv() => {
                if let Some(item) = maybe_item {
                    app.apply_sse_item(item);
                }
            }
            key_ready = tokio::task::spawn_blocking(|| event::poll(Duration::from_millis(50))) => {
                if let Ok(Ok(true)) = key_ready {
                    if let Ok(Event::Key(key)) = event::read() {
                        match app.handle_key(key) {
                            TuiAction::None => {}
                            TuiAction::Quit => break,
                            TuiAction::ConnectToken(tok) => {
                                client = client.with_token(Some(tok));
                                refresh_all(&client, &mut app).await;
                            }
                            TuiAction::RefreshAll => {
                                refresh_all(&client, &mut app).await;
                            }
                            TuiAction::InspectJob(jid) => {
                                load_job_events(&client, &mut app, jid).await;
                                if let Some(h) = active_sse_task.take() {
                                    h.abort();
                                }
                                let cursor = app
                                    .model
                                    .selected_job_events
                                    .last()
                                    .map(|e| e.seq)
                                    .unwrap_or(0);
                                let c = client.clone();
                                let tx = sse_tx.clone();
                                active_sse_task = Some(tokio::spawn(async move {
                                    if let Ok(mut rx) = c.stream_job_events(jid, cursor).await {
                                        while let Some(res) = rx.recv().await {
                                            if let Ok(item) = res {
                                                if tx.send(item).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }));
                            }
                            TuiAction::InspectRun(rid) => {
                                load_run_details(&client, &mut app, rid).await;
                            }
                            TuiAction::InspectBatchJob(jid) => {
                                match client.get_job_batch_status(jid).await {
                                    Ok(resp) => {
                                        app.model.selected_batch_status = Some(resp.status);
                                        app.error_banner = None;
                                    }
                                    Err(e) => app.set_error(&e),
                                }
                            }
                            TuiAction::GeneratePlan { location, policy_path, output } => {
                                let policy_toml = if policy_path.is_empty() {
                                    None
                                } else {
                                    match std::fs::read_to_string(&policy_path) {
                                        Ok(s) => Some(s),
                                        Err(e) => {
                                            app.error_banner = Some(format!("read policy failed: {e}"));
                                            continue;
                                        }
                                    }
                                };
                                match client
                                    .generate_plan(&PlanBody {
                                        location: location.clone(),
                                        policy_toml,
                                        target_schema_path: None,
                                    })
                                    .await
                                {
                                    Ok(resp) => app.set_repair_review(location, output, resp.plan),
                                    Err(e) => app.set_error(&e),
                                }
                            }
                            TuiAction::SubmitScan { path, profile, plugins } => {
                                let prof = parse_profile(&profile);
                                let target = if path.starts_with("s3://") || std::path::Path::new(&path).is_dir() {
                                    ScanTarget::LocalDirectory { path: path.into() }
                                } else {
                                    ScanTarget::LocalFile { path: path.into() }
                                };
                                let req = StartScanRequest {
                                    target,
                                    profile: prof,
                                    options: Default::default(),
                                    scan_id: None,
                                    redaction: None,
                                    plugins,
                                };
                                match client.submit_scan(&req, Some(&generate_idempotency_key("tui-scan"))).await {
                                    Ok(sub) => {
                                        app.status_banner = format!("Scan queued: {}", sub.job_id);
                                        app.error_banner = None;
                                        refresh_all(&client, &mut app).await;
                                    }
                                    Err(e) => app.set_error(&e),
                                }
                            }
                            TuiAction::SubmitRepair => {
                                if let Some(review) = &app.model.repair_review {
                                    let body = RepairJobBody {
                                        source: review.source.clone(),
                                        output: review.output.clone(),
                                        plan: review.plan.clone(),
                                        authorize: review.authorized_list(),
                                    };
                                    match client
                                        .submit_repair(&body, Some(&generate_idempotency_key("tui-repair")))
                                        .await
                                    {
                                        Ok(sub) => {
                                            app.status_banner = format!("Repair queued: {}", sub.job_id);
                                            app.error_banner = None;
                                            refresh_all(&client, &mut app).await;
                                        }
                                        Err(e) => app.set_error(&e),
                                    }
                                }
                            }
                            TuiAction::CancelJob(jid) => {
                                match client.cancel_job(jid).await {
                                    Ok(resp) => {
                                        app.status_banner = resp.message;
                                        app.error_banner = None;
                                        refresh_all(&client, &mut app).await;
                                    }
                                    Err(e) => app.set_error(&e),
                                }
                            }
                            TuiAction::ResumeBatch(run_dir) => {
                                match client
                                    .submit_batch_resume(
                                        &run_dir,
                                        &BatchResumeBody { jobs: None, dry_run: false },
                                        Some(&generate_idempotency_key("tui-batch-resume")),
                                    )
                                    .await
                                {
                                    Ok(sub) => {
                                        app.status_banner = format!("Batch resume queued: {}", sub.job_id);
                                        app.error_banner = None;
                                        refresh_all(&client, &mut app).await;
                                    }
                                    Err(e) => app.set_error(&e),
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(h) = active_sse_task.take() {
        h.abort();
    }
    Ok(())
}

async fn refresh_all(client: &OperatorClient, app: &mut TuiApp) {
    match client.whoami().await {
        Ok(who) => {
            app.model.connection = ConnectionState::Connected {
                base_url: client.base_url().to_string(),
                principal: WhoAmIResponseSnapshot::from(&who),
            };
            app.error_banner = None;
        }
        Err(e) => {
            app.set_error(&e);
            return;
        }
    }

    if let Ok(ov) = client.overview().await {
        app.model.overview = Some(ov);
    }
    if let Ok(jobs) =
        client.list_jobs(&JobListQuery { status: None, kind: None, limit: 50, offset: 0 }).await
    {
        app.model.jobs = jobs.items;
        if let Some(first) = app.model.jobs.first() {
            if app.active_job_id.is_none() {
                app.active_job_id = Some(JobId(first.job_id));
            }
        }
    }
    if let Some(jid) = app.active_job_id {
        load_job_events(client, app, jid).await;
    }
    if let Ok(runs) = client.list_runs(&RunListQuery { limit: 25, offset: 0 }).await {
        app.model.runs = runs.items;
        if app.active_run_id.is_none() {
            if let Some(first) = app.model.runs.first() {
                app.active_run_id = Some(first.run_id);
            }
        }
    }
    if let Some(rid) = app.active_run_id {
        load_run_details(client, app, rid).await;
    }
    if let Ok(plugins) = client.list_plugins().await {
        app.model.plugins = Some(plugins);
    }
    app.status_banner = format!("Refreshed at {}", chrono::Utc::now().format("%H:%M:%S UTC"));
}

async fn load_job_events(client: &OperatorClient, app: &mut TuiApp, jid: JobId) {
    if let Ok(ev) = client
        .get_job_events(
            jid,
            &JobEventListQuery { after_seq: 0, limit: 200, kind: None, stage: None },
        )
        .await
    {
        app.model.selected_job_events = ev.items;
    }
}

async fn load_run_details(client: &OperatorClient, app: &mut TuiApp, rid: RunId) {
    if let Ok(f) = client
        .list_findings(
            rid,
            &PageQuery {
                limit: 100,
                offset: 0,
                inspection_status: None,
                severity: None,
                code: None,
            },
        )
        .await
    {
        app.model.findings = f.items;
        app.selected_finding_idx = 0;
    }
    if let Ok(a) = client
        .list_assets(
            rid,
            &PageQuery {
                limit: 100,
                offset: 0,
                inspection_status: None,
                severity: None,
                code: None,
            },
        )
        .await
    {
        app.model.assets = a.items;
    }
}

fn parse_profile(s: &str) -> ScanProfile {
    match s.trim().to_ascii_lowercase().as_str() {
        "quick" => ScanProfile::Quick,
        "deep" => ScanProfile::Deep,
        "baseline" => ScanProfile::Baseline,
        _ => ScanProfile::Standard,
    }
}
