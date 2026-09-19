use std::path::PathBuf;

use parqonaut_app::{
    BatchCheckRequest, BatchPlanRequest, BatchRepairRequest, BatchResumeRequest,
    BatchStatusRequest, BatchVerifyRequest, ParqonautApp, APP_REQUEST_SCHEMA_VERSION,
};

use crate::repair::map_application_error;
use parqonaut_orchestrator::{write_plan, CancelFlag};
use tokio::signal;

fn to_utf8(path: PathBuf) -> Result<camino::Utf8PathBuf, Box<dyn std::error::Error>> {
    path.try_into().map_err(|_| "path must be valid UTF-8".into())
}

pub async fn run_check(config: PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let path = to_utf8(config)?;
    let app = ParqonautApp::cli();
    let report = app.batch_check(BatchCheckRequest {
        schema_version: APP_REQUEST_SCHEMA_VERSION,
        config_path: path,
    });
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if report.ok {
        println!("Batch configuration OK ({} datasets)", report.dataset_count);
        for w in &report.warnings {
            eprintln!("warning: {w}");
        }
    } else {
        for err in &report.errors {
            eprintln!("error: {err}");
        }
    }
    std::process::exit(if report.ok { 0 } else { 1 });
}

pub async fn run_plan(
    config: PathBuf,
    output: Option<PathBuf>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = to_utf8(config)?;
    let app = ParqonautApp::cli();
    let plan = app
        .batch_plan(BatchPlanRequest {
            schema_version: APP_REQUEST_SCHEMA_VERSION,
            config_path: path,
        })
        .map_err(map_application_error)?
        .plan;
    if let Some(out) = output {
        let out_path = to_utf8(out)?;
        write_plan(&plan, &out_path)?;
        if !json {
            eprintln!("Wrote batch plan to {}", out_path);
        }
    }
    if json {
        println!("{}", plan.to_json_pretty()?);
    } else {
        let summary = plan.summary();
        println!("Batch plan {}", plan.batch_plan_id.0);
        println!("  datasets: {}", summary.dataset_count);
        println!("  safe ops: {}", summary.safe_operations);
        println!("  review-required ops: {}", summary.review_required_operations);
        println!("  blocked ops: {}", summary.blocked_operations);
        println!("  output_root: {}", plan.output_root);
    }
    Ok(())
}

pub async fn run_repair(
    plan_path: PathBuf,
    jobs: Option<u32>,
    dry_run: bool,
    json: bool,
    interrupt_after: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancelFlag::new();
    let cancel_watch = cancel.clone();
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            eprintln!("Interrupted — stopping new dataset work…");
            cancel_watch.cancel();
        }
    });

    let app = ParqonautApp::cli();
    let result = app
        .batch_repair(
            BatchRepairRequest {
                schema_version: APP_REQUEST_SCHEMA_VERSION,
                plan_path: to_utf8(plan_path)?,
                jobs,
                dry_run,
                interrupt_after_completed: interrupt_after,
            },
            cancel.clone(),
        )
        .await
        .map_err(map_application_error)?;
    print_execution_result(&result.execution, json)?;
    if result.execution.already_completed {
        return Ok(());
    }
    if cancel.is_cancelled() {
        std::process::exit(130);
    }
    if let Some(outcome) = &result.execution.outcome {
        let all_ok =
            outcome.datasets.iter().all(|d| d.state.is_terminal() && d.error_class.is_none());
        if !all_ok {
            std::process::exit(2);
        }
    }
    Ok(())
}

pub async fn run_resume(
    run_dir: PathBuf,
    jobs: Option<u32>,
    dry_run: bool,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancelFlag::new();
    let cancel_watch = cancel.clone();
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            eprintln!("Interrupted — stopping new dataset work…");
            cancel_watch.cancel();
        }
    });

    let app = ParqonautApp::cli();
    let result = app
        .batch_resume(
            BatchResumeRequest {
                schema_version: APP_REQUEST_SCHEMA_VERSION,
                run_dir: to_utf8(run_dir)?,
                jobs,
                dry_run,
            },
            cancel.clone(),
        )
        .await
        .map_err(map_application_error)?;
    print_execution_result(&result.execution, json)?;
    if result.execution.already_completed {
        if !json {
            println!("Run already completed; resume is a no-op");
        }
        return Ok(());
    }
    if cancel.is_cancelled() {
        std::process::exit(130);
    }
    if let Some(outcome) = &result.execution.outcome {
        let all_ok =
            outcome.datasets.iter().all(|d| d.state.is_terminal() && d.error_class.is_none());
        if !all_ok {
            std::process::exit(2);
        }
    }
    Ok(())
}

pub async fn run_status(run_dir: PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let app = ParqonautApp::cli();
    let status = app
        .batch_status(BatchStatusRequest {
            schema_version: APP_REQUEST_SCHEMA_VERSION,
            run_dir: to_utf8(run_dir)?,
        })
        .map_err(map_application_error)?
        .status;
    if json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        println!("Run {}", status.run_id);
        println!("  plan: {}", status.batch_plan_id);
        println!("  batch: {}", status.batch_name);
        println!("  output_root: {}", status.output_root);
        println!("  completed: {}", status.completed);
        println!("  resumable: {}", status.resumable);
        println!("  succeeded: {}", status.state_counts.succeeded);
        println!("  failed: {}", status.state_counts.failed_permanent);
        println!("  recoverable: {}", status.state_counts.failed_recoverable);
        for ds in &status.datasets {
            if !ds.state.is_terminal() || ds.error_message.is_some() {
                println!(
                    "  {} {:?}{}",
                    ds.dataset_id.0,
                    ds.state,
                    ds.error_message.as_ref().map(|m| format!(" — {m}")).unwrap_or_default()
                );
            }
        }
    }
    Ok(())
}

pub async fn run_verify(run_dir: PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let app = ParqonautApp::cli();
    let report = app
        .batch_verify(BatchVerifyRequest {
            schema_version: APP_REQUEST_SCHEMA_VERSION,
            run_dir: to_utf8(run_dir)?,
        })
        .map_err(map_application_error)?
        .report;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "Batch verification: {} passed, {} failed, {} skipped",
            report.passed, report.failed, report.skipped
        );
        for ds in &report.datasets {
            println!("  {} {:?} — {}", ds.dataset_id, ds.outcome, ds.message);
        }
    }
    if !report.ok {
        std::process::exit(1);
    }
    Ok(())
}

fn print_execution_result(
    result: &parqonaut_orchestrator::BatchExecutionResult,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(dry) = &result.dry_run {
        if json {
            println!("{}", serde_json::to_string_pretty(dry)?);
        } else {
            println!("Dry run for plan {}", dry.batch_plan_id);
            println!("  datasets: {}", dry.dataset_count);
            println!("  creates_journal: {}", dry.creates_journal);
            for ds in &dry.datasets {
                println!("  {} -> {}", ds.dataset_id, ds.output_path);
            }
        }
        return Ok(());
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "run_id": result.run_id.0,
                "run_dir": result.run_dir.as_str(),
                "already_completed": result.already_completed,
                "outcome": result.outcome,
            }))?
        );
    } else if !result.already_completed {
        println!("Run {}", result.run_id.0);
        println!("  dir: {}", result.run_dir);
        if let Some(outcome) = &result.outcome {
            println!("  peak concurrency: {}", outcome.peak_concurrent_datasets);
            for ds in &outcome.datasets {
                println!("  {} {:?}", ds.dataset_id.0, ds.state);
            }
        }
    }
    Ok(())
}
