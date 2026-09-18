use std::path::PathBuf;

use camino::Utf8PathBuf;
use parqonaut_orchestrator::{
    check_config, execute_batch, plan_batch, read_status, resume_batch, verify_run, write_plan,
    BatchRunOptions, CancelFlag,
};
use tokio::signal;

fn to_utf8(path: PathBuf) -> Result<Utf8PathBuf, Box<dyn std::error::Error>> {
    path.try_into().map_err(|_| "path must be valid UTF-8".into())
}

pub async fn run_check(config: PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let path = to_utf8(config)?;
    let report = check_config(&path);
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
    let plan = plan_batch(&path)?;
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

    let options = BatchRunOptions {
        dry_run,
        jobs,
        cancel: cancel.clone(),
        interrupt_after_completed: interrupt_after,
    };

    let result = execute_batch(&to_utf8(plan_path)?, options).await?;
    print_execution_result(&result, json)?;
    if result.already_completed {
        return Ok(());
    }
    if cancel.is_cancelled() {
        std::process::exit(130);
    }
    if let Some(outcome) = &result.outcome {
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

    let options =
        BatchRunOptions { dry_run, jobs, cancel: cancel.clone(), interrupt_after_completed: None };
    let result = resume_batch(&to_utf8(run_dir)?, options).await?;
    print_execution_result(&result, json)?;
    if result.already_completed {
        if !json {
            println!("Run already completed; resume is a no-op");
        }
        return Ok(());
    }
    if cancel.is_cancelled() {
        std::process::exit(130);
    }
    if let Some(outcome) = &result.outcome {
        let all_ok =
            outcome.datasets.iter().all(|d| d.state.is_terminal() && d.error_class.is_none());
        if !all_ok {
            std::process::exit(2);
        }
    }
    Ok(())
}

pub async fn run_status(run_dir: PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let status = read_status(&to_utf8(run_dir)?)?;
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
    let report = verify_run(&to_utf8(run_dir)?)?;
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
