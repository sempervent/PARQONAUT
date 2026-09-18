use std::fs;
use std::path::PathBuf;

use camino::Utf8PathBuf;
use parqonaut_repair::{
    diagnose, fmt, generate_plan, scan_directory, verify_repair, RepairExecutor, RepairPolicy,
};

pub async fn run_plan(
    path: PathBuf,
    json: bool,
    output: Option<PathBuf>,
    policy_toml: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = to_utf8(path)?;
    let policy = RepairPolicy::from_toml_optional(policy_toml.as_deref())?;
    let scan = scan_directory(&root)?;
    let plan = generate_plan(&root, &scan, &policy)?;

    if let Some(out) = output {
        fs::write(&out, plan.to_json_pretty()?)?;
        if !json {
            eprintln!("Wrote plan to {}", out.display());
            println!("{}", fmt::format_plan_human(&plan));
        }
    } else if json {
        println!("{}", plan.to_json_pretty()?);
    } else {
        println!("{}", fmt::format_plan_human(&plan));
    }
    Ok(())
}

pub async fn run_diagnose(path: PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let root = to_utf8(path)?;
    let policy = RepairPolicy::default();
    let scan = scan_directory(&root)?;
    let dx = diagnose(&root, &scan, &policy)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&dx)?);
    } else {
        println!("PARQONAUT diagnosis");
        println!("Files: {}", dx.parquet_file_count);
        println!("Rows: {}", dx.total_rows);
        println!("\nFindings:");
        for f in &dx.findings {
            println!("  [{:?}] {} — {}", f.severity, f.code, f.summary);
        }
    }
    Ok(())
}

pub async fn run_repair(
    path: PathBuf,
    plan_path: PathBuf,
    output: PathBuf,
    authorize_review: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = to_utf8(path)?;
    let plan_bytes = fs::read(&plan_path)?;
    let plan = parqonaut_repair::RepairPlan::from_json(&plan_bytes)?;
    let scan = scan_directory(&root)?;
    let out = to_utf8(output)?;

    let executor = RepairExecutor { authorize_review };
    let report = executor.execute(&plan, &out, &scan)?;

    println!("Repair execution complete");
    println!("  Plan: {}", report.plan_id);
    println!("  Executed: {}", report.operations_executed.len());
    println!("  Skipped: {}", report.operations_skipped.len());
    println!("  Output: {}", report.output_path);
    Ok(())
}

pub async fn run_verify(
    before: PathBuf,
    after: PathBuf,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let before_root = to_utf8(before)?;
    let after_root = to_utf8(after)?;
    let policy = RepairPolicy::default();
    let before_scan = scan_directory(&before_root)?;
    let after_scan = scan_directory(&after_root)?;
    let report = verify_repair(&before_scan, &after_scan, &before_root, &after_root, &policy)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Verification outcome: {:?}", report.outcome);
        for inv in &report.invariant_results {
            println!("  {:24} {}", inv.name, if inv.passed { "PASS" } else { "FAIL" });
        }
        println!("  resolved findings: {}", report.resolved_finding_codes.len());
        println!("  remaining findings: {}", report.remaining_finding_codes.len());
        println!("  new findings: {}", report.new_finding_codes.len());
    }
    Ok(())
}

pub async fn run_doctor(
    path: PathBuf,
    repair: bool,
    output: Option<PathBuf>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = to_utf8(path)?;
    let policy = RepairPolicy::default();
    let scan = scan_directory(&root)?;
    let dx = diagnose(&root, &scan, &policy)?;
    let plan = generate_plan(&root, &scan, &policy)?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "diagnosis": dx,
                "plan": plan,
            }))?
        );
        return Ok(());
    }

    println!("PARQONAUT dataset diagnosis\n");
    println!("Files: {}", dx.parquet_file_count);
    println!("Rows: {}\n", dx.total_rows);

    println!("Findings:");
    for f in &dx.findings {
        println!("  [{}] {}", f.code, f.summary);
    }

    println!("\n{}", fmt::format_plan_human(&plan));

    if repair {
        let out = to_utf8(output.ok_or("doctor --repair requires --output")?)?;
        let executor = RepairExecutor::default();
        let exec_report = executor.execute(&plan, &out, &scan)?;
        println!("\nDataset fingerprint verified.");
        println!("Executing safe repairs...");
        println!("  executed: {:?}", exec_report.operations_executed);
        println!("  skipped: {:?}", exec_report.operations_skipped);

        println!("\nRescanning output...");
        let after_scan = scan_directory(&out)?;
        let verification = verify_repair(&scan, &after_scan, &root, &out, &policy)?;
        println!("\nVerification:");
        for inv in &verification.invariant_results {
            println!("  {:24} {}", inv.name, if inv.passed { "PASS" } else { "FAIL" });
        }
        println!("  repaired findings      {}", verification.resolved_finding_codes.len());
        println!("  remaining findings     {}", verification.remaining_finding_codes.len());
        println!("  new findings           {}", verification.new_finding_codes.len());
        println!("\nOutcome: {:?}", verification.outcome);
        if !verification.remaining_finding_codes.is_empty() {
            println!("\nRemaining:");
            for c in &verification.remaining_finding_codes {
                println!("  {c}");
            }
        }
    }

    Ok(())
}

fn to_utf8(path: PathBuf) -> Result<Utf8PathBuf, Box<dyn std::error::Error>> {
    Utf8PathBuf::from_path_buf(path).map_err(|p| format!("non-UTF8 path: {}", p.display()).into())
}
