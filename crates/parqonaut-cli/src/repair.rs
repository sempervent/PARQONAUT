use std::fs;
use std::path::PathBuf;

use camino::Utf8PathBuf;
use parqonaut_repair::{
    canonical_plan_json, diagnose, diff_plans, evaluate_check, fmt, generate_plan, scan_directory,
    verify_repair, EffectivePolicy, ExecutionManifest, RepairAuthorization, RepairExecutor,
    RepairPlan,
};

pub async fn run_plan(
    path: PathBuf,
    json: bool,
    canonical: bool,
    output: Option<PathBuf>,
    policy_toml: Option<String>,
    target_schema: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = to_utf8(path)?;
    let policy = EffectivePolicy::from_toml(policy_toml.as_deref())?;
    let scan = scan_directory(&root)?;
    let target = load_target_schema(target_schema)?;
    let plan = generate_plan(&root, &scan, &policy, target)?;

    if let Some(out) = output {
        fs::write(&out, plan.to_json_pretty()?)?;
        if !json {
            eprintln!("Wrote plan to {}", out.display());
            println!("{}", fmt::format_plan_human(&plan));
        }
    } else if canonical {
        println!("{}", canonical_plan_json(&plan)?);
    } else if json {
        println!("{}", plan.to_json_pretty()?);
    } else {
        println!("{}", fmt::format_plan_human(&plan));
    }
    Ok(())
}

pub async fn run_plan_diff(
    left: PathBuf,
    right: PathBuf,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let left_plan = load_plan(left)?;
    let right_plan = load_plan(right)?;
    let diff = diff_plans(&left_plan, &right_plan);
    if json {
        println!("{}", serde_json::to_string_pretty(&diff)?);
    } else {
        println!("Plan diff:");
        println!("  added: {:?}", diff.operations_added);
        println!("  removed: {:?}", diff.operations_removed);
        println!("  changed: {:?}", diff.operations_changed);
        println!("  policy_changed: {}", diff.policy_changed);
        println!("  dataset_fingerprint_changed: {}", diff.dataset_fingerprint_changed);
    }
    Ok(())
}

pub async fn run_check(
    path: PathBuf,
    policy_path: Option<PathBuf>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = to_utf8(path)?;
    let policy_toml = policy_path.map(fs::read_to_string).transpose()?;
    let policy = EffectivePolicy::from_toml(policy_toml.as_deref())?;
    let scan = scan_directory(&root)?;
    let report = evaluate_check(&root, &scan, &policy)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{}", report.summary);
    }
    std::process::exit(report.exit_code as i32);
}

pub async fn run_diagnose(path: PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let root = to_utf8(path)?;
    let policy = EffectivePolicy::default();
    let scan = scan_directory(&root)?;
    let dx = diagnose(&root, &scan, &policy.repair)?;

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
    authorize: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = to_utf8(path)?;
    let plan = load_plan(plan_path)?;
    let scan = scan_directory(&root)?;
    let out = to_utf8(output)?;

    let executor = RepairExecutor {
        authorization: RepairAuthorization::from_ids(authorize),
        verify_before_publish: true,
    };
    let report = executor.execute(&plan, &out, &scan)?;

    println!("Repair execution complete");
    println!("  Plan: {}", report.plan_id);
    println!("  Executed: {}", report.operations_executed.len());
    println!("  Skipped: {}", report.operations_skipped.len());
    println!("  Output: {}", report.output_path);
    println!("  Manifest: {}", report.manifest_path);
    Ok(())
}

pub async fn run_verify(
    before: PathBuf,
    after: PathBuf,
    manifest: Option<PathBuf>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let before_root = to_utf8(before)?;
    let after_root = to_utf8(after)?;
    let policy = EffectivePolicy::default();
    let before_scan = scan_directory(&before_root)?;
    let after_scan = scan_directory(&after_root)?;
    let manifest_obj = match manifest {
        Some(p) => Some(ExecutionManifest::read_json(&to_utf8(p)?)?),
        None => None,
    };
    let report = verify_repair(
        &before_scan,
        &after_scan,
        &before_root,
        &after_root,
        &policy.repair,
        manifest_obj.as_ref(),
    )?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Verification outcome: {:?}", report.outcome);
        for inv in &report.invariant_results {
            println!("  {:24} {}", inv.name, if inv.passed { "PASS" } else { "FAIL" });
        }
        println!("  resolved findings: {}", report.resolved_finding_codes.len());
        println!("  remaining findings: {}", report.remaining_finding_codes.len());
        println!("  blocked findings: {}", report.blocked_finding_codes.len());
        println!("  new findings: {}", report.new_finding_codes.len());
    }
    Ok(())
}

pub async fn run_doctor(
    path: PathBuf,
    policy_toml: Option<String>,
    repair: bool,
    output: Option<PathBuf>,
    authorize: Vec<String>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = to_utf8(path)?;
    let policy = EffectivePolicy::from_toml(policy_toml.as_deref())?;
    let scan = scan_directory(&root)?;
    let dx = diagnose(&root, &scan, &policy.repair)?;
    let plan = generate_plan(&root, &scan, &policy, None)?;

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
        let executor = RepairExecutor {
            authorization: RepairAuthorization::from_ids(authorize),
            verify_before_publish: true,
        };
        let exec_report = executor.execute(&plan, &out, &scan)?;
        println!("\nDataset fingerprint verified.");
        println!("Executing repairs...");
        println!("  executed: {:?}", exec_report.operations_executed);
        println!("  skipped: {:?}", exec_report.operations_skipped);

        let after_scan = scan_directory(&out)?;
        let manifest =
            ExecutionManifest::read_json(&Utf8PathBuf::from(&exec_report.manifest_path))?;
        let verification =
            verify_repair(&scan, &after_scan, &root, &out, &policy.repair, Some(&manifest))?;
        println!("\nVerification:");
        for inv in &verification.invariant_results {
            println!("  {:24} {}", inv.name, if inv.passed { "PASS" } else { "FAIL" });
        }
        println!("  repaired findings      {}", verification.resolved_finding_codes.len());
        println!("  remaining findings     {}", verification.remaining_finding_codes.len());
        println!("  blocked findings       {}", verification.blocked_finding_codes.len());
        println!("  new findings           {}", verification.new_finding_codes.len());
        println!("\nOutcome: {:?}", verification.outcome);
    }

    Ok(())
}

fn load_plan(path: PathBuf) -> Result<RepairPlan, Box<dyn std::error::Error>> {
    Ok(RepairPlan::from_json(&fs::read(path)?)?)
}

fn load_target_schema(
    path: Option<PathBuf>,
) -> Result<Option<Vec<parqonaut_repair::FieldDescriptor>>, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let bytes = fs::read(path)?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn to_utf8(path: PathBuf) -> Result<Utf8PathBuf, Box<dyn std::error::Error>> {
    Utf8PathBuf::from_path_buf(path).map_err(|p| format!("non-UTF8 path: {}", p.display()).into())
}
