use std::fs;
use std::path::PathBuf;

use camino::Utf8PathBuf;
use parqonaut_repair::{
    canonical_plan_json, diagnose_location, diff_plans, evaluate_check_for_location, fmt,
    generate_plan_for_location, location_root, requires_storage_execution, verify_repair,
    EffectivePolicy, ExecutionManifest, ExecutionReport, RepairAuthorization, RepairBackend,
    RepairExecutor, RepairPlan,
};
use parqonaut_storage::location::DatasetLocation;
use uuid::Uuid;

use crate::location::{parse_dataset_location, resolve_backend};

pub async fn run_plan(
    path: String,
    json: bool,
    canonical: bool,
    output: Option<PathBuf>,
    policy_toml: Option<String>,
    target_schema: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let location = parse_dataset_location(&path)?;
    let policy = EffectivePolicy::from_toml(policy_toml.as_deref())?;
    let target = load_target_schema(target_schema)?;
    let backend = resolve_backend(&location).await?;
    let plan = generate_plan_for_location(&location, &backend, &policy, target).await?;

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
    path: String,
    policy_path: Option<PathBuf>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let location = parse_dataset_location(&path)?;
    let policy_toml = policy_path.map(fs::read_to_string).transpose()?;
    let policy = EffectivePolicy::from_toml(policy_toml.as_deref())?;
    let backend = resolve_backend(&location).await?;
    let report = evaluate_check_for_location(&location, &backend, &policy).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{}", report.summary);
    }
    std::process::exit(report.exit_code as i32);
}

pub async fn run_diagnose(path: String, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let location = parse_dataset_location(&path)?;
    let policy = EffectivePolicy::default();
    let backend = resolve_backend(&location).await?;
    let dx = diagnose_location(&location, &backend, &policy).await?;

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
    path: String,
    plan_path: PathBuf,
    output: String,
    authorize: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = parse_dataset_location(&path)?;
    let output_loc = parse_dataset_location(&output)?;
    let plan = load_plan(plan_path)?;
    let source_backend = resolve_backend(&source).await?;
    let scan = source_backend.scan(&source).await?;

    let executor = RepairExecutor {
        authorization: RepairAuthorization::from_ids(authorize),
        verify_before_publish: true,
    };

    let report = if requires_storage_execution(&source, &output_loc) {
        let output_backend = resolve_backend(&output_loc).await?;
        let run_id = Uuid::new_v4().to_string();
        execute_storage_with_backends(
            &executor,
            &plan,
            &output_loc,
            &scan,
            &source_backend,
            &output_backend,
            &run_id,
        )
        .await?
    } else {
        let out = location_root(&output_loc);
        executor.execute(&plan, &out, &scan)?
    };

    println!("Repair execution complete");
    println!("  Plan: {}", report.plan_id);
    println!("  Executed: {}", report.operations_executed.len());
    println!("  Skipped: {}", report.operations_skipped.len());
    println!("  Output: {}", report.output_path);
    println!("  Manifest: {}", report.manifest_path);
    Ok(())
}

pub async fn run_verify(
    before: String,
    after: PathBuf,
    manifest: Option<PathBuf>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let before_location = parse_dataset_location(&before)?;
    let after_root = to_utf8(after)?;
    let before_root = location_root(&before_location);
    let policy = EffectivePolicy::default();
    let backend = resolve_backend(&before_location).await?;
    let before_scan = backend.scan(&before_location).await?;
    let after_scan = parqonaut_repair::scan_directory(&after_root)?;
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
    path: String,
    policy_toml: Option<String>,
    repair: bool,
    output: Option<String>,
    authorize: Vec<String>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let location = parse_dataset_location(&path)?;
    let root = location_root(&location);
    let policy = EffectivePolicy::from_toml(policy_toml.as_deref())?;
    let backend = resolve_backend(&location).await?;
    let scan = backend.scan(&location).await?;
    let dx = parqonaut_repair::diagnose(&root, &scan, &policy.repair)?;
    let plan = parqonaut_repair::generate_plan(&root, &scan, &policy, None)?;

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
        let output_str = output.ok_or("doctor --repair requires --output")?;
        let output_loc = parse_dataset_location(&output_str)?;
        let executor = RepairExecutor {
            authorization: RepairAuthorization::from_ids(authorize),
            verify_before_publish: true,
        };
        let exec_report = if requires_storage_execution(&location, &output_loc) {
            let output_backend = resolve_backend(&output_loc).await?;
            let run_id = Uuid::new_v4().to_string();
            execute_storage_with_backends(
                &executor,
                &plan,
                &output_loc,
                &scan,
                &backend,
                &output_backend,
                &run_id,
            )
            .await?
        } else {
            let out = location_root(&output_loc);
            executor.execute(&plan, &out, &scan)?
        };
        println!("\nDataset fingerprint verified.");
        println!("Executing repairs...");
        println!("  executed: {:?}", exec_report.operations_executed);
        println!("  skipped: {:?}", exec_report.operations_skipped);

        let after_root = match &output_loc {
            DatasetLocation::Local(local) => local.path.clone(),
            DatasetLocation::S3(_) => root.clone(),
        };
        let after_scan = if matches!(output_loc, DatasetLocation::Local(_)) {
            parqonaut_repair::scan_directory(&after_root)?
        } else {
            scan.clone()
        };
        if matches!(output_loc, DatasetLocation::S3(_)) {
            println!("\nVerification ran during staged repair before remote publication.");
        } else {
            let manifest =
                ExecutionManifest::read_json(&Utf8PathBuf::from(&exec_report.manifest_path))?;
            let verification = verify_repair(
                &scan,
                &after_scan,
                &root,
                &after_root,
                &policy.repair,
                Some(&manifest),
            )?;
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

async fn execute_storage_with_backends(
    executor: &RepairExecutor,
    plan: &RepairPlan,
    output: &DatasetLocation,
    scan: &paraclete_types::ScanReport,
    source: &RepairBackend,
    output_backend: &RepairBackend,
    run_id: &str,
) -> Result<ExecutionReport, parqonaut_repair::RepairError> {
    source.execute_plan(executor, plan, output, scan, output_backend, run_id).await
}
