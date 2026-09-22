use std::fs;
use std::path::PathBuf;

use camino::Utf8PathBuf;
use parqonaut_app::{
    ApplicationError, CheckRequest, DiagnoseRequest, ParqonautApp, PlanRequest, RepairRequest,
    VerifyRequest, APP_REQUEST_SCHEMA_VERSION,
};
use parqonaut_repair::{
    canonical_plan_json, diff_plans, fmt, location_root, verify_repair, EffectivePolicy,
    ExecutionManifest, RepairAuthorization, RepairExecutor, RepairPlan,
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
    let app = ParqonautApp::cli();
    let plan = app
        .plan(PlanRequest::new(location, policy, target))
        .await
        .map_err(map_application_error)?
        .plan;

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
    let app = ParqonautApp::cli();
    let report =
        app.check(CheckRequest::new(location, policy)).await.map_err(map_application_error)?.report;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{}", report.summary);
    }
    std::process::exit(report.exit_code as i32);
}

pub async fn run_diagnose(path: String, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let location = parse_dataset_location(&path)?;
    let app = ParqonautApp::cli();
    let dx = app
        .diagnose(DiagnoseRequest::new(location, EffectivePolicy::default()))
        .await
        .map_err(map_application_error)?
        .report;

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
    let app = ParqonautApp::cli();
    let report = app
        .repair(RepairRequest {
            schema_version: APP_REQUEST_SCHEMA_VERSION,
            source,
            output: output_loc,
            plan,
            authorize,
        })
        .await
        .map_err(map_application_error)?
        .execution;

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
    let manifest_path = manifest.map(to_utf8).transpose()?;
    let app = ParqonautApp::cli();
    let report = app
        .verify(VerifyRequest {
            schema_version: APP_REQUEST_SCHEMA_VERSION,
            before: before_location,
            after_local_path: after_root,
            manifest_path,
            policy: EffectivePolicy::default(),
        })
        .await
        .map_err(map_application_error)?
        .report;

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
        let exec_report = if parqonaut_repair::requires_storage_execution(&location, &output_loc) {
            let output_backend = resolve_backend(&output_loc).await?;
            let run_id = Uuid::new_v4().to_string();
            backend
                .execute_plan(&executor, &plan, &output_loc, &scan, &output_backend, &run_id)
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

/// Preserve legacy CLI error strings from direct `parqonaut_repair` usage.
pub(crate) fn map_application_error(e: ApplicationError) -> Box<dyn std::error::Error> {
    match e {
        ApplicationError::StaleSource(details) => {
            format!("dataset changed since plan generation (expected fingerprint {details})").into()
        }
        ApplicationError::ReviewRequired => "review-required operation not authorized".into(),
        ApplicationError::BlockedRepair(msg) => msg.into(),
        ApplicationError::RepairFailed(msg) => msg.into(),
        ApplicationError::ScanFailed(msg) => msg.into(),
        ApplicationError::TargetNotFound(msg) => msg.into(),
        ApplicationError::InvalidRequest(msg) => msg.into(),
        ApplicationError::LocationNotAllowed(msg) => msg.into(),
        ApplicationError::BatchFailed(msg) => msg.into(),
        ApplicationError::Conflict(msg) => msg.into(),
        ApplicationError::Internal(msg) => msg.into(),
        ApplicationError::PluginHost(msg) => msg.into(),
        ApplicationError::PluginExecutionDisabled => {
            "plugin execution is disabled on this server".into()
        }
        ApplicationError::PluginNotAllowed(name) => format!("plugin not allowed: {name}").into(),
        ApplicationError::PluginNotFound(name) => format!("plugin not found: {name}").into(),
        ApplicationError::PluginIncompatible(name) => format!("plugin incompatible: {name}").into(),
        ApplicationError::PluginStale { name, expected, actual } => {
            format!("stale plugin {name}: expected digest {expected}, current {actual}").into()
        }
        ApplicationError::PluginCancelled => "plugin execution cancelled".into(),
    }
}
