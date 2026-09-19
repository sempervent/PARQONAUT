use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use assert_cmd::cargo::cargo_bin_cmd;
use parqonaut_orchestrator::BatchPlan;
use predicates::prelude::PredicateBooleanExt;
use serde_json::Value;
use serial_test::serial;
use tempfile::TempDir;

fn repo_root() -> camino::Utf8PathBuf {
    camino::Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize_utf8().unwrap()
}

fn shipwreck_config() -> camino::Utf8PathBuf {
    repo_root().join("fixtures/phase4/shipwreck/batch.toml")
}

fn shipwreck_root() -> camino::Utf8PathBuf {
    repo_root().join("fixtures/phase4/shipwreck")
}

fn generate_fixtures() {
    let status = Command::new("cargo")
        .args([
            "run",
            "-p",
            "parqonaut-orchestrator",
            "--bin",
            "generate-phase4-fixtures",
            "--",
            "fixtures/phase4/shipwreck",
        ])
        .current_dir(repo_root().as_std_path())
        .status()
        .unwrap();
    assert!(status.success());
}

fn run_batch(args: &[&str]) -> assert_cmd::assert::Assert {
    let mut cmd = cargo_bin_cmd!("parqonaut");
    cmd.args(args).current_dir(repo_root().as_std_path()).assert()
}

fn strip_file_prefix(uri: &str) -> &str {
    uri.strip_prefix("file://").unwrap_or(uri)
}

fn rewrite_plan_outputs(plan: &mut BatchPlan, out: &Path) {
    let old_root = strip_file_prefix(&plan.output_root).to_string();
    let new_root = out.join("repaired");
    let new_root_str = new_root.to_string_lossy().into_owned();
    plan.output_root = new_root_str.clone();
    plan.run_root = new_root_str.clone();
    for ds in &mut plan.datasets {
        let rel = strip_file_prefix(&ds.output_path)
            .strip_prefix(&old_root)
            .unwrap_or(strip_file_prefix(&ds.output_path))
            .trim_start_matches('/');
        ds.output_path = format!("{new_root_str}/{rel}");
    }
}

fn plan_and_rewrite(out: &Path) -> PathBuf {
    let plan_path = out.join("batch-plan.json");
    run_batch(&[
        "batch",
        "plan",
        "--config",
        shipwreck_config().as_str(),
        "--output",
        plan_path.to_str().unwrap(),
    ])
    .success();
    let mut plan: BatchPlan =
        serde_json::from_str(&fs::read_to_string(&plan_path).unwrap()).unwrap();
    rewrite_plan_outputs(&mut plan, out);
    fs::write(&plan_path, serde_json::to_string_pretty(&plan).unwrap()).unwrap();
    plan_path
}

fn run_dir_for(out: &Path) -> PathBuf {
    fs::read_dir(out.join("repaired/.parqonaut/runs")).unwrap().next().unwrap().unwrap().path()
}

fn dataset_id_from_json(value: &Value) -> String {
    value
        .as_str()
        .or_else(|| value.get("0").and_then(|v| v.as_str()))
        .unwrap_or("unknown")
        .to_string()
}

fn terminal_state_map(dir: &Path) -> HashMap<String, String> {
    let run_dir = run_dir_for(dir);
    let status = Command::new(cargo_bin_cmd!("parqonaut").get_program())
        .args(["batch", "status", "--run-dir", run_dir.to_str().unwrap(), "--json"])
        .current_dir(repo_root().as_std_path())
        .output()
        .unwrap();
    let json: Value = serde_json::from_slice(&status.stdout).unwrap();
    json["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| (dataset_id_from_json(&d["dataset_id"]), d["state"].as_str().unwrap().to_string()))
        .collect()
}

#[test]
#[serial]
fn batch_check_and_collision_rejection() {
    generate_fixtures();
    run_batch(&["batch", "check", "--config", shipwreck_config().as_str()])
        .success()
        .stdout(predicates::str::contains("configuration OK"));

    let tmp = TempDir::new().unwrap();
    let cfg = tmp.path().join("batch.toml");
    fs::write(
        &cfg,
        r#"
schema_version = 1
[batch]
name = "collision"
max_concurrency = 1
output_root = "out"
[[datasets]]
id = "a"
path = "src/a"
[[datasets]]
id = "b"
path = "src/b"
output = "shared"
[[datasets]]
id = "c"
path = "src/c"
output = "shared"
"#,
    )
    .unwrap();
    fs::create_dir_all(tmp.path().join("src/a")).unwrap();
    fs::create_dir_all(tmp.path().join("src/b")).unwrap();
    fs::create_dir_all(tmp.path().join("src/c")).unwrap();
    run_batch(&["batch", "check", "--config", cfg.to_str().unwrap()]).failure();
}

#[test]
#[serial]
fn jobs_zero_rejected() {
    generate_fixtures();
    let tmp = TempDir::new().unwrap();
    let plan_path = plan_and_rewrite(tmp.path());
    run_batch(&["batch", "repair", "--plan", plan_path.to_str().unwrap(), "--jobs", "0"])
        .failure()
        .stderr(predicates::str::contains("jobs must be >= 1"));
}

#[test]
#[serial]
fn jobs_equivalence() {
    generate_fixtures();
    let tmp = TempDir::new().unwrap();
    let out1 = tmp.path().join("run-j1");
    let out2 = tmp.path().join("run-j4");
    fs::create_dir_all(&out1).unwrap();
    fs::create_dir_all(&out2).unwrap();

    let shared_plan = plan_and_rewrite(&tmp.path().join("shared-plan"));
    for (out, jobs) in [(&out1, "1"), (&out2, "4")] {
        let plan_path = out.join("batch-plan.json");
        let mut plan: BatchPlan =
            serde_json::from_str(&fs::read_to_string(&shared_plan).unwrap()).unwrap();
        rewrite_plan_outputs(&mut plan, out);
        fs::write(&plan_path, serde_json::to_string_pretty(&plan).unwrap()).unwrap();
        run_batch(&["batch", "repair", "--plan", plan_path.to_str().unwrap(), "--jobs", jobs])
            .code(2);
    }

    assert_eq!(terminal_state_map(&out1), terminal_state_map(&out2));
}

#[tokio::test]
#[serial]
async fn interrupt_resume_and_idempotent_second_resume() {
    generate_fixtures();
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("interrupt-out");
    fs::create_dir_all(&out).unwrap();
    let plan_path = plan_and_rewrite(&out);

    run_batch(&[
        "batch",
        "repair",
        "--plan",
        plan_path.to_str().unwrap(),
        "--jobs",
        "1",
        "--interrupt-after",
        "2",
    ])
    .code(130);

    let run_dir = run_dir_for(&out);
    run_batch(&["batch", "status", "--run-dir", run_dir.to_str().unwrap()]).success();
    run_batch(&["batch", "resume", "--run-dir", run_dir.to_str().unwrap(), "--jobs", "1"]).code(2);
    let outputs_before = fs::read_dir(out.join("repaired")).unwrap().filter_map(Result::ok).count();
    run_batch(&["batch", "resume", "--run-dir", run_dir.to_str().unwrap()]).success();
    let outputs_after = fs::read_dir(out.join("repaired")).unwrap().filter_map(Result::ok).count();
    assert_eq!(outputs_before, outputs_after);
}

#[test]
#[serial]
fn stale_plan_rejected_on_resume() {
    generate_fixtures();
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("stale-out");
    fs::create_dir_all(&out).unwrap();
    let plan_path = plan_and_rewrite(&out);

    run_batch(&[
        "batch",
        "repair",
        "--plan",
        plan_path.to_str().unwrap(),
        "--jobs",
        "1",
        "--interrupt-after",
        "1",
    ])
    .code(130);

    let run_dir = run_dir_for(&out);
    let mut plan: BatchPlan =
        serde_json::from_str(&fs::read_to_string(run_dir.join("batch-plan.json")).unwrap())
            .unwrap();
    plan.config_fingerprint = "stale-fingerprint".into();
    fs::write(run_dir.join("batch-plan.json"), serde_json::to_string_pretty(&plan).unwrap())
        .unwrap();

    run_batch(&["batch", "resume", "--run-dir", run_dir.to_str().unwrap()]).failure().stderr(
        predicates::str::contains("identity mismatch").or(predicates::str::contains("mismatch")),
    );
}

#[test]
#[serial]
fn stale_source_rejected_at_execution() {
    generate_fixtures();
    let tmp = TempDir::new().unwrap();
    let fleet = tmp.path().join("fleet");
    let stale_src = fleet.join("stale-src");
    fs::create_dir_all(&stale_src).unwrap();
    for i in 0..3 {
        let name = format!("snapshot-{i:02}.parquet");
        fs::copy(shipwreck_root().join("05-stale-source").join(&name), stale_src.join(&name))
            .unwrap();
    }

    let cfg = fleet.join("batch.toml");
    fs::write(
        &cfg,
        r#"
schema_version = 1
[batch]
name = "stale-only"
max_concurrency = 1
output_root = "out"
[[datasets]]
id = "stale-source"
path = "stale-src"
"#,
    )
    .unwrap();
    let cfg = cfg.canonicalize().unwrap();

    let out = tmp.path().join("stale-run");
    fs::create_dir_all(&out).unwrap();
    let plan_path = out.join("batch-plan.json");
    run_batch(&[
        "batch",
        "plan",
        "--config",
        cfg.to_str().unwrap(),
        "--output",
        plan_path.to_str().unwrap(),
    ])
    .success();
    let mut plan: BatchPlan =
        serde_json::from_str(&fs::read_to_string(&plan_path).unwrap()).unwrap();
    rewrite_plan_outputs(&mut plan, &out);
    fs::write(&plan_path, serde_json::to_string_pretty(&plan).unwrap()).unwrap();

    fs::copy(stale_src.join("snapshot-00.parquet"), stale_src.join("snapshot-03.parquet")).unwrap();

    run_batch(&["batch", "repair", "--plan", plan_path.to_str().unwrap(), "--jobs", "1"]).code(2);

    let run_dir = run_dir_for(&out);
    let status = Command::new(cargo_bin_cmd!("parqonaut").get_program())
        .args(["batch", "status", "--run-dir", run_dir.to_str().unwrap(), "--json"])
        .current_dir(repo_root().as_std_path())
        .output()
        .unwrap();
    let json: Value = serde_json::from_slice(&status.stdout).unwrap();
    let stale = json["datasets"].as_array().unwrap().iter().find(|d| {
        d["dataset_id"].as_str() == Some("stale-source")
            || d["dataset_id"]["0"].as_str() == Some("stale-source")
    });
    assert!(stale.is_some(), "missing stale-source row: {json}");
    assert_eq!(stale.unwrap()["state"].as_str().unwrap(), "stale_source");
}

#[test]
#[serial]
fn dry_run_has_no_persistent_side_effects() {
    generate_fixtures();
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("dry-run-out");
    fs::create_dir_all(&out).unwrap();
    let plan_path = out.join("batch-plan.json");
    run_batch(&[
        "batch",
        "plan",
        "--config",
        shipwreck_config().as_str(),
        "--output",
        plan_path.to_str().unwrap(),
    ])
    .success();
    let mut plan: BatchPlan =
        serde_json::from_str(&fs::read_to_string(&plan_path).unwrap()).unwrap();
    rewrite_plan_outputs(&mut plan, &out);
    fs::write(&plan_path, serde_json::to_string_pretty(&plan).unwrap()).unwrap();

    run_batch(&["batch", "repair", "--plan", plan_path.to_str().unwrap(), "--dry-run"]).success();

    assert!(!out.join("repaired").exists());
    for entry in fs::read_dir(&out).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy();
        assert_eq!(name, "batch-plan.json", "unexpected artifact: {name}");
    }
}

#[test]
#[serial]
fn dry_run_resume_has_no_journal_mutation() {
    generate_fixtures();
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("dry-resume");
    fs::create_dir_all(&out).unwrap();
    let plan_path = plan_and_rewrite(&out);

    run_batch(&[
        "batch",
        "repair",
        "--plan",
        plan_path.to_str().unwrap(),
        "--jobs",
        "1",
        "--interrupt-after",
        "1",
    ])
    .code(130);

    let run_dir = run_dir_for(&out);
    let journal_before = fs::metadata(run_dir.join("journal.sqlite")).unwrap().len();
    run_batch(&["batch", "resume", "--run-dir", run_dir.to_str().unwrap(), "--dry-run"]).success();
    let journal_after = fs::metadata(run_dir.join("journal.sqlite")).unwrap().len();
    assert_eq!(journal_before, journal_after);
}

#[test]
#[serial]
fn concurrent_destination_lock_rejection() {
    generate_fixtures();
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("lock-out");
    fs::create_dir_all(&out).unwrap();
    let plan_path = plan_and_rewrite(&out);

    let plan: BatchPlan = serde_json::from_str(&fs::read_to_string(&plan_path).unwrap()).unwrap();
    let target = plan.datasets[0].output_path.clone();
    let lock_path = format!("{target}.parqonaut.lock");
    if let Some(parent) = Path::new(&lock_path).parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(&lock_path, "foreign-run-id\n").unwrap();

    run_batch(&["batch", "repair", "--plan", plan_path.to_str().unwrap(), "--jobs", "1"]).code(2);

    let run_dir = run_dir_for(&out);
    let status = Command::new(cargo_bin_cmd!("parqonaut").get_program())
        .args(["batch", "status", "--run-dir", run_dir.to_str().unwrap(), "--json"])
        .current_dir(repo_root().as_std_path())
        .output()
        .unwrap();
    let json: Value = serde_json::from_slice(&status.stdout).unwrap();
    let locked = json["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["dataset_id"].as_str() == Some(&plan.datasets[0].dataset_id.0));
    assert!(locked.is_some());
    assert!(
        locked.unwrap()["error_message"].as_str().unwrap_or("").contains("locked"),
        "expected lock failure message"
    );
    let _ = fs::remove_file(&lock_path);
}

#[test]
#[serial]
fn status_and_verify_from_persisted_journal() {
    generate_fixtures();
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("persist-out");
    fs::create_dir_all(&out).unwrap();
    let plan_path = plan_and_rewrite(&out);

    run_batch(&[
        "batch",
        "repair",
        "--plan",
        plan_path.to_str().unwrap(),
        "--jobs",
        "2",
        "--interrupt-after",
        "3",
    ])
    .code(130);

    let run_dir = run_dir_for(&out);
    let run_dir_str = run_dir.to_str().unwrap();

    let status1 = Command::new(cargo_bin_cmd!("parqonaut").get_program())
        .args(["batch", "status", "--run-dir", run_dir_str, "--json"])
        .current_dir(repo_root().as_std_path())
        .output()
        .unwrap();
    assert!(status1.status.success());
    let status2 = Command::new(cargo_bin_cmd!("parqonaut").get_program())
        .env_clear()
        .args(["batch", "status", "--run-dir", run_dir_str, "--json"])
        .current_dir(repo_root().as_std_path())
        .output()
        .unwrap();
    assert!(status2.status.success());
    assert_eq!(status1.stdout, status2.stdout);

    let verify1 = Command::new(cargo_bin_cmd!("parqonaut").get_program())
        .args(["batch", "verify", "--run-dir", run_dir_str, "--json"])
        .current_dir(repo_root().as_std_path())
        .output()
        .unwrap();
    assert!(verify1.status.success() || verify1.status.code() == Some(1));
}

#[test]
#[serial]
fn batch_plan_deterministic_serialization() {
    generate_fixtures();
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("plan-det");
    fs::create_dir_all(&out).unwrap();
    let p1 = out.join("plan1.json");
    let p2 = out.join("plan2.json");
    run_batch(&[
        "batch",
        "plan",
        "--config",
        shipwreck_config().as_str(),
        "--output",
        p1.to_str().unwrap(),
    ])
    .success();
    run_batch(&[
        "batch",
        "plan",
        "--config",
        shipwreck_config().as_str(),
        "--output",
        p2.to_str().unwrap(),
    ])
    .success();
    let a: BatchPlan = serde_json::from_str(&fs::read_to_string(&p1).unwrap()).unwrap();
    let b: BatchPlan = serde_json::from_str(&fs::read_to_string(&p2).unwrap()).unwrap();
    assert_eq!(a.batch_plan_id, b.batch_plan_id);
    assert_eq!(a.config_fingerprint, b.config_fingerprint);
    assert_eq!(a.datasets.len(), b.datasets.len());
    for (da, db) in a.datasets.iter().zip(b.datasets.iter()) {
        assert_eq!(da.repair_plan.plan_id, db.repair_plan.plan_id);
        assert_eq!(
            da.repair_plan.dataset_fingerprint.digest,
            db.repair_plan.dataset_fingerprint.digest
        );
    }
}
