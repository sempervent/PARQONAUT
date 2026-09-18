use assert_cmd::Command;
use std::fs;
use std::path::PathBuf;
use std::process::Command as StdCommand;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn ensure_phase3_fixtures() {
    let root = workspace_root().join("fixtures/phase3/frankenlake-v2");
    if root.exists() {
        return;
    }
    let status = StdCommand::new("cargo")
        .args(["run", "-p", "parqonaut-repair", "--bin", "generate-phase3-fixtures", "--"])
        .arg(workspace_root().join("fixtures/phase3"))
        .current_dir(workspace_root())
        .status()
        .expect("generate phase3 fixtures");
    assert!(status.success());
}

fn plan_json(root: &std::path::Path) -> serde_json::Value {
    let out = tempfile::NamedTempFile::new().unwrap();
    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "plan",
            root.to_str().unwrap(),
            "--policy",
            workspace_root().join("fixtures/phase3/policy.toml").to_str().unwrap(),
            "--output",
            out.path().to_str().unwrap(),
        ])
        .assert()
        .success();
    serde_json::from_slice(&fs::read(out.path()).unwrap()).unwrap()
}

#[test]
fn frankenlake_v2_plan_sections() {
    ensure_phase3_fixtures();
    let root = workspace_root().join("fixtures/phase3/frankenlake-v2");
    let plan = plan_json(&root);
    let ops = plan["operations"].as_array().unwrap();
    let safe = ops.iter().filter(|o| o["safety"] == "safe").count();
    let review = ops.iter().filter(|o| o["safety"] == "review_required").count();
    let blocked = ops.iter().filter(|o| o["safety"] == "blocked").count();
    assert!(safe >= 4, "expected safe ops, got {safe}");
    assert_eq!(review, 2, "expected 2 review-required schema ops");
    assert_eq!(blocked, 1, "expected 1 blocked schema conflict");
    assert_eq!(plan["schema_version"], 1);
}

#[test]
fn unauthorized_review_required_skipped() {
    ensure_phase3_fixtures();
    let root = workspace_root().join("fixtures/phase3/frankenlake-v2");
    let plan_path = tempfile::NamedTempFile::new().unwrap();
    let out = workspace_root().join("target/test-phase3-unauth");
    let _ = fs::remove_dir_all(&out);

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "plan",
            root.to_str().unwrap(),
            "--policy",
            workspace_root().join("fixtures/phase3/policy.toml").to_str().unwrap(),
            "--output",
            plan_path.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "repair",
            root.to_str().unwrap(),
            "--plan",
            plan_path.path().to_str().unwrap(),
            "--output",
            out.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("Skipped"));

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(out.join(".parqonaut-manifest.json")).unwrap()).unwrap();
    let skipped = manifest["operations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|o| o["executed"] == false && o["safety"] == "review_required")
        .count();
    assert_eq!(skipped, 2);
}

#[test]
fn frankenlake_v2_authorized_repair_and_verify() {
    ensure_phase3_fixtures();
    let root = workspace_root();
    let source = root.join("fixtures/phase3/frankenlake-v2");
    let plan_path = root.join("target/frankenlake-v2.plan.json");
    let out = root.join("target/frankenlake-v2-repaired");
    let _ = fs::remove_dir_all(&out);

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "plan",
            source.to_str().unwrap(),
            "--policy",
            root.join("fixtures/phase3/policy.toml").to_str().unwrap(),
            "--output",
            plan_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let plan: serde_json::Value = serde_json::from_slice(&fs::read(&plan_path).unwrap()).unwrap();
    let review_ids: Vec<String> = plan["operations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|o| o["safety"] == "review_required")
        .map(|o| o["operation_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(review_ids.len(), 2);

    let mut repair_cmd = Command::cargo_bin("parqonaut").unwrap();
    repair_cmd.args([
        "repair",
        source.to_str().unwrap(),
        "--plan",
        plan_path.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
    ]);
    for id in &review_ids {
        repair_cmd.arg("--authorize").arg(id);
    }
    repair_cmd.assert().success();

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "verify",
            source.to_str().unwrap(),
            out.to_str().unwrap(),
            "--manifest",
            out.join(".parqonaut-manifest.json").to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("row_count"))
        .stdout(predicates::str::contains("PASS"));

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "check",
            out.to_str().unwrap(),
            "--policy",
            root.join("fixtures/phase3/ci-policy.toml").to_str().unwrap(),
        ])
        .assert()
        .code(4);
}

#[test]
fn explicit_target_does_not_override_unsafe_conversion() {
    ensure_phase3_fixtures();
    let root = workspace_root().join("fixtures/phase3/explicit-target");
    let target = tempfile::NamedTempFile::new().unwrap();
    fs::write(
        target.path(),
        r#"[
          {"name":"temperature","physical_type":"INT32","nullable":false},
          {"name":"sensor_id","physical_type":"BYTE_ARRAY","nullable":false},
          {"name":"reading","physical_type":"INT32","nullable":false}
        ]"#,
    )
    .unwrap();

    let out = tempfile::NamedTempFile::new().unwrap();
    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "plan",
            root.to_str().unwrap(),
            "--target-schema",
            target.path().to_str().unwrap(),
            "--output",
            out.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    let plan: serde_json::Value = serde_json::from_slice(&fs::read(out.path()).unwrap()).unwrap();
    assert!(
        plan["schema_conflicts"].as_array().is_some_and(|c| !c.is_empty())
            || plan["operations"].as_array().unwrap().iter().any(|o| o["safety"] == "blocked")
    );
}

#[test]
fn plan_replay_roundtrip() {
    ensure_phase3_fixtures();
    let root = workspace_root().join("fixtures/phase3/frankenlake-v2");
    let plan_path = tempfile::NamedTempFile::new().unwrap();
    Command::cargo_bin("parqonaut")
        .unwrap()
        .args(["plan", root.to_str().unwrap(), "--output", plan_path.path().to_str().unwrap()])
        .assert()
        .success();

    let bytes = fs::read(plan_path.path()).unwrap();
    let plan: parqonaut_repair::RepairPlan =
        parqonaut_repair::RepairPlan::from_json(&bytes).unwrap();
    assert_eq!(plan.schema_version, parqonaut_repair::PLAN_SCHEMA_VERSION);
}
