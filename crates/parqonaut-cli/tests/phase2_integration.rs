use assert_cmd::Command;
use std::path::PathBuf;
use std::process::Command as StdCommand;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn ensure_phase2_fixtures() {
    let root = workspace_root().join("fixtures/phase2/frankenlake");
    if root.exists() {
        return;
    }
    let status = StdCommand::new("cargo")
        .args(["run", "-p", "parqonaut-repair", "--bin", "generate-phase2-fixtures", "--"])
        .arg(workspace_root().join("fixtures/phase2"))
        .current_dir(workspace_root())
        .status()
        .expect("generate fixtures");
    assert!(status.success());
}

#[test]
fn frankenlake_doctor_diagnosis() {
    ensure_phase2_fixtures();
    let path = workspace_root().join("fixtures/phase2/frankenlake");
    Command::cargo_bin("parqonaut")
        .unwrap()
        .args(["doctor", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicates::str::contains("PARQONAUT dataset diagnosis"))
        .stdout(predicates::str::contains("SAFE"))
        .stdout(predicates::str::contains("REVIEW REQUIRED"));
}

#[test]
fn frankenlake_repair_and_verify() {
    ensure_phase2_fixtures();
    let root = workspace_root();
    let source = root.join("fixtures/phase2/frankenlake");
    let output = root.join("target/test-frankenlake-repaired");

    let _ = std::fs::remove_dir_all(&output);

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "doctor",
            source.to_str().unwrap(),
            "--repair",
            "--output",
            output.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("Verification"))
        .stdout(predicates::str::contains("row_count"))
        .stdout(predicates::str::contains("schema_drift"));

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args(["plan", output.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicates::str::contains("REVIEW REQUIRED"))
        .stdout(predicates::str::contains("schema-widen"));
}

#[test]
fn stale_plan_rejected() {
    ensure_phase2_fixtures();
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("small-files");
    std::fs::create_dir_all(&source).unwrap();
    for entry in std::fs::read_dir(workspace_root().join("fixtures/phase2/small-files")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), source.join(entry.file_name())).unwrap();
    }

    let plan_path = temp.path().join("plan.json");
    let out = temp.path().join("out");

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args(["plan", source.to_str().unwrap(), "--output", plan_path.to_str().unwrap()])
        .assert()
        .success();

    let parquet = std::fs::read_dir(&source)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().map(|x| x == "parquet").unwrap_or(false))
        .expect("parquet file");
    std::fs::copy(parquet.path(), source.join("stale-marker.parquet")).unwrap();

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "repair",
            source.to_str().unwrap(),
            "--plan",
            plan_path.to_str().unwrap(),
            "--output",
            out.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicates::str::contains("dataset changed"));
}
