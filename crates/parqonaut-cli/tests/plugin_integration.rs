use assert_cmd::Command;
use std::path::PathBuf;

fn plugins_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")
}

fn sdk_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/parqonaut_plugins/src")
}

fn scan_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/csv")
}

fn base_cmd() -> Command {
    let mut cmd = Command::cargo_bin("prqnt").unwrap();
    cmd.env("PARQONAUT_PLUGIN_ROOTS", plugins_root());
    cmd.env("PARQONAUT_PLUGIN_SDK_PATH", sdk_path());
    cmd
}

#[test]
fn plugin_list_json() {
    base_cmd()
        .args(["--json", "plugin", "list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("example-rules"));
}

#[test]
fn plugin_inspect_example_rules() {
    base_cmd()
        .args(["plugin", "inspect", "example-rules"])
        .assert()
        .success()
        .stdout(predicates::str::contains("digest:"));
}

#[test]
fn plugin_validate_example_rules() {
    let path = plugins_root().join("example-rules");
    base_cmd()
        .args(["plugin", "validate", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicates::str::contains("valid: example-rules"));
}

#[test]
fn scan_without_plugin_does_not_run_example_rules() {
    let out = base_cmd().args(["scan", scan_fixture().to_str().unwrap()]).assert().success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout);
    assert!(stdout.contains("Scan complete"));
    assert!(!stdout.contains("plugin.example_rules"));
}

#[test]
fn two_plugins_preserve_cli_order_in_metadata() {
    let out = base_cmd()
        .args([
            "--json",
            "scan",
            scan_fixture().to_str().unwrap(),
            "--plugin",
            "plugin-b",
            "--plugin",
            "plugin-a",
        ])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout);
    let report: serde_json::Value = serde_json::from_str(&stdout).expect("scan json");
    let meta = report.get("plugin_metadata").expect("plugin_metadata");
    let resolved = meta.get("resolved_order").and_then(|v| v.as_array()).expect("resolved_order");
    let names: Vec<String> = resolved.iter().filter_map(|v| v.as_str().map(String::from)).collect();
    assert_eq!(names, vec!["plugin-b".to_string(), "plugin-a".to_string()]);
    let executions = meta.get("executions").and_then(|v| v.as_array()).expect("executions");
    let exec_names: Vec<String> = executions
        .iter()
        .filter_map(|e| e.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();
    assert_eq!(exec_names, vec!["plugin-b".to_string(), "plugin-a".to_string()]);
}

#[test]
fn scan_with_explicit_plugin() {
    base_cmd()
        .args(["scan", scan_fixture().to_str().unwrap(), "--plugin", "example-rules"])
        .assert()
        .success()
        .stdout(predicates::str::contains("plugin.example_rules"));
}
