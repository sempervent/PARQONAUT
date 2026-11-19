//! Public CLI contract tests for `prqnt`: subcommand help, JSON envelopes, and exit codes.

use std::path::PathBuf;

use assert_cmd::Command;
use serde_json::Value;
use tempfile::tempdir;

fn fixture(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(rel)
}

fn prqnt() -> Command {
    Command::cargo_bin("prqnt").expect("prqnt binary")
}

#[test]
fn all_documented_subcommands_expose_help() {
    for args in [
        vec!["--help"],
        vec!["scan", "--help"],
        vec!["plugin", "--help"],
        vec!["plugin", "list", "--help"],
        vec!["plugin", "inspect", "--help"],
        vec!["plugin", "validate", "--help"],
        vec!["inspect", "--help"],
        vec!["partition", "--help"],
        vec!["merge", "--help"],
        vec!["split", "--help"],
        vec!["transform", "--help"],
        vec!["rewrite", "--help"],
        vec!["diagnose", "--help"],
        vec!["plan", "--help"],
        vec!["plan-diff", "--help"],
        vec!["check", "--help"],
        vec!["repair", "--help"],
        vec!["verify", "--help"],
        vec!["doctor", "--help"],
        vec!["batch", "--help"],
        vec!["batch", "check", "--help"],
        vec!["batch", "plan", "--help"],
        vec!["batch", "repair", "--help"],
        vec!["batch", "status", "--help"],
        vec!["batch", "resume", "--help"],
        vec!["batch", "verify", "--help"],
        vec!["serve", "--help"],
        vec!["tui", "--help"],
        vec!["convert", "--help"],
    ] {
        prqnt().args(&args).assert().success();
    }
}

#[test]
fn json_error_envelope_and_nonzero_exit_code_on_missing_input() {
    let out = prqnt()
        .args(["--json", "scan", "/nonexistent/parqonaut/missing.parquet"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).unwrap();
    let v: Value = serde_json::from_str(stderr.trim()).expect("JSON error envelope on stderr");
    assert_eq!(v["ok"], false);
    assert!(v["error"].as_str().is_some_and(|s| !s.is_empty()));
}

#[test]
fn json_contracts_for_scan_diagnose_plan_check_verify_plugin_serve_openapi() {
    let single = fixture("scan/single_parquet/data.parquet");
    let single_dir = fixture("scan/single_parquet");
    let tmp = tempdir().unwrap();

    // 1. scan --json
    let scan_out = prqnt().args(["--json", "scan", single.to_str().unwrap()]).output().unwrap();
    assert!(scan_out.status.success());
    let scan_v: Value = serde_json::from_slice(&scan_out.stdout).unwrap();
    assert!(scan_v["summary"].is_object());
    assert!(scan_v["metadata"]["contract_schema"].is_string());
    assert!(scan_v["metadata"]["report_format"].is_string());

    // 2. diagnose --json
    let dx_out =
        prqnt().args(["--json", "diagnose", single_dir.to_str().unwrap()]).output().unwrap();
    assert!(dx_out.status.success());
    let dx_v: Value = serde_json::from_slice(&dx_out.stdout).unwrap();
    assert!(dx_v["parquet_file_count"].as_u64().unwrap() >= 1);

    // 3. plan --json and plan --canonical
    let canon_out = prqnt()
        .args(["--json", "plan", single_dir.to_str().unwrap(), "--canonical"])
        .output()
        .unwrap();
    assert!(canon_out.status.success());
    let canon_v: Value = serde_json::from_slice(&canon_out.stdout).unwrap();
    assert_eq!(canon_v["schema_version"], 1);

    let plan_out = prqnt().args(["--json", "plan", single_dir.to_str().unwrap()]).output().unwrap();
    assert!(plan_out.status.success());
    let plan_file = tmp.path().join("plan.json");
    std::fs::write(&plan_file, &plan_out.stdout).unwrap();

    // 4. plan-diff --json
    let diff_out = prqnt()
        .args(["--json", "plan-diff", plan_file.to_str().unwrap(), plan_file.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        diff_out.status.success(),
        "plan-diff failed: {}",
        String::from_utf8_lossy(&diff_out.stderr)
    );
    let diff_v: Value = serde_json::from_slice(&diff_out.stdout).unwrap();
    assert_eq!(diff_v["policy_changed"], false);

    // 5. check --json (healthy single parquet exits 0)
    let check_out =
        prqnt().args(["--json", "check", single_dir.to_str().unwrap()]).output().unwrap();
    assert_eq!(check_out.status.code(), Some(0));
    let check_v: Value = serde_json::from_slice(&check_out.stdout).unwrap();
    assert_eq!(check_v["exit_code"], 0);

    // 6. plugin list --json
    let plugin_out = prqnt().args(["--json", "plugin", "list"]).output().unwrap();
    assert!(plugin_out.status.success());
    let plugin_v: Value = serde_json::from_slice(&plugin_out.stdout).unwrap();
    assert!(plugin_v.is_array() || plugin_v["plugins"].is_array());

    // 7. serve --print-openapi
    let openapi_out = prqnt().args(["serve", "--print-openapi"]).output().unwrap();
    assert!(openapi_out.status.success());
    let openapi_v: Value = serde_json::from_slice(&openapi_out.stdout).unwrap();
    assert_eq!(openapi_v["info"]["title"], "PARQONAUT API");
    assert!(openapi_v["paths"]["/api/v1/health/live"].is_object());
    assert!(openapi_v["paths"]["/api/v1/health/ready"].is_object());
}

#[test]
fn convert_partition_merge_split_and_batch_check_contracts() {
    let tmp = tempdir().unwrap();
    let csv = tmp.path().join("in.csv");
    std::fs::write(&csv, "id,region,val\n1,us,10\n2,eu,20\n3,us,30\n").unwrap();
    let pq = tmp.path().join("out.parquet");

    // convert csv -> parquet with --json-progress
    let conv_out = prqnt()
        .args(["convert", csv.to_str().unwrap(), "-o", pq.to_str().unwrap(), "--json-progress"])
        .output()
        .unwrap();
    assert!(conv_out.status.success());
    assert!(pq.exists());

    // partition -> merge -> split
    let part_dir = tmp.path().join("part");
    prqnt()
        .args([
            "partition",
            pq.to_str().unwrap(),
            "--output",
            part_dir.to_str().unwrap(),
            "--by",
            "region",
        ])
        .assert()
        .success();

    let merged = tmp.path().join("merged.parquet");
    prqnt()
        .args(["merge", part_dir.to_str().unwrap(), "--output", merged.to_str().unwrap()])
        .assert()
        .success();
    assert!(merged.exists());

    let split_dir = tmp.path().join("split");
    prqnt()
        .args(["split", merged.to_str().unwrap(), "--output", split_dir.to_str().unwrap()])
        .assert()
        .success();
    assert!(split_dir.exists());

    // batch check --json
    let batch_cfg = tmp.path().join("batch.toml");
    std::fs::write(
        &batch_cfg,
        format!(
            "schema_version = 1\n\n[batch]\nname = \"cli-contract\"\noutput_root = \"{}\"\n\n[[datasets]]\nid = \"d1\"\npath = \"{}\"\n",
            tmp.path().join("batch_out").display(),
            fixture("scan/single_parquet").display()
        ),
    )
    .unwrap();
    let bchk = prqnt()
        .args(["--json", "batch", "check", "--config", batch_cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        bchk.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&bchk.stdout),
        String::from_utf8_lossy(&bchk.stderr)
    );
    let bchk_v: Value = serde_json::from_slice(&bchk.stdout).unwrap();
    assert_eq!(bchk_v["ok"], true);
}
