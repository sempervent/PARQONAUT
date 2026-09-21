use std::path::PathBuf;

use parqonaut_transform::{compile_plan, execute_plan, parse_spec};
use tempfile::tempdir;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn normalize_strings_plugin_local_transform() {
    std::env::set_var(
        "PARQONAUT_PLUGIN_ROOTS",
        repo_root().join("fixtures/plugins").to_string_lossy().to_string(),
    );
    std::env::set_var(
        "PARQONAUT_PLUGIN_SDK_PATH",
        repo_root()
            .join("python/parqonaut_plugins/src")
            .to_string_lossy()
            .to_string(),
    );

    let spec_path = repo_root().join("fixtures/transform/specs/plugin-normalize-strings.yaml");
    let spec = parse_spec(&spec_path).expect("parse spec");
    let out_dir = tempdir().expect("tempdir");
    let mut spec = spec;
    spec.output = Some(
        out_dir
            .path()
            .join("out.parquet")
            .to_string_lossy()
            .into_owned(),
    );
    spec.input = Some(
        repo_root()
            .join("fixtures/transform/partition-basic/input.parquet")
            .to_string_lossy()
            .into_owned(),
    );

    let plan = compile_plan(&spec).expect("compile");
    assert!(plan
        .segments
        .iter()
        .any(|s| matches!(s, parqonaut_transform::CompiledSegment::Fused(f) if f.ops.iter().any(
            |op| matches!(op, parqonaut_transform::FusedOperation::Plugin(p) if p.name == "normalize-strings")
        ))));

    let report = execute_plan(&plan).expect("execute");
    assert_eq!(report.files_written, 1);

    let out_path = spec.output.as_ref().unwrap();
    let meta = std::fs::metadata(out_path).expect("output exists");
    assert!(meta.len() > 0);
}

#[test]
fn scan_only_plugin_rejected_at_compile() {
    std::env::set_var(
        "PARQONAUT_PLUGIN_ROOTS",
        repo_root().join("fixtures/plugins").to_string_lossy().to_string(),
    );
    std::env::set_var(
        "PARQONAUT_PLUGIN_SDK_PATH",
        repo_root()
            .join("python/parqonaut_plugins/src")
            .to_string_lossy()
            .to_string(),
    );
    let mut spec = parse_spec(&repo_root().join("fixtures/transform/specs/plugin-normalize-strings.yaml"))
        .expect("parse");
    spec.input = Some(
        repo_root()
            .join("fixtures/transform/partition-basic/input.parquet")
            .to_string_lossy()
            .into_owned(),
    );
    spec.steps[0].operation = parqonaut_transform::Operation::Plugin {
        plugin: "example-rules".into(),
        config: serde_json::json!({}),
    };
    let err = compile_plan(&spec).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("batch_transform") || msg.contains("does not declare"),
        "unexpected error: {msg}"
    );
}

#[test]
fn stale_plugin_rejected_at_execution() {
    std::env::set_var(
        "PARQONAUT_PLUGIN_ROOTS",
        repo_root().join("fixtures/plugins").to_string_lossy().to_string(),
    );
    std::env::set_var(
        "PARQONAUT_PLUGIN_SDK_PATH",
        repo_root()
            .join("python/parqonaut_plugins/src")
            .to_string_lossy()
            .to_string(),
    );

    let spec_path = repo_root().join("fixtures/transform/specs/plugin-normalize-strings.yaml");
    let mut spec = parse_spec(&spec_path).expect("parse");
    let out_dir = tempdir().expect("tempdir");
    spec.output = Some(
        out_dir
            .path()
            .join("out.parquet")
            .to_string_lossy()
            .into_owned(),
    );
    spec.input = Some(
        repo_root()
            .join("fixtures/transform/partition-basic/input.parquet")
            .to_string_lossy()
            .into_owned(),
    );

    let mut plan = compile_plan(&spec).expect("compile");
    if let parqonaut_transform::CompiledSegment::Fused(fused) = &mut plan.segments[0] {
        if let parqonaut_transform::FusedOperation::Plugin(p) = &mut fused.ops[0] {
            p.digest = "deadbeef".into();
        }
    }
    let err = execute_plan(&plan).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("STALE PLUGIN"), "got {msg}");
}
