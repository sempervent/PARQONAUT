use std::path::PathBuf;

use parqonaut_transform::{compile_plan, execute_plan, parse_spec};
use tempfile::tempdir;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn plugin_transform_zero_intermediate_io() {
    std::env::set_var(
        "PARQONAUT_PLUGIN_ROOTS",
        repo_root().join("fixtures/plugins").to_string_lossy().to_string(),
    );
    std::env::set_var(
        "PARQONAUT_PLUGIN_SDK_PATH",
        repo_root().join("python/parqonaut_plugins/src").to_string_lossy().to_string(),
    );
    let work = tempdir().unwrap();
    let input = repo_root().join("fixtures/transform/partition-basic/input.parquet");
    let output = work.path().join("out.parquet");
    let mut spec =
        parse_spec(repo_root().join("fixtures/transform/specs/plugin-normalize-strings.yaml"))
            .expect("parse");
    spec.input = Some(input.to_string_lossy().into_owned());
    spec.output = Some(output.to_string_lossy().into_owned());
    spec.options.overwrite = true;
    let plan = compile_plan(&spec).expect("compile");
    assert!(plan.intermediate_roots.is_empty());
    let report = execute_plan(&plan).expect("execute");
    assert_eq!(report.intermediate_files_created, 0);
    assert_eq!(report.intermediate_bytes_written, 0);
}
