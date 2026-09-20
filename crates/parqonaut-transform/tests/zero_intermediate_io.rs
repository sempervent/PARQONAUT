use parqonaut_transform::{compile_plan, execute_plan, parse_spec};
use std::path::PathBuf;

#[test]
fn rewrite_partition_spec_has_zero_intermediate_io() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let work = tempfile::tempdir().unwrap();
    let input = work.path().join("input.parquet");
    std::fs::copy(root.join("fixtures/transform/partition-basic/input.parquet"), &input)
        .expect("copy fixture input");

    let mut spec = parse_spec(root.join("fixtures/transform/specs/rewrite-partition.yaml"))
        .expect("parse spec");
    *spec.input.as_mut().unwrap() = input.to_string_lossy().into_owned();
    *spec.output.as_mut().unwrap() = work.path().join("processed").to_string_lossy().into_owned();
    spec.options.overwrite = true;

    let plan = compile_plan(&spec).expect("compile");
    assert!(plan.fully_streamable(), "expected fully streamable plan");
    assert!(plan.intermediate_roots.is_empty());
    let report = execute_plan(&plan).expect("execute");
    assert_eq!(report.intermediate_files_created, 0);
    assert_eq!(report.intermediate_bytes_written, 0);
    assert_eq!(report.intermediate_files_read, 0);
    assert_eq!(report.intermediate_bytes_read, 0);
}
