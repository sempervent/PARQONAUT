use parqonaut_repair::{generate_plan, RepairPolicy, PLAN_SCHEMA_VERSION};

#[test]
fn plan_json_has_schema_version() {
    let json = include_str!("../../../fixtures/reports/minimal_report.json");
    let scan: paraclete_types::ScanReport = serde_json::from_str(json).unwrap();
    let root = camino::Utf8Path::new("fixtures/phase1/single_parquet");
    if !root.exists() {
        return;
    }
    let plan = generate_plan(root, &scan, &RepairPolicy::default()).unwrap();
    assert_eq!(plan.schema_version, PLAN_SCHEMA_VERSION);
    let serialized = plan.to_json_pretty().unwrap();
    assert!(serialized.contains("\"schema_version\": 1"));
}

#[test]
fn deterministic_plan_ids() {
    let root = camino::Utf8Path::new("fixtures/phase2/small-files");
    if !root.exists() {
        return;
    }
    let scan = parqonaut_repair::scan_directory(root).unwrap();
    let policy = RepairPolicy::default();
    let p1 = generate_plan(root, &scan, &policy).unwrap();
    let p2 = generate_plan(root, &scan, &policy).unwrap();
    assert_eq!(p1.plan_id, p2.plan_id);
    assert_eq!(p1.operations.len(), p2.operations.len());
}
