//! Ensures repair workflows route through `parqonaut-app` (not direct orchestrator/repair calls).

#[test]
fn repair_cli_module_uses_parqonaut_app() {
    let repair_src = include_str!("../src/repair.rs");
    assert!(
        repair_src.contains("ParqonautApp::cli()"),
        "repair.rs should construct ParqonautApp via cli()"
    );
    assert!(
        repair_src.contains(".repair(") && repair_src.contains("RepairRequest"),
        "repair command should call app.repair with RepairRequest"
    );
    assert!(
        repair_src.contains(".diagnose(") && repair_src.contains(".check("),
        "diagnose and check should delegate to the app facade"
    );
}

#[test]
fn batch_cli_module_uses_parqonaut_app() {
    let batch_src = include_str!("../src/batch.rs");
    assert!(
        batch_src.contains("ParqonautApp::cli()"),
        "batch.rs should construct ParqonautApp via cli()"
    );
    assert!(
        batch_src.contains(".batch_repair(") && batch_src.contains(".batch_check("),
        "batch workflows should delegate to the app facade"
    );
}
