use parqonaut_repair::{
    canonical_plan_json, generate_plan, scan_directory, EffectivePolicy, RepairExecutor,
    RepairPlan, RepairSafety, PLAN_SCHEMA_VERSION,
};

fn workspace_root() -> camino::Utf8PathBuf {
    camino::Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn ensure_phase3() {
    let root = workspace_root().join("fixtures/phase3/frankenlake-v2");
    if root.exists() {
        return;
    }
    std::process::Command::new("cargo")
        .args(["run", "-p", "parqonaut-repair", "--bin", "generate-phase3-fixtures", "--"])
        .arg(workspace_root().join("fixtures/phase3"))
        .current_dir(workspace_root())
        .env("CARGO_BUILD_JOBS", "1")
        .status()
        .expect("generate fixtures");
}

fn ensure_phase2() {
    let root = workspace_root().join("fixtures/phase2/small-files");
    if root.exists() {
        return;
    }
    std::process::Command::new("cargo")
        .args(["run", "-p", "parqonaut-repair", "--bin", "generate-phase2-fixtures", "--"])
        .arg(workspace_root().join("fixtures/phase2"))
        .current_dir(workspace_root())
        .env("CARGO_BUILD_JOBS", "1")
        .status()
        .expect("generate phase2 fixtures");
}

fn assert_matches_golden(name: &str, plan: &RepairPlan) {
    let canonical = canonical_plan_json(plan).unwrap();
    let golden = std::fs::read_to_string(
        workspace_root().join(format!("fixtures/plans/{name}.canonical.json")),
    )
    .unwrap_or_else(|_| {
        panic!("missing golden fixture: {name}.canonical.json — run generate-golden-plans")
    });
    assert_eq!(canonical, golden.trim(), "golden mismatch for {name}");
}

#[test]
fn frankenlake_v2_mixed_plan_contract() {
    ensure_phase3();
    let root = workspace_root().join("fixtures/phase3/frankenlake-v2");
    let policy_toml =
        std::fs::read_to_string(workspace_root().join("fixtures/phase3/policy.toml")).unwrap();
    let policy = EffectivePolicy::from_toml(Some(&policy_toml)).unwrap();
    let scan = scan_directory(&root).unwrap();
    let plan = generate_plan(&root, &scan, &policy, None).unwrap();
    assert_eq!(plan.schema_version, PLAN_SCHEMA_VERSION);
    assert!(plan.policy_fingerprint.len() >= 32);

    let safe = plan.operations.iter().filter(|o| o.safety == RepairSafety::Safe).count();
    let review =
        plan.operations.iter().filter(|o| o.safety == RepairSafety::ReviewRequired).count();
    let blocked = plan.operations.iter().filter(|o| o.safety == RepairSafety::Blocked).count();
    assert!(safe >= 4);
    assert_eq!(review, 2);
    assert_eq!(blocked, 1);

    assert_matches_golden("mixed", &plan);
    assert_matches_golden("frankenlake-v2", &plan);
}

#[test]
fn safe_only_plan_contract() {
    ensure_phase2();
    let root = workspace_root().join("fixtures/phase2/small-files");
    let policy = EffectivePolicy::default();
    let scan = scan_directory(&root).unwrap();
    let plan = generate_plan(&root, &scan, &policy, None).unwrap();
    assert!(plan.operations.iter().all(|o| o.safety == RepairSafety::Safe));
    assert_eq!(plan.schema_version, PLAN_SCHEMA_VERSION);
    assert_matches_golden("safe-only", &plan);
}

#[test]
fn review_required_rename_plan_contract() {
    ensure_phase3();
    let root = workspace_root().join("fixtures/phase3/rename-map");
    let policy_toml =
        std::fs::read_to_string(workspace_root().join("fixtures/phase3/rename-policy.toml"))
            .unwrap();
    let policy = EffectivePolicy::from_toml(Some(&policy_toml)).unwrap();
    let scan = scan_directory(&root).unwrap();
    let plan = generate_plan(&root, &scan, &policy, None).unwrap();
    assert!(plan
        .operations
        .iter()
        .any(|o| matches!(o.action, parqonaut_repair::RepairAction::RenameColumn { .. })));
    assert!(plan.operations.iter().all(|o| o.safety == RepairSafety::ReviewRequired));
    assert_matches_golden("review-required", &plan);
    assert_matches_golden("rename", &plan);
}

#[test]
fn unresolvable_schema_conflict_plan() {
    ensure_phase3();
    let root = workspace_root().join("fixtures/phase3/explicit-target");
    let policy = EffectivePolicy::default();
    let scan = scan_directory(&root).unwrap();
    let plan = generate_plan(&root, &scan, &policy, None).unwrap();
    assert!(plan.schema_conflicts.as_ref().is_some_and(|c| !c.is_empty()));
    assert!(plan.operations.iter().any(|o| o.safety == RepairSafety::Blocked));
    assert_matches_golden("unresolvable", &plan);
}

#[test]
fn stale_fingerprint_rejected() {
    ensure_phase3();
    let root = workspace_root().join("fixtures/phase3/frankenlake-v2");
    let policy_toml =
        std::fs::read_to_string(workspace_root().join("fixtures/phase3/policy.toml")).unwrap();
    let policy = EffectivePolicy::from_toml(Some(&policy_toml)).unwrap();
    let scan = scan_directory(&root).unwrap();
    let mut plan = generate_plan(&root, &scan, &policy, None).unwrap();
    plan.dataset_fingerprint.digest = "deadbeef".repeat(8);
    let out = tempfile::tempdir().unwrap();
    let output = out.path().join("repaired-output");
    let err = RepairExecutor::default()
        .execute(&plan, camino::Utf8Path::new(output.to_str().unwrap()), &scan)
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("dataset changed") || msg.contains("fingerprint"),
        "unexpected error: {err}"
    );
}

#[test]
fn plan_determinism() {
    ensure_phase3();
    let root = workspace_root().join("fixtures/phase3/frankenlake-v2");
    let policy_toml =
        std::fs::read_to_string(workspace_root().join("fixtures/phase3/policy.toml")).unwrap();
    let policy = EffectivePolicy::from_toml(Some(&policy_toml)).unwrap();
    let scan = scan_directory(&root).unwrap();
    let p1 = generate_plan(&root, &scan, &policy, None).unwrap();
    let p2 = generate_plan(&root, &scan, &policy, None).unwrap();
    assert_eq!(p1.plan_id, p2.plan_id);
    assert_eq!(canonical_plan_json(&p1).unwrap(), canonical_plan_json(&p2).unwrap());
}

#[test]
fn unsupported_plan_version_rejected() {
    let plan = RepairPlan {
        schema_version: 99,
        plan_id: "test".into(),
        parqonaut_version: "0.0.0".into(),
        dataset_root: ".".into(),
        dataset_fingerprint: parqonaut_repair::DatasetFingerprint {
            version: 1,
            root: ".".into(),
            entries: vec![],
            digest: "abc".into(),
        },
        policy_fingerprint: "abc".into(),
        generated_at: chrono::Utc::now(),
        source_scan_id: uuid::Uuid::nil(),
        policy: EffectivePolicy::default(),
        operations: vec![],
        expected_outcomes: vec![],
        diagnosis_findings: vec![],
        schema_diff: None,
        schema_conflicts: None,
        target_schema: None,
    };
    let err = plan.validate_version().unwrap_err();
    assert!(err.to_string().contains("99"));
}
