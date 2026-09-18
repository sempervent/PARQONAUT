use parqonaut_repair::{RepairAction, RepairPolicy, RepairSafety};

#[test]
fn safe_actions_classified() {
    assert_eq!(
        RepairAction::MergeSmallFiles { target_bytes: 1, input_paths: vec![] }.default_safety(),
        RepairSafety::Safe
    );
    assert_eq!(
        RepairAction::AlignSchema { field: "x".into(), from_type: "a".into(), to_type: "b".into() }
            .default_safety(),
        RepairSafety::ReviewRequired
    );
}

#[test]
fn policy_defaults_documented() {
    let p = RepairPolicy::default();
    assert_eq!(p.default_compression, "zstd");
    assert_eq!(p.target_row_group_mb, 128);
    assert_eq!(p.small_file_threshold_mb, 16);
    assert_eq!(p.merge_target_mb, 256);
}
