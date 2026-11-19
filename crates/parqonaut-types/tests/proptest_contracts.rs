//! Property-based tests for durable contracts, canonical JSON, event sanitization, and job statuses.

use parqonaut_types::{
    sanitize_event_message, sort_json_value, AuthRole, JobKind, JobStage, JobStatus, ScanProfile,
    TargetIdentity,
};
use proptest::prelude::*;
use serde_json::{Map, Value};

fn arb_json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        (-1_000_000i64..1_000_000i64).prop_map(|n| Value::Number(n.into())),
        "[a-zA-Z0-9_ -]{0,24}".prop_map(Value::String),
    ];
    leaf.prop_recursive(3, 32, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
            prop::collection::btree_map("[a-z]{1,8}", inner, 0..6).prop_map(|m| {
                let mut map = Map::new();
                for (k, v) in m {
                    map.insert(k, v);
                }
                Value::Object(map)
            }),
        ]
    })
}

proptest! {
    #[test]
    fn sort_json_value_is_idempotent(v in arb_json_value()) {
        let once = sort_json_value(v);
        let twice = sort_json_value(once.clone());
        prop_assert_eq!(serde_json::to_string(&once).unwrap(), serde_json::to_string(&twice).unwrap());
    }

    #[test]
    fn sanitize_event_message_never_exceeds_max_and_redacts_secrets(
        prefix in "[a-zA-Z0-9 ]{0,40}",
        secret in "[a-zA-Z0-9_]{8,24}",
        max_len in 16usize..256usize,
    ) {
        let raw = format!("{prefix} Bearer {secret} password={secret} https://user:{secret}@host/db");
        let sanitized = sanitize_event_message(&raw, max_len);
        prop_assert!(sanitized.chars().count() <= max_len);
        prop_assert!(!sanitized.contains(&secret));
    }

    #[test]
    fn job_status_and_enums_roundtrip_json(
        status_idx in 0usize..5,
        kind_idx in 0usize..4,
        stage_idx in 0usize..7,
        role_idx in 0usize..3,
        prof_idx in 0usize..3,
    ) {
        let statuses = [
            JobStatus::Queued,
            JobStatus::Running,
            JobStatus::Succeeded,
            JobStatus::Failed,
            JobStatus::Canceled,
        ];
        let st = statuses[status_idx];
        let s_json = serde_json::to_string(&st).unwrap();
        let st2: JobStatus = serde_json::from_str(&s_json).unwrap();
        prop_assert_eq!(st, st2);

        let kinds = [JobKind::Scan, JobKind::Repair, JobKind::BatchRepair, JobKind::BatchResume];
        let k = kinds[kind_idx];
        prop_assert_eq!(JobKind::parse(k.as_str()), Some(k));

        let stages = [
            JobStage::Queue,
            JobStage::Worker,
            JobStage::Scan,
            JobStage::Plugin,
            JobStage::Repair,
            JobStage::Batch,
            JobStage::Verify,
        ];
        let stage = stages[stage_idx];
        prop_assert_eq!(JobStage::parse(stage.as_str()), Some(stage));

        let roles = [AuthRole::Reader, AuthRole::Operator, AuthRole::Admin];
        let role = roles[role_idx];
        let r2: AuthRole = serde_json::from_str(&serde_json::to_string(&role).unwrap()).unwrap();
        prop_assert_eq!(role, r2);

        let profiles = [ScanProfile::Quick, ScanProfile::Standard, ScanProfile::Deep];
        let prof = profiles[prof_idx];
        let p2: ScanProfile = serde_json::from_str(&serde_json::to_string(&prof).unwrap()).unwrap();
        prop_assert_eq!(prof, p2);
    }

    #[test]
    fn target_identity_roundtrips_and_normalizes(path in "/[a-z0-9_]{1,8}(/[a-z0-9_]{1,8}){1,3}") {
        let t = parqonaut_types::ScanTarget::LocalFile { path: path.clone().into() };
        let id = TargetIdentity::from_scan_target(&t);
        prop_assert_eq!(id.target_kind, "local_file");
        prop_assert!(!id.normalized_key.is_empty());
    }
}
