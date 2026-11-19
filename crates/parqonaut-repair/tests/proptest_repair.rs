//! Property-based tests for schema compatibility, fingerprint determinism, path safety, and authorization.

use parqonaut_repair::{
    classify_conversion, locations_overlap, resolve_canonical_schema, Compatibility,
    FieldDescriptor, FileSchemaView, PhysicalType, RepairAuthorization, RepairSafety, SchemaPolicy,
};
use parqonaut_storage::location::DatasetLocation;
use proptest::prelude::*;

fn arb_phys() -> impl Strategy<Value = PhysicalType> {
    prop_oneof![
        Just(PhysicalType::Boolean),
        Just(PhysicalType::Int32),
        Just(PhysicalType::Int64),
        Just(PhysicalType::Float32),
        Just(PhysicalType::Float64),
        Just(PhysicalType::Utf8),
    ]
}

proptest! {
    #[test]
    fn schema_compatibility_is_reflexive(ty in arb_phys(), nullable in any::<bool>()) {
        let policy = SchemaPolicy::default();
        let c = classify_conversion(&ty, &ty, nullable, nullable, &policy);
        prop_assert_eq!(c, Compatibility::Lossless);
    }

    #[test]
    fn narrowing_nullable_to_nonnull_requires_review_or_blocks(ty in arb_phys()) {
        let policy = SchemaPolicy::default();
        let c = classify_conversion(&ty, &ty, true, false, &policy);
        prop_assert_ne!(c, Compatibility::Lossless);
    }

    #[test]
    fn canonical_schema_resolution_is_file_order_invariant(
        col_names in prop::collection::hash_set("[a-z]{2,8}", 1..5),
    ) {
        let fields: Vec<FieldDescriptor> = col_names
            .iter()
            .map(|n| FieldDescriptor { name: n.clone(), physical_type: "INT32".into(), nullable: false })
            .collect();
        let views_fwd: Vec<FileSchemaView> = (0..4)
            .map(|i| FileSchemaView { path: format!("part-{i}.parquet"), fields: fields.clone() })
            .collect();
        let mut views_rev = views_fwd.clone();
        views_rev.reverse();

        let r1 = resolve_canonical_schema(&views_fwd, None, &SchemaPolicy::default());
        let r2 = resolve_canonical_schema(&views_rev, None, &SchemaPolicy::default());
        prop_assert_eq!(r1.target_schema, r2.target_schema);
        prop_assert_eq!(r1.conflicts.len(), r2.conflicts.len());
    }

    #[test]
    fn authorization_enforces_safety_tiers(
        op_id in "[a-z0-9-]{4,16}",
        authorized in any::<bool>(),
    ) {
        let auth = if authorized {
            RepairAuthorization::from_ids([op_id.clone()])
        } else {
            RepairAuthorization::default()
        };
        prop_assert!(auth.resolve(&op_id, RepairSafety::Safe).is_ok());
        prop_assert_eq!(auth.resolve(&op_id, RepairSafety::ReviewRequired).is_ok(), authorized);
        prop_assert!(auth.resolve(&op_id, RepairSafety::Destructive).is_err());
        prop_assert!(auth.resolve(&op_id, RepairSafety::Blocked).is_err());
    }

    #[test]
    fn locations_overlap_is_symmetric_and_reflexive(
        a in "/[a-z]{1,6}(/[a-z]{1,6}){0,2}",
        b in "/[a-z]{1,6}(/[a-z]{1,6}){0,2}",
    ) {
        let la = DatasetLocation::parse(&a).unwrap();
        let lb = DatasetLocation::parse(&b).unwrap();
        prop_assert!(locations_overlap(&la, &la));
        prop_assert_eq!(locations_overlap(&la, &lb), locations_overlap(&lb, &la));
    }
}
