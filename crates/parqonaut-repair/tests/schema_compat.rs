use parqonaut_repair::{
    classify_conversion, compute_schema_diff, resolve_canonical_schema, Compatibility,
    EffectivePolicy, FieldDescriptor, FileSchemaView, PhysicalType, SchemaPolicy,
};

fn field(name: &str, ty: &str, nullable: bool) -> FieldDescriptor {
    FieldDescriptor { name: name.into(), physical_type: ty.into(), nullable }
}

fn view(path: &str, fields: Vec<FieldDescriptor>) -> FileSchemaView {
    FileSchemaView { path: path.into(), fields }
}

#[test]
fn int32_to_int64_is_lossless() {
    let policy = SchemaPolicy::default();
    let c = classify_conversion(&PhysicalType::Int32, &PhysicalType::Int64, false, false, &policy);
    assert_eq!(c, Compatibility::Lossless);
}

#[test]
fn utf8_to_int64_is_unsupported() {
    let policy = SchemaPolicy::default();
    let c = classify_conversion(&PhysicalType::Utf8, &PhysicalType::Int64, false, false, &policy);
    assert_eq!(c, Compatibility::Unsupported);
}

#[test]
fn nullable_widening_is_lossless() {
    let policy = SchemaPolicy::default();
    let c = classify_conversion(&PhysicalType::Int32, &PhysicalType::Int32, false, true, &policy);
    assert_eq!(c, Compatibility::Lossless);
}

#[test]
fn mixed_sensor_id_is_unresolvable() {
    let views = vec![
        view("a", vec![field("sensor_id", "BYTE_ARRAY", false)]),
        view("b", vec![field("sensor_id", "INT64", false)]),
    ];
    let resolution = resolve_canonical_schema(&views, None, &SchemaPolicy::default());
    assert_eq!(resolution.conflicts.len(), 1);
    assert_eq!(resolution.conflicts[0].field, "sensor_id");
}

#[test]
fn numeric_widening_resolves_canonical() {
    let views = vec![
        view("a", vec![field("value", "INT32", false)]),
        view("b", vec![field("value", "INT64", false)]),
    ];
    let diff = compute_schema_diff(&views, &SchemaPolicy::default());
    let resolution = resolve_canonical_schema(&views, None, &SchemaPolicy::default());
    assert!(resolution.conflicts.is_empty());
    assert_eq!(resolution.target_schema.unwrap()[0].physical_type, "INT64");
    assert_eq!(
        diff.fields[0].difference,
        parqonaut_repair::SchemaDifferenceKind::CompatibleNumericWidening
    );
}

#[test]
fn policy_fingerprint_is_stable() {
    let p1 = EffectivePolicy::default();
    let p2 = EffectivePolicy::default();
    assert_eq!(p1.fingerprint(), p2.fingerprint());
}
