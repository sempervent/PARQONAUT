use parqonaut_stream::schema::{widen_types, TypeKind};
use parqonaut_workflow::SchemaConflictPolicy;
use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
struct VectorFile {
    vectors: Vec<Vector>,
}

#[derive(Deserialize)]
struct Vector {
    name: String,
    left: String,
    right: String,
    policy: String,
    expect: Option<String>,
    expect_error: Option<bool>,
}

fn parse_kind(s: &str) -> TypeKind {
    match s {
        "Null" => TypeKind::Null,
        "I8" => TypeKind::I8,
        "I16" => TypeKind::I16,
        "I32" => TypeKind::I32,
        "I64" => TypeKind::I64,
        "F32" => TypeKind::F32,
        "F64" => TypeKind::F64,
        "Utf8" => TypeKind::Utf8,
        "Binary" => TypeKind::Binary,
        "Date" => TypeKind::Date,
        "Datetime" => TypeKind::Datetime,
        other => panic!("unknown kind {other}"),
    }
}

fn parse_policy(s: &str) -> SchemaConflictPolicy {
    match s {
        "strict" => SchemaConflictPolicy::Strict,
        "widen" => SchemaConflictPolicy::Widen,
        "stringify" => SchemaConflictPolicy::Stringify,
        other => panic!("unknown policy {other}"),
    }
}

#[test]
fn shared_compatibility_vectors() {
    let path =
        concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/schema/compatibility-vectors.json");
    let data = fs::read_to_string(path).unwrap();
    let file: VectorFile = serde_json::from_str(&data).unwrap();
    for v in file.vectors {
        let left = parse_kind(&v.left);
        let right = parse_kind(&v.right);
        let policy = parse_policy(&v.policy);
        let stringify = matches!(policy, SchemaConflictPolicy::Stringify);
        let result = widen_types(&left, &right, stringify, policy);
        if v.expect_error.unwrap_or(false) {
            assert!(result.is_err(), "vector {} should error", v.name);
        } else {
            let got = result.unwrap_or_else(|e| panic!("vector {} failed: {e}", v.name));
            let expect = parse_kind(v.expect.as_ref().unwrap());
            assert_eq!(got, expect, "vector {}", v.name);
        }
    }
}
