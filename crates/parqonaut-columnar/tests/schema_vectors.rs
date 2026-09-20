use parqonaut_columnar::schema::{TypeKind, widen_types};
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
    #[serde(default)]
    expect: Option<String>,
    #[serde(default)]
    expect_error: bool,
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
    let raw = fs::read_to_string(path).expect("read vectors");
    let file: VectorFile = serde_json::from_str(&raw).expect("parse vectors");
    for v in file.vectors {
        let left = parse_kind(&v.left);
        let right = parse_kind(&v.right);
        let policy = parse_policy(&v.policy);
        let stringify = matches!(policy, SchemaConflictPolicy::Stringify);
        let result = widen_types(&left, &right, stringify, policy);
        if v.expect_error {
            assert!(result.is_err(), "case {} expected error", v.name);
            continue;
        }
        let got = result.unwrap_or_else(|e| panic!("case {} failed: {e}", v.name));
        let expect = parse_kind(v.expect.as_ref().expect("expect"));
        assert_eq!(got, expect, "case {}", v.name);
    }
}
