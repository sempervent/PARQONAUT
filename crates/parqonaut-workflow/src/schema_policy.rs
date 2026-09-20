use serde::{Deserialize, Serialize};

/// How to resolve incompatible column types during stream unification.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaConflictPolicy {
    #[default]
    Strict,
    Widen,
    Stringify,
}

/// Named cases for cross-engine compatibility tests (see `tests/schema_vectors.rs`).
#[allow(dead_code)]
pub fn compatibility_case_names() -> &'static [&'static str] {
    &["int32+int64", "float32+float64", "string+int strict", "string+int stringify"]
}
