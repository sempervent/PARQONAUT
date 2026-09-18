use serde::{Deserialize, Serialize};

use super::compatibility::Compatibility;

fn is_zero(v: &u64) -> bool {
    *v == 0
}

/// Dot-separated field path (nested paths supported when present in source metadata).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FieldPath(pub Vec<String>);

impl FieldPath {
    pub fn root(name: impl Into<String>) -> Self {
        Self(vec![name.into()])
    }

    pub fn display(&self) -> String {
        self.0.join(".")
    }
}

/// Normalized physical/logical field descriptor for comparison.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FieldDescriptor {
    pub name: String,
    pub physical_type: String,
    pub nullable: bool,
}

/// Kind of structural difference between two field descriptors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaDifferenceKind {
    Identical,
    ColumnOrderDifference,
    NullableDifference,
    CompatibleNumericWidening,
    CompatibleTypePromotion,
    MissingColumn,
    ExtraColumn,
    ColumnNameDifference,
    IncompatibleTypeChange,
    NestedSchemaDifference,
}

/// One field-level diff entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDifference {
    pub path: FieldPath,
    pub left: Option<FieldDescriptor>,
    pub right: Option<FieldDescriptor>,
    pub difference: SchemaDifferenceKind,
    pub compatibility: Compatibility,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub left_file_count: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub right_file_count: u64,
}

/// Structured schema diff across a dataset inventory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaDiff {
    pub fields: Vec<FieldDifference>,
    pub canonical_schema: Vec<FieldDescriptor>,
}
