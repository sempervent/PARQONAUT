//! Stream schema unification — canonical implementation in `parqonaut-columnar`.

use crate::error::{MawError, Result};
use arrow::datatypes::Schema;
use parqonaut_columnar::schema::{
    self, unify_types as columnar_unify, widen_types as columnar_widen,
};
use parqonaut_workflow::SchemaConflictPolicy;

pub use parqonaut_columnar::schema::{TypeKind, UnifiedSchema};

pub fn type_from_arrow(dt: &arrow::datatypes::DataType) -> Result<TypeKind> {
    schema::TypeKind::from_arrow_type(dt).map_err(|e| MawError::Schema(e.to_string()))
}

pub fn unified_from_schemas(
    schemas: &[Schema],
    policy: SchemaConflictPolicy,
) -> Result<UnifiedSchema> {
    schema::UnifiedSchema::from_schemas(schemas, policy)
        .map_err(|e| MawError::Schema(e.to_string()))
}

pub fn unify_types(
    left: &TypeKind,
    right: &TypeKind,
    policy: SchemaConflictPolicy,
) -> Result<TypeKind> {
    columnar_unify(left, right, policy).map_err(|e| MawError::Schema(e.to_string()))
}

pub fn widen_types(
    left: &TypeKind,
    right: &TypeKind,
    stringify_conflicts: bool,
    policy: SchemaConflictPolicy,
) -> Result<TypeKind> {
    columnar_widen(left, right, stringify_conflicts, policy)
        .map_err(|e| MawError::Schema(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parqonaut_workflow::SchemaConflictPolicy;

    #[test]
    fn test_type_widening() {
        let widen = SchemaConflictPolicy::Widen;
        assert_eq!(
            widen_types(&TypeKind::Null, &TypeKind::I32, false, widen).unwrap(),
            TypeKind::I32
        );
        assert_eq!(
            widen_types(&TypeKind::I32, &TypeKind::I64, false, widen).unwrap(),
            TypeKind::I64
        );
        assert_eq!(
            widen_types(&TypeKind::Date, &TypeKind::Datetime, false, widen).unwrap(),
            TypeKind::Datetime
        );
    }

    #[test]
    fn test_stringify_conflicts() {
        let strict = SchemaConflictPolicy::Strict;
        let stringify = SchemaConflictPolicy::Stringify;
        assert_eq!(
            widen_types(&TypeKind::I32, &TypeKind::Utf8, true, stringify).unwrap(),
            TypeKind::Utf8
        );
        assert!(widen_types(&TypeKind::I32, &TypeKind::Utf8, false, strict).is_err());
    }
}
