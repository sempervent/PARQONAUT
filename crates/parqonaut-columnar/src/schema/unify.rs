use parqonaut_workflow::SchemaConflictPolicy;

use super::kind::TypeKind;
use crate::error::ColumnarError;

pub fn unify_types(
    left: &TypeKind,
    right: &TypeKind,
    policy: SchemaConflictPolicy,
) -> Result<TypeKind, ColumnarError> {
    let stringify_conflicts = matches!(policy, SchemaConflictPolicy::Stringify);
    widen_types(left, right, stringify_conflicts, policy)
}

pub fn widen_types(
    left: &TypeKind,
    right: &TypeKind,
    stringify_conflicts: bool,
    policy: SchemaConflictPolicy,
) -> Result<TypeKind, ColumnarError> {
    use TypeKind::*;

    if left == &Null {
        return Ok(right.clone());
    }
    if right == &Null {
        return Ok(left.clone());
    }

    if left == right {
        return Ok(left.clone());
    }

    match (left, right) {
        (Bool, I8) | (I8, Bool) => Ok(I8),
        (Bool, I16) | (I16, Bool) => Ok(I16),
        (Bool, I32) | (I32, Bool) => Ok(I32),
        (Bool, I64) | (I64, Bool) => Ok(I64),
        (Bool, F32) | (F32, Bool) => Ok(F32),
        (Bool, F64) | (F64, Bool) => Ok(F64),

        (I8, I16) | (I16, I8) => Ok(I16),
        (I8, I32) | (I32, I8) => Ok(I32),
        (I8, I64) | (I64, I8) => Ok(I64),
        (I16, I32) | (I32, I16) => Ok(I32),
        (I16, I64) | (I64, I16) => Ok(I64),
        (I32, I64) | (I64, I32) => Ok(I64),

        (I8, F32) | (F32, I8) => Ok(F32),
        (I8, F64) | (F64, I8) => Ok(F64),
        (I16, F32) | (F32, I16) => Ok(F32),
        (I16, F64) | (F64, I16) => Ok(F64),
        (I32, F32) | (F32, I32) => Ok(F32),
        (I32, F64) | (F64, I32) => Ok(F64),
        (I64, F32) | (F32, I64) => Ok(F64),
        (I64, F64) | (F64, I64) => Ok(F64),

        (F32, F64) | (F64, F32) => Ok(F64),

        (Date, Datetime) | (Datetime, Date) => Ok(Datetime),

        (Utf8, _) | (_, Utf8) if stringify_conflicts => Ok(Utf8),
        (Binary, _) | (_, Binary) if stringify_conflicts => Ok(Utf8),

        _ if matches!(policy, SchemaConflictPolicy::Strict) => Err(ColumnarError::Schema(format!(
            "Cannot unify incompatible types under strict policy: {left:?} and {right:?}"
        ))),
        _ => Err(ColumnarError::Schema(format!(
            "Cannot unify incompatible types: {left:?} and {right:?}"
        ))),
    }
}
