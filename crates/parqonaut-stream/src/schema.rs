use crate::error::{MawError, Result};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parqonaut_workflow::SchemaConflictPolicy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TypeKind {
    Null,
    Bool,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Utf8,
    Date,
    Datetime,
    Binary,
}

impl TypeKind {
    pub fn from_arrow_type(dt: &DataType) -> Result<Self> {
        match dt {
            DataType::Null => Ok(TypeKind::Null),
            DataType::Boolean => Ok(TypeKind::Bool),
            DataType::Int8 => Ok(TypeKind::I8),
            DataType::Int16 => Ok(TypeKind::I16),
            DataType::Int32 => Ok(TypeKind::I32),
            DataType::Int64 => Ok(TypeKind::I64),
            DataType::Float32 => Ok(TypeKind::F32),
            DataType::Float64 => Ok(TypeKind::F64),
            DataType::Utf8 | DataType::LargeUtf8 => Ok(TypeKind::Utf8),
            DataType::Binary | DataType::LargeBinary => Ok(TypeKind::Binary),
            DataType::Date32 => Ok(TypeKind::Date),
            DataType::Date64 => Ok(TypeKind::Datetime),
            DataType::Timestamp(_, _) => Ok(TypeKind::Datetime),
            other => Err(MawError::Schema(format!(
                "unsupported Arrow type for unification: {other:?}"
            ))),
        }
    }

    pub fn to_arrow_type(&self) -> DataType {
        match self {
            TypeKind::Null => DataType::Null,
            TypeKind::Bool => DataType::Boolean,
            TypeKind::I8 => DataType::Int8,
            TypeKind::I16 => DataType::Int16,
            TypeKind::I32 => DataType::Int32,
            TypeKind::I64 => DataType::Int64,
            TypeKind::F32 => DataType::Float32,
            TypeKind::F64 => DataType::Float64,
            TypeKind::Utf8 => DataType::Utf8,
            TypeKind::Date => DataType::Date32,
            TypeKind::Datetime => DataType::Timestamp(TimeUnit::Millisecond, None),
            TypeKind::Binary => DataType::Binary,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UnifiedSchema {
    pub schema: Schema,
    pub column_mapping: HashMap<String, String>,
    pub type_mapping: HashMap<String, TypeKind>,
}

impl Default for UnifiedSchema {
    fn default() -> Self {
        Self {
            schema: Schema::empty(),
            column_mapping: HashMap::new(),
            type_mapping: HashMap::new(),
        }
    }
}

impl UnifiedSchema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_schemas(schemas: &[Schema], policy: SchemaConflictPolicy) -> Result<Self> {
        let mut unified = Self::new();
        let mut column_types: HashMap<String, TypeKind> = HashMap::new();
        let mut column_order: Vec<String> = Vec::new();

        for schema in schemas {
            for field in schema.fields() {
                let column_name = field.name().to_string();
                let type_kind = TypeKind::from_arrow_type(field.data_type())?;

                if let Some(existing_type) = column_types.get(&column_name) {
                    let widened = unify_types(existing_type, &type_kind, policy)?;
                    column_types.insert(column_name, widened);
                } else {
                    column_order.push(column_name.clone());
                    column_types.insert(column_name, type_kind);
                }
            }
        }

        let mut fields = Vec::new();
        for column_name in column_order {
            let type_kind = &column_types[&column_name];
            let arrow_type = type_kind.to_arrow_type();
            fields.push(Field::new(column_name.clone(), arrow_type, true));
        }

        unified.schema = Schema::new(fields);
        unified.type_mapping = column_types;

        Ok(unified)
    }

    pub fn get_column_type(&self, column: &str) -> Option<&TypeKind> {
        self.type_mapping.get(column)
    }

    pub fn get_unified_column_name(&self, original: &str) -> String {
        self.column_mapping.get(original).cloned().unwrap_or_else(|| original.to_string())
    }
}

pub fn unify_types(
    left: &TypeKind,
    right: &TypeKind,
    policy: SchemaConflictPolicy,
) -> Result<TypeKind> {
    let stringify_conflicts = matches!(policy, SchemaConflictPolicy::Stringify);
    widen_types(left, right, stringify_conflicts, policy)
}

pub fn widen_types(
    left: &TypeKind,
    right: &TypeKind,
    stringify_conflicts: bool,
    policy: SchemaConflictPolicy,
) -> Result<TypeKind> {
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

        _ if matches!(policy, SchemaConflictPolicy::Strict) => Err(MawError::Schema(format!(
            "Cannot unify incompatible types under strict policy: {:?} and {:?}",
            left, right
        ))),
        _ => Err(MawError::Schema(format!(
            "Cannot unify incompatible types: {:?} and {:?}",
            left, right
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
