use crate::error::{MawError, Result};
use arrow2::datatypes::{DataType, Field, Schema};
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
    pub fn from_arrow_type(dt: &DataType) -> Self {
        match dt {
            DataType::Null => TypeKind::Null,
            DataType::Boolean => TypeKind::Bool,
            DataType::Int8 => TypeKind::I8,
            DataType::Int16 => TypeKind::I16,
            DataType::Int32 => TypeKind::I32,
            DataType::Int64 => TypeKind::I64,
            DataType::Float32 => TypeKind::F32,
            DataType::Float64 => TypeKind::F64,
            DataType::Utf8 => TypeKind::Utf8,
            DataType::Binary => TypeKind::Binary,
            DataType::Date32 => TypeKind::Date,
            DataType::Date64 => TypeKind::Datetime,
            DataType::Timestamp(_, _) => TypeKind::Datetime,
            _ => TypeKind::Utf8, // Default to string for unknown types
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
            TypeKind::Datetime => {
                DataType::Timestamp(arrow2::datatypes::TimeUnit::Millisecond, None)
            }
            TypeKind::Binary => DataType::Binary,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UnifiedSchema {
    pub schema: Schema,
    pub column_mapping: HashMap<String, String>, // original -> unified name
    pub type_mapping: HashMap<String, TypeKind>, // column -> type
}

impl Default for UnifiedSchema {
    fn default() -> Self {
        Self {
            schema: Schema::from(vec![]),
            column_mapping: HashMap::new(),
            type_mapping: HashMap::new(),
        }
    }
}

impl UnifiedSchema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_schemas(schemas: &[Schema], stringify_conflicts: bool) -> Result<Self> {
        let mut unified = Self::new();
        let mut column_types: HashMap<String, TypeKind> = HashMap::new();

        // Collect all columns and their types
        for schema in schemas {
            for field in &schema.fields {
                let column_name = &field.name;
                let type_kind = TypeKind::from_arrow_type(field.data_type());

                if let Some(existing_type) = column_types.get(column_name) {
                    // Type conflict - need to widen
                    let widened = widen_types(existing_type, &type_kind, stringify_conflicts)?;
                    column_types.insert(column_name.clone(), widened);
                } else {
                    column_types.insert(column_name.clone(), type_kind);
                }
            }
        }

        // Build unified schema
        let mut fields = Vec::new();
        let mut sorted_columns: Vec<_> = column_types.keys().collect();
        sorted_columns.sort();

        for column_name in sorted_columns {
            let type_kind = &column_types[column_name];
            let arrow_type = type_kind.to_arrow_type();
            let field = Field::new(column_name, arrow_type, true); // nullable
            fields.push(field);
        }

        unified.schema = Schema::from(fields);
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

/// Widens two types according to the deterministic widening rules
pub fn widen_types(
    left: &TypeKind,
    right: &TypeKind,
    stringify_conflicts: bool,
) -> Result<TypeKind> {
    use TypeKind::*;

    // Handle nulls
    if left == &Null {
        return Ok(right.clone());
    }
    if right == &Null {
        return Ok(left.clone());
    }

    // Same type
    if left == right {
        return Ok(left.clone());
    }

    // Type widening rules
    match (left, right) {
        // Bool + Number -> Number
        (Bool, I8) | (I8, Bool) => Ok(I8),
        (Bool, I16) | (I16, Bool) => Ok(I16),
        (Bool, I32) | (I32, Bool) => Ok(I32),
        (Bool, I64) | (I64, Bool) => Ok(I64),
        (Bool, F32) | (F32, Bool) => Ok(F32),
        (Bool, F64) | (F64, Bool) => Ok(F64),

        // Integer widening
        (I8, I16) | (I16, I8) => Ok(I16),
        (I8, I32) | (I32, I8) => Ok(I32),
        (I8, I64) | (I64, I8) => Ok(I64),
        (I16, I32) | (I32, I16) => Ok(I32),
        (I16, I64) | (I64, I16) => Ok(I64),
        (I32, I64) | (I64, I32) => Ok(I64),

        // Integer + Float -> Float
        (I8, F32) | (F32, I8) => Ok(F32),
        (I8, F64) | (F64, I8) => Ok(F64),
        (I16, F32) | (F32, I16) => Ok(F32),
        (I16, F64) | (F64, I16) => Ok(F64),
        (I32, F32) | (F32, I32) => Ok(F32),
        (I32, F64) | (F64, I32) => Ok(F64),
        (I64, F32) | (F32, I64) => Ok(F64),
        (I64, F64) | (F64, I64) => Ok(F64),

        // Float widening
        (F32, F64) | (F64, F32) => Ok(F64),

        // Date + Datetime -> Datetime
        (Date, Datetime) | (Datetime, Date) => Ok(Datetime),

        // String conflicts
        (Utf8, _) | (_, Utf8) if stringify_conflicts => Ok(Utf8),
        (Binary, _) | (_, Binary) if stringify_conflicts => Ok(Utf8),

        // Default: error for incompatible types
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
        assert_eq!(widen_types(&TypeKind::Null, &TypeKind::I32, false).unwrap(), TypeKind::I32);
        assert_eq!(widen_types(&TypeKind::I32, &TypeKind::Null, false).unwrap(), TypeKind::I32);
        assert_eq!(widen_types(&TypeKind::I32, &TypeKind::I64, false).unwrap(), TypeKind::I64);
        assert_eq!(widen_types(&TypeKind::I32, &TypeKind::F64, false).unwrap(), TypeKind::F64);
        assert_eq!(widen_types(&TypeKind::Bool, &TypeKind::I32, false).unwrap(), TypeKind::I32);
        assert_eq!(
            widen_types(&TypeKind::Date, &TypeKind::Datetime, false).unwrap(),
            TypeKind::Datetime
        );
    }

    #[test]
    fn test_stringify_conflicts() {
        assert_eq!(widen_types(&TypeKind::I32, &TypeKind::Utf8, true).unwrap(), TypeKind::Utf8);
        assert!(widen_types(&TypeKind::I32, &TypeKind::Utf8, false).is_err());
    }
}
