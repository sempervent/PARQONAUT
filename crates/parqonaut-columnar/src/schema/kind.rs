use arrow::datatypes::{DataType, TimeUnit};

use crate::error::ColumnarError;

/// Stream/repair unification kind (v0.8-compatible subset used by compatibility vectors).
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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
    pub fn from_arrow_type(dt: &DataType) -> Result<Self, ColumnarError> {
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
            other => Err(ColumnarError::Schema(format!(
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
