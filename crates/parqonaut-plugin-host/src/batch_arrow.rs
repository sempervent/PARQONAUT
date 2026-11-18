//! Arrow schema support checks for batch-transform protocol v1.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::error::PluginHostError;

/// Protocol v1: schema metadata is not part of equality (see `require_schema_equal` in batch_schema).
pub fn validate_schema_supported(schema: &Schema) -> Result<(), PluginHostError> {
    for field in schema.fields() {
        validate_field_supported(field)?;
    }
    Ok(())
}

fn validate_field_supported(field: &Field) -> Result<(), PluginHostError> {
    validate_datatype_supported(field.data_type(), field.name())
}

fn validate_datatype_supported(dt: &DataType, path: &str) -> Result<(), PluginHostError> {
    match dt {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary => Ok(()),
        DataType::Timestamp(unit, _tz) => match unit {
            TimeUnit::Second
            | TimeUnit::Millisecond
            | TimeUnit::Microsecond
            | TimeUnit::Nanosecond => Ok(()),
        },
        DataType::Decimal128(_, _) => Ok(()),
        DataType::Dictionary(key, value) => {
            validate_datatype_supported(key, path)?;
            validate_datatype_supported(value, path)
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            Err(unsupported(path, dt))
        }
        DataType::Struct(_) => Err(unsupported(path, dt)),
        DataType::Map(_, _) | DataType::Union(_, _) => Err(unsupported(path, dt)),
        DataType::FixedSizeBinary(_) => Err(unsupported(path, dt)),
        other => Err(unsupported(path, other)),
    }
}

fn unsupported(path: &str, dt: &DataType) -> PluginHostError {
    PluginHostError::PluginUnsupportedArrowType(format!(
        "field `{path}` type {dt:?} is not supported in batch-transform v1"
    ))
}
