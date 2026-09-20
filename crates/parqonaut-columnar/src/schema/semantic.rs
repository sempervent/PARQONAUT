//! PARQONAUT-stable semantic types (durable fingerprints; not Arrow Debug strings).

use arrow::datatypes::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::error::ColumnarError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SemanticType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Date32,
    Date64,
    Timestamp { unit: TimestampUnit, timezone: TimestampTimezone },
    Decimal { precision: u8, scale: i8 },
    Dictionary { value: Box<SemanticType> },
    List { field: Box<SemanticField> },
    LargeList { field: Box<SemanticField> },
    Struct { fields: Vec<SemanticField> },
    Map { key: Box<SemanticType>, value: Box<SemanticField> },
    FixedSizeBinary { length: i32 },
    Unsupported { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticField {
    pub name: String,
    pub nullable: bool,
    pub data_type: SemanticType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampTimezone {
    None,
    Utc,
    Named(String),
}

impl SemanticType {
    pub fn from_arrow(dt: &DataType) -> Self {
        match dt {
            DataType::Null => SemanticType::Null,
            DataType::Boolean => SemanticType::Boolean,
            DataType::Int8 => SemanticType::Int8,
            DataType::Int16 => SemanticType::Int16,
            DataType::Int32 => SemanticType::Int32,
            DataType::Int64 => SemanticType::Int64,
            DataType::UInt8 => SemanticType::UInt8,
            DataType::UInt16 => SemanticType::UInt16,
            DataType::UInt32 => SemanticType::UInt32,
            DataType::UInt64 => SemanticType::UInt64,
            DataType::Float32 => SemanticType::Float32,
            DataType::Float64 => SemanticType::Float64,
            DataType::Utf8 => SemanticType::Utf8,
            DataType::LargeUtf8 => SemanticType::LargeUtf8,
            DataType::Binary => SemanticType::Binary,
            DataType::LargeBinary => SemanticType::LargeBinary,
            DataType::Date32 => SemanticType::Date32,
            DataType::Date64 => SemanticType::Date64,
            DataType::Timestamp(unit, tz) => SemanticType::Timestamp {
                unit: match unit {
                    TimeUnit::Second => TimestampUnit::Second,
                    TimeUnit::Millisecond => TimestampUnit::Millisecond,
                    TimeUnit::Microsecond => TimestampUnit::Microsecond,
                    TimeUnit::Nanosecond => TimestampUnit::Nanosecond,
                },
                timezone: match tz {
                    None => TimestampTimezone::None,
                    Some(s) if s.as_ref() == "+00:00" || s.as_ref() == "UTC" => {
                        TimestampTimezone::Utc
                    }
                    Some(s) => TimestampTimezone::Named(s.to_string()),
                },
            },
            DataType::Decimal128(p, s) => SemanticType::Decimal { precision: *p, scale: *s },
            DataType::Dictionary(_, value) => {
                SemanticType::Dictionary { value: Box::new(SemanticType::from_arrow(value)) }
            }
            DataType::List(field) => {
                SemanticType::List { field: Box::new(SemanticField::from_arrow_field(field)) }
            }
            DataType::LargeList(field) => {
                SemanticType::LargeList { field: Box::new(SemanticField::from_arrow_field(field)) }
            }
            DataType::Struct(fields) => SemanticType::Struct {
                fields: fields
                    .iter()
                    .map(|f| SemanticField::from_arrow_field(f.as_ref()))
                    .collect(),
            },
            DataType::Map(field, _) => {
                let entries = field.data_type();
                if let DataType::Struct(entry_fields) = entries {
                    if entry_fields.len() == 2 {
                        return SemanticType::Map {
                            key: Box::new(SemanticType::from_arrow(entry_fields[0].data_type())),
                            value: Box::new(SemanticField::from_arrow_field(&entry_fields[1])),
                        };
                    }
                }
                SemanticType::Unsupported { reason: format!("map type not normalized: {dt:?}") }
            }
            DataType::FixedSizeBinary(len) => SemanticType::FixedSizeBinary { length: *len },
            other => {
                SemanticType::Unsupported { reason: format!("unsupported Arrow type: {other:?}") }
            }
        }
    }

    pub fn fingerprint_stable_json(&self) -> Result<String, ColumnarError> {
        serde_json::to_string(self)
            .map_err(|e| ColumnarError::Schema(format!("semantic type JSON: {e}")))
    }
}

impl SemanticField {
    fn from_arrow_field(field: &arrow::datatypes::Field) -> Self {
        Self {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            data_type: SemanticType::from_arrow(field.data_type()),
        }
    }
}
