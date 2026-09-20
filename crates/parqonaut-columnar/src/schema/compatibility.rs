use serde::{Deserialize, Serialize};

/// Loss classification for a type conversion (repair / transform safety).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Compatibility {
    Lossless,
    ConditionallyLossless,
    PotentiallyLossy,
    Unsupported,
}

/// Normalized physical type labels used by the compatibility lattice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalType {
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
    Boolean,
    Utf8,
    Binary,
    Timestamp,
    Date32,
    Decimal,
    Unknown(String),
}

impl PhysicalType {
    pub fn parse(raw: &str) -> Self {
        let upper = raw.to_uppercase();
        if upper.contains("INT8") || upper.starts_with("INT(8)") {
            Self::Int8
        } else if upper.contains("INT16") {
            Self::Int16
        } else if upper.contains("INT32") {
            Self::Int32
        } else if upper.contains("INT64") {
            Self::Int64
        } else if upper.contains("UINT8") {
            Self::UInt8
        } else if upper.contains("UINT16") {
            Self::UInt16
        } else if upper.contains("UINT32") {
            Self::UInt32
        } else if upper.contains("UINT64") {
            Self::UInt64
        } else if upper.contains("FLOAT") && !upper.contains("DOUBLE") {
            Self::Float32
        } else if upper.contains("DOUBLE") {
            Self::Float64
        } else if upper.contains("BOOL") {
            Self::Boolean
        } else if upper.contains("STRING")
            || upper.contains("UTF8")
            || (upper.contains("BYTE_ARRAY") && upper.contains("STRING"))
        {
            Self::Utf8
        } else if upper.contains("BYTE_ARRAY") {
            Self::Binary
        } else if upper.contains("TIMESTAMP") {
            Self::Timestamp
        } else if upper.contains("DATE32") || upper.contains("DATE") {
            Self::Date32
        } else if upper.contains("DECIMAL") {
            Self::Decimal
        } else {
            Self::Unknown(raw.to_string())
        }
    }

    pub fn widening_rank(&self) -> Option<u8> {
        match self {
            Self::Int8 => Some(0),
            Self::Int16 => Some(1),
            Self::Int32 => Some(2),
            Self::Int64 => Some(3),
            Self::UInt8 => Some(10),
            Self::UInt16 => Some(11),
            Self::UInt32 => Some(12),
            Self::UInt64 => Some(13),
            Self::Float32 => Some(20),
            Self::Float64 => Some(21),
            _ => None,
        }
    }
}

/// Classify conversion from `from` to `to` under widening/nullable policy flags.
pub fn classify_conversion(
    from: &PhysicalType,
    to: &PhysicalType,
    from_nullable: bool,
    to_nullable: bool,
    allow_numeric_widening: bool,
    allow_nullable_widening: bool,
) -> Compatibility {
    if from == to && from_nullable == to_nullable {
        return Compatibility::Lossless;
    }

    if from == to && !from_nullable && to_nullable && allow_nullable_widening {
        return Compatibility::Lossless;
    }

    if from != to {
        if !allow_numeric_widening {
            return Compatibility::Unsupported;
        }
        match (from, to) {
            (PhysicalType::Int32, PhysicalType::Int64)
            | (PhysicalType::Int16, PhysicalType::Int32)
            | (PhysicalType::Int16, PhysicalType::Int64)
            | (PhysicalType::Int8, PhysicalType::Int16)
            | (PhysicalType::Int8, PhysicalType::Int32)
            | (PhysicalType::Int8, PhysicalType::Int64)
            | (PhysicalType::UInt32, PhysicalType::UInt64)
            | (PhysicalType::UInt16, PhysicalType::UInt32)
            | (PhysicalType::UInt16, PhysicalType::UInt64)
            | (PhysicalType::UInt8, PhysicalType::UInt16)
            | (PhysicalType::UInt8, PhysicalType::UInt32)
            | (PhysicalType::UInt8, PhysicalType::UInt64)
            | (PhysicalType::Float32, PhysicalType::Float64) => {
                if from_nullable == to_nullable
                    || (!from_nullable && to_nullable && allow_nullable_widening)
                {
                    return Compatibility::Lossless;
                }
                return Compatibility::ConditionallyLossless;
            }
            (PhysicalType::Int32, PhysicalType::Float64)
            | (PhysicalType::Int64, PhysicalType::Float64) => {
                return Compatibility::PotentiallyLossy;
            }
            (PhysicalType::Utf8, PhysicalType::Int64)
            | (PhysicalType::Int64, PhysicalType::Utf8)
            | (PhysicalType::Utf8, PhysicalType::Int32)
            | (PhysicalType::Int32, PhysicalType::Utf8) => return Compatibility::Unsupported,
            _ => return Compatibility::Unsupported,
        }
    }

    if from_nullable && !to_nullable {
        return Compatibility::PotentiallyLossy;
    }

    Compatibility::Unsupported
}
