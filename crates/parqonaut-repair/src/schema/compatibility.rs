use crate::schema_policy::SchemaPolicy;

pub use parqonaut_columnar::schema::{classify_conversion as classify_conversion_flags, Compatibility, PhysicalType};

/// Classify conversion from `from` to `to` under repair schema policy.
pub fn classify_conversion(
    from: &PhysicalType,
    to: &PhysicalType,
    from_nullable: bool,
    to_nullable: bool,
    policy: &SchemaPolicy,
) -> Compatibility {
    classify_conversion_flags(
        from,
        to,
        from_nullable,
        to_nullable,
        policy.allow_numeric_widening,
        policy.allow_nullable_widening,
    )
}
