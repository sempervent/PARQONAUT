//! Deterministic schema comparison, compatibility, and canonical resolution.

pub mod canonical;
pub mod compatibility;
pub mod diff;
pub mod types;

pub use canonical::{
    resolve_canonical_schema, views_from_inventory, SchemaResolution, UnresolvableSchemaConflict,
};
pub use compatibility::{classify_conversion, Compatibility, PhysicalType};
pub use diff::{compute_schema_diff, explain_schema_conflict, FileSchemaView};
pub use types::{FieldDescriptor, FieldDifference, FieldPath, SchemaDiff, SchemaDifferenceKind};
