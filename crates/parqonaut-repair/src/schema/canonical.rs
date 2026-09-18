use serde::{Deserialize, Serialize};

use super::diff::{compute_schema_diff, field_from_definition, FileSchemaView};
use super::types::{FieldDescriptor, SchemaDiff};
use crate::schema_policy::SchemaPolicy;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnresolvableSchemaConflict {
    pub field: String,
    pub variants: Vec<FieldDescriptor>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResolution {
    pub diff: SchemaDiff,
    pub conflicts: Vec<UnresolvableSchemaConflict>,
    pub target_schema: Option<Vec<FieldDescriptor>>,
}

/// Resolve canonical target schema from file views or explicit override.
pub fn resolve_canonical_schema(
    views: &[FileSchemaView],
    explicit: Option<&[FieldDescriptor]>,
    policy: &SchemaPolicy,
) -> SchemaResolution {
    let diff = compute_schema_diff(views, policy);
    let mut conflicts = Vec::new();

    for field_diff in &diff.fields {
        if field_diff.compatibility == crate::schema::compatibility::Compatibility::Unsupported
            || field_diff.compatibility
                == crate::schema::compatibility::Compatibility::PotentiallyLossy
        {
            conflicts.push(UnresolvableSchemaConflict {
                field: field_diff.path.display(),
                variants: vec![
                    field_diff.left.clone().unwrap_or_default(),
                    field_diff.right.clone().unwrap_or_default(),
                ],
                reason: "No deterministic canonical type under current schema policy".into(),
            });
        }
    }

    let target_schema = explicit.map(|s| s.to_vec()).or_else(|| {
        if conflicts.is_empty() {
            Some(diff.canonical_schema.clone())
        } else {
            None
        }
    });

    SchemaResolution { diff, conflicts, target_schema }
}

/// Build file schema views from parquet inspections in inventory.
pub fn views_from_inventory(inventory: &crate::inventory::DatasetInventory) -> Vec<FileSchemaView> {
    inventory
        .parquet_files
        .iter()
        .map(|f| FileSchemaView {
            path: f.path.as_str().to_string(),
            fields: f.fields.iter().map(field_from_definition).collect(),
        })
        .collect()
}
