use std::collections::BTreeMap;

use parqonaut_types::FieldDefinition;

use super::compatibility::{classify_conversion, PhysicalType};
use super::types::{FieldDescriptor, FieldDifference, FieldPath, SchemaDiff, SchemaDifferenceKind};
use crate::schema_policy::SchemaPolicy;

/// Per-file schema view used for dataset-level diff.
#[derive(Debug, Clone)]
pub struct FileSchemaView {
    pub path: String,
    pub fields: Vec<FieldDescriptor>,
}

impl FileSchemaView {
    pub fn from_snapshot(path: String, snapshot: &parqonaut_types::SchemaSnapshot) -> Self {
        let fields = snapshot.fields.iter().map(field_from_definition).collect();
        Self { path, fields }
    }
}

pub fn field_from_definition(f: &FieldDefinition) -> FieldDescriptor {
    FieldDescriptor {
        name: f.name.clone(),
        physical_type: f.logical_type.split_whitespace().next().unwrap_or("UNKNOWN").to_string(),
        nullable: f.nullable,
    }
}

/// Compute structured schema diff across multiple file schemas.
pub fn compute_schema_diff(views: &[FileSchemaView], policy: &SchemaPolicy) -> SchemaDiff {
    let mut field_files: BTreeMap<String, BTreeMap<String, FieldDescriptor>> = BTreeMap::new();

    for view in views {
        for field in &view.fields {
            field_files
                .entry(field.name.clone())
                .or_default()
                .insert(view.path.clone(), field.clone());
        }
    }

    let mut differences = Vec::new();
    let mut canonical = Vec::new();

    for (name, per_file) in &field_files {
        let variants: Vec<&FieldDescriptor> = per_file.values().collect();
        if variants.is_empty() {
            continue;
        }

        let canonical_field = pick_canonical_field(&variants, policy);
        canonical.push(canonical_field.clone());

        let mut distinct: BTreeMap<(String, bool), u64> = BTreeMap::new();
        for v in &variants {
            *distinct.entry((v.physical_type.clone(), v.nullable)).or_default() += 1;
        }

        if distinct.len() == 1 {
            let only = variants[0];
            let kind = if only.nullable != canonical_field.nullable {
                SchemaDifferenceKind::NullableDifference
            } else if only.physical_type != canonical_field.physical_type {
                SchemaDifferenceKind::CompatibleNumericWidening
            } else {
                SchemaDifferenceKind::Identical
            };
            if kind != SchemaDifferenceKind::Identical {
                differences.push(FieldDifference {
                    path: FieldPath::root(name),
                    left: Some(only.clone()),
                    right: Some(canonical_field.clone()),
                    difference: kind,
                    compatibility: classify_conversion(
                        &PhysicalType::parse(&only.physical_type),
                        &PhysicalType::parse(&canonical_field.physical_type),
                        only.nullable,
                        canonical_field.nullable,
                        policy,
                    ),
                    left_file_count: variants.len() as u64,
                    right_file_count: 0,
                });
            }
            continue;
        }

        let from_field = variants
            .iter()
            .find(|v| {
                v.physical_type != canonical_field.physical_type
                    || v.nullable != canonical_field.nullable
            })
            .map(|v| (*v).clone())
            .unwrap_or_else(|| canonical_field.clone());

        let left_count = variants
            .iter()
            .filter(|v| {
                v.physical_type == from_field.physical_type && v.nullable == from_field.nullable
            })
            .count() as u64;

        let compat = classify_conversion(
            &PhysicalType::parse(&from_field.physical_type),
            &PhysicalType::parse(&canonical_field.physical_type),
            from_field.nullable,
            canonical_field.nullable,
            policy,
        );

        let kind = if compat == crate::schema::compatibility::Compatibility::Unsupported {
            SchemaDifferenceKind::IncompatibleTypeChange
        } else if from_field.physical_type != canonical_field.physical_type {
            SchemaDifferenceKind::CompatibleNumericWidening
        } else if from_field.nullable != canonical_field.nullable {
            SchemaDifferenceKind::NullableDifference
        } else {
            SchemaDifferenceKind::Identical
        };

        if kind == SchemaDifferenceKind::Identical {
            continue;
        }

        differences.push(FieldDifference {
            path: FieldPath::root(name),
            left: Some(from_field),
            right: Some(canonical_field.clone()),
            difference: kind,
            compatibility: compat,
            left_file_count: left_count,
            right_file_count: variants.len() as u64 - left_count,
        });
    }

    canonical.sort_by(|a, b| a.name.cmp(&b.name));
    differences.sort_by_key(|a| a.path.display());

    SchemaDiff { fields: differences, canonical_schema: canonical }
}

fn pick_canonical_field(variants: &[&FieldDescriptor], policy: &SchemaPolicy) -> FieldDescriptor {
    if variants.is_empty() {
        return FieldDescriptor::default();
    }
    let mut best = variants[0].clone();
    for v in variants.iter().skip(1) {
        let compat = classify_conversion(
            &PhysicalType::parse(&best.physical_type),
            &PhysicalType::parse(&v.physical_type),
            best.nullable,
            v.nullable,
            policy,
        );
        let v_rank = PhysicalType::parse(&v.physical_type).widening_rank().unwrap_or(0);
        let best_rank = PhysicalType::parse(&best.physical_type).widening_rank().unwrap_or(0);
        if compat == crate::schema::compatibility::Compatibility::Lossless && v_rank > best_rank {
            best = (*v).clone();
        } else if v.nullable && !best.nullable && policy.allow_nullable_widening {
            best.nullable = true;
        }
    }
    best
}

/// Human-readable explanation for a schema conflict.
pub fn explain_schema_conflict(diff: &FieldDifference) -> String {
    format!(
        "Schema conflict: column \"{}\"\n\n  left: {:?} ({} files)\n  canonical: {:?}\n  difference: {:?}\n  compatibility: {:?}",
        diff.path.display(),
        diff.left,
        diff.left_file_count,
        diff.right,
        diff.difference,
        diff.compatibility
    )
}
