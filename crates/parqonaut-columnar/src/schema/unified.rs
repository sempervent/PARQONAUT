use std::collections::HashMap;

use arrow::datatypes::{Field, Schema};
use parqonaut_workflow::SchemaConflictPolicy;

use super::kind::TypeKind;
use super::unify::unify_types;
use crate::error::ColumnarError;

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

    pub fn from_schemas(
        schemas: &[Schema],
        policy: SchemaConflictPolicy,
    ) -> Result<Self, ColumnarError> {
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
