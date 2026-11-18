use crate::error::{Result, TransformError};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SchemaTransform {
    rename_map: HashMap<String, String>,
    cast_map: HashMap<String, DataType>,
}

impl SchemaTransform {
    pub fn new(
        rename_map: Option<HashMap<String, String>>,
        cast_map: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        let cast = if let Some(cast_str) = cast_map {
            let mut typed = HashMap::new();
            for (col, type_str) in cast_str {
                let dt = parse_type(&type_str)?;
                typed.insert(col, dt);
            }
            Some(typed)
        } else {
            None
        };

        Ok(Self { rename_map: rename_map.unwrap_or_default(), cast_map: cast.unwrap_or_default() })
    }
}

fn parse_type(type_str: &str) -> Result<DataType> {
    match type_str.to_lowercase().as_str() {
        "int8" | "i8" => Ok(DataType::Int8),
        "int16" | "i16" => Ok(DataType::Int16),
        "int32" | "i32" => Ok(DataType::Int32),
        "int64" | "i64" => Ok(DataType::Int64),
        "uint8" | "u8" => Ok(DataType::UInt8),
        "uint16" | "u16" => Ok(DataType::UInt16),
        "uint32" | "u32" => Ok(DataType::UInt32),
        "uint64" | "u64" => Ok(DataType::UInt64),
        "float32" | "f32" => Ok(DataType::Float32),
        "float64" | "f64" => Ok(DataType::Float64),
        "string" | "utf8" => Ok(DataType::Utf8),
        "bool" | "boolean" => Ok(DataType::Boolean),
        _ => Err(TransformError::InvalidInput(format!("Unsupported type: {}", type_str))),
    }
}

impl crate::engine::pipeline::Transform for SchemaTransform {
    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut new_fields = Vec::new();
        let mut new_columns = Vec::new();

        for (idx, field) in schema.fields().iter().enumerate() {
            let new_name =
                self.rename_map.get(field.name()).cloned().unwrap_or_else(|| field.name().clone());

            let new_type = self
                .cast_map
                .get(field.name())
                .cloned()
                .unwrap_or_else(|| field.data_type().clone());

            let new_field = Field::new(&new_name, new_type.clone(), field.is_nullable());
            new_fields.push(new_field);

            let column = batch.column(idx);
            // Cast if needed
            let new_column = if new_type != *field.data_type() {
                // Simplified - would need proper casting logic
                column.clone()
            } else {
                column.clone()
            };
            new_columns.push(new_column);
        }

        let new_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(new_schema, new_columns).map_err(TransformError::Arrow)
    }

    fn name(&self) -> &str {
        "schema"
    }
}
