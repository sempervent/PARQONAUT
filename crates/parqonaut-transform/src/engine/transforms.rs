use crate::engine::pipeline::Transform;
use crate::error::{ParqknifeError, Result};
use arrow::array::*;
use arrow::compute;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub struct ProjectionTransform {
    columns: Vec<String>,
}

impl ProjectionTransform {
    pub fn new(columns: Vec<String>) -> Self {
        Self { columns }
    }
}

impl Transform for ProjectionTransform {
    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut indices = Vec::new();
        let mut new_fields = Vec::new();

        for col_name in &self.columns {
            let idx = schema.index_of(col_name).map_err(|_| {
                ParqknifeError::InvalidInput(format!("Column not found: {}", col_name))
            })?;
            indices.push(idx);
            new_fields.push(schema.field(idx).clone());
        }

        let new_schema = Arc::new(Schema::new(new_fields));
        let columns: Result<Vec<_>> =
            indices.iter().map(|&idx| Ok(batch.column(idx).clone())).collect();

        RecordBatch::try_new(new_schema, columns?).map_err(ParqknifeError::Arrow)
    }

    fn name(&self) -> &str {
        "projection"
    }
}

#[derive(Default)]
pub struct CompressionTransform {
    // This is a placeholder - compression is handled at write time
}

impl CompressionTransform {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Transform for CompressionTransform {
    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // Compression is applied during Parquet writing, not as a transform
        Ok(batch)
    }

    fn name(&self) -> &str {
        "compression"
    }
}
