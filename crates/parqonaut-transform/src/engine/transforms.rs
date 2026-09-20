use crate::engine::cast::parse_cast_target;
use crate::engine::filter::{parse_filter, FilterTransform};
use crate::engine::pipeline::{Pipeline, Transform};
use crate::error::{ParqknifeError, Result};
use arrow::array::*;
use arrow::compute::{self, cast};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
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

pub struct RenameColumnsTransform {
    renames: HashMap<String, String>,
}

impl RenameColumnsTransform {
    pub fn new(renames: HashMap<String, String>) -> Self {
        Self { renames }
    }
}

impl Transform for RenameColumnsTransform {
    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut new_fields: Vec<Field> =
            schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        for (from, to) in &self.renames {
            if from == to {
                return Err(ParqknifeError::InvalidInput("rename from and to must differ".into()));
            }
            let idx = schema
                .index_of(from)
                .map_err(|_| ParqknifeError::InvalidInput(format!("column not found: {from}")))?;
            if schema.fields().iter().any(|f| f.name() == to) {
                return Err(ParqknifeError::InvalidInput(format!(
                    "target column already exists: {to}"
                )));
            }
            let field = schema.field(idx);
            new_fields[idx] = Field::new(to, field.data_type().clone(), field.is_nullable());
        }
        let out_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(out_schema, batch.columns().to_vec()).map_err(ParqknifeError::Arrow)
    }

    fn name(&self) -> &str {
        "rename"
    }
}

pub struct CastColumnsTransform {
    casts: HashMap<String, String>,
}

impl CastColumnsTransform {
    pub fn new(casts: HashMap<String, String>) -> Self {
        Self { casts }
    }
}

impl Transform for CastColumnsTransform {
    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut new_fields: Vec<Field> =
            schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        for (col, target) in &self.casts {
            let idx = schema
                .index_of(col)
                .map_err(|_| ParqknifeError::InvalidInput(format!("column not found: {col}")))?;
            let field = schema.field(idx);
            let (target_dt, target_nullable) =
                parse_cast_target(field.data_type(), field.is_nullable(), target)?;
            new_fields[idx] = Field::new(col, target_dt.clone(), target_nullable);
            let array = columns[idx].clone();
            columns[idx] = if array.data_type() == &target_dt {
                array
            } else {
                cast(&array, &target_dt)
                    .map_err(|e| ParqknifeError::InvalidInput(format!("cast failed: {e}")))?
            };
        }
        let out_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(out_schema, columns).map_err(ParqknifeError::Arrow)
    }

    fn name(&self) -> &str {
        "cast"
    }
}

/// Build an in-memory transform pipeline from fusible rewrite operations.
pub fn pipeline_from_rewrite_ops(
    projection: Option<Vec<String>>,
    filter: Option<&str>,
    rename: Option<HashMap<String, String>>,
    cast: Option<HashMap<String, String>>,
) -> Result<Pipeline> {
    let mut pipeline = Pipeline::new();
    if let Some(cols) = projection {
        pipeline.add_transform(ProjectionTransform::new(cols));
    }
    if let Some(expr) = filter {
        let parsed = parse_filter(expr)?;
        pipeline.add_transform(FilterTransform::new(parsed));
    }
    if let Some(map) = rename {
        pipeline.add_transform(RenameColumnsTransform::new(map));
    }
    if let Some(map) = cast {
        pipeline.add_transform(CastColumnsTransform::new(map));
    }
    Ok(pipeline)
}
