use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{Result, TransformError};
use crate::output::{compression_from_str, ParquetWriter};

/// Rewrite Parquet, renaming `from` column to `to` without changing values.
pub fn rewrite_parquet_with_rename(
    input: &str,
    output: &str,
    from: &str,
    to: &str,
    compression: Option<&str>,
    row_group_size_mb: Option<u64>,
    rebuild_stats: bool,
) -> Result<()> {
    if from == to {
        return Err(TransformError::InvalidInput("rename from and to must differ".into()));
    }
    let file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let from_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == from)
        .ok_or_else(|| TransformError::InvalidInput(format!("column not found: {from}")))?;
    if schema.fields().iter().any(|f| f.name() == to) {
        return Err(TransformError::InvalidInput(format!("target column already exists: {to}")));
    }

    let mut new_fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let field = schema.field(from_idx);
    new_fields[from_idx] = Arc::new(Field::new(to, field.data_type().clone(), field.is_nullable()));
    let out_schema = Arc::new(Schema::new(new_fields));

    if let Some(parent) = Path::new(output).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let comp = compression.map(compression_from_str).transpose()?;
    let out_file = File::create(output)?;
    let mut writer = ParquetWriter::new(
        BufWriter::new(out_file),
        out_schema.clone(),
        comp,
        row_group_size_mb,
        rebuild_stats,
    )?;

    for batch_result in builder.build()? {
        let batch = batch_result?;
        let out_batch = RecordBatch::try_new(out_schema.clone(), batch.columns().to_vec())
            .map_err(|e| TransformError::InvalidInput(format!("rename batch failed: {e}")))?;
        writer.write_batch(out_batch)?;
    }
    writer.close()?;
    Ok(())
}
