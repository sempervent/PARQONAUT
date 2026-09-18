use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{ParqknifeError, Result};
use crate::output::{compression_from_str, ParquetWriter};

/// Rewrite Parquet, casting `column` to `target_type` label (INT32/INT64/...).
pub fn rewrite_parquet_with_cast(
    input: &str,
    output: &str,
    column: &str,
    target_type: &str,
    compression: Option<&str>,
    row_group_size_mb: Option<u64>,
    rebuild_stats: bool,
) -> Result<()> {
    let file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let field_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == column)
        .ok_or_else(|| ParqknifeError::InvalidInput(format!("column not found: {column}")))?;

    let current = schema.field(field_idx);
    let (target_dt, target_nullable) =
        parse_target(current.data_type(), current.is_nullable(), target_type)?;

    let mut new_fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    new_fields[field_idx] = Arc::new(Field::new(column, target_dt.clone(), target_nullable));
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
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        let array = columns[field_idx].clone();
        let casted = if array.data_type() == &target_dt {
            array
        } else {
            cast(&array, &target_dt)
                .map_err(|e| ParqknifeError::InvalidInput(format!("cast failed: {e}")))?
        };
        columns[field_idx] = casted;
        let out_batch = RecordBatch::try_new(out_schema.clone(), columns)
            .map_err(|e| ParqknifeError::InvalidInput(e.to_string()))?;
        writer.write_batch(out_batch)?;
    }
    writer.close()?;
    Ok(())
}

fn parse_target(
    current: &DataType,
    current_nullable: bool,
    label: &str,
) -> Result<(DataType, bool)> {
    let lower = label.to_lowercase();
    if let Some(v) = lower.strip_prefix("nullable=") {
        let nullable = v == "true";
        return Ok((current.clone(), nullable));
    }
    let upper = label.to_uppercase();
    let dt = if upper.contains("INT64") {
        DataType::Int64
    } else if upper.contains("INT32") {
        DataType::Int32
    } else if upper.contains("UTF8") || upper.contains("STRING") {
        DataType::Utf8
    } else if upper.contains("FLOAT") || upper.contains("DOUBLE") {
        DataType::Float64
    } else {
        return Err(ParqknifeError::InvalidInput(format!("unsupported cast target: {label}")));
    };
    Ok((dt, current_nullable))
}

/// Convenience cast helpers used in tests.
pub fn cast_i32_to_i64(values: &[i32]) -> Int64Array {
    Int64Array::from_iter_values(values.iter().map(|v| i64::from(*v)))
}

pub fn as_i32(values: &[i32]) -> Int32Array {
    Int32Array::from_iter_values(values.iter().copied())
}
