use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::info;

use crate::error::{ParqknifeError, Result};
use crate::output::{compression_from_str, ParquetWriter};

/// Merge sorted Parquet inputs into one or more output files capped at `target_bytes`.
/// Returns paths of written output files (relative to `output_dir` when possible).
pub fn merge_parquet_files(
    inputs: &[String],
    output_dir: &Path,
    output_basename: &str,
    target_bytes: u64,
    compression: Option<&str>,
    row_group_size_mb: Option<u64>,
    rebuild_stats: bool,
) -> Result<Vec<PathBuf>> {
    if inputs.is_empty() {
        return Err(ParqknifeError::InvalidInput("no inputs for merge".into()));
    }
    std::fs::create_dir_all(output_dir)?;

    let comp = compression.map(compression_from_str).transpose()?;
    let mut outputs = Vec::new();
    let mut part_idx = 0usize;
    let mut current_bytes: u64 = 0;
    let mut writer: Option<ParquetWriter> = None;
    let mut schema = None;

    let mut sorted_inputs = inputs.to_vec();
    sorted_inputs.sort();

    for input in sorted_inputs {
        let file = File::open(&input)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let file_schema = builder.schema().clone();
        if schema.is_none() {
            schema = Some(file_schema.clone());
        } else if schema.as_ref() != Some(&file_schema) {
            return Err(ParqknifeError::InvalidInput(format!(
                "schema mismatch during merge at {input}"
            )));
        }

        let reader = builder.build()?;
        for batch_result in reader {
            let batch: RecordBatch = batch_result?;
            let batch_bytes = estimate_batch_bytes(&batch);
            if writer.is_none() || (current_bytes + batch_bytes > target_bytes && current_bytes > 0)
            {
                if let Some(w) = writer.take() {
                    w.close()?;
                }
                let out_path =
                    output_dir.join(format!("{output_basename}-part-{part_idx:04}.parquet"));
                part_idx += 1;
                current_bytes = 0;
                let out_file = File::create(&out_path)?;
                writer = Some(ParquetWriter::new(
                    BufWriter::new(out_file),
                    file_schema.clone(),
                    comp,
                    row_group_size_mb,
                    rebuild_stats,
                )?);
                outputs.push(out_path);
            }
            if let Some(ref mut w) = writer {
                w.write_batch(batch)?;
                current_bytes += batch_bytes;
            }
        }
    }

    if let Some(w) = writer.take() {
        w.close()?;
    }

    info!(parts = outputs.len(), "merge complete");
    Ok(outputs)
}

fn estimate_batch_bytes(batch: &RecordBatch) -> u64 {
    batch.get_array_memory_size() as u64
}

/// Rewrite a single Parquet file with optional compression and row-group sizing.
pub fn rewrite_parquet_file(
    input: &str,
    output: &str,
    compression: Option<&str>,
    row_group_size_mb: Option<u64>,
    rebuild_stats: bool,
) -> Result<()> {
    let file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let comp = compression.map(compression_from_str).transpose()?;

    if let Some(parent) = Path::new(output).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let out_file = File::create(output)?;
    let mut writer = ParquetWriter::new(
        BufWriter::new(out_file),
        schema.clone(),
        comp,
        row_group_size_mb,
        rebuild_stats,
    )?;

    for batch_result in builder.build()? {
        writer.write_batch(batch_result?)?;
    }
    writer.close()?;
    Ok(())
}
