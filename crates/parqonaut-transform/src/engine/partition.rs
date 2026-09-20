use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, StringArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::info;

use crate::error::{ParqknifeError, Result};
use crate::output::{compression_from_str, ParquetWriter};

/// Hive-style NULL partition token (documented; escaped literals use `__PQ_ESC__` prefix).
pub const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

const ESCAPE_PREFIX: &str = "__PQ_ESC__";

/// Encode a partition column value for filesystem paths (reversible for common cases).
pub fn encode_partition_value(raw: Option<&str>) -> String {
    let Some(s) = raw else {
        return HIVE_DEFAULT_PARTITION.to_string();
    };
    if s == HIVE_DEFAULT_PARTITION {
        return format!("{ESCAPE_PREFIX}{HIVE_DEFAULT_PARTITION}");
    }
    if s.contains('/') || s.contains('\\') || s.contains('\0') {
        return format!(
            "{ESCAPE_PREFIX}{}",
            s.replace('\\', "\\\\").replace('/', "\\/").replace('\0', "\\0")
        );
    }
    s.to_string()
}

struct OpenPartition {
    writer: ParquetWriter,
    path: PathBuf,
    part_index: usize,
}

pub fn partition_parquet_file(
    input: &str,
    output_dir: &Path,
    partition_by: &[String],
    max_open_partitions: usize,
    compression: Option<&str>,
    row_group_size_mb: Option<u64>,
) -> Result<Vec<PathBuf>> {
    if partition_by.is_empty() {
        return Err(ParqknifeError::InvalidInput("partition_by required".into()));
    }
    if max_open_partitions == 0 {
        return Err(ParqknifeError::InvalidInput("max_open_partitions must be > 0".into()));
    }

    std::fs::create_dir_all(output_dir)?;
    let comp = compression.map(compression_from_str).transpose()?;

    let file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    for col in partition_by {
        schema.index_of(col).map_err(|_| {
            ParqknifeError::InvalidInput(format!("partition column not in schema: {col}"))
        })?;
    }

    let mut writers: HashMap<String, OpenPartition> = HashMap::new();
    let mut lru: VecDeque<String> = VecDeque::new();
    let mut outputs = Vec::new();
    let mut part_counters: HashMap<String, usize> = HashMap::new();

    for batch_result in builder.build()? {
        let batch: RecordBatch = batch_result?;
        let keys = partition_keys(&batch, partition_by)?;
        for (key, sub_batch) in keys {
            if !writers.contains_key(&key) {
                if writers.len() >= max_open_partitions {
                    if let Some(evict) = lru.pop_front() {
                        if let Some(open) = writers.remove(&evict) {
                            open.writer.close()?;
                        }
                    }
                }
                let dir = partition_dir(output_dir, partition_by, &key);
                std::fs::create_dir_all(&dir)?;
                let idx = part_counters.entry(key.clone()).or_insert(0);
                let file_name = format!("part-{idx:05}.parquet");
                *idx += 1;
                let out_path = dir.join(&file_name);
                let out_file = File::create(&out_path)?;
                let w = ParquetWriter::new(
                    BufWriter::new(out_file),
                    schema.clone(),
                    comp,
                    row_group_size_mb,
                    false,
                )?;
                writers.insert(
                    key.clone(),
                    OpenPartition { writer: w, path: out_path.clone(), part_index: *idx - 1 },
                );
                outputs.push(out_path);
                lru.push_back(key.clone());
            } else if let Some(pos) = lru.iter().position(|k| k == &key) {
                lru.remove(pos);
                lru.push_back(key.clone());
            }
            writers.get_mut(&key).unwrap().writer.write_batch(sub_batch)?;
        }
    }

    for (_, open) in writers.drain() {
        open.writer.close()?;
    }

    info!(files = outputs.len(), "partition complete");
    Ok(outputs)
}

fn partition_dir(base: &Path, columns: &[String], key: &str) -> PathBuf {
    let values: Vec<&str> = key.split('\x1f').collect();
    let mut path = base.to_path_buf();
    for (col, val) in columns.iter().zip(values.iter()) {
        path.push(format!("{col}={val}"));
    }
    path
}

fn partition_keys(batch: &RecordBatch, cols: &[String]) -> Result<Vec<(String, RecordBatch)>> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(Vec::new());
    }

    let mut arrays = Vec::new();
    for c in cols {
        let idx = batch
            .schema()
            .index_of(c)
            .map_err(|_| ParqknifeError::InvalidInput(format!("missing partition column {c}")))?;
        arrays.push(batch.column(idx));
    }

    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for row in 0..n {
        let mut parts = Vec::with_capacity(cols.len());
        for arr in &arrays {
            let encoded = if arr.is_null(row) {
                encode_partition_value(None)
            } else {
                let s = array_value_at(arr.as_ref(), row)?;
                encode_partition_value(Some(&s))
            };
            parts.push(encoded);
        }
        let key = parts.join("\x1f");
        groups.entry(key).or_default().push(row);
    }

    let mut out = Vec::new();
    for (key, rows) in groups {
        let mut mask = vec![false; n];
        for &i in &rows {
            mask[i] = true;
        }
        let mask_arr = BooleanArray::from(mask);
        let filtered = filter_record_batch(batch, &mask_arr)?;
        out.push((key, filtered));
    }
    Ok(out)
}

fn array_value_at(arr: &dyn Array, row: usize) -> Result<String> {
    if arr.is_null(row) {
        return Ok(String::new());
    }
    match arr.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(a.value(row).to_string())
        }
        other => Err(ParqknifeError::InvalidInput(format!(
            "unsupported partition column type: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_and_escape_partition_values() {
        assert_eq!(encode_partition_value(None), HIVE_DEFAULT_PARTITION);
        assert_eq!(
            encode_partition_value(Some(HIVE_DEFAULT_PARTITION)),
            format!("{ESCAPE_PREFIX}{HIVE_DEFAULT_PARTITION}")
        );
        assert!(encode_partition_value(Some("a/b")).starts_with(ESCAPE_PREFIX));
    }
}
