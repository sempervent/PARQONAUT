//! Split/partition/rewrite routing through dual-backend columnar I/O.

use std::path::Path;

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parqonaut_columnar::{BatchSink, BatchSource, BatchStream, ColumnarError};
use parqonaut_storage::columnar::{StorageParquetBatchSink, StorageParquetBatchSource};
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
use parqonaut_workflow::NoOpProgressObserver;

use crate::columnar_io::{ensure_s3_for_remote, needs_storage_routing, ColumnarPipelineIo};
use crate::engine::{
    encode_partition_value, merge_parquet_files, partition_parquet_file, partition_record_batches,
    split_parquet_file,
};
use crate::error::{ParqknifeError, Result};
use crate::remote::{merge_parquet_storage, rewrite_parquet_storage, run_io_runtime};

pub fn split_parquet_routed(
    io: &ColumnarPipelineIo,
    input: &str,
    output: &str,
    target_size_mb: u64,
) -> Result<Vec<String>> {
    ensure_s3_for_remote(input, output)?;
    if !needs_storage_routing(input, output) {
        let outs = split_parquet_file(
            input,
            Path::new(output),
            "part",
            target_size_mb * 1024 * 1024,
            None,
            None,
            false,
        )?;
        return Ok(outs.into_iter().map(|p| p.to_string_lossy().into_owned()).collect());
    }
    split_parquet_storage(io, input, output, target_size_mb * 1024 * 1024)
}

pub fn partition_parquet_routed(
    io: &ColumnarPipelineIo,
    input: &str,
    output: &str,
    partition_by: &[String],
    max_open: usize,
) -> Result<Vec<String>> {
    ensure_s3_for_remote(input, output)?;
    if !needs_storage_routing(input, output) {
        let outs =
            partition_parquet_file(input, Path::new(output), partition_by, max_open, None, None)?;
        return Ok(outs.into_iter().map(|p| p.to_string_lossy().into_owned()).collect());
    }
    partition_parquet_storage(io, input, output, partition_by, max_open)
}

pub fn merge_parquet_routed(
    io: &ColumnarPipelineIo,
    inputs: &[String],
    output: &str,
    _row_group_size_mb: Option<u64>,
) -> Result<()> {
    ensure_s3_for_remote(inputs.first().map(String::as_str).unwrap_or(""), output)?;
    if needs_storage_routing(inputs.first().map(String::as_str).unwrap_or(""), output) {
        merge_parquet_storage(io, inputs, output)?;
        return Ok(());
    }
    let out = Path::new(output);
    let (dir, base) = if out.extension().is_some() {
        (
            out.parent().unwrap_or_else(|| Path::new(".")),
            out.file_stem().and_then(|s| s.to_str()).unwrap_or("merged"),
        )
    } else {
        (out, "merged")
    };
    merge_parquet_files(
        inputs,
        dir,
        base,
        512 * 1024 * 1024,
        None,
        _row_group_size_mb,
        false,
        out.extension().is_some().then_some(out),
    )?;
    Ok(())
}

pub fn rewrite_parquet_routed(io: &ColumnarPipelineIo, input: &str, output: &str) -> Result<()> {
    ensure_s3_for_remote(input, output)?;
    if needs_storage_routing(input, output) {
        rewrite_parquet_storage(io, input, output)?;
        return Ok(());
    }
    crate::rewrite_parquet_file(input, output, None, None, false)?;
    Ok(())
}

fn split_parquet_storage(
    io: &ColumnarPipelineIo,
    input: &str,
    output_prefix: &str,
    target_bytes: u64,
) -> Result<Vec<String>> {
    let input_loc = ObjectLocation::parse(input).map_err(map_storage)?;
    let io = io.clone();
    let output_prefix = output_prefix.to_string();
    run_io_runtime(move |handle| {
        handle.block_on(async move {
            split_parquet_storage_async(&io, input_loc, &output_prefix, target_bytes).await
        })
    })
}

fn hive_relative_path(columns: &[String], key: &str, part_index: usize) -> String {
    let values: Vec<&str> = key.split('\x1f').collect();
    let mut parts = Vec::new();
    for (col, val) in columns.iter().zip(values.iter()) {
        parts.push(format!("{col}={val}"));
    }
    parts.push(format!("part-{part_index:05}.parquet"));
    parts.join("/")
}

async fn split_parquet_storage_async(
    io: &ColumnarPipelineIo,
    input_loc: ObjectLocation,
    output_prefix: &str,
    target_bytes: u64,
) -> Result<Vec<String>> {
    let source_backend = io.backend_for(&input_loc);
    let source = StorageParquetBatchSource::new(source_backend, input_loc);
    let schema = source.schema().map_err(map_columnar)?;
    let mut stream = Box::new(source).into_stream().map_err(map_columnar)?;

    let mut outputs = Vec::new();
    let mut part_idx = 0usize;
    let mut buffer: Vec<RecordBatch> = Vec::new();
    let mut buffer_bytes: u64 = 0;

    while let Some(item) = stream.next().await {
        let batch = item.map_err(map_columnar)?;
        let est = batch.get_array_memory_size() as u64;
        if !buffer.is_empty() && buffer_bytes + est > target_bytes {
            let uri = flush_part(io, output_prefix, part_idx, schema.clone(), &buffer).await?;
            outputs.push(uri);
            part_idx += 1;
            buffer.clear();
            buffer_bytes = 0;
        }
        buffer_bytes += est;
        buffer.push(batch);
    }
    if !buffer.is_empty() {
        let uri = flush_part(io, output_prefix, part_idx, schema, &buffer).await?;
        outputs.push(uri);
    }
    Ok(outputs)
}

async fn flush_part(
    io: &ColumnarPipelineIo,
    output_prefix: &str,
    part_idx: usize,
    schema: arrow::datatypes::SchemaRef,
    batches: &[RecordBatch],
) -> Result<String> {
    let rel = format!("part-{part_idx:05}.parquet");
    let out_loc = object_under_prefix(output_prefix, &rel)?;
    let sink_backend = io.backend_for(&out_loc);
    let owned: Vec<RecordBatch> = batches.to_vec();
    let stream: BatchStream = Box::pin(futures::stream::iter(owned.into_iter().map(Ok)));
    let mut sink = StorageParquetBatchSink::new(sink_backend, out_loc.clone());
    sink.write_stream(schema, stream, &NoOpProgressObserver).map_err(map_columnar)?;
    Ok(out_loc.display_uri())
}

fn partition_parquet_storage(
    io: &ColumnarPipelineIo,
    input: &str,
    output_prefix: &str,
    partition_by: &[String],
    max_open: usize,
) -> Result<Vec<String>> {
    let input_loc = ObjectLocation::parse(input).map_err(map_storage)?;
    let io = io.clone();
    let output_prefix = output_prefix.to_string();
    let partition_by = partition_by.to_vec();
    run_io_runtime(move |handle| {
        handle.block_on(async move {
            partition_parquet_storage_async(&io, input_loc, &output_prefix, &partition_by, max_open)
                .await
        })
    })
}

async fn partition_parquet_storage_async(
    io: &ColumnarPipelineIo,
    input_loc: ObjectLocation,
    output_prefix: &str,
    partition_by: &[String],
    max_open: usize,
) -> Result<Vec<String>> {
    use std::collections::{HashMap, VecDeque};

    use crate::engine::partition::partition_keys;

    let source_backend = io.backend_for(&input_loc);
    let source = StorageParquetBatchSource::new(source_backend, input_loc);
    let schema = source.schema().map_err(map_columnar)?;
    let mut stream = Box::new(source).into_stream().map_err(map_columnar)?;

    let mut buffers: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    let mut part_counters: HashMap<String, usize> = HashMap::new();
    let mut lru: VecDeque<String> = VecDeque::new();
    let mut outputs = Vec::new();

    while let Some(item) = stream.next().await {
        let batch = item.map_err(map_columnar)?;
        for (key, sub_batch) in partition_keys(&batch, partition_by)? {
            if !buffers.contains_key(&key) {
                if buffers.len() >= max_open {
                    if let Some(evict) = lru.pop_front() {
                        if let Some(batches) = buffers.remove(&evict) {
                            let idx = part_counters.get(&evict).copied().unwrap_or(0);
                            let rel = hive_relative_path(partition_by, &evict, idx);
                            part_counters.insert(evict.clone(), idx + 1);
                            let obj = object_under_prefix(output_prefix, &rel)?;
                            flush_to_object(io, &obj, schema.clone(), &batches).await?;
                            outputs.push(obj.display_uri());
                        }
                    }
                }
                part_counters.entry(key.clone()).or_insert(0);
                buffers.insert(key.clone(), Vec::new());
                lru.push_back(key.clone());
            } else if let Some(pos) = lru.iter().position(|k| k == &key) {
                lru.remove(pos);
                lru.push_back(key.clone());
            }
            buffers.get_mut(&key).unwrap().push(sub_batch);
        }
    }

    for (key, batches) in buffers.drain() {
        let idx = part_counters.get(&key).copied().unwrap_or(0);
        let rel = hive_relative_path(partition_by, &key, idx);
        let obj = object_under_prefix(output_prefix, &rel)?;
        flush_to_object(io, &obj, schema.clone(), &batches).await?;
        outputs.push(obj.display_uri());
    }
    Ok(outputs)
}

async fn flush_to_object(
    io: &ColumnarPipelineIo,
    dest: &ObjectLocation,
    schema: arrow::datatypes::SchemaRef,
    batches: &[RecordBatch],
) -> Result<()> {
    let owned: Vec<RecordBatch> = batches.to_vec();
    let stream: BatchStream = Box::pin(futures::stream::iter(owned.into_iter().map(Ok)));
    let sink_backend = io.backend_for(dest);
    let mut sink = StorageParquetBatchSink::new(sink_backend, dest.clone());
    sink.write_stream(schema, stream, &NoOpProgressObserver).map_err(map_columnar)?;
    Ok(())
}

fn object_under_prefix(prefix: &str, relative: &str) -> Result<ObjectLocation> {
    let dataset = DatasetLocation::parse(prefix).map_err(map_storage)?;
    match dataset {
        DatasetLocation::Local(l) => {
            let mut path = l.path.to_path_buf();
            for comp in relative.split('/') {
                if comp.is_empty() || comp == "." {
                    continue;
                }
                if comp == ".." {
                    return Err(ParqknifeError::InvalidInput(
                        "partition path must not contain '..'".into(),
                    ));
                }
                path.push(comp);
            }
            Ok(ObjectLocation::Local { path })
        }
        DatasetLocation::S3(s) => {
            let key = s.object_key(relative).map_err(map_storage)?;
            Ok(ObjectLocation::S3 { bucket: s.bucket, key })
        }
    }
}

fn map_storage(e: parqonaut_storage::error::StorageError) -> ParqknifeError {
    ParqknifeError::InvalidInput(e.to_string())
}

fn map_columnar(e: ColumnarError) -> ParqknifeError {
    ParqknifeError::InvalidInput(e.to_string())
}
