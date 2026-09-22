//! Split/partition/rewrite routing through dual-backend columnar I/O.

use std::path::Path;

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parqonaut_columnar::{BatchSink, BatchSource, BatchStream, ColumnarError};
use parqonaut_storage::columnar::{write_parquet_batch_stream, StorageParquetBatchSource};
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
use parqonaut_workflow::NoOpProgressObserver;

use crate::columnar_io::{ensure_s3_for_remote, needs_storage_routing, ColumnarPipelineIo};
use crate::engine::{
    encode_partition_value, merge_parquet_files, partition_parquet_file, partition_record_batches,
    pipeline_from_rewrite_ops, split_parquet_file, Pipeline,
};
use crate::error::{ParqknifeError, Result};
use crate::remote::{merge_parquet_storage, rewrite_parquet_storage, run_io_runtime};
use crate::spec::fused_chain::FusedTransformChain;
use crate::spec::TransformRunContext;
use crate::{FusedOperation, FusedPlanSegment};

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
    write_parquet_batch_stream(
        sink_backend,
        out_loc.clone(),
        schema,
        stream,
        &NoOpProgressObserver,
    )
    .await
    .map_err(map_columnar)?;
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
    write_parquet_batch_stream(sink_backend, dest.clone(), schema, stream, &NoOpProgressObserver)
        .await
        .map_err(map_columnar)?;
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

/// Fused rewrite/partition segment with independent source/sink backends.
pub fn execute_fused_segment_routed(
    io: &ColumnarPipelineIo,
    fused: &FusedPlanSegment,
    run: &TransformRunContext,
) -> Result<(u64, u64)> {
    let primary_in = fused.inputs.first().map(String::as_str).unwrap_or("");
    ensure_s3_for_remote(primary_in, &fused.output)?;
    let io = io.clone();
    let fused = fused.clone();
    let run = run.clone();
    run_io_runtime(move |handle| {
        handle.block_on(async move { execute_fused_segment_routed_async(&io, &fused, &run).await })
    })
}

async fn execute_fused_segment_routed_async(
    io: &ColumnarPipelineIo,
    fused: &FusedPlanSegment,
    run: &TransformRunContext,
) -> Result<(u64, u64)> {
    let has_plugin = fused.ops.iter().any(|op| matches!(op, FusedOperation::Plugin(_)));
    let chain = if has_plugin {
        Some(std::sync::Arc::new(std::sync::Mutex::new(FusedTransformChain::from_segment(
            fused,
            &fused.execution_id,
            run.clone(),
        )?)))
    } else {
        None
    };
    let pipeline = if has_plugin { None } else { fused_pipeline_from_ops(fused)? };
    let mut partition_by: Option<Vec<String>> = None;
    for op in &fused.ops {
        if let FusedOperation::Partition { partition_by: cols } = op {
            partition_by = Some(cols.clone());
        }
    }

    let files_read = fused.inputs.len() as u64;
    if let Some(cols) = partition_by {
        let outs = fused_partition_to_storage(
            io,
            &fused.inputs,
            &fused.output,
            &cols,
            64,
            pipeline,
            chain.clone(),
            run,
        )
        .await?;
        return Ok((outs.len() as u64, files_read));
    }

    if fused.output.ends_with(".parquet") {
        fused_rewrite_to_storage(io, &fused.inputs, &fused.output, pipeline, chain.clone(), run)
            .await?;
        if let Some(chain) = chain {
            if let Ok(mut guard) = chain.lock() {
                guard.finish()?;
            }
        }
        return Ok((1, files_read));
    }

    Err(ParqknifeError::SpecError(format!(
        "unsupported fused routed output layout: {}",
        fused.output
    )))
}

fn fused_pipeline_from_ops(fused: &FusedPlanSegment) -> Result<Option<Pipeline>> {
    fused
        .ops
        .iter()
        .find_map(|op| match op {
            FusedOperation::Rewrite { projection, filter, rename, cast, .. } => {
                Some(pipeline_from_rewrite_ops(
                    projection.clone(),
                    filter.as_deref(),
                    rename.clone(),
                    cast.clone(),
                ))
            }
            _ => None,
        })
        .transpose()
}

async fn fused_rewrite_to_storage(
    io: &ColumnarPipelineIo,
    inputs: &[String],
    output: &str,
    pipeline: Option<Pipeline>,
    chain: Option<std::sync::Arc<std::sync::Mutex<FusedTransformChain>>>,
    run: &TransformRunContext,
) -> Result<()> {
    let out_loc = ObjectLocation::parse(output).map_err(map_storage)?;
    let sink_backend = io.backend_for(&out_loc);
    let schema = schema_from_inputs(io, inputs).await?;
    let stream = fused_input_stream(io, inputs, pipeline, chain, run);
    write_parquet_batch_stream(
        sink_backend,
        out_loc,
        schema,
        Box::pin(stream),
        &NoOpProgressObserver,
    )
    .await
    .map_err(map_columnar)?;
    Ok(())
}

async fn schema_from_inputs(
    io: &ColumnarPipelineIo,
    inputs: &[String],
) -> Result<arrow::datatypes::SchemaRef> {
    let first = inputs
        .first()
        .ok_or_else(|| ParqknifeError::SpecError("fused segment has no inputs".into()))?;
    let loc = ObjectLocation::parse(first).map_err(map_storage)?;
    let backend = io.backend_for(&loc);
    let source = StorageParquetBatchSource::new(backend, loc);
    source.schema().map_err(map_columnar)
}

fn fused_input_stream(
    io: &ColumnarPipelineIo,
    inputs: &[String],
    pipeline: Option<Pipeline>,
    chain: Option<std::sync::Arc<std::sync::Mutex<FusedTransformChain>>>,
    run: &TransformRunContext,
) -> BatchStream {
    Box::pin(FusedInputStream {
        io: io.clone(),
        inputs: inputs.to_vec(),
        input_idx: 0,
        current: None,
        pipeline,
        chain,
        pending_transform: None,
        run: run.clone(),
    })
}

struct FusedInputStream {
    io: ColumnarPipelineIo,
    inputs: Vec<String>,
    input_idx: usize,
    current: Option<BatchStream>,
    pipeline: Option<Pipeline>,
    chain: Option<std::sync::Arc<std::sync::Mutex<FusedTransformChain>>>,
    pending_transform: Option<tokio::task::JoinHandle<std::result::Result<RecordBatch, String>>>,
    run: TransformRunContext,
}

impl futures::Stream for FusedInputStream {
    type Item = std::result::Result<RecordBatch, ColumnarError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if self
                .run
                .cancel_token()
                .as_ref()
                .is_some_and(parqonaut_plugin_host::CancelToken::is_cancelled)
            {
                if let Some(chain) = &self.chain {
                    if let Ok(guard) = chain.lock() {
                        guard.cancel_plugins();
                    }
                }
                return std::task::Poll::Ready(Some(Err(ColumnarError::Other(
                    "transform cancelled".into(),
                ))));
            }

            if let Some(handle) = self.pending_transform.as_mut() {
                match std::future::Future::poll(std::pin::Pin::new(handle), cx) {
                    std::task::Poll::Ready(Ok(Ok(batch))) => {
                        self.pending_transform = None;
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }
                    std::task::Poll::Ready(Ok(Err(e))) => {
                        self.pending_transform = None;
                        return std::task::Poll::Ready(Some(Err(ColumnarError::Other(e))));
                    }
                    std::task::Poll::Ready(Err(e)) => {
                        self.pending_transform = None;
                        return std::task::Poll::Ready(Some(Err(ColumnarError::Other(format!(
                            "plugin transform join: {e}"
                        )))));
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }

            if self.current.is_none() {
                if self.input_idx >= self.inputs.len() {
                    return std::task::Poll::Ready(None);
                }
                let uri = self.inputs[self.input_idx].clone();
                self.input_idx += 1;
                let loc = match ObjectLocation::parse(&uri) {
                    Ok(l) => l,
                    Err(e) => {
                        return std::task::Poll::Ready(Some(Err(ColumnarError::Other(
                            e.to_string(),
                        ))));
                    }
                };
                let backend = self.io.backend_for(&loc);
                let source = StorageParquetBatchSource::new(backend, loc);
                match Box::new(source).into_stream() {
                    Ok(stream) => self.current = Some(stream),
                    Err(e) => return std::task::Poll::Ready(Some(Err(e))),
                }
            }

            let Some(current) = self.current.as_mut() else {
                continue;
            };
            match std::pin::Pin::new(current).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(batch))) => {
                    if let Some(chain) = &self.chain {
                        let chain = chain.clone();
                        self.pending_transform = Some(tokio::task::spawn_blocking(move || {
                            chain
                                .lock()
                                .map_err(|e| e.to_string())
                                .and_then(|mut guard| guard.apply(batch).map_err(|e| e.to_string()))
                        }));
                        continue;
                    }
                    let batch = if let Some(p) = &self.pipeline {
                        match p.execute(batch) {
                            Ok(b) => b,
                            Err(e) => {
                                return std::task::Poll::Ready(Some(Err(ColumnarError::Other(
                                    e.to_string(),
                                ))));
                            }
                        }
                    } else {
                        batch
                    };
                    return std::task::Poll::Ready(Some(Ok(batch)));
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Some(Err(e)))
                }
                std::task::Poll::Ready(None) => {
                    self.current = None;
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}

async fn fused_partition_to_storage(
    io: &ColumnarPipelineIo,
    inputs: &[String],
    output_prefix: &str,
    partition_by: &[String],
    max_open: usize,
    pipeline: Option<Pipeline>,
    chain: Option<std::sync::Arc<std::sync::Mutex<FusedTransformChain>>>,
    run: &TransformRunContext,
) -> Result<Vec<String>> {
    use std::collections::{HashMap, VecDeque};

    use crate::engine::partition::partition_keys;

    let schema = schema_from_inputs(io, inputs).await?;
    let mut stream = fused_input_stream(io, inputs, pipeline, chain.clone(), run);
    let chain_for_finish = chain.clone();

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
    if let Some(chain) = chain_for_finish {
        if let Ok(mut guard) = chain.lock() {
            guard.finish()?;
        }
    }
    Ok(outputs)
}
