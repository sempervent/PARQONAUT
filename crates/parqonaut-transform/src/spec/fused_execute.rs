use std::path::Path;

use futures::StreamExt;
use parqonaut_columnar::io::LocalParquetBatchSink;
use parqonaut_columnar::io::LocalParquetBatchSource;
use parqonaut_columnar::pipeline::IntermediateIoCounters;
use parqonaut_columnar::{BatchSink, BatchSource};
use parqonaut_workflow::NoOpProgressObserver;

use crate::columnar_io::{columnar_io_from_env, location_is_remote};
use crate::engine::partition_record_batches;
use crate::engine::pipeline_from_rewrite_ops;
use crate::engine::Pipeline;
use crate::error::{ParqknifeError, Result};
use crate::spec::fused_chain::FusedTransformChain;
use crate::spec::plan::{CompiledSegment, ExecutablePlan, FusedOperation, FusedPlanSegment};
use crate::spec::plugin_run::TransformRunContext;
use crate::storage_routing::execute_fused_segment_routed;
use arrow::record_batch::RecordBatch;

pub fn execute_fused_plan(
    plan: &ExecutablePlan,
    io: &mut IntermediateIoCounters,
) -> Result<(u64, u64)> {
    let mut files_read = 0u64;
    let mut files_written = 0u64;
    let run = TransformRunContext::noop();
    for segment in &plan.segments {
        match segment {
            CompiledSegment::Fused(fused) => {
                let (w, r) = execute_fused_segment(fused, &run)?;
                files_read += r;
                files_written += w;
                if fused.is_intermediate {
                    io.record_write(w, 0);
                }
            }
            CompiledSegment::Barrier(barrier) => {
                return Err(ParqknifeError::SpecError(format!(
                    "plan is not fully streamable: barrier {}",
                    barrier.reason
                )));
            }
        }
    }
    Ok((files_read, files_written))
}

pub fn execute_fused_segment(
    fused: &FusedPlanSegment,
    run: &TransformRunContext,
) -> Result<(u64, u64)> {
    let has_plugin = fused.ops.iter().any(|op| matches!(op, FusedOperation::Plugin(_)));
    let routing = has_plugin
        || fused.inputs.iter().any(|i| location_is_remote(i))
        || location_is_remote(&fused.output);
    if routing {
        crate::columnar_io::ensure_s3_for_remote(
            fused.inputs.first().map(String::as_str).unwrap_or(""),
            &fused.output,
        )?;
        let io = columnar_io_from_env()?;
        return execute_fused_segment_routed(&io, fused, run);
    }

    let chain = if has_plugin {
        Some(FusedTransformChain::from_segment(fused, &fused.execution_id, run.clone())?)
    } else {
        None
    };

    let pipeline = if has_plugin {
        None
    } else {
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
            .transpose()?
    };

    let files_read = fused.inputs.len() as u64;
    let mut compression: Option<&str> = None;
    let mut row_group_size_mb: Option<u64> = None;
    let mut partition_by: Option<Vec<String>> = None;

    for op in &fused.ops {
        match op {
            FusedOperation::Rewrite { compression: comp, row_group_size_mb: rg, .. } => {
                compression = comp.as_ref().map(|c| compression_label(c));
                row_group_size_mb = *rg;
            }
            FusedOperation::Partition { partition_by: cols } => {
                partition_by = Some(cols.clone());
            }
            FusedOperation::Merge { row_group_size_mb: rg } => {
                row_group_size_mb = *rg;
            }
            FusedOperation::Plugin(_) => {}
        }
    }

    let batches = stream_local_batches(fused.inputs.clone(), pipeline, chain)?;

    if let Some(cols) = partition_by {
        let outs = partition_record_batches(
            batches,
            Path::new(&fused.output),
            &cols,
            64,
            compression,
            row_group_size_mb,
        )?;
        return Ok((outs.len() as u64, files_read));
    }

    if fused.output.ends_with(".parquet") {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| ParqknifeError::SpecError("fused rewrite produced no batches".into()))?;
        let stream: parqonaut_columnar::BatchStream =
            Box::pin(futures::stream::iter(batches.into_iter().map(Ok)));
        let mut sink = LocalParquetBatchSink::new(&fused.output);
        sink.write_stream(schema, stream, &NoOpProgressObserver)
            .map_err(|e| ParqknifeError::InvalidInput(e.to_string()))?;
        return Ok((1, files_read));
    }

    Err(ParqknifeError::SpecError(format!("unsupported fused output layout: {}", fused.output)))
}

fn stream_local_batches(
    inputs: Vec<String>,
    pipeline: Option<Pipeline>,
    mut chain: Option<FusedTransformChain>,
) -> Result<Vec<RecordBatch>> {
    let work = move || {
        let rt = tokio::runtime::Runtime::new().map_err(ParqknifeError::Io)?;
        rt.block_on(async move {
            let mut batches = Vec::new();
            for input in inputs {
                let mut stream = Box::new(LocalParquetBatchSource::new(input)).into_stream()?;
                while let Some(item) = stream.next().await {
                    let mut batch = item?;
                    if let Some(c) = chain.as_mut() {
                        batch = c.apply(batch)?;
                    } else if let Some(p) = &pipeline {
                        batch = p.execute(batch)?;
                    }
                    batches.push(batch);
                }
            }
            if let Some(mut c) = chain {
                c.finish()?;
            }
            Ok(batches)
        })
    };
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(work).join().expect("fused batch collection thread join")
    } else {
        work()
    }
}

fn compression_label(c: &crate::spec::types::Compression) -> &'static str {
    match c {
        crate::spec::types::Compression::Uncompressed => "uncompressed",
        crate::spec::types::Compression::Snappy => "snappy",
        crate::spec::types::Compression::Gzip => "gzip",
        crate::spec::types::Compression::Lzo => "lzo",
        crate::spec::types::Compression::Brotli => "brotli",
        crate::spec::types::Compression::Lz4 => "lz4",
        crate::spec::types::Compression::Zstd => "zstd",
        crate::spec::types::Compression::Lz4Raw => "lz4_raw",
    }
}
