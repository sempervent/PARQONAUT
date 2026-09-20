use std::path::Path;

use futures::StreamExt;
use parqonaut_columnar::io::LocalParquetBatchSource;
use parqonaut_columnar::pipeline::IntermediateIoCounters;
use parqonaut_columnar::{BatchSink, BatchSource};
use parqonaut_workflow::NoOpProgressObserver;

use crate::engine::partition_record_batches;
use crate::engine::pipeline_from_rewrite_ops;
use crate::error::{ParqknifeError, Result};
use crate::spec::plan::{CompiledSegment, ExecutablePlan, FusedOperation, FusedPlanSegment};

pub fn execute_fused_plan(
    plan: &ExecutablePlan,
    io: &mut IntermediateIoCounters,
) -> Result<(u64, u64)> {
    let mut files_read = 0u64;
    let mut files_written = 0u64;
    for segment in &plan.segments {
        match segment {
            CompiledSegment::Fused(fused) => {
                if fused.storage != crate::spec::plan::StorageKind::Local {
                    return Err(ParqknifeError::SpecError(
                        "fused in-memory execution supports local paths only (remote via storage adapters)".into(),
                    ));
                }
                let (w, r) = execute_fused_segment(fused)?;
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

pub fn execute_fused_segment(fused: &FusedPlanSegment) -> Result<(u64, u64)> {
    let pipeline = fused
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
        .transpose()?;

    let mut batches = Vec::new();
    let inputs = fused.inputs.clone();
    block_on_async(async {
        for input in inputs {
            let mut stream = Box::new(LocalParquetBatchSource::new(input)).into_stream()?;
            while let Some(item) = stream.next().await {
                let mut batch = item?;
                if let Some(p) = &pipeline {
                    batch = p.execute(batch)?;
                }
                batches.push(batch);
            }
        }
        Ok::<(), ParqknifeError>(())
    })?;
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
        }
    }

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
        let mut sink = parqonaut_columnar::io::LocalParquetBatchSink::new(&fused.output);
        sink.write_stream(schema, stream, &NoOpProgressObserver)
            .map_err(|e| ParqknifeError::InvalidInput(e.to_string()))?;
        return Ok((1, files_read));
    }

    Err(ParqknifeError::SpecError(format!("unsupported fused output layout: {}", fused.output)))
}

fn block_on_async<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let run = || {
        let rt = tokio::runtime::Runtime::new().map_err(ParqknifeError::Io)?;
        rt.block_on(future)
    };
    if tokio::runtime::Handle::try_current().is_ok() {
        // `prqnt` runs under tokio; nested runtimes panic unless we leave the worker thread first.
        tokio::task::block_in_place(run)
    } else {
        run()
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
