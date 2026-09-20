//! Rewrite/merge across local and object-store locations via storage backends.

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use parqonaut_columnar::{BatchSink, BatchSource, ColumnarError, WriteSummary};
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::columnar::{StorageParquetBatchSink, StorageParquetBatchSource};
use parqonaut_storage::location::ObjectLocation;
use parqonaut_workflow::NoOpProgressObserver;

use crate::columnar_io::ColumnarPipelineIo;
use crate::error::{ParqknifeError, Result};

/// Supported endpoint pairing for columnar rewrite/merge (local ↔ S3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteIoKind {
    LocalToLocal,
    LocalToRemote,
    RemoteToLocal,
    RemoteToRemote,
}

pub fn classify_io(input: &str, output: &str) -> Result<RemoteIoKind> {
    let in_remote = ObjectLocation::parse(input).map(|o| o.is_remote()).unwrap_or(false);
    let out_remote = ObjectLocation::parse(output).map(|o| o.is_remote()).unwrap_or(false);
    Ok(match (in_remote, out_remote) {
        (false, false) => RemoteIoKind::LocalToLocal,
        (false, true) => RemoteIoKind::LocalToRemote,
        (true, false) => RemoteIoKind::RemoteToLocal,
        (true, true) => RemoteIoKind::RemoteToRemote,
    })
}

/// Stream-copy Parquet from `input` to `output` with independent source/sink backends.
pub fn rewrite_parquet_storage(
    io: &ColumnarPipelineIo,
    input: &str,
    output: &str,
) -> Result<WriteSummary> {
    let input = input.to_string();
    let output = output.to_string();
    let io = io.clone();
    run_io_runtime(move |handle| {
        handle.block_on(async move { rewrite_parquet_storage_async(&io, &input, &output).await })
    })
}

async fn rewrite_parquet_storage_async(
    io: &ColumnarPipelineIo,
    input: &str,
    output: &str,
) -> Result<WriteSummary> {
    let input_loc =
        ObjectLocation::parse(input).map_err(|e| ParqknifeError::InvalidInput(e.to_string()))?;
    let output_loc =
        ObjectLocation::parse(output).map_err(|e| ParqknifeError::InvalidInput(e.to_string()))?;

    let source_backend = io.backend_for(&input_loc);
    let sink_backend = io.backend_for(&output_loc);
    let progress = NoOpProgressObserver;
    let source = StorageParquetBatchSource::new(source_backend, input_loc);
    let schema = source.schema().map_err(map_columnar)?;
    let stream = Box::new(source).into_stream().map_err(map_columnar)?;
    let mut sink = StorageParquetBatchSink::new(sink_backend, output_loc);
    sink.write_stream(schema, stream, &progress).map_err(map_columnar)
}

/// Merge multiple Parquet inputs into one output path/object via the storage backend.
pub fn merge_parquet_storage(
    io: &ColumnarPipelineIo,
    inputs: &[String],
    output: &str,
) -> Result<WriteSummary> {
    let inputs = inputs.to_vec();
    let output = output.to_string();
    let io = io.clone();
    run_io_runtime(move |handle| {
        handle.block_on(async move { merge_parquet_storage_async(&io, &inputs, &output).await })
    })
}

async fn merge_parquet_storage_async(
    io: &ColumnarPipelineIo,
    inputs: &[String],
    output: &str,
) -> Result<WriteSummary> {
    if inputs.is_empty() {
        return Err(ParqknifeError::InvalidInput("no inputs for merge".into()));
    }
    let mut sorted = inputs.to_vec();
    sorted.sort();

    let output_loc =
        ObjectLocation::parse(output).map_err(|e| ParqknifeError::InvalidInput(e.to_string()))?;
    let progress = NoOpProgressObserver;

    let first = ObjectLocation::parse(&sorted[0])
        .map_err(|e| ParqknifeError::InvalidInput(e.to_string()))?;
    let first_backend = io.backend_for(&first);
    let schema: SchemaRef =
        StorageParquetBatchSource::new(first_backend, first).schema().map_err(map_columnar)?;

    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let io_clone = io.clone();
    let inputs_clone = sorted.clone();
    let schema_check = schema.clone();
    tokio::spawn(async move {
        for input in inputs_clone {
            let loc = match ObjectLocation::parse(&input) {
                Ok(l) => l,
                Err(e) => {
                    let _ = tx.send(Err(ColumnarError::Other(e.to_string()))).await;
                    return;
                }
            };
            let source_backend = io_clone.backend_for(&loc);
            let source = StorageParquetBatchSource::new(source_backend, loc);
            let file_schema = match source.schema() {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            if file_schema.as_ref() != schema_check.as_ref() {
                let _ = tx
                    .send(Err(ColumnarError::Validation(format!(
                        "schema mismatch during merge at {input}"
                    ))))
                    .await;
                return;
            }
            let mut stream = match Box::new(source).into_stream() {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            while let Some(batch) = stream.next().await {
                if tx.send(batch).await.is_err() {
                    return;
                }
            }
        }
    });

    let stream = Box::pin(futures::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    })) as parqonaut_columnar::BatchStream;

    let sink_backend = io.backend_for(&output_loc);
    let mut sink = StorageParquetBatchSink::new(sink_backend, output_loc);
    sink.write_stream(schema, stream, &progress).map_err(map_columnar)
}

/// Local filesystem backend rooted at `root` (used for local paths in the remote matrix).
pub fn default_local_backend(
    root: impl AsRef<Path>,
) -> Arc<parqonaut_storage::LocalStorageBackend> {
    Arc::new(parqonaut_storage::LocalStorageBackend::new(root))
}

pub fn run_io_runtime<F, T>(f: F) -> T
where
    F: FnOnce(tokio::runtime::Handle) -> T + Send + 'static,
    T: Send + 'static,
{
    let work = || {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime required for storage I/O");
        f(rt.handle().clone())
    };
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(work).join().expect("storage I/O thread join")
    } else {
        work()
    }
}

fn map_columnar(err: ColumnarError) -> ParqknifeError {
    ParqknifeError::InvalidInput(err.to_string())
}
