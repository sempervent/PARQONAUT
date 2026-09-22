use std::io::Write;
use std::sync::mpsc;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use parqonaut_columnar::{
    emit_batch_written, validate_batch, BatchSink, BatchStream, ColumnarError, WriteSummary,
};
use parqonaut_workflow::ProgressObserver;

use crate::backend::StorageBackend;
use crate::error::StorageError;
use crate::location::ObjectLocation;

#[derive(Debug, Clone)]
pub struct StorageParquetWriteOptions {
    pub compression: Compression,
    pub max_row_group_size_bytes: usize,
}

impl Default for StorageParquetWriteOptions {
    fn default() -> Self {
        Self { compression: Compression::SNAPPY, max_row_group_size_bytes: 128 * 1024 * 1024 }
    }
}

/// Parquet batch sink streaming through [`ObjectWriteStream`] (multipart on S3).
pub struct StorageParquetBatchSink<B: StorageBackend + ?Sized + 'static> {
    backend: Arc<B>,
    object: ObjectLocation,
    options: StorageParquetWriteOptions,
}

impl<B: StorageBackend + ?Sized + 'static> StorageParquetBatchSink<B> {
    pub fn new(backend: Arc<B>, object: ObjectLocation) -> Self {
        Self { backend, object, options: StorageParquetWriteOptions::default() }
    }

    pub fn with_options(mut self, options: StorageParquetWriteOptions) -> Self {
        self.options = options;
        self
    }
}

impl<B: StorageBackend + Send + Sync + ?Sized + 'static> BatchSink for StorageParquetBatchSink<B> {
    fn write_stream(
        &mut self,
        schema: SchemaRef,
        stream: BatchStream,
        progress: &dyn ProgressObserver,
    ) -> Result<WriteSummary, ColumnarError> {
        run_async(write_stream_async(
            Arc::clone(&self.backend),
            self.object.clone(),
            self.options.clone(),
            schema,
            stream,
            progress,
        ))
    }
}

fn run_async<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        _ => tokio::runtime::Runtime::new()
            .expect("tokio runtime required for storage columnar I/O")
            .block_on(future),
    }
}

/// Async Parquet upload from a batch stream (no blocking `BatchSink` wrapper).
pub async fn write_parquet_batch_stream<B: StorageBackend + ?Sized>(
    backend: Arc<B>,
    object: ObjectLocation,
    schema: SchemaRef,
    stream: BatchStream,
    progress: &dyn ProgressObserver,
) -> Result<WriteSummary, ColumnarError> {
    write_stream_async(
        backend,
        object,
        StorageParquetWriteOptions::default(),
        schema,
        stream,
        progress,
    )
    .await
}

const BATCH_QUEUE: usize = 8;
const BYTE_CHUNK_QUEUE: usize = 8;

struct ChunkWriter {
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl Write for ChunkWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        self.tx.blocking_send(buf.to_vec()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "upload consumer gone")
        })?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn write_stream_async<B: StorageBackend + ?Sized>(
    backend: Arc<B>,
    object: ObjectLocation,
    options: StorageParquetWriteOptions,
    schema: SchemaRef,
    mut stream: BatchStream,
    progress: &dyn ProgressObserver,
) -> Result<WriteSummary, ColumnarError> {
    let (batch_tx, batch_rx) = mpsc::sync_channel::<RecordBatch>(BATCH_QUEUE);
    let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(BYTE_CHUNK_QUEUE);

    let schema_enc = schema.clone();
    let encode = tokio::task::spawn_blocking(move || {
        let props = WriterProperties::builder()
            .set_compression(options.compression)
            .set_max_row_group_size(options.max_row_group_size_bytes)
            .build();
        let mut writer =
            ArrowWriter::try_new(ChunkWriter { tx: chunk_tx }, schema_enc, Some(props))
                .map_err(ColumnarError::from)?;
        while let Ok(batch) = batch_rx.recv() {
            writer.write(&batch).map_err(ColumnarError::from)?;
        }
        writer.close().map_err(ColumnarError::from)?;
        Ok::<(), ColumnarError>(())
    });

    let mut object_stream = backend
        .write_stream(&object, None)
        .await
        .map_err(|e| ColumnarError::Other(e.to_string()))?;

    let upload = tokio::spawn(async move {
        while let Some(chunk) = chunk_rx.recv().await {
            object_stream
                .write_all(&chunk)
                .await
                .map_err(|e| ColumnarError::Other(e.to_string()))?;
        }
        object_stream.finish().await.map_err(map_storage_err)
    });

    let sink_batch_delay_ms = std::env::var("PARQONAUT_TEST_SINK_BATCH_DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(std::time::Duration::from_millis);

    let mut rows_written = 0u64;
    while let Some(item) = stream.next().await {
        let batch: RecordBatch = item?;
        validate_batch(&schema, &batch)?;
        let rows = batch.num_rows() as u64;
        rows_written += rows;
        emit_batch_written(progress, rows);
        tokio::task::block_in_place(|| batch_tx.send(batch))
            .map_err(|_| ColumnarError::Other("parquet encoder task stopped".into()))?;
        if let Some(delay) = sink_batch_delay_ms {
            tokio::time::sleep(delay).await;
        }
    }
    drop(batch_tx);

    encode.await.map_err(|e| ColumnarError::Other(format!("encode join: {e}")))??;
    let bytes_written =
        upload.await.map_err(|e| ColumnarError::Other(format!("upload join: {e}")))?? as u64;

    Ok(WriteSummary { rows_written, bytes_written, files_written: 1 })
}

fn map_storage_err(err: StorageError) -> ColumnarError {
    ColumnarError::Other(err.to_string())
}
