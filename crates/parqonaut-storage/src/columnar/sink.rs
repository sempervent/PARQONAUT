use std::io::Write;
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
use crate::stream::ObjectWriteStream;

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

async fn write_stream_async<B: StorageBackend + ?Sized>(
    backend: Arc<B>,
    object: ObjectLocation,
    options: StorageParquetWriteOptions,
    schema: SchemaRef,
    mut stream: BatchStream,
    progress: &dyn ProgressObserver,
) -> Result<WriteSummary, ColumnarError> {
    let mut object_stream = backend
        .write_stream(&object, None)
        .await
        .map_err(|e| ColumnarError::Other(e.to_string()))?;
    let mut writer = ArrowWriter::try_new(
        ObjectWriteAdapter { stream: &mut object_stream },
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_compression(options.compression)
                .set_max_row_group_size(options.max_row_group_size_bytes)
                .build(),
        ),
    )
    .map_err(ColumnarError::from)?;

    let mut rows_written = 0u64;
    while let Some(item) = stream.next().await {
        let batch: RecordBatch = item?;
        validate_batch(&schema, &batch)?;
        writer.write(&batch).map_err(ColumnarError::from)?;
        let rows = batch.num_rows() as u64;
        rows_written += rows;
        emit_batch_written(progress, rows);
    }
    writer.close().map_err(ColumnarError::from)?;
    let bytes_written = object_stream.finish().await.map_err(map_storage_err)?;
    Ok(WriteSummary { rows_written, bytes_written, files_written: 1 })
}

fn map_storage_err(err: StorageError) -> ColumnarError {
    ColumnarError::Other(err.to_string())
}

struct ObjectWriteAdapter<'a> {
    stream: &'a mut ObjectWriteStream,
}

impl Write for ObjectWriteAdapter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        run_async(self.stream.write_all(buf)).map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
