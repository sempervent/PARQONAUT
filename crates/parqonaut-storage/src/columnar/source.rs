use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::parquet_to_arrow_schema;

use parqonaut_columnar::{BatchSource, BatchStream, ColumnarError};

use crate::backend::StorageBackend;
use crate::columnar::async_reader::StorageAsyncFileReader;
use crate::location::ObjectLocation;

/// Options for storage-backed Parquet reads.
#[derive(Debug, Clone)]
pub struct StorageParquetReadOptions {
    pub batch_size: usize,
}

impl Default for StorageParquetReadOptions {
    fn default() -> Self {
        Self { batch_size: 8_192 }
    }
}

/// Parquet batch source using ranged reads via [`StorageBackend`].
pub struct StorageParquetBatchSource<B: StorageBackend + ?Sized + 'static> {
    backend: Arc<B>,
    object: ObjectLocation,
    options: StorageParquetReadOptions,
}

impl<B: StorageBackend + ?Sized + 'static> StorageParquetBatchSource<B> {
    pub fn new(backend: Arc<B>, object: ObjectLocation) -> Self {
        Self { backend, object, options: StorageParquetReadOptions::default() }
    }

    pub fn with_options(mut self, options: StorageParquetReadOptions) -> Self {
        self.options = options;
        self
    }

    fn load_schema(backend: Arc<B>, object: ObjectLocation) -> Result<SchemaRef, ColumnarError> {
        let reader = run_async(StorageAsyncFileReader::open(backend, object))
            .map_err(|e| ColumnarError::Other(e.to_string()))?;
        let file_meta = reader.metadata().file_metadata();
        let schema =
            parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
                .map_err(ColumnarError::from)?;
        Ok(Arc::new(schema))
    }
}

fn run_async<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle)
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
        {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        _ => tokio::runtime::Runtime::new()
            .expect("tokio runtime required for storage columnar I/O")
            .block_on(future),
    }
}

impl<B: StorageBackend + Send + Sync + ?Sized + 'static> BatchSource
    for StorageParquetBatchSource<B>
{
    fn schema(&self) -> Result<SchemaRef, ColumnarError> {
        Self::load_schema(Arc::clone(&self.backend), self.object.clone())
    }

    fn into_stream(self: Box<Self>) -> Result<BatchStream, ColumnarError> {
        let backend = self.backend;
        let object = self.object;
        let batch_size = self.options.batch_size.max(1);
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            let result = async {
                let file_reader = StorageAsyncFileReader::open(backend, object.clone())
                    .await
                    .map_err(|e| ColumnarError::Other(e.to_string()))?;
                let builder = ParquetRecordBatchStreamBuilder::new(file_reader)
                    .await
                    .map_err(ColumnarError::from)?;
                let mut stream =
                    builder.with_batch_size(batch_size).build().map_err(ColumnarError::from)?;
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(ColumnarError::from)?;
                    if tx.send(Ok(batch)).await.is_err() {
                        break;
                    }
                }
                Ok::<(), ColumnarError>(())
            }
            .await;
            if let Err(e) = result {
                let _ = tx.send(Err(e)).await;
            }
        });

        Ok(Box::pin(futures::stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|item| (item, rx))
        })))
    }
}
