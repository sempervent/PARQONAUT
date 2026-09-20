use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::batch::{emit_batch_written, validate_batch, BatchSink, BatchSource, WriteSummary};
use crate::error::ColumnarError;
use crate::io::util::spawn_blocking_producer;
use crate::stream::BatchStream;
use parqonaut_workflow::ProgressObserver;

/// Default row batch size for local Parquet reads.
pub const DEFAULT_PARQUET_BATCH_SIZE: usize = 8_192;

/// Local Parquet file source (bounded [`RecordBatch`]es via the Parquet Arrow reader).
pub struct LocalParquetBatchSource {
    path: PathBuf,
    batch_size: usize,
}

impl LocalParquetBatchSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), batch_size: DEFAULT_PARQUET_BATCH_SIZE }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }
}

impl BatchSource for LocalParquetBatchSource {
    fn schema(&self) -> Result<SchemaRef, ColumnarError> {
        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        Ok(builder.schema().clone())
    }

    fn into_stream(self: Box<Self>) -> Result<BatchStream, ColumnarError> {
        let path = self.path;
        let batch_size = self.batch_size;
        Ok(spawn_blocking_producer(0, move |tx| {
            let result = (|| -> Result<(), ColumnarError> {
                let file = File::open(&path)?;
                let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
                    .with_batch_size(batch_size)
                    .build()?;
                for batch in reader {
                    let batch = batch.map_err(|e| ColumnarError::Arrow(e.to_string()))?;
                    if tx.blocking_send(Ok(batch)).is_err() {
                        break;
                    }
                }
                Ok(())
            })();
            if let Err(e) = result {
                let _ = tx.blocking_send(Err(e));
            }
        }))
    }
}

#[derive(Debug, Clone)]
pub struct ParquetWriteOptions {
    pub compression: Compression,
    pub max_row_group_size_bytes: usize,
}

impl Default for ParquetWriteOptions {
    fn default() -> Self {
        Self { compression: Compression::SNAPPY, max_row_group_size_bytes: 128 * 1024 * 1024 }
    }
}

/// Local Parquet file sink ([`ArrowWriter`]).
pub struct LocalParquetBatchSink {
    path: PathBuf,
    options: ParquetWriteOptions,
}

impl LocalParquetBatchSink {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), options: ParquetWriteOptions::default() }
    }

    pub fn with_options(mut self, options: ParquetWriteOptions) -> Self {
        self.options = options;
        self
    }
}

fn next_batch(stream: &mut BatchStream) -> Option<crate::stream::BatchResult> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(stream.next()))
    } else {
        tokio::runtime::Runtime::new().ok()?.block_on(stream.next())
    }
}

impl BatchSink for LocalParquetBatchSink {
    fn write_stream(
        &mut self,
        schema: SchemaRef,
        mut stream: BatchStream,
        progress: &dyn ProgressObserver,
    ) -> Result<WriteSummary, ColumnarError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(&self.path)?;
        let props = WriterProperties::builder()
            .set_compression(self.options.compression)
            .set_max_row_group_size(self.options.max_row_group_size_bytes)
            .build();
        let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema.clone(), Some(props))?;

        let mut rows_written = 0u64;
        while let Some(item) = next_batch(&mut stream) {
            let batch: RecordBatch = item?;
            validate_batch(&schema, &batch)?;
            writer.write(&batch)?;
            let rows = batch.num_rows() as u64;
            rows_written += rows;
            emit_batch_written(progress, rows);
        }
        writer.close()?;
        let bytes_written = std::fs::metadata(&self.path)?.len();
        Ok(WriteSummary { rows_written, bytes_written, files_written: 1 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parqonaut_workflow::NoOpProgressObserver;
    use tempfile::tempdir;

    #[test]
    fn local_parquet_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempdir().unwrap();
            let path = dir.path().join("roundtrip.parquet");
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["x", "y"])),
                ],
            )
            .unwrap();

            let stream: BatchStream = Box::pin(futures::stream::iter(vec![Ok(batch)]));
            let mut sink = LocalParquetBatchSink::new(&path);
            sink.write_stream(schema.clone(), stream, &NoOpProgressObserver).unwrap();

            let source = LocalParquetBatchSource::new(&path);
            assert_eq!(source.schema().unwrap().fields().len(), 2);
            let mut out = Box::new(source).into_stream().unwrap();
            let read = out.next().await.unwrap().unwrap();
            assert_eq!(read.num_rows(), 2);
        });
    }
}
