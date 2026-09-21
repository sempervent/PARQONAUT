//! [`BatchSource`] / [`BatchSink`] adapters backed by [`StorageBackend`].

mod async_reader;
mod sink;
mod source;

pub use sink::{write_parquet_batch_stream, StorageParquetBatchSink, StorageParquetWriteOptions};
pub use source::{StorageParquetBatchSource, StorageParquetReadOptions};
