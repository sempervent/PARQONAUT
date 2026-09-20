//! [`BatchSource`] / [`BatchSink`] adapters backed by [`StorageBackend`].

mod async_reader;
mod sink;
mod source;

pub use sink::{StorageParquetBatchSink, StorageParquetWriteOptions};
pub use source::{StorageParquetBatchSource, StorageParquetReadOptions};
