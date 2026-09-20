//! Local batch sources and sinks (Parquet and CSV).

mod csv;
mod local_parquet;
mod util;

pub use csv::{CsvReadOptions, LocalCsvBatchSource};
pub use local_parquet::{LocalParquetBatchSink, LocalParquetBatchSource, ParquetWriteOptions};
pub use util::spawn_blocking_producer;
