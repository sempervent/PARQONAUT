//! Canonical columnar execution boundary (Apache Arrow RecordBatch streams).
#![forbid(unsafe_code)]

mod batch;
mod error;
mod stream;

pub use batch::{BatchSink, BatchSource, WriteSummary};
pub use error::ColumnarError;
pub use stream::{BatchResult, BatchStream, DEFAULT_STREAM_CHANNEL_CAPACITY};
