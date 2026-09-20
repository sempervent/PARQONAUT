//! Canonical columnar execution boundary (Apache Arrow RecordBatch streams).
#![forbid(unsafe_code)]

mod batch;
mod channel;
mod error;
pub mod schema;
mod stream;

pub use batch::{BatchSink, BatchSource, WriteSummary};
pub use channel::{BackpressureStats, relay_stream};
pub use error::ColumnarError;
pub use stream::{BatchResult, BatchStream, DEFAULT_STREAM_CHANNEL_CAPACITY};
