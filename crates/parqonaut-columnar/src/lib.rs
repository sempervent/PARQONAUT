//! Canonical columnar execution boundary (Apache Arrow RecordBatch streams).
#![forbid(unsafe_code)]

mod batch;
mod channel;
mod error;
pub mod io;
pub mod pipeline;
pub mod schema;
mod stream;

pub use batch::{emit_batch_written, validate_batch, BatchSink, BatchSource, WriteSummary};
pub use channel::{relay_stream, BackpressureStats};
pub use error::ColumnarError;
pub use pipeline::{CancellationToken, ExecutionPlan, IntermediateIoCounters, PipelineStage};
pub use stream::{BatchResult, BatchStream, DEFAULT_STREAM_CHANNEL_CAPACITY};
