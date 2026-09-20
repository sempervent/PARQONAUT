use std::pin::Pin;

use arrow::record_batch::RecordBatch;
use futures::Stream;

use crate::error::ColumnarError;

pub type BatchResult = Result<RecordBatch, ColumnarError>;

pub type BatchStream = Pin<Box<dyn Stream<Item = BatchResult> + Send>>;

/// Default bounded channel capacity between pipeline stages (queued batches).
pub const DEFAULT_STREAM_CHANNEL_CAPACITY: usize = 4;
