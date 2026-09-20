use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};

use crate::channel::{relay_stream, BackpressureStats};
use crate::error::ColumnarError;
use crate::pipeline::cancel::CancellationToken;
use crate::stream::{BatchResult, BatchStream, DEFAULT_STREAM_CHANNEL_CAPACITY};

/// Apply a synchronous batch transform across an async stream with bounded backpressure.
pub fn map_stream<F>(
    input: BatchStream,
    schema: SchemaRef,
    cancel: CancellationToken,
    transform: F,
) -> (BatchStream, Arc<BackpressureStats>)
where
    F: Fn(RecordBatch) -> BatchResult + Send + Sync + 'static,
{
    let stats = Arc::new(BackpressureStats::default());
    let stats_relay = Arc::clone(&stats);
    let relayed = relay_stream(
        input,
        DEFAULT_STREAM_CHANNEL_CAPACITY,
        Some(stats_relay),
        move || cancel.is_cancelled(),
    );

    let mapped = relayed.filter_map(move |item| {
        let schema = Arc::clone(&schema);
        let transform = &transform;
        futures::future::ready(match item {
            Ok(batch) => match transform(batch) {
                Ok(b) => Some(Ok(b)),
                Err(e) => Some(Err(e)),
            },
            Err(ColumnarError::Cancelled) => Some(Err(ColumnarError::Cancelled)),
            Err(e) => Some(Err(e)),
        })
    });

    (Box::pin(mapped), stats)
}
