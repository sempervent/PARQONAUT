use std::sync::Arc;

use crate::channel::{relay_stream, BackpressureStats};
use crate::pipeline::cancel::CancellationToken;
use crate::stream::{BatchStream, DEFAULT_STREAM_CHANNEL_CAPACITY};

/// Relay a batch stream through the default bounded channel with peak-depth instrumentation.
pub fn relay_with_backpressure(
    input: BatchStream,
    cancel: CancellationToken,
) -> (BatchStream, Arc<BackpressureStats>) {
    let stats = Arc::new(BackpressureStats::default());
    let stats_relay = Arc::clone(&stats);
    let out = relay_stream(input, DEFAULT_STREAM_CHANNEL_CAPACITY, Some(stats_relay), move || {
        cancel.is_cancelled()
    });
    (out, stats)
}
