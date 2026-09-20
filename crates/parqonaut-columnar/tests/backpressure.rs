use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parqonaut_columnar::{relay_stream, BackpressureStats, BatchStream, DEFAULT_STREAM_CHANNEL_CAPACITY};
fn batch_stream(n: usize) -> BatchStream {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batches: Vec<_> = (0..n)
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![i as i32]))],
            )
            .unwrap()
        })
        .collect();
    Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peak_queued_batches_bounded() {
    let stats = Arc::new(BackpressureStats::default());
    let slow = batch_stream(32);
    let relayed = relay_stream(slow, DEFAULT_STREAM_CHANNEL_CAPACITY, Some(stats.clone()), || false);
    let mut relayed = relayed;
    // Consumer reads slowly so the channel can fill.
    while relayed.next().await.is_some() {
        tokio::task::yield_now().await;
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(
        stats.peak() <= DEFAULT_STREAM_CHANNEL_CAPACITY,
        "peak queued {} exceeds capacity {}",
        stats.peak(),
        DEFAULT_STREAM_CHANNEL_CAPACITY
    );
}
