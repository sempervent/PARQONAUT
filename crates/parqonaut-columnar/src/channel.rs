use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::ColumnarError;
use crate::stream::{BatchResult, BatchStream, DEFAULT_STREAM_CHANNEL_CAPACITY};

#[derive(Debug, Default)]
pub struct BackpressureStats {
    pub peak_queued_batches: AtomicUsize,
}

impl BackpressureStats {
    pub fn record_depth(&self, depth: usize) {
        let mut current = self.peak_queued_batches.load(Ordering::Relaxed);
        while depth > current {
            match self.peak_queued_batches.compare_exchange_weak(
                current,
                depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => current = v,
            }
        }
    }

    pub fn peak(&self) -> usize {
        self.peak_queued_batches.load(Ordering::Relaxed)
    }
}

/// Relay a batch stream through a bounded channel (default capacity 4).
pub fn relay_stream(
    input: BatchStream,
    capacity: usize,
    stats: Option<Arc<BackpressureStats>>,
    mut cancelled: impl FnMut() -> bool + Send + 'static,
) -> BatchStream {
    let cap = if capacity == 0 {
        DEFAULT_STREAM_CHANNEL_CAPACITY
    } else {
        capacity
    };
    let (tx, rx) = mpsc::channel::<BatchResult>(cap);

    tokio::spawn(async move {
        let mut input = input;
        while let Some(item) = input.next().await {
            if cancelled() {
                let _ = tx.send(Err(ColumnarError::Cancelled)).await;
                break;
            }
            if tx.send(item).await.is_err() {
                break;
            }
            if let Some(ref s) = stats {
                s.record_depth(tx.max_capacity() - tx.capacity());
            }
        }
    });

    Box::pin(futures::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    }))
}
