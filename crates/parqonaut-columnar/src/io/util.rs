use tokio::sync::mpsc;

use crate::stream::{BatchResult, BatchStream, DEFAULT_STREAM_CHANNEL_CAPACITY};

/// Run a blocking producer on the blocking pool and expose batches as a [`BatchStream`].
pub fn spawn_blocking_producer<F>(capacity: usize, produce: F) -> BatchStream
where
    F: FnOnce(mpsc::Sender<BatchResult>) + Send + 'static,
{
    let cap = if capacity == 0 { DEFAULT_STREAM_CHANNEL_CAPACITY } else { capacity };
    let (tx, rx) = mpsc::channel::<BatchResult>(cap);
    tokio::task::spawn_blocking(move || produce(tx));
    Box::pin(futures::stream::unfold(rx, |mut rx| async { rx.recv().await.map(|item| (item, rx)) }))
}
