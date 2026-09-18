use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Semaphore;
use tracing::debug;

/// Tracks active and peak concurrent dataset executions.
#[derive(Debug, Default)]
pub struct ConcurrencyMetrics {
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl ConcurrencyMetrics {
    pub fn record_start(&self) -> usize {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        active
    }

    pub fn record_end(&self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn peak_concurrent(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
}

/// Bounded work scheduler for batch dataset execution.
#[derive(Debug, Clone)]
pub struct BatchScheduler {
    max_jobs: usize,
}

impl BatchScheduler {
    pub fn new(max_jobs: u32) -> Self {
        Self {
            max_jobs: max_jobs.max(1) as usize,
        }
    }

    pub fn max_jobs(&self) -> usize {
        self.max_jobs
    }

    /// Run async tasks with at most `max_jobs` active at once and record peak concurrency.
    pub async fn run_bounded<T, F, Fut>(
        &self,
        tasks: Vec<F>,
        metrics: Arc<ConcurrencyMetrics>,
    ) -> Vec<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let max_jobs = self.max_jobs;
        let semaphore = Arc::new(Semaphore::new(max_jobs));
        let mut join_set = tokio::task::JoinSet::new();

        for task in tasks {
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");
            let metrics = Arc::clone(&metrics);
            join_set.spawn(async move {
                let _permit = permit;
                let active = metrics.record_start();
                debug!(active, max_jobs, "dataset task started");
                let result = task().await;
                metrics.record_end();
                result
            });
        }

        let mut results = Vec::with_capacity(join_set.len());
        while let Some(joined) = join_set.join_next().await {
            results.push(joined.expect("dataset task panicked"));
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::Duration;

    #[tokio::test]
    async fn respects_jobs_bound() {
        let scheduler = BatchScheduler::new(2);
        let metrics = Arc::new(ConcurrencyMetrics::default());
        let peak = Arc::new(AtomicUsize::new(0));

        let tasks: Vec<_> = (0..6)
            .map(|_| {
                let peak = Arc::clone(&peak);
                move || {
                    let peak = Arc::clone(&peak);
                    async move {
                        let active = peak.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                        peak.fetch_max(active, AtomicOrdering::SeqCst);
                        tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(25)))
                            .await
                            .unwrap();
                        peak.fetch_sub(1, AtomicOrdering::SeqCst);
                    }
                }
            })
            .collect();

        scheduler.run_bounded(tasks, metrics.clone()).await;
        assert!(metrics.peak_concurrent() <= 2, "peak={}", metrics.peak_concurrent());
    }
}
