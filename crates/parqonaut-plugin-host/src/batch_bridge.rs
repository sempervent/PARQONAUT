//! Bounded-channel batch plugin stage (one child process, dedicated worker thread).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;

use crate::batch_arrow::validate_schema_supported;
use crate::batch_execute::BatchPluginSession;
use crate::catalog::CatalogEntry;
use crate::error::PluginHostError;
use crate::scan_execute::{CancelToken, PluginRuntimeConfig};

enum WorkerMsg {
    Batch(RecordBatch),
    Finish,
    Abort,
}

/// Observed queue/backpressure metrics for a batch plugin stage.
#[derive(Debug, Default)]
pub struct BatchBridgeMetrics {
    pub host_to_plugin_capacity: usize,
    pub plugin_to_host_capacity: usize,
    host_to_plugin_inflight: AtomicUsize,
    plugin_to_host_inflight: AtomicUsize,
    host_to_plugin_peak: AtomicUsize,
    plugin_to_host_peak: AtomicUsize,
}

impl BatchBridgeMetrics {
    fn record_host_send(&self) {
        let n = self.host_to_plugin_inflight.fetch_add(1, Ordering::SeqCst) + 1;
        let mut peak = self.host_to_plugin_peak.load(Ordering::SeqCst);
        while n > peak {
            match self.host_to_plugin_peak.compare_exchange_weak(
                peak,
                n,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }

    fn record_host_recv(&self) {
        self.host_to_plugin_inflight.fetch_sub(1, Ordering::SeqCst);
    }

    fn record_plugin_send(&self) {
        let n = self.plugin_to_host_inflight.fetch_add(1, Ordering::SeqCst) + 1;
        let mut peak = self.plugin_to_host_peak.load(Ordering::SeqCst);
        while n > peak {
            match self.plugin_to_host_peak.compare_exchange_weak(
                peak,
                n,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }

    fn record_plugin_recv(&self) {
        self.plugin_to_host_inflight.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn host_to_plugin_peak(&self) -> usize {
        self.host_to_plugin_peak.load(Ordering::SeqCst)
    }

    pub fn plugin_to_host_peak(&self) -> usize {
        self.plugin_to_host_peak.load(Ordering::SeqCst)
    }

    pub fn snapshot(&self) -> BatchBridgeMetricsSnapshot {
        BatchBridgeMetricsSnapshot {
            host_to_plugin_capacity: self.host_to_plugin_capacity,
            plugin_to_host_capacity: self.plugin_to_host_capacity,
            host_to_plugin_peak: self.host_to_plugin_peak(),
            plugin_to_host_peak: self.plugin_to_host_peak(),
        }
    }
}

/// Point-in-time bridge queue metrics (for tests and observability).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchBridgeMetricsSnapshot {
    pub host_to_plugin_capacity: usize,
    pub plugin_to_host_capacity: usize,
    pub host_to_plugin_peak: usize,
    pub plugin_to_host_peak: usize,
}

/// Runs one batch-transform plugin subprocess; host sends batches, receives transformed batches.
pub struct BatchPluginBridge {
    inbound: SyncSender<WorkerMsg>,
    outbound: Receiver<Result<RecordBatch, PluginHostError>>,
    worker: Option<JoinHandle<()>>,
    cancel: Option<CancelToken>,
    metrics: Arc<BatchBridgeMetrics>,
}

impl BatchPluginBridge {
    pub fn start(
        entry: &CatalogEntry,
        config: serde_json::Value,
        execution_id: &str,
        runtime: &PluginRuntimeConfig,
        expected_digest: &str,
        queue_depth: usize,
        cancel: Option<CancelToken>,
    ) -> Result<Self, PluginHostError> {
        if queue_depth == 0 {
            return Err(PluginHostError::InvalidResponse(
                "batch bridge queue depth must be >= 1".into(),
            ));
        }
        let (in_tx, in_rx) = sync_channel::<WorkerMsg>(queue_depth);
        let (out_tx, out_rx) = sync_channel(queue_depth);
        let metrics = Arc::new(BatchBridgeMetrics {
            host_to_plugin_capacity: queue_depth,
            plugin_to_host_capacity: queue_depth,
            ..BatchBridgeMetrics::default()
        });
        let entry_root = entry.root.clone();
        let digest = entry.digest.clone();
        let manifest = entry.manifest.clone();
        let runtime = runtime.clone();
        let execution_id = execution_id.to_string();
        let expected_digest = expected_digest.to_string();
        let config = config.clone();
        let metrics_worker = Arc::clone(&metrics);
        let cancel_worker = cancel.clone();

        let worker = thread::spawn(move || {
            let run = || -> Result<(), PluginHostError> {
                let entry = CatalogEntry {
                    root: entry_root,
                    digest,
                    manifest,
                    compatibility: crate::catalog::PluginCompatibility::Compatible,
                };
                let mut session = BatchPluginSession::start(
                    &entry,
                    config,
                    &execution_id,
                    &runtime,
                    Some(expected_digest.as_str()),
                    cancel_worker.clone(),
                )?;
                loop {
                    if cancel_worker.as_ref().is_some_and(CancelToken::is_cancelled) {
                        session.abort()?;
                        return Err(PluginHostError::Cancelled);
                    }
                    match in_rx.recv() {
                        Ok(WorkerMsg::Batch(batch)) => {
                            metrics_worker.record_host_recv();
                            validate_schema_supported(batch.schema().as_ref())?;
                            let out = session.transform(batch)?;
                            metrics_worker.record_plugin_send();
                            out_tx.send(Ok(out)).map_err(|_| {
                                PluginHostError::ProtocolViolation(
                                    "plugin output channel closed".into(),
                                )
                            })?;
                        }
                        Ok(WorkerMsg::Finish) => {
                            session.finish()?;
                            break;
                        }
                        Ok(WorkerMsg::Abort) => {
                            session.abort()?;
                            return Err(PluginHostError::Cancelled);
                        }
                        Err(_) => break,
                    }
                }
                Ok(())
            };
            if let Err(e) = run() {
                let _ = out_tx.send(Err(e));
            }
        });

        Ok(Self { inbound: in_tx, outbound: out_rx, worker: Some(worker), cancel, metrics })
    }

    pub fn metrics(&self) -> &BatchBridgeMetrics {
        &self.metrics
    }

    pub fn cancel(&self) {
        if let Some(c) = &self.cancel {
            c.cancel();
        }
        let _ = self.inbound.send(WorkerMsg::Abort);
    }

    pub fn transform(&self, batch: RecordBatch) -> Result<RecordBatch, PluginHostError> {
        if self.cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
            return Err(PluginHostError::Cancelled);
        }
        validate_schema_supported(batch.schema().as_ref())?;
        self.metrics.record_host_send();
        self.inbound
            .send(WorkerMsg::Batch(batch))
            .map_err(|_| PluginHostError::ProtocolViolation("plugin worker stopped".into()))?;
        let res = self
            .outbound
            .recv()
            .map_err(|_| PluginHostError::ProtocolViolation("plugin worker stopped".into()))?;
        self.metrics.record_plugin_recv();
        res
    }

    pub fn finish(&mut self) -> Result<(), PluginHostError> {
        if self.cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
            let _ = self.inbound.send(WorkerMsg::Abort);
        } else {
            let _ = self.inbound.send(WorkerMsg::Finish);
        }
        if let Some(h) = self.worker.take() {
            let _ = h.join();
        }
        while let Ok(res) = self.outbound.try_recv() {
            res?;
        }
        Ok(())
    }
}
