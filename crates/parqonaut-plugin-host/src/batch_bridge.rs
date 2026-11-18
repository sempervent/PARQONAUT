//! Bounded-channel batch plugin stage (one child process, dedicated worker thread).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;

use crate::batch_arrow::validate_schema_supported;
use crate::batch_execute::BatchPluginSession;
use crate::catalog::{CatalogEntry, PluginCompatibility};
use crate::error::PluginHostError;
use crate::scan_execute::{CancelToken, PluginRuntimeConfig};

struct InboundState {
    capacity: usize,
    queue: VecDeque<RecordBatch>,
    aborted: bool,
    closed: bool,
}

struct OutboundState {
    capacity: usize,
    queue: VecDeque<RecordBatch>,
    aborted: bool,
    closed: bool,
    error: Option<PluginHostError>,
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
    inbound: Arc<(Mutex<InboundState>, Condvar)>,
    outbound: Arc<(Mutex<OutboundState>, Condvar)>,
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
        if cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
            return Err(PluginHostError::Cancelled);
        }
        if entry.compatibility != PluginCompatibility::Compatible {
            let req = entry.manifest.requires_parqonaut.clone().unwrap_or_default();
            return Err(PluginHostError::HostVersionMismatch {
                name: entry.manifest.name.clone(),
                requirement: req,
            });
        }

        let inbound = Arc::new((
            Mutex::new(InboundState {
                capacity: queue_depth,
                queue: VecDeque::with_capacity(queue_depth),
                aborted: false,
                closed: false,
            }),
            Condvar::new(),
        ));
        let outbound = Arc::new((
            Mutex::new(OutboundState {
                capacity: queue_depth,
                queue: VecDeque::with_capacity(queue_depth),
                aborted: false,
                closed: false,
                error: None,
            }),
            Condvar::new(),
        ));
        let metrics = Arc::new(BatchBridgeMetrics {
            host_to_plugin_capacity: queue_depth,
            plugin_to_host_capacity: queue_depth,
            ..BatchBridgeMetrics::default()
        });

        let entry_clone = entry.clone();
        let runtime = runtime.clone();
        let execution_id = execution_id.to_string();
        let expected_digest = expected_digest.to_string();
        let metrics_worker = Arc::clone(&metrics);
        let cancel_worker = cancel.clone();
        let inbound_worker = Arc::clone(&inbound);
        let outbound_worker = Arc::clone(&outbound);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<Result<(), PluginHostError>>(1);

        let worker = thread::spawn(move || {
            let mut session = match BatchPluginSession::start(
                &entry_clone,
                config,
                &execution_id,
                &runtime,
                Some(expected_digest.as_str()),
                cancel_worker.clone(),
            ) {
                Ok(s) => {
                    let _ = ready_tx.send(Ok(()));
                    s
                }
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            };

            let result: Result<(), PluginHostError> = (|| {
                loop {
                    if cancel_worker.as_ref().is_some_and(CancelToken::is_cancelled) {
                        session.abort()?;
                        return Err(PluginHostError::Cancelled);
                    }
                    let msg = {
                        let (lock, cv) = &*inbound_worker;
                        let mut guard = lock.lock().expect("inbound lock");
                        loop {
                            if guard.aborted
                                || cancel_worker.as_ref().is_some_and(CancelToken::is_cancelled)
                            {
                                drop(guard);
                                session.abort()?;
                                return Err(PluginHostError::Cancelled);
                            }
                            if let Some(m) = guard.queue.pop_front() {
                                cv.notify_all();
                                break Some(m);
                            }
                            if guard.closed {
                                break None;
                            }
                            guard = cv.wait(guard).expect("inbound wait");
                        }
                    };

                    match msg {
                        Some(batch) => {
                            metrics_worker.record_host_recv();
                            validate_schema_supported(batch.schema().as_ref())?;
                            let out = session.transform(batch)?;
                            let (lock, cv) = &*outbound_worker;
                            let mut guard = lock.lock().expect("outbound lock");
                            while guard.queue.len() >= guard.capacity
                                && !guard.aborted
                                && !guard.closed
                                && !cancel_worker.as_ref().is_some_and(CancelToken::is_cancelled)
                            {
                                guard = cv.wait(guard).expect("outbound wait");
                            }
                            if guard.aborted
                                || cancel_worker.as_ref().is_some_and(CancelToken::is_cancelled)
                            {
                                drop(guard);
                                session.abort()?;
                                return Err(PluginHostError::Cancelled);
                            }
                            if guard.closed {
                                return Err(PluginHostError::ProtocolViolation(
                                    "plugin output channel closed".into(),
                                ));
                            }
                            metrics_worker.record_plugin_send();
                            guard.queue.push_back(out);
                            cv.notify_all();
                        }
                        None => {
                            session.finish()?;
                            break;
                        }
                    }
                }
                Ok(())
            })();

            {
                let (lock, cv) = &*inbound_worker;
                let mut guard = lock.lock().expect("inbound lock");
                guard.closed = true;
                cv.notify_all();
            }
            {
                let (lock, cv) = &*outbound_worker;
                let mut guard = lock.lock().expect("outbound lock");
                guard.closed = true;
                if let Err(e) = result {
                    if guard.error.is_none() {
                        guard.error = Some(e);
                    }
                }
                cv.notify_all();
            }
        });

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self { inbound, outbound, worker: Some(worker), cancel, metrics }),
            Ok(Err(e)) => {
                let _ = worker.join();
                Err(e)
            }
            Err(_) => {
                let _ = worker.join();
                Err(PluginHostError::ProtocolViolation(
                    "plugin worker exited during startup".into(),
                ))
            }
        }
    }

    pub fn metrics(&self) -> &BatchBridgeMetrics {
        &self.metrics
    }

    pub fn cancel(&self) {
        if let Some(c) = &self.cancel {
            c.cancel();
        }
        {
            let (lock, cv) = &*self.inbound;
            let mut guard = lock.lock().expect("inbound lock");
            guard.aborted = true;
            cv.notify_all();
        }
        {
            let (lock, cv) = &*self.outbound;
            let mut guard = lock.lock().expect("outbound lock");
            guard.aborted = true;
            cv.notify_all();
        }
    }

    fn get_terminal_error(&self) -> Option<PluginHostError> {
        let (lock, _) = &*self.outbound;
        lock.lock().expect("outbound lock").error.as_ref().map(clone_host_error)
    }

    pub fn transform(&self, batch: RecordBatch) -> Result<RecordBatch, PluginHostError> {
        if self.cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
            return Err(PluginHostError::Cancelled);
        }
        validate_schema_supported(batch.schema().as_ref())?;

        {
            let (lock, cv) = &*self.inbound;
            let mut guard = lock.lock().expect("inbound lock");
            while guard.queue.len() >= guard.capacity && !guard.aborted && !guard.closed {
                if self.cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
                    return Err(PluginHostError::Cancelled);
                }
                guard = cv.wait(guard).expect("inbound wait");
            }
            if guard.aborted || self.cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
                return Err(PluginHostError::Cancelled);
            }
            if guard.closed {
                return Err(self.get_terminal_error().unwrap_or_else(|| {
                    PluginHostError::ProtocolViolation("plugin worker stopped".into())
                }));
            }
            self.metrics.record_host_send();
            guard.queue.push_back(batch);
            cv.notify_all();
        }

        let (lock, cv) = &*self.outbound;
        let mut guard = lock.lock().expect("outbound lock");
        loop {
            if let Some(out) = guard.queue.pop_front() {
                self.metrics.record_plugin_recv();
                cv.notify_all();
                return Ok(out);
            }
            if guard.aborted || self.cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
                return Err(PluginHostError::Cancelled);
            }
            if guard.closed {
                return Err(guard.error.as_ref().map(clone_host_error).unwrap_or_else(|| {
                    PluginHostError::ProtocolViolation("plugin worker stopped".into())
                }));
            }
            guard = cv.wait(guard).expect("outbound wait");
        }
    }

    pub fn finish(&mut self) -> Result<(), PluginHostError> {
        if self.worker.is_none() {
            return Ok(());
        }
        let cancelled = self.cancel.as_ref().is_some_and(CancelToken::is_cancelled);
        {
            let (lock, cv) = &*self.inbound;
            let mut guard = lock.lock().expect("inbound lock");
            if cancelled {
                guard.aborted = true;
            } else {
                guard.closed = true;
            }
            cv.notify_all();
        }
        if let Some(h) = self.worker.take() {
            let _ = h.join();
        }
        if let Some(err) = self.get_terminal_error() {
            return Err(err);
        }
        Ok(())
    }
}

fn clone_host_error(err: &PluginHostError) -> PluginHostError {
    match err {
        PluginHostError::Io(e) => PluginHostError::Io(std::io::Error::new(e.kind(), e.to_string())),
        PluginHostError::Protocol(e) => PluginHostError::ProtocolViolation(e.to_string()),
        PluginHostError::Json(e) => PluginHostError::InvalidResponse(e.to_string()),
        PluginHostError::NotFound(s) => PluginHostError::NotFound(s.clone()),
        PluginHostError::DuplicateName { name, left, right } => PluginHostError::DuplicateName {
            name: name.clone(),
            left: left.clone(),
            right: right.clone(),
        },
        PluginHostError::InvalidManifest { path, detail } => {
            PluginHostError::InvalidManifest { path: path.clone(), detail: detail.clone() }
        }
        PluginHostError::ProtocolVersionMismatch { detail } => {
            PluginHostError::ProtocolVersionMismatch { detail: detail.clone() }
        }
        PluginHostError::HostVersionMismatch { name, requirement } => {
            PluginHostError::HostVersionMismatch {
                name: name.clone(),
                requirement: requirement.clone(),
            }
        }
        PluginHostError::RuntimeUnavailable(s) => PluginHostError::RuntimeUnavailable(s.clone()),
        PluginHostError::EntrypointInvalid { name, detail } => {
            PluginHostError::EntrypointInvalid { name: name.clone(), detail: detail.clone() }
        }
        PluginHostError::SpawnFailed(s) => PluginHostError::SpawnFailed(s.clone()),
        PluginHostError::Timeout { timeout_ms } => {
            PluginHostError::Timeout { timeout_ms: *timeout_ms }
        }
        PluginHostError::ProcessFailed { code, detail } => {
            PluginHostError::ProcessFailed { code: *code, detail: detail.clone() }
        }
        PluginHostError::ProtocolViolation(s) => PluginHostError::ProtocolViolation(s.clone()),
        PluginHostError::ResponseTooLarge { size, max } => {
            PluginHostError::ResponseTooLarge { size: *size, max: *max }
        }
        PluginHostError::InvalidResponse(s) => PluginHostError::InvalidResponse(s.clone()),
        PluginHostError::PluginDigestMismatch { expected, actual } => {
            PluginHostError::PluginDigestMismatch {
                expected: expected.clone(),
                actual: actual.clone(),
            }
        }
        PluginHostError::InvalidPath(s) => PluginHostError::InvalidPath(s.clone()),
        PluginHostError::InvalidEvidence(s) => PluginHostError::InvalidEvidence(s.clone()),
        PluginHostError::ResultPolicyViolation(s) => {
            PluginHostError::ResultPolicyViolation(s.clone())
        }
        PluginHostError::UnsupportedPhase { name, phase } => {
            PluginHostError::UnsupportedPhase { name: name.clone(), phase: phase.clone() }
        }
        PluginHostError::PluginSchemaMismatch(s) => {
            PluginHostError::PluginSchemaMismatch(s.clone())
        }
        PluginHostError::PluginOutputExpansionExceeded(s) => {
            PluginHostError::PluginOutputExpansionExceeded(s.clone())
        }
        PluginHostError::StalePlugin { expected, actual } => {
            PluginHostError::StalePlugin { expected: expected.clone(), actual: actual.clone() }
        }
        PluginHostError::PluginUnsupportedArrowType(s) => {
            PluginHostError::PluginUnsupportedArrowType(s.clone())
        }
        PluginHostError::Cancelled => PluginHostError::Cancelled,
    }
}

impl Drop for BatchPluginBridge {
    fn drop(&mut self) {
        if self.worker.is_some() {
            self.cancel();
            if let Some(h) = self.worker.take() {
                let _ = h.join();
            }
        }
    }
}
