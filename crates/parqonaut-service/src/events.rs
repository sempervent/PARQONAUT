//! Bounded operational event recorder, live broadcast fan-out, and ProgressObserver bridge.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use parqonaut_store::{AppendJobEvent, StoreBackend};
use parqonaut_types::{sanitize_event_message, JobEvent, JobEventRetentionPolicy, JobId, JobStage};
use parqonaut_workflow::{ProgressEvent, ProgressEventKind, ProgressMetrics, ProgressObserver};
use tokio::sync::{broadcast, mpsc, oneshot};

const LIVE_BROADCAST_CAPACITY: usize = 256;

enum RecorderMsg {
    Event(Box<AppendJobEvent>),
    Flush(oneshot::Sender<()>),
}

/// Bounded asynchronous operational event recorder with per-job coalescing and live SSE fan-out.
#[derive(Debug, Clone)]
pub struct JobEventRecorder {
    store: StoreBackend,
    policy: JobEventRetentionPolicy,
    tx: mpsc::Sender<RecorderMsg>,
    live: broadcast::Sender<JobEvent>,
    coalesced: Arc<Mutex<HashMap<JobId, u64>>>,
    worker_started: Arc<std::sync::atomic::AtomicBool>,
    rx_slot: Arc<Mutex<Option<mpsc::Receiver<RecorderMsg>>>>,
}

impl JobEventRecorder {
    pub fn new(store: StoreBackend, policy: JobEventRetentionPolicy) -> Self {
        let (tx, rx) = mpsc::channel(policy.channel_capacity.max(4));
        let (live, _) = broadcast::channel(LIVE_BROADCAST_CAPACITY);
        let rec = Self {
            store,
            policy,
            tx,
            live,
            coalesced: Arc::new(Mutex::new(HashMap::new())),
            worker_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            rx_slot: Arc::new(Mutex::new(Some(rx))),
        };
        rec.ensure_flusher_started();
        rec
    }

    pub fn policy(&self) -> JobEventRetentionPolicy {
        self.policy
    }

    pub fn subscribe(&self) -> broadcast::Receiver<JobEvent> {
        self.live.subscribe()
    }

    pub fn broadcast_existing(&self, event: JobEvent) {
        let _ = self.live.send(event);
    }

    pub fn coalesced_count(&self, job_id: JobId) -> u64 {
        self.coalesced.lock().expect("coalesced lock").get(&job_id).copied().unwrap_or(0)
    }

    fn ensure_flusher_started(&self) {
        if self
            .worker_started
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.worker_started.store(false, std::sync::atomic::Ordering::SeqCst);
            return;
        };
        let Some(mut rx) = self.rx_slot.lock().expect("rx lock").take() else {
            return;
        };
        let store = self.store.clone();
        let live = self.live.clone();
        let max_events = self.policy.max_events_per_job;
        let coalesced = Arc::clone(&self.coalesced);

        handle.spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    RecorderMsg::Event(mut input) => {
                        let dropped = {
                            let mut map = coalesced.lock().expect("coalesced lock");
                            map.remove(&input.job_id).unwrap_or(0)
                        };
                        if dropped > 0 {
                            input.metrics.coalesced_events =
                                input.metrics.coalesced_events.saturating_add(dropped);
                        }
                        match store.append_job_event(*input, max_events).await {
                            Ok(ev) => {
                                let _ = live.send(ev);
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "failed to persist buffered job event");
                            }
                        }
                    }
                    RecorderMsg::Flush(done) => {
                        let _ = done.send(());
                    }
                }
            }
        });
    }

    /// Persists a critical lifecycle/terminal event directly and broadcasts it to live subscribers.
    pub async fn record_lifecycle(
        &self,
        mut input: AppendJobEvent,
    ) -> Result<JobEvent, parqonaut_store::StoreError> {
        self.ensure_flusher_started();
        let dropped = {
            let mut map = self.coalesced.lock().expect("coalesced lock");
            map.remove(&input.job_id).unwrap_or(0)
        };
        if dropped > 0 {
            input.metrics.coalesced_events = input.metrics.coalesced_events.saturating_add(dropped);
        }
        let ev = self.store.append_job_event(input, self.policy.max_events_per_job).await?;
        let _ = self.live.send(ev.clone());
        Ok(ev)
    }

    /// Enqueues a high-frequency progress update without blocking workers; coalesces on backpressure.
    pub fn record_nonblocking(&self, input: AppendJobEvent) {
        self.ensure_flusher_started();
        let is_critical = input.kind.is_critical_lifecycle();
        let job_id = input.job_id;
        match self.tx.try_send(RecorderMsg::Event(Box::new(input))) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(RecorderMsg::Event(item))) => {
                if is_critical {
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        let store = self.store.clone();
                        let live = self.live.clone();
                        let max_events = self.policy.max_events_per_job;
                        let coalesced = Arc::clone(&self.coalesced);
                        handle.spawn(async move {
                            let mut item = *item;
                            let dropped = coalesced
                                .lock()
                                .expect("coalesced lock")
                                .remove(&item.job_id)
                                .unwrap_or(0);
                            if dropped > 0 {
                                item.metrics.coalesced_events =
                                    item.metrics.coalesced_events.saturating_add(dropped);
                            }
                            if let Ok(ev) = store.append_job_event(item, max_events).await {
                                let _ = live.send(ev);
                            }
                        });
                    }
                } else {
                    let mut map = self.coalesced.lock().expect("coalesced lock");
                    *map.entry(job_id).or_insert(0) += 1;
                    metrics::counter!("parqonaut_job_events_coalesced_total").increment(1);
                }
            }
            Err(mpsc::error::TrySendError::Full(RecorderMsg::Flush(_))) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Waits for all currently queued buffered events to be flushed to the store.
    pub async fn flush(&self) {
        self.ensure_flusher_started();
        let (tx, rx) = oneshot::channel();
        if self.tx.send(RecorderMsg::Flush(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }

    pub fn observer(&self, job_id: JobId, stage: JobStage) -> Arc<JobProgressObserver> {
        Arc::new(JobProgressObserver { job_id, stage, recorder: self.clone() })
    }
}

/// Bridges engine [`ProgressObserver`] emissions to [`JobEventRecorder`].
pub struct JobProgressObserver {
    job_id: JobId,
    stage: JobStage,
    recorder: JobEventRecorder,
}

impl ProgressObserver for JobProgressObserver {
    fn emit(&self, event: ProgressEvent) {
        let stage = match event.kind {
            ProgressEventKind::PluginStarted
            | ProgressEventKind::PluginProgress
            | ProgressEventKind::PluginCompleted
            | ProgressEventKind::PluginFailed => JobStage::Plugin,
            ProgressEventKind::VerificationStarted | ProgressEventKind::VerificationCompleted => {
                JobStage::Verify
            }
            ProgressEventKind::OutputCommitted => JobStage::Publish,
            _ => self.stage,
        };
        let message = event
            .message
            .as_deref()
            .map(|m| sanitize_event_message(m, 512))
            .or_else(|| event.path.as_deref().map(|p| sanitize_event_message(p, 512)));
        let plugin_name = if stage == JobStage::Plugin { event.path.clone() } else { None };
        let input = AppendJobEvent {
            job_id: self.job_id,
            occurred_at: Utc::now(),
            kind: event.kind,
            stage,
            is_terminal: event.kind.is_terminal(),
            metrics: event.metrics,
            message,
            run_id: None,
            plugin_name,
            plugin_digest: None,
        };
        self.recorder.record_nonblocking(input);
    }
}

pub fn lifecycle_event(
    job_id: JobId,
    kind: ProgressEventKind,
    stage: JobStage,
    metrics: ProgressMetrics,
    message: impl Into<String>,
) -> AppendJobEvent {
    let raw = message.into();
    AppendJobEvent {
        job_id,
        occurred_at: Utc::now(),
        kind,
        stage,
        is_terminal: kind.is_terminal(),
        metrics,
        message: Some(sanitize_event_message(&raw, 512)),
        run_id: None,
        plugin_name: None,
        plugin_digest: None,
    }
}
