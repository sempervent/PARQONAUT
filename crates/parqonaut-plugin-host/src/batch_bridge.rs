//! Bounded-channel batch plugin stage (one child process, dedicated worker thread).

use std::path::PathBuf;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;

use crate::batch_execute::BatchPluginSession;
use crate::catalog::CatalogEntry;
use crate::error::PluginHostError;
use crate::scan_execute::PluginRuntimeConfig;

enum WorkerMsg {
    Batch(RecordBatch),
    Finish,
}

/// Runs one batch-transform plugin subprocess; host sends batches, receives transformed batches.
pub struct BatchPluginBridge {
    inbound: SyncSender<WorkerMsg>,
    outbound: Receiver<Result<RecordBatch, PluginHostError>>,
    worker: Option<JoinHandle<()>>,
}

impl BatchPluginBridge {
    pub fn start(
        entry: &CatalogEntry,
        config: serde_json::Value,
        execution_id: &str,
        runtime: &PluginRuntimeConfig,
        expected_digest: &str,
        queue_depth: usize,
    ) -> Result<Self, PluginHostError> {
        let (in_tx, in_rx) = sync_channel::<WorkerMsg>(queue_depth);
        let (out_tx, out_rx) = sync_channel(queue_depth);
        let entry_root = entry.root.clone();
        let digest = entry.digest.clone();
        let manifest = entry.manifest.clone();
        let runtime = runtime.clone();
        let execution_id = execution_id.to_string();
        let expected_digest = expected_digest.to_string();
        let config = config.clone();

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
                )?;
                loop {
                    match in_rx.recv() {
                        Ok(WorkerMsg::Batch(batch)) => {
                            let out = session.transform(batch)?;
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
                        Err(_) => break,
                    }
                }
                Ok(())
            };
            if let Err(e) = run() {
                let _ = out_tx.send(Err(e));
            }
        });

        Ok(Self { inbound: in_tx, outbound: out_rx, worker: Some(worker) })
    }

    pub fn transform(&self, batch: RecordBatch) -> Result<RecordBatch, PluginHostError> {
        self.inbound
            .send(WorkerMsg::Batch(batch))
            .map_err(|_| PluginHostError::ProtocolViolation("plugin worker stopped".into()))?;
        self.outbound
            .recv()
            .map_err(|_| PluginHostError::ProtocolViolation("plugin worker stopped".into()))?
    }

    pub fn finish(mut self) -> Result<(), PluginHostError> {
        let _ = self.inbound.send(WorkerMsg::Finish);
        if let Some(h) = self.worker.take() {
            let _ = h.join();
        }
        while let Ok(res) = self.outbound.try_recv() {
            res?;
        }
        Ok(())
    }
}
