//! Plugin stage progress + transform report provenance during fused execution.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use parqonaut_workflow::NoOpProgressObserver;
use parqonaut_workflow::{
    PluginTransformRecord, ProgressEvent, ProgressEventKind, ProgressMetrics, ProgressObserver,
};

use crate::spec::plugin::PinnedBatchPlugin;

const PROGRESS_MIN_INTERVAL: Duration = Duration::from_millis(200);
const PROGRESS_MIN_BATCHES: u64 = 32;

#[derive(Clone)]
pub struct TransformRunContext {
    progress: Arc<dyn ProgressObserver>,
    inner: Arc<Mutex<RunInner>>,
}

struct RunInner {
    stages: Vec<PluginStageRun>,
}

struct PluginStageRun {
    pinned: PinnedBatchPlugin,
    started: Instant,
    started_event_sent: bool,
    terminal_sent: bool,
    last_progress: Instant,
    input_batches: u64,
    output_batches: u64,
    input_rows: u64,
    output_rows: u64,
    status: StageStatus,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StageStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TransformRunContext {
    pub fn new(progress: Arc<dyn ProgressObserver>) -> Self {
        Self { progress, inner: Arc::new(Mutex::new(RunInner { stages: Vec::new() })) }
    }

    pub fn noop() -> Self {
        Self::new(Arc::new(NoOpProgressObserver))
    }

    pub fn register_plugin(&self, pinned: PinnedBatchPlugin) -> usize {
        let mut guard = self.inner.lock().expect("plugin run lock");
        let idx = guard.stages.len();
        guard.stages.push(PluginStageRun {
            pinned,
            started: Instant::now(),
            started_event_sent: false,
            terminal_sent: false,
            last_progress: Instant::now(),
            input_batches: 0,
            output_batches: 0,
            input_rows: 0,
            output_rows: 0,
            status: StageStatus::Running,
        });
        idx
    }

    pub fn on_plugin_batch(&self, stage: usize, input_rows: u64, output_rows: u64) {
        let started_msg = {
            let mut guard = self.inner.lock().expect("plugin run lock");
            let Some(run) = guard.stages.get_mut(stage) else { return };
            if !run.started_event_sent {
                run.started_event_sent = true;
                run.last_progress = Instant::now();
                Some(format!(
                    "plugin {} v{} ({})",
                    run.pinned.name, run.pinned.version, run.pinned.digest
                ))
            } else {
                None
            }
        };
        if let Some(msg) = started_msg {
            self.emit(
                ProgressEventKind::PluginStarted,
                stage,
                ProgressMetrics::default(),
                Some(msg),
            );
        }

        let progress_emit = {
            let mut guard = self.inner.lock().expect("plugin run lock");
            let Some(run) = guard.stages.get_mut(stage) else { return };
            run.input_batches += 1;
            run.output_batches += 1;
            run.input_rows += input_rows;
            run.output_rows += output_rows;
            let due = run.last_progress.elapsed() >= PROGRESS_MIN_INTERVAL
                || (run.input_batches % PROGRESS_MIN_BATCHES == 0);
            if !due {
                return;
            }
            run.last_progress = Instant::now();
            ProgressMetrics {
                plugin_input_batches: run.input_batches,
                plugin_output_batches: run.output_batches,
                plugin_input_rows: run.input_rows,
                plugin_output_rows: run.output_rows,
                elapsed_ms: run.started.elapsed().as_millis() as u64,
                ..ProgressMetrics::default()
            }
        };
        self.emit(ProgressEventKind::PluginProgress, stage, progress_emit, None);
    }

    pub fn complete_plugin(&self, stage: usize) {
        self.finish_stage(
            stage,
            StageStatus::Completed,
            ProgressEventKind::PluginCompleted,
            "completed",
        );
    }

    pub fn fail_plugin(&self, stage: usize, class: &str) {
        self.finish_stage(stage, StageStatus::Failed, ProgressEventKind::PluginFailed, class);
    }

    pub fn cancel_plugin(&self, stage: usize) {
        self.finish_stage(
            stage,
            StageStatus::Cancelled,
            ProgressEventKind::PluginFailed,
            "cancelled",
        );
    }

    fn finish_stage(
        &self,
        stage: usize,
        status: StageStatus,
        kind: ProgressEventKind,
        label: &str,
    ) {
        let mut guard = self.inner.lock().expect("plugin run lock");
        let Some(run) = guard.stages.get_mut(stage) else { return };
        if run.terminal_sent {
            return;
        }
        run.terminal_sent = true;
        run.status = status;
        let metrics = ProgressMetrics {
            plugin_input_batches: run.input_batches,
            plugin_output_batches: run.output_batches,
            plugin_input_rows: run.input_rows,
            plugin_output_rows: run.output_rows,
            elapsed_ms: run.started.elapsed().as_millis() as u64,
            ..ProgressMetrics::default()
        };
        let msg = Some(label.to_string());
        drop(guard);
        self.emit(kind, stage, metrics, msg);
    }

    pub fn take_plugin_executions(&self) -> Vec<PluginTransformRecord> {
        let guard = self.inner.lock().expect("plugin run lock");
        guard
            .stages
            .iter()
            .map(|run| PluginTransformRecord {
                name: run.pinned.name.clone(),
                version: run.pinned.version.clone(),
                digest: run.pinned.digest.clone(),
                protocol_version: run.pinned.protocol_version,
                duration_ms: run.started.elapsed().as_millis() as u64,
                input_batches: run.input_batches,
                output_batches: run.output_batches,
                input_rows: run.input_rows,
                output_rows: run.output_rows,
                status: match run.status {
                    StageStatus::Completed => "completed".into(),
                    StageStatus::Failed => "failed".into(),
                    StageStatus::Cancelled => "cancelled".into(),
                    StageStatus::Running => "running".into(),
                },
            })
            .collect()
    }

    fn stage_pinned(&self, stage: usize) -> Option<PinnedBatchPlugin> {
        self.inner.lock().ok()?.stages.get(stage).map(|s| s.pinned.clone())
    }

    fn emit(
        &self,
        kind: ProgressEventKind,
        _stage: usize,
        metrics: ProgressMetrics,
        message: Option<String>,
    ) {
        self.progress.emit(ProgressEvent { kind, path: None, metrics, message });
    }
}
