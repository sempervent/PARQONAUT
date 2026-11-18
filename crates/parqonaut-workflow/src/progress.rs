use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ProgressEventKind {
    JobQueued,
    JobClaimed,
    JobRecovered,
    CancelRequested,
    JobCancelled,
    ExecutionStarted,
    Started,
    InputsDiscovered,
    InputDiscovered,
    SchemaResolved,
    OperationStarted,
    FileStarted,
    BatchProcessed,
    FileCompleted,
    CheckpointSaved,
    OperationCompleted,
    OutputCommitted,
    VerificationStarted,
    VerificationCompleted,
    ExecutionCompleted,
    Completed,
    ExecutionFailed,
    Failed,
    PluginStarted,
    PluginProgress,
    PluginCompleted,
    PluginFailed,
    ProgressCoalesced,
}

impl ProgressEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::JobQueued => "job_queued",
            Self::JobClaimed => "job_claimed",
            Self::JobRecovered => "job_recovered",
            Self::CancelRequested => "cancel_requested",
            Self::JobCancelled => "job_cancelled",
            Self::ExecutionStarted => "execution_started",
            Self::Started => "started",
            Self::InputsDiscovered => "inputs_discovered",
            Self::InputDiscovered => "input_discovered",
            Self::SchemaResolved => "schema_resolved",
            Self::OperationStarted => "operation_started",
            Self::FileStarted => "file_started",
            Self::BatchProcessed => "batch_processed",
            Self::FileCompleted => "file_completed",
            Self::CheckpointSaved => "checkpoint_saved",
            Self::OperationCompleted => "operation_completed",
            Self::OutputCommitted => "output_committed",
            Self::VerificationStarted => "verification_started",
            Self::VerificationCompleted => "verification_completed",
            Self::ExecutionCompleted => "execution_completed",
            Self::Completed => "completed",
            Self::ExecutionFailed => "execution_failed",
            Self::Failed => "failed",
            Self::PluginStarted => "plugin_started",
            Self::PluginProgress => "plugin_progress",
            Self::PluginCompleted => "plugin_completed",
            Self::PluginFailed => "plugin_failed",
            Self::ProgressCoalesced => "progress_coalesced",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "job_queued" => Some(Self::JobQueued),
            "job_claimed" => Some(Self::JobClaimed),
            "job_recovered" => Some(Self::JobRecovered),
            "cancel_requested" => Some(Self::CancelRequested),
            "job_cancelled" => Some(Self::JobCancelled),
            "execution_started" => Some(Self::ExecutionStarted),
            "started" => Some(Self::Started),
            "inputs_discovered" => Some(Self::InputsDiscovered),
            "input_discovered" => Some(Self::InputDiscovered),
            "schema_resolved" => Some(Self::SchemaResolved),
            "operation_started" => Some(Self::OperationStarted),
            "file_started" => Some(Self::FileStarted),
            "batch_processed" => Some(Self::BatchProcessed),
            "file_completed" => Some(Self::FileCompleted),
            "checkpoint_saved" => Some(Self::CheckpointSaved),
            "operation_completed" => Some(Self::OperationCompleted),
            "output_committed" => Some(Self::OutputCommitted),
            "verification_started" => Some(Self::VerificationStarted),
            "verification_completed" => Some(Self::VerificationCompleted),
            "execution_completed" => Some(Self::ExecutionCompleted),
            "completed" => Some(Self::Completed),
            "execution_failed" => Some(Self::ExecutionFailed),
            "failed" => Some(Self::Failed),
            "plugin_started" => Some(Self::PluginStarted),
            "plugin_progress" => Some(Self::PluginProgress),
            "plugin_completed" => Some(Self::PluginCompleted),
            "plugin_failed" => Some(Self::PluginFailed),
            "progress_coalesced" => Some(Self::ProgressCoalesced),
            _ => None,
        }
    }

    /// True for job lifecycle transitions and terminal markers that must not be dropped or pruned.
    pub fn is_critical_lifecycle(self) -> bool {
        matches!(
            self,
            Self::JobQueued
                | Self::JobClaimed
                | Self::JobRecovered
                | Self::CancelRequested
                | Self::JobCancelled
                | Self::ExecutionStarted
                | Self::Started
                | Self::InputsDiscovered
                | Self::OperationStarted
                | Self::OperationCompleted
                | Self::OutputCommitted
                | Self::VerificationStarted
                | Self::VerificationCompleted
                | Self::ExecutionCompleted
                | Self::Completed
                | Self::ExecutionFailed
                | Self::Failed
                | Self::PluginStarted
                | Self::PluginCompleted
                | Self::PluginFailed
        )
    }

    /// True when this event marks a terminal job state.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::JobCancelled
                | Self::ExecutionCompleted
                | Self::Completed
                | Self::ExecutionFailed
                | Self::Failed
        )
    }
}

#[derive(
    Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, JsonSchema, utoipa::ToSchema,
)]
pub struct ProgressMetrics {
    pub files_total: u64,
    pub files_done: u64,
    pub rows: u64,
    pub bytes: u64,
    pub elapsed_ms: u64,
    #[serde(default)]
    pub plugin_input_batches: u64,
    #[serde(default)]
    pub plugin_output_batches: u64,
    #[serde(default)]
    pub plugin_input_rows: u64,
    #[serde(default)]
    pub plugin_output_rows: u64,
    #[serde(default)]
    pub datasets_total: u64,
    #[serde(default)]
    pub datasets_done: u64,
    #[serde(default)]
    pub datasets_failed: u64,
    #[serde(default)]
    pub coalesced_events: u64,
}

impl ProgressMetrics {
    /// Returns a completion fraction in `0.0..=1.0` only when a finite workload total is known.
    /// Returns `None` (indeterminate) when totals are unknown; never infers progress from `elapsed_ms`.
    pub fn completion_fraction(&self) -> Option<f64> {
        if self.datasets_total > 0 {
            let done = self.datasets_done.min(self.datasets_total);
            return Some(done as f64 / self.datasets_total as f64);
        }
        if self.files_total > 0 {
            let done = self.files_done.min(self.files_total);
            return Some(done as f64 / self.files_total as f64);
        }
        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct ProgressEvent {
    pub kind: ProgressEventKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub metrics: ProgressMetrics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

pub trait ProgressObserver: Send + Sync {
    fn emit(&self, event: ProgressEvent);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_fraction_never_fabricates_from_elapsed_time() {
        let m = ProgressMetrics { elapsed_ms: 12_345, rows: 100, ..Default::default() };
        assert_eq!(m.completion_fraction(), None);

        let known_files = ProgressMetrics { files_total: 4, files_done: 2, ..Default::default() };
        assert_eq!(known_files.completion_fraction(), Some(0.5));

        let known_ds =
            ProgressMetrics { datasets_total: 10, datasets_done: 3, ..Default::default() };
        assert_eq!(known_ds.completion_fraction(), Some(0.3));
    }
}
