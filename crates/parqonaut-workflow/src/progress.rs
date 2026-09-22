use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProgressEventKind {
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
    ExecutionCompleted,
    Completed,
    ExecutionFailed,
    Failed,
    PluginStarted,
    PluginProgress,
    PluginCompleted,
    PluginFailed,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
