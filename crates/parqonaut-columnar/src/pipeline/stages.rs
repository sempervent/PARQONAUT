use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PipelineStage {
    Source(SourceStage),
    Transform(TransformStage),
    Sink(SinkStage),
    Barrier(BarrierStage),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceStage {
    pub label: String,
    pub inputs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransformStage {
    pub label: String,
    pub transforms: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SinkKind {
    SingleParquet,
    PartitionedDirectory,
    MultiFileDirectory,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SinkStage {
    pub label: String,
    pub output: String,
    pub sink_kind: SinkKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BarrierStage {
    pub reason: String,
    /// When true, execution materializes Parquet under `.parqonaut-spec-*` before continuing.
    pub materializes_intermediate: bool,
}
