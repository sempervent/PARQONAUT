//! Transport- and engine-neutral contracts for transform/stream workflows (v0.8+).

mod observers;
mod progress;
mod schema_policy;
mod stream_checkpoint;
mod transform_plan;

pub use observers::{
    CollectingProgressObserver, JsonLinesProgressObserver, NoOpProgressObserver,
    TerminalProgressObserver, TracingProgressObserver,
};
pub use progress::{ProgressEvent, ProgressEventKind, ProgressMetrics, ProgressObserver};
pub use schema_policy::SchemaConflictPolicy;
pub use stream_checkpoint::{StreamCheckpoint, StreamExecutionIdentity};
pub use transform_plan::{
    TransformOperation, TransformPlan, TransformReport, TransformSpecVersion,
    TRANSFORM_SPEC_SCHEMA_VERSION,
};
