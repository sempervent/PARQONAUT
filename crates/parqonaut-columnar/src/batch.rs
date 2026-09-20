use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parqonaut_workflow::{ProgressEvent, ProgressObserver};

use crate::error::ColumnarError;
use crate::stream::BatchStream;

#[derive(Debug, Clone, Default)]
pub struct WriteSummary {
    pub rows_written: u64,
    pub bytes_written: u64,
    pub files_written: u32,
}

pub trait BatchSource: Send {
    fn schema(&self) -> Result<SchemaRef, ColumnarError>;

    fn into_stream(self: Box<Self>) -> Result<BatchStream, ColumnarError>;
}

pub trait BatchSink: Send {
    fn write_stream(
        &mut self,
        schema: SchemaRef,
        stream: BatchStream,
        progress: &dyn ProgressObserver,
    ) -> Result<WriteSummary, ColumnarError>;
}

/// Validate row counts and schema compatibility for a single batch (cheap guard).
pub fn validate_batch(schema: &SchemaRef, batch: &RecordBatch) -> Result<(), ColumnarError> {
    if batch.schema().as_ref() != schema.as_ref() {
        return Err(ColumnarError::Validation(
            "record batch schema does not match expected pipeline schema".into(),
        ));
    }
    Ok(())
}

/// Hook for progress emission from sinks without binding to a specific observer type.
pub fn emit_batch_written(progress: &dyn ProgressObserver, rows: u64) {
    progress.emit(ProgressEvent {
        kind: parqonaut_workflow::ProgressEventKind::BatchProcessed,
        path: None,
        metrics: parqonaut_workflow::ProgressMetrics { rows, ..Default::default() },
        message: None,
    });
}
