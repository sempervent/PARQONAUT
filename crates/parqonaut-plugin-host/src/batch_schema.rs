use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::error::PluginHostError;

pub fn require_schema_equal(input: &Schema, output: &Schema) -> Result<(), PluginHostError> {
    if input != output {
        return Err(PluginHostError::PluginSchemaMismatch(
            "output schema must equal input schema (schema-preserving v1)".into(),
        ));
    }
    Ok(())
}

pub fn enforce_row_policy(
    input: &RecordBatch,
    output: &RecordBatch,
    max_rows: u64,
    max_factor: f64,
) -> Result<(), PluginHostError> {
    let in_rows = input.num_rows() as u64;
    let out_rows = output.num_rows() as u64;
    if out_rows > max_rows {
        return Err(PluginHostError::PluginOutputExpansionExceeded(format!(
            "output rows {out_rows} exceed max {max_rows}"
        )));
    }
    if in_rows == 0 {
        if out_rows != 0 {
            return Err(PluginHostError::PluginOutputExpansionExceeded(
                "zero-row input must produce zero-row output (batch-transform v1)".into(),
            ));
        }
        return Ok(());
    }
    let factor = out_rows as f64 / in_rows as f64;
    if factor > max_factor {
        return Err(PluginHostError::PluginOutputExpansionExceeded(format!(
            "expansion factor {factor} exceeds max {max_factor}"
        )));
    }
    Ok(())
}
