use crate::error::{ParqknifeError, Result};
use crate::io::resolve_inputs;
use crate::spec::types::{Operation, Spec};
use parqonaut_workflow::TransformPlan;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Resolved execution plan (typed; no raw YAML at execution time).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutablePlan {
    pub execution_id: String,
    pub schema_version: u32,
    pub original_input: String,
    pub final_output: String,
    pub overwrite: bool,
    pub steps: Vec<ResolvedStep>,
    #[serde(default)]
    pub intermediate_roots: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedStep {
    pub index: usize,
    pub operation: Operation,
    pub inputs: Vec<String>,
    pub output: String,
    pub is_intermediate: bool,
    pub storage: StorageKind,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageKind {
    Local,
    S3,
}

impl ExecutablePlan {
    pub fn to_transform_plan(&self) -> TransformPlan {
        TransformPlan {
            schema_version: self.schema_version,
            input: self.original_input.clone(),
            output: self.final_output.clone(),
            steps: self.steps.iter().map(|s| operation_to_contract(&s.operation)).collect(),
        }
    }
}

fn operation_to_contract(op: &Operation) -> parqonaut_workflow::TransformOperation {
    match op {
        Operation::Rewrite { compression, row_group_size_mb, .. } => {
            parqonaut_workflow::TransformOperation::Rewrite {
                compression: compression.as_ref().map(|c| format!("{c:?}").to_lowercase()),
                row_group_size_mb: *row_group_size_mb,
            }
        }
        Operation::Partition { partition_by } => {
            parqonaut_workflow::TransformOperation::Partition { columns: partition_by.clone() }
        }
        Operation::Merge { row_group_size_mb } => {
            parqonaut_workflow::TransformOperation::Merge { row_group_size_mb: *row_group_size_mb }
        }
        Operation::Split { target_size_mb, target_row_groups } => {
            parqonaut_workflow::TransformOperation::Split {
                target_size_mb: *target_size_mb,
                target_row_groups: *target_row_groups,
            }
        }
    }
}

fn storage_kind(path: &str) -> StorageKind {
    if path.starts_with("s3://") {
        StorageKind::S3
    } else {
        StorageKind::Local
    }
}

fn path_exists_nonempty(path: &Path) -> bool {
    if path.is_file() {
        return true;
    }
    path.is_dir() && std::fs::read_dir(path).map(|mut d| d.next().is_some()).unwrap_or(false)
}

pub fn compile_plan(spec: &Spec) -> Result<ExecutablePlan> {
    validate_spec(spec)?;
    let execution_id = Uuid::new_v4().to_string();
    let input = spec.input.as_ref().unwrap();
    let final_output = spec.output.as_ref().unwrap();
    let initial_inputs = resolve_inputs(input)?;
    if initial_inputs.is_empty() {
        return Err(ParqknifeError::SpecError("no input files resolved".into()));
    }

    if final_output.starts_with("s3://") {
        return Err(ParqknifeError::SpecError(
            "s3:// output is not supported for transform specs in v0.8 (local only)".into(),
        ));
    }
    if input.starts_with("s3://") {
        return Err(ParqknifeError::SpecError(
            "s3:// input is not supported for transform specs in v0.8 (local only)".into(),
        ));
    }

    if !spec.options.overwrite && path_exists_nonempty(Path::new(final_output)) {
        return Err(ParqknifeError::SpecError(format!(
            "output already exists and overwrite is false: {final_output}"
        )));
    }

    let staging_base = Path::new(final_output)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(".parqonaut-spec-{execution_id}"));

    let mut steps = Vec::new();
    let mut intermediate_roots = Vec::new();
    let mut current_inputs = initial_inputs.clone();
    let step_count = spec.steps.len();

    for (idx, step) in spec.steps.iter().enumerate() {
        let is_last = idx + 1 == step_count;
        let step_output = if is_last {
            final_output.clone()
        } else {
            let dir = staging_base.join(format!("step-{idx:03}"));
            intermediate_roots.push(dir.to_string_lossy().into_owned());
            dir.to_string_lossy().into_owned()
        };

        validate_step(&step.operation, &current_inputs, &step_output)?;

        steps.push(ResolvedStep {
            index: idx,
            operation: step.operation.clone(),
            inputs: current_inputs.clone(),
            output: step_output.clone(),
            is_intermediate: !is_last,
            storage: storage_kind(&step_output),
        });

        current_inputs = predict_outputs(&step.operation, &step_output, &current_inputs)?;
    }

    Ok(ExecutablePlan {
        execution_id,
        schema_version: spec.schema_version,
        original_input: input.clone(),
        final_output: final_output.clone(),
        overwrite: spec.options.overwrite,
        steps,
        intermediate_roots,
    })
}

fn validate_step(operation: &Operation, inputs: &[String], output: &str) -> Result<()> {
    match operation {
        Operation::Rewrite { .. } => {
            if inputs.is_empty() {
                return Err(ParqknifeError::SpecError("rewrite requires inputs".into()));
            }
        }
        Operation::Partition { partition_by } => {
            if partition_by.is_empty() {
                return Err(ParqknifeError::SpecError("partition-by required".into()));
            }
            if inputs.len() != 1 {
                return Err(ParqknifeError::SpecError(
                    "partition requires exactly one Parquet input".into(),
                ));
            }
        }
        Operation::Merge { .. } => {
            if inputs.is_empty() {
                return Err(ParqknifeError::SpecError("merge requires inputs".into()));
            }
        }
        Operation::Split { .. } => {
            if inputs.len() != 1 {
                return Err(ParqknifeError::SpecError(
                    "split requires exactly one Parquet input".into(),
                ));
            }
        }
    }
    if output.trim().is_empty() {
        return Err(ParqknifeError::SpecError("step output path required".into()));
    }
    Ok(())
}

fn predict_outputs(operation: &Operation, output: &str, inputs: &[String]) -> Result<Vec<String>> {
    match operation {
        Operation::Rewrite { .. } => {
            if inputs.len() == 1 && output.ends_with(".parquet") {
                Ok(vec![output.to_string()])
            } else {
                Ok(inputs
                    .iter()
                    .map(|inp| {
                        let name = Path::new(inp)
                            .file_name()
                            .and_then(|s| s.to_str())
                            .unwrap_or("out.parquet");
                        Path::new(output).join(name).to_string_lossy().into_owned()
                    })
                    .collect())
            }
        }
        Operation::Partition { .. } => resolve_inputs(output),
        Operation::Merge { .. } => {
            let out = Path::new(output);
            if out.extension().is_some() {
                Ok(vec![output.to_string()])
            } else {
                Ok(vec![out.join("merged.parquet").to_string_lossy().into_owned()])
            }
        }
        Operation::Split { .. } => resolve_inputs(output),
    }
}

pub fn validate_spec(spec: &Spec) -> Result<()> {
    if spec.steps.is_empty() {
        return Err(ParqknifeError::SpecError("spec must contain at least one step".into()));
    }
    let _ =
        spec.input.as_ref().ok_or_else(|| ParqknifeError::SpecError("input is required".into()))?;
    let _ = spec
        .output
        .as_ref()
        .ok_or_else(|| ParqknifeError::SpecError("output is required".into()))?;

    for step in &spec.steps {
        match &step.operation {
            Operation::Partition { partition_by } if partition_by.is_empty() => {
                return Err(ParqknifeError::SpecError("partition-by required".into()));
            }
            _ => {}
        }
    }
    Ok(())
}
