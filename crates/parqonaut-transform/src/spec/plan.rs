use crate::error::{ParqknifeError, Result};
use crate::io::resolve_inputs;
use crate::spec::plugin::resolve_pinned_batch_plugin;
use crate::spec::plugin::PinnedBatchPlugin;
use crate::spec::types::{Operation, Spec};
use parqonaut_columnar::pipeline::{
    BarrierStage, ExecutionPlan, PipelineStage, SinkKind, SinkStage, SourceStage, TransformStage,
};
use parqonaut_workflow::TransformPlan;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    /// Legacy per-step view (barrier steps and materialized fallbacks).
    pub steps: Vec<ResolvedStep>,
    #[serde(default)]
    pub intermediate_roots: Vec<String>,
    /// Fused in-memory segments and explicit barriers (v0.9).
    #[serde(default)]
    pub segments: Vec<CompiledSegment>,
    /// Stage boundaries for dry-run / observability.
    #[serde(default)]
    pub pipeline: ExecutionPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // FusedPlanSegment carries op vectors; barrier stays small.
pub enum CompiledSegment {
    Fused(FusedPlanSegment),
    Barrier(BarrierPlanSegment),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusedPlanSegment {
    pub step_indices: Vec<usize>,
    pub inputs: Vec<String>,
    pub output: String,
    pub ops: Vec<FusedOperation>,
    pub is_intermediate: bool,
    pub storage: StorageKind,
    pub execution_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarrierPlanSegment {
    pub reason: String,
    pub step: ResolvedStep,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum FusedOperation {
    Rewrite {
        #[serde(default)]
        compression: Option<crate::spec::types::Compression>,
        #[serde(default)]
        row_group_size_mb: Option<u64>,
        #[serde(default)]
        projection: Option<Vec<String>>,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        rename: Option<HashMap<String, String>>,
        #[serde(default)]
        cast: Option<HashMap<String, String>>,
        #[serde(default)]
        rebuild_stats: bool,
    },
    Partition {
        partition_by: Vec<String>,
    },
    Merge {
        #[serde(default)]
        row_group_size_mb: Option<u64>,
    },
    Plugin(PinnedBatchPlugin),
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

    pub fn fully_streamable(&self) -> bool {
        self.intermediate_roots.is_empty()
            && self.segments.iter().all(|s| matches!(s, CompiledSegment::Fused(_)))
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
        Operation::Plugin { plugin, .. } => parqonaut_workflow::TransformOperation::Rewrite {
            compression: Some(format!("plugin:{plugin}")),
            row_group_size_mb: None,
        },
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

fn is_barrier_operation(op: &Operation) -> bool {
    matches!(op, Operation::Split { .. })
}

fn barrier_reason(op: &Operation) -> Option<&'static str> {
    match op {
        Operation::Split { .. } => Some(
            "split requires row-group size accounting and multi-file emission; not stream-fusible in v0.9",
        ),
        _ => None,
    }
}

fn operation_to_fused(op: &Operation) -> Result<FusedOperation> {
    match op {
        Operation::Rewrite {
            compression,
            row_group_size_mb,
            projection,
            filter,
            schema,
            metadata,
            rebuild_stats,
        } => {
            if metadata.is_some() {
                return Err(ParqknifeError::SpecError(
                    "rewrite metadata updates require a materialized barrier step".into(),
                ));
            }
            Ok(FusedOperation::Rewrite {
                compression: *compression,
                row_group_size_mb: *row_group_size_mb,
                projection: projection.clone(),
                filter: filter.clone(),
                rename: schema.as_ref().and_then(|s| s.rename.clone()),
                cast: schema.as_ref().and_then(|s| s.cast.clone()),
                rebuild_stats: *rebuild_stats,
            })
        }
        Operation::Partition { partition_by } => {
            Ok(FusedOperation::Partition { partition_by: partition_by.clone() })
        }
        Operation::Merge { row_group_size_mb } => {
            Ok(FusedOperation::Merge { row_group_size_mb: *row_group_size_mb })
        }
        Operation::Split { .. } => Err(ParqknifeError::SpecError("split is not fusible".into())),
        Operation::Plugin { plugin, config } => {
            let pinned = resolve_pinned_batch_plugin(plugin, config.clone())?;
            Ok(FusedOperation::Plugin(pinned))
        }
    }
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

    if !spec.options.overwrite
        && !final_output.starts_with("s3://")
        && path_exists_nonempty(Path::new(final_output))
    {
        return Err(ParqknifeError::SpecError(format!(
            "output already exists and overwrite is false: {final_output}"
        )));
    }

    let staging_base = Path::new(final_output)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(".parqonaut-spec-{execution_id}"));

    let mut segments: Vec<CompiledSegment> = Vec::new();
    let mut steps: Vec<ResolvedStep> = Vec::new();
    let mut intermediate_roots: Vec<String> = Vec::new();
    let mut pipeline = ExecutionPlan::default();

    let mut current_inputs = initial_inputs.clone();
    let step_count = spec.steps.len();
    let mut idx = 0usize;

    while idx < step_count {
        let step = &spec.steps[idx];
        if is_barrier_operation(&step.operation) {
            let is_last = idx + 1 == step_count;
            let step_output = if is_last {
                final_output.clone()
            } else {
                let dir = staging_base.join(format!("step-{idx:03}"));
                intermediate_roots.push(dir.to_string_lossy().into_owned());
                dir.to_string_lossy().into_owned()
            };
            validate_step(&step.operation, &current_inputs, &step_output)?;
            let reason = barrier_reason(&step.operation).unwrap_or("barrier").to_string();
            let resolved = ResolvedStep {
                index: idx,
                operation: step.operation.clone(),
                inputs: current_inputs.clone(),
                output: step_output.clone(),
                is_intermediate: !is_last,
                storage: storage_kind(&step_output),
            };
            pipeline.push(PipelineStage::Barrier(BarrierStage {
                reason: reason.clone(),
                materializes_intermediate: !is_last || !current_inputs.is_empty(),
            }));
            segments.push(CompiledSegment::Barrier(BarrierPlanSegment {
                reason,
                step: resolved.clone(),
            }));
            steps.push(resolved);
            current_inputs = predict_outputs(&step.operation, &step_output, &current_inputs)?;
            idx += 1;
            continue;
        }

        let fused_start = idx;
        let mut fused_ops = Vec::new();
        let mut fused_indices = Vec::new();
        while idx < step_count && !is_barrier_operation(&spec.steps[idx].operation) {
            fused_ops.push(operation_to_fused(&spec.steps[idx].operation)?);
            fused_indices.push(idx);
            idx += 1;
        }
        let is_last = idx == step_count;
        let segment_output = if is_last {
            final_output.clone()
        } else {
            let dir = staging_base.join(format!("step-{fused_start:03}"));
            intermediate_roots.push(dir.to_string_lossy().into_owned());
            dir.to_string_lossy().into_owned()
        };

        for &step_idx in &fused_indices {
            let step = &spec.steps[step_idx];
            validate_step(&step.operation, &current_inputs, &segment_output)?;
            steps.push(ResolvedStep {
                index: step_idx,
                operation: step.operation.clone(),
                inputs: if step_idx == fused_start {
                    current_inputs.clone()
                } else {
                    vec![segment_output.clone()]
                },
                output: segment_output.clone(),
                is_intermediate: !is_last,
                storage: storage_kind(&segment_output),
            });
        }

        let transform_labels: Vec<String> = fused_ops.iter().map(fused_op_label).collect();
        pipeline.push(PipelineStage::Source(SourceStage {
            label: format!("source-step-{fused_start}"),
            inputs: current_inputs.clone(),
        }));
        if !transform_labels.is_empty() {
            pipeline.push(PipelineStage::Transform(TransformStage {
                label: format!("fused-{fused_start}-{idx}"),
                transforms: transform_labels,
            }));
        }
        pipeline.push(PipelineStage::Sink(SinkStage {
            label: format!("sink-step-{fused_start}"),
            output: segment_output.clone(),
            sink_kind: sink_kind_for_ops(&fused_ops),
        }));

        segments.push(CompiledSegment::Fused(FusedPlanSegment {
            step_indices: fused_indices,
            inputs: current_inputs.clone(),
            output: segment_output.clone(),
            ops: fused_ops,
            is_intermediate: !is_last,
            storage: storage_kind(&segment_output),
            execution_id: execution_id.clone(),
        }));

        current_inputs = predict_outputs_from_fused(segments.last().unwrap(), &current_inputs)?;
    }

    Ok(ExecutablePlan {
        execution_id,
        schema_version: spec.schema_version,
        original_input: input.clone(),
        final_output: final_output.clone(),
        overwrite: spec.options.overwrite,
        steps,
        intermediate_roots,
        segments,
        pipeline,
    })
}

fn sink_kind_for_ops(ops: &[FusedOperation]) -> SinkKind {
    match ops.last() {
        Some(FusedOperation::Partition { .. }) => SinkKind::PartitionedDirectory,
        Some(FusedOperation::Merge { .. }) => SinkKind::MultiFileDirectory,
        _ => SinkKind::SingleParquet,
    }
}

fn fused_op_label(op: &FusedOperation) -> String {
    match op {
        FusedOperation::Rewrite { .. } => "rewrite".into(),
        FusedOperation::Partition { partition_by } => {
            format!("partition({})", partition_by.join(","))
        }
        FusedOperation::Merge { .. } => "merge".into(),
        FusedOperation::Plugin(p) => format!("plugin({})", p.name),
    }
}

fn predict_outputs_from_fused(segment: &CompiledSegment, inputs: &[String]) -> Result<Vec<String>> {
    let CompiledSegment::Fused(fused) = segment else {
        return Ok(inputs.to_vec());
    };
    let last = fused.ops.last().expect("fused segment");
    match last {
        FusedOperation::Rewrite { .. } | FusedOperation::Plugin(_) => {
            if inputs.len() == 1 && fused.output.ends_with(".parquet") {
                Ok(vec![fused.output.clone()])
            } else {
                Ok(inputs
                    .iter()
                    .map(|inp| {
                        let name = Path::new(inp)
                            .file_name()
                            .and_then(|s| s.to_str())
                            .unwrap_or("out.parquet");
                        Path::new(&fused.output).join(name).to_string_lossy().into_owned()
                    })
                    .collect())
            }
        }
        FusedOperation::Partition { .. } => resolve_inputs(&fused.output),
        FusedOperation::Merge { .. } => {
            let out = Path::new(&fused.output);
            if out.extension().is_some() {
                Ok(vec![fused.output.clone()])
            } else {
                Ok(vec![out.join("merged.parquet").to_string_lossy().into_owned()])
            }
        }
    }
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
        Operation::Plugin { .. } => {
            if inputs.is_empty() {
                return Err(ParqknifeError::SpecError("plugin step requires inputs".into()));
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
        Operation::Plugin { .. } => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::types::{Options, Step};

    fn rewrite_partition_spec(input: &str, output: &str) -> Spec {
        Spec {
            schema_version: 1,
            input: Some(input.into()),
            output: Some(output.into()),
            steps: vec![
                Step {
                    operation: Operation::Rewrite {
                        compression: None,
                        row_group_size_mb: None,
                        projection: None,
                        filter: None,
                        schema: None,
                        metadata: None,
                        rebuild_stats: false,
                    },
                    options: Default::default(),
                },
                Step {
                    operation: Operation::Partition { partition_by: vec!["region".into()] },
                    options: Default::default(),
                },
            ],
            options: Options { overwrite: true, ..Default::default() },
        }
    }

    #[test]
    fn fuse_rewrite_partition_has_no_intermediate_roots() {
        let input = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/transform/partition-basic/input.parquet");
        let spec = rewrite_partition_spec(&input.to_string_lossy(), "/tmp/out");
        let plan = compile_plan(&spec).unwrap();
        assert!(plan.intermediate_roots.is_empty());
        assert_eq!(plan.segments.len(), 1);
        assert!(matches!(plan.segments[0], CompiledSegment::Fused(_)));
    }
}
