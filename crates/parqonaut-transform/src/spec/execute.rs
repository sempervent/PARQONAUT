use crate::engine::{
    merge_parquet_files, partition_parquet_file, rewrite_parquet_file, rewrite_parquet_with_cast,
    rewrite_parquet_with_rename, split_parquet_file,
};
use crate::error::{ParqknifeError, Result};
use crate::io::resolve_inputs;
use crate::spec::fused_execute::execute_fused_segment;
use crate::spec::plan::{compile_plan, CompiledSegment, ExecutablePlan, ResolvedStep};
use crate::spec::plugin_run::TransformRunContext;
use crate::spec::types::{Compression, Operation, Spec};
use parqonaut_columnar::IntermediateIoCounters;
use parqonaut_workflow::{TransformReport, TRANSFORM_SPEC_SCHEMA_VERSION};
use std::path::{Path, PathBuf};
use std::time::Instant;

pub use crate::spec::plan::{
    compile_plan as compile_transform_plan, ExecutablePlan as ResolvedTransformPlan,
};

pub fn dry_run_report(spec: &Spec) -> Result<TransformReport> {
    let plan = compile_plan(spec)?;
    plan_to_dry_run_report(&plan)
}

fn plan_to_dry_run_report(plan: &ExecutablePlan) -> Result<TransformReport> {
    Ok(TransformReport {
        schema_version: TRANSFORM_SPEC_SCHEMA_VERSION,
        execution_id: Some(plan.execution_id.clone()),
        operations_attempted: plan.steps.len() as u64,
        operations_completed: 0,
        source_locations: vec![plan.original_input.clone()],
        final_destination: Some(plan.final_output.clone()),
        intermediate_locations: plan.intermediate_roots.clone(),
        input_files: plan.steps.first().map(|s| s.inputs.clone()).unwrap_or_default(),
        output_files: vec![plan.final_output.clone()],
        rows_read: 0,
        rows_written: 0,
        files_read: 0,
        files_written: 0,
        bytes_read: 0,
        bytes_written: 0,
        intermediate_files_created: 0,
        intermediate_bytes_written: 0,
        intermediate_files_read: 0,
        intermediate_bytes_read: 0,
        rows: 0,
        bytes: 0,
        elapsed_ms: 0,
        warnings: vec![],
        failures: vec![],
        plan: Some(
            serde_json::to_value(plan).map_err(|e| ParqknifeError::SpecError(e.to_string()))?,
        ),
        plugin_executions: vec![],
    })
}

pub fn execute_spec(spec: &Spec) -> Result<TransformReport> {
    let plan = compile_plan(spec)?;
    execute_plan(&plan)
}

pub fn execute_plan(plan: &ExecutablePlan) -> Result<TransformReport> {
    execute_plan_with_run(plan, &TransformRunContext::noop())
}

pub fn execute_plan_with_run(
    plan: &ExecutablePlan,
    run: &TransformRunContext,
) -> Result<TransformReport> {
    let started = Instant::now();
    let mut completed = 0u64;
    let warnings: Vec<String> = Vec::new();
    let mut files_read = 0u64;
    let mut files_written = 0u64;
    let mut intermediate_io = IntermediateIoCounters::default();
    for segment in &plan.segments {
        match segment {
            CompiledSegment::Fused(fused) => {
                let (written, read) = execute_fused_segment(fused, &run)?;
                files_read += read;
                files_written += written;
                if fused.is_intermediate {
                    record_intermediate_dir(&mut intermediate_io, &fused.output);
                }
                completed += fused.step_indices.len() as u64;
            }
            CompiledSegment::Barrier(barrier) => {
                let (written, read) = execute_step(&barrier.step)?;
                files_read += read;
                files_written += written;
                if barrier.step.is_intermediate {
                    record_intermediate_dir(&mut intermediate_io, &barrier.step.output);
                    for input in &barrier.step.inputs {
                        if plan.intermediate_roots.iter().any(|r| input.starts_with(r)) {
                            record_intermediate_file(&mut intermediate_io, input, true);
                        }
                    }
                }
                completed += 1;
            }
        }
    }

    if plan
        .intermediate_roots
        .iter()
        .all(|root| std::fs::remove_dir_all(root).is_ok() || !Path::new(root).exists())
    {
        // cleaned intermediates on success
    }

    let final_outputs = plan
        .steps
        .last()
        .map(|s| resolve_inputs(&s.output).unwrap_or_default())
        .unwrap_or_default();

    Ok(TransformReport {
        schema_version: TRANSFORM_SPEC_SCHEMA_VERSION,
        execution_id: Some(plan.execution_id.clone()),
        operations_attempted: plan.steps.len() as u64,
        operations_completed: completed,
        source_locations: vec![plan.original_input.clone()],
        final_destination: Some(plan.final_output.clone()),
        intermediate_locations: vec![],
        input_files: plan.steps.first().map(|s| s.inputs.clone()).unwrap_or_default(),
        output_files: final_outputs,
        rows_read: 0,
        rows_written: 0,
        files_read,
        files_written,
        bytes_read: 0,
        bytes_written: 0,
        intermediate_files_created: intermediate_io.intermediate_files_created(),
        intermediate_bytes_written: intermediate_io.bytes_written,
        intermediate_files_read: intermediate_io.files_read,
        intermediate_bytes_read: intermediate_io.bytes_read,
        rows: 0,
        bytes: 0,
        elapsed_ms: started.elapsed().as_millis() as u64,
        warnings,
        failures: vec![],
        plan: None,
        plugin_executions: run.take_plugin_executions(),
    })
}

fn record_intermediate_dir(io: &mut IntermediateIoCounters, root: &str) {
    let root_path = Path::new(root);
    if !root_path.exists() {
        return;
    }
    walk_intermediate_files(root_path, io);
}

fn walk_intermediate_files(path: &Path, io: &mut IntermediateIoCounters) {
    if path.is_file() {
        record_intermediate_file(io, &path.to_string_lossy(), false);
        return;
    }
    let Ok(read_dir) = std::fs::read_dir(path) else { return };
    for entry in read_dir.flatten() {
        let p = entry.path();
        if p.is_dir() {
            walk_intermediate_files(&p, io);
        } else {
            record_intermediate_file(io, &p.to_string_lossy(), false);
        }
    }
}

fn record_intermediate_file(io: &mut IntermediateIoCounters, path: &str, is_read: bool) {
    let Ok(meta) = std::fs::metadata(path) else { return };
    if is_read {
        io.record_read(1, meta.len());
    } else {
        io.record_write(1, meta.len());
    }
}

fn execute_step(step: &ResolvedStep) -> Result<(u64, u64)> {
    let read = step.inputs.len() as u64;
    match &step.operation {
        Operation::Rewrite {
            compression,
            row_group_size_mb,
            projection: _,
            filter: _,
            schema,
            metadata: _,
            rebuild_stats,
        } => {
            std::fs::create_dir_all(Path::new(&step.output)).ok();
            let comp = compression.as_ref().map(|c| compression_label(c).to_string());
            let mut written = 0u64;
            for input in &step.inputs {
                let out_path = rewrite_output_path(&step.output, input, step.inputs.len());
                if let Some(schema_ops) = schema {
                    let tmp = out_path.with_extension("tmp.parquet");
                    rewrite_parquet_file(
                        input,
                        tmp.to_str().unwrap(),
                        comp.as_deref(),
                        *row_group_size_mb,
                        *rebuild_stats,
                    )?;
                    let mut current = tmp.to_string_lossy().into_owned();
                    if let Some(renames) = &schema_ops.rename {
                        for (from, to) in renames {
                            let next = out_path.with_extension("rename.tmp.parquet");
                            rewrite_parquet_with_rename(
                                &current,
                                next.to_str().unwrap(),
                                from,
                                to,
                                comp.as_deref(),
                                *row_group_size_mb,
                                *rebuild_stats,
                            )?;
                            let _ = std::fs::remove_file(&current);
                            current = next.to_string_lossy().into_owned();
                        }
                    }
                    if let Some(casts) = &schema_ops.cast {
                        for (col, to_type) in casts {
                            let next = out_path.with_extension("cast.tmp.parquet");
                            rewrite_parquet_with_cast(
                                &current,
                                next.to_str().unwrap(),
                                col,
                                to_type,
                                comp.as_deref(),
                                *row_group_size_mb,
                                *rebuild_stats,
                            )?;
                            let _ = std::fs::remove_file(&current);
                            current = next.to_string_lossy().into_owned();
                        }
                    }
                    std::fs::rename(&current, &out_path)?;
                } else {
                    rewrite_parquet_file(
                        input,
                        out_path.to_str().unwrap(),
                        comp.as_deref(),
                        *row_group_size_mb,
                        *rebuild_stats,
                    )?;
                }
                written += 1;
            }
            Ok((written, read))
        }
        Operation::Partition { partition_by } => {
            partition_parquet_file(
                &step.inputs[0],
                Path::new(&step.output),
                partition_by,
                64,
                None,
                None,
            )?;
            let outs = resolve_inputs(&step.output)?;
            Ok((outs.len() as u64, read))
        }
        Operation::Merge { row_group_size_mb } => {
            let out = Path::new(&step.output);
            let (dir, base) = if out.extension().is_some() {
                (
                    out.parent().unwrap_or_else(|| Path::new(".")),
                    out.file_stem().and_then(|s| s.to_str()).unwrap_or("merged"),
                )
            } else {
                (out, "merged")
            };
            let exact = out.extension().is_some().then_some(out);
            let outputs = merge_parquet_files(
                &step.inputs,
                dir,
                base,
                512 * 1024 * 1024,
                None,
                *row_group_size_mb,
                false,
                exact,
            )?;
            Ok((outputs.len() as u64, read))
        }
        Operation::Split { target_size_mb, target_row_groups: _ } => {
            let mb = target_size_mb.unwrap_or(512);
            split_parquet_file(
                &step.inputs[0],
                Path::new(&step.output),
                "part",
                mb * 1024 * 1024,
                None,
                None,
                false,
            )?;
            let outs = resolve_inputs(&step.output)?;
            Ok((outs.len() as u64, read))
        }
        Operation::Plugin { .. } => Err(ParqknifeError::SpecError(
            "plugin transform steps are only supported via fused execution".into(),
        )),
    }
}

fn rewrite_output_path(output: &str, input: &str, input_count: usize) -> PathBuf {
    let out = Path::new(output);
    if input_count == 1 && output.ends_with(".parquet") {
        return out.to_path_buf();
    }
    let name = Path::new(input).file_name().and_then(|s| s.to_str()).unwrap_or("out.parquet");
    out.join(name)
}

fn compression_label(c: &Compression) -> &'static str {
    match c {
        Compression::Uncompressed => "uncompressed",
        Compression::Snappy => "snappy",
        Compression::Gzip => "gzip",
        Compression::Lzo => "lzo",
        Compression::Brotli => "brotli",
        Compression::Lz4 => "lz4",
        Compression::Zstd => "zstd",
        Compression::Lz4Raw => "lz4_raw",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::types::{Options, Step};
    use tempfile::tempdir;

    #[test]
    fn dry_run_leaves_no_artifacts() {
        let dir = tempdir().unwrap();
        let inp = dir.path().join("a.parquet");
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/dummy.parquet");
        std::fs::copy(fixture, &inp).unwrap();
        let spec_path = dir.path().join("spec.yaml");
        std::fs::write(
            &spec_path,
            format!(
                r#"schema-version: 1
input: {}
output: {}
steps:
  - operation:
      type: rewrite
      compression: zstd
"#,
                inp.display(),
                dir.path().join("out.parquet").display()
            ),
        )
        .unwrap();
        let spec = crate::parse_spec(&spec_path).unwrap();
        let before: Vec<_> =
            std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().path()).collect();
        let report = dry_run_report(&spec).unwrap();
        let after: Vec<_> =
            std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().path()).collect();
        assert_eq!(before.len(), after.len());
        assert!(!dir.path().join(".parqonaut-spec").exists());
        let plan_json = report.plan.as_ref().unwrap();
        assert!(plan_json.get("pipeline").is_some());
    }
}
