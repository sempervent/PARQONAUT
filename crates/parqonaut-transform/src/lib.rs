//! Parquet transformation engine (parqknife lineage).
//!
//! Transform scaffolding (S3 I/O, spec wiring, partition writer).
//! `#![allow(dead_code, ...)]` covers unwired modules until wired through `prqnt`.

#![allow(dead_code, unused_imports, unused_variables)]

pub mod cli;
pub mod columnar_io;
pub mod engine;
pub mod error;
pub mod io;
pub mod output;
pub mod remote;
pub mod spec;
pub mod storage_routing;

pub use columnar_io::{columnar_io_from_env, ColumnarPipelineIo};
pub use engine::*;
pub use engine::{
    merge_parquet_files, partition_parquet_file, rewrite_parquet_file, rewrite_parquet_with_cast,
    rewrite_parquet_with_rename, split_parquet_file, HIVE_DEFAULT_PARTITION,
};
pub use error::*;
pub use io::*;
pub use output::*;
pub use parqonaut_workflow::TransformReport;
pub use remote::{
    classify_io, default_local_backend, merge_parquet_storage, rewrite_parquet_storage,
    RemoteIoKind,
};
pub use spec::*;
pub use storage_routing::{
    merge_parquet_routed, partition_parquet_routed, rewrite_parquet_routed, split_parquet_routed,
};

use cli::opts::Commands;

/// Inspect Parquet file(s) at `input` (supports globs).
pub async fn inspect(input: &str, stats: bool) -> Result<()> {
    let cmd = Commands::Inspect { stats };
    cli::run_inspect(&cmd, Some(input)).await
}

/// Rewrite Parquet from `input` to `output` with optional transforms.
/// Run a declarative transform spec (YAML/JSON).
pub fn transform_spec(
    path: &std::path::Path,
    check_only: bool,
    dry_run: bool,
) -> Result<TransformReport> {
    use parqonaut_workflow::TransformReport;
    let spec = parse_spec(path)?;
    if check_only {
        validate_spec(&spec)?;
        let _plan = crate::spec::compile_plan(&spec)?;
        return Ok(TransformReport {
            schema_version: parqonaut_workflow::TRANSFORM_SPEC_SCHEMA_VERSION,
            operations_attempted: spec.steps.len() as u64,
            source_locations: spec.input.clone().map(|s| vec![s]).unwrap_or_default(),
            ..TransformReport::default()
        });
    }
    if dry_run {
        return dry_run_report(&spec);
    }
    execute_spec(&spec)
}

pub async fn partition(
    input: &str,
    output: &str,
    partition_by: &str,
    max_open_partitions: usize,
) -> Result<()> {
    let cols: Vec<String> =
        partition_by.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    let inputs = resolve_inputs(input)?;
    if inputs.len() != 1 {
        return Err(ParqknifeError::InvalidInput(
            "partition requires exactly one input Parquet file".into(),
        ));
    }
    let io = columnar_io::columnar_io_from_env()?;
    storage_routing::partition_parquet_routed(&io, &inputs[0], output, &cols, max_open_partitions)?;
    Ok(())
}

pub async fn merge(input: &str, output: &str, row_group_size_mb: Option<u64>) -> Result<()> {
    let cmd = Commands::Merge { row_group_size_mb };
    cli::run_merge(&cmd, Some(input), Some(output)).await
}

pub async fn split(input: &str, output: &str, target_size_mb: Option<u64>) -> Result<()> {
    let mb = target_size_mb.unwrap_or(512);
    let inputs = resolve_inputs(input)?;
    if inputs.len() != 1 {
        return Err(ParqknifeError::InvalidInput("split requires exactly one input".into()));
    }
    let io = columnar_io::columnar_io_from_env()?;
    storage_routing::split_parquet_routed(&io, &inputs[0], output, mb)?;
    Ok(())
}

use std::path::Path;

pub async fn rewrite(
    input: &str,
    output: &str,
    compression: Option<String>,
    row_group_size_mb: Option<u64>,
    projection: Option<String>,
    filter: Option<String>,
    rebuild_stats: bool,
) -> Result<()> {
    let cmd =
        Commands::Rewrite { compression, row_group_size_mb, projection, filter, rebuild_stats };
    cli::run_rewrite(&cmd, Some(input), Some(output)).await
}
