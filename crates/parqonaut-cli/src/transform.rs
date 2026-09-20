use parqonaut_transform::{
    inspect as transform_inspect, merge as transform_merge, partition as transform_partition,
    rewrite as transform_rewrite, split as transform_split, transform_spec, TransformReport,
};
use std::path::PathBuf;

pub async fn run_inspect(input: PathBuf, stats: bool) -> Result<(), Box<dyn std::error::Error>> {
    transform_inspect(&input.to_string_lossy(), stats).await?;
    Ok(())
}

pub async fn run_rewrite(
    input: PathBuf,
    output: PathBuf,
    compression: Option<String>,
    row_group_size_mb: Option<u64>,
    projection: Option<String>,
    filter: Option<String>,
    rebuild_stats: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    transform_rewrite(
        &input.to_string_lossy(),
        &output.to_string_lossy(),
        compression,
        row_group_size_mb,
        projection,
        filter,
        rebuild_stats,
    )
    .await?;
    Ok(())
}

pub async fn run_partition(
    input: PathBuf,
    output: PathBuf,
    partition_by: String,
    max_open_partitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    transform_partition(
        &input.to_string_lossy(),
        &output.to_string_lossy(),
        &partition_by,
        max_open_partitions,
    )
    .await?;
    Ok(())
}

pub async fn run_merge(
    input: String,
    output: PathBuf,
    row_group_size_mb: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    transform_merge(&input, &output.to_string_lossy(), row_group_size_mb).await?;
    Ok(())
}

pub async fn run_split(
    input: PathBuf,
    output: PathBuf,
    target_size_mb: Option<u64>,
    target_row_groups: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = target_row_groups;
    transform_split(&input.to_string_lossy(), &output.to_string_lossy(), target_size_mb).await?;
    Ok(())
}

pub fn run_transform_spec(
    spec: PathBuf,
    check: bool,
    dry_run: bool,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let report: TransformReport = transform_spec(&spec, check, dry_run)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if dry_run || check {
        println!("operations: {}", report.operations_attempted);
        println!("inputs: {}", report.input_files.len());
        for path in &report.input_files {
            println!("  - {path}");
        }
    }
    Ok(())
}
