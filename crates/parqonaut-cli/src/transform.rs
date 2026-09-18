use parqonaut_transform::inspect as transform_inspect;
use parqonaut_transform::rewrite as transform_rewrite;
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
