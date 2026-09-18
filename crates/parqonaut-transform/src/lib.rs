//! Parquet transformation engine (parqknife lineage).
//!
//! Phase 1 retains inherited scaffolding (S3 stubs, spec wiring, partition writer).
//! `#![allow(dead_code, ...)]` covers unwired modules until Phase 2.

#![allow(dead_code, unused_imports, unused_variables)]

pub mod cli;
pub mod engine;
pub mod error;
pub mod io;
pub mod output;
pub mod spec;

pub use engine::*;
pub use engine::{merge_parquet_files, rewrite_parquet_file};
pub use error::*;
pub use io::*;
pub use output::*;
pub use spec::*;

use cli::opts::Commands;

/// Inspect Parquet file(s) at `input` (supports globs).
pub async fn inspect(input: &str, stats: bool) -> Result<()> {
    let cmd = Commands::Inspect { stats };
    cli::run_inspect(&cmd, Some(input)).await
}

/// Rewrite Parquet from `input` to `output` with optional transforms.
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
