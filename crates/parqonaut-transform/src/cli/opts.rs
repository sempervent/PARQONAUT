use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "parqknife")]
#[command(about = "A general-purpose Parquet swiss army knife")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Input file(s) or glob pattern (local or s3://)
    #[arg(short, long, global = true)]
    pub input: Option<String>,

    /// Output path (file or directory, local or s3://)
    #[arg(short, long, global = true)]
    pub output: Option<String>,

    /// Spec file (YAML or JSON)
    #[arg(short, long, global = true)]
    pub spec: Option<PathBuf>,

    /// Dry run (print plan without executing)
    #[arg(long, global = true)]
    pub dry_run: bool,

    /// Number of parallel workers
    #[arg(long, global = true, default_value_t = num_cpus::get())]
    pub concurrency: usize,

    /// Batch size in rows
    #[arg(long, global = true)]
    pub batch_rows: Option<usize>,

    /// Batch size in bytes
    #[arg(long, global = true)]
    pub batch_bytes: Option<u64>,

    /// Fail fast on first error
    #[arg(long, global = true)]
    pub fail_fast: bool,

    /// Best effort (continue on errors)
    #[arg(long, global = true, default_value_t = true)]
    pub best_effort: bool,

    /// Report file path (JSON)
    #[arg(long, global = true)]
    pub report: Option<PathBuf>,

    /// Use atomic directory swaps
    #[arg(long, global = true, default_value_t = true)]
    pub atomic: bool,

    /// Overwrite existing files
    #[arg(long, global = true)]
    pub overwrite: bool,

    /// Progress bars
    #[arg(long, global = true)]
    pub progress: bool,

    /// JSON structured logging
    #[arg(long, global = true)]
    pub json_logs: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Inspect Parquet file(s): schema, row groups, compression, metadata
    Inspect {
        /// Show detailed statistics
        #[arg(long)]
        stats: bool,
    },
    /// Rewrite Parquet file(s) with transformations
    Rewrite {
        /// Compression codec (uncompressed, snappy, gzip, zstd, etc.)
        #[arg(long)]
        compression: Option<String>,

        /// Target row group size in MB
        #[arg(long)]
        row_group_size_mb: Option<u64>,

        /// Projection: comma-separated column names
        #[arg(long)]
        projection: Option<String>,

        /// Filter expression
        #[arg(long)]
        filter: Option<String>,

        /// Rebuild statistics
        #[arg(long)]
        rebuild_stats: bool,
    },
    /// Partition files into Hive-style directories
    Partition {
        /// Partition columns (comma-separated)
        #[arg(long)]
        partition_by: String,
    },
    /// Merge multiple Parquet files into fewer files
    Merge {
        /// Target row group size in MB
        #[arg(long)]
        row_group_size_mb: Option<u64>,
    },
    /// Split large Parquet files into smaller files
    Split {
        /// Target file size in MB
        #[arg(long)]
        target_size_mb: Option<u64>,

        /// Target number of row groups per file
        #[arg(long)]
        target_row_groups: Option<usize>,
    },
}
