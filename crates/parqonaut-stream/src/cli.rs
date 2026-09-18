use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "maw",
    about = "A high-performance CLI for streaming and concatenating CSV and Parquet files",
    version = env!("CARGO_PKG_VERSION")
)]
pub struct Cli {
    /// Input files, directories, or globs. Use '-' for stdin.
    #[arg(required = true)]
    pub inputs: Vec<String>,

    /// Output file path
    #[arg(short = 'o', long = "out")]
    pub out: Option<PathBuf>,

    /// Output format (csv or parquet)
    #[arg(long = "out-format", value_enum)]
    pub out_format: Option<OutputFormat>,

    // CSV input options
    /// CSV delimiter character
    #[arg(long)]
    pub delimiter: Option<char>,

    /// CSV quote character
    #[arg(long)]
    pub quote: Option<char>,

    /// Treat CSV as having no headers
    #[arg(long)]
    pub no_headers: bool,

    /// Text encoding for CSV files
    #[arg(long, default_value = "utf8")]
    pub encoding: String,

    /// NA/null values to recognize
    #[arg(long, default_value = "NA,null,\\N")]
    pub na: String,

    // Schema options
    /// Columns to include (whitelist)
    #[arg(long)]
    pub columns: Option<String>,

    /// Columns to exclude (blacklist)
    #[arg(long)]
    pub exclude: Option<String>,

    /// Rename columns (format: old=new)
    #[arg(long)]
    pub rename: Vec<String>,

    /// Reorder columns alphabetically
    #[arg(long)]
    pub reorder: bool,

    /// Coerce type conflicts to strings
    #[arg(long)]
    pub stringify_conflicts: bool,

    /// Number of rows to sample for schema inference
    #[arg(long, default_value = "1000")]
    pub infer_rows: usize,

    // Rolling output options
    /// Roll output files by size (bytes)
    #[arg(long)]
    pub roll_by_bytes: Option<u64>,

    /// Roll output files by row count
    #[arg(long)]
    pub roll_by_rows: Option<u64>,

    // Compression options
    /// Compression algorithm
    #[arg(long, value_enum, default_value = "none")]
    pub compression: Compression,

    /// ZSTD compression level (1-19)
    #[arg(long, default_value = "3")]
    pub zstd_level: u32,

    // Performance options
    /// Number of concurrent readers
    #[arg(long, default_value = "4")]
    pub concurrency: usize,

    /// Writer buffer size in MB
    #[arg(long, default_value = "64")]
    pub writer_buffer: usize,

    /// Memory budget in MB
    #[arg(long, default_value = "1024")]
    pub mem_budget: usize,

    /// Don't recurse into subdirectories
    #[arg(long)]
    pub no_recursive: bool,

    /// Follow symbolic links
    #[arg(long)]
    pub follow_symlinks: bool,

    // State and resume options
    /// State file path for resumable operations
    #[arg(long)]
    pub state: Option<PathBuf>,

    /// Resume from state file
    #[arg(long)]
    pub resume: bool,

    /// Verify output integrity
    #[arg(long)]
    pub verify: bool,

    // Output options
    /// Show progress bar
    #[arg(long, default_value = "true")]
    pub progress: bool,

    /// No progress bar
    #[arg(long)]
    pub no_progress: bool,

    /// JSON structured logging
    #[arg(long)]
    pub json_logs: bool,

    /// Print execution plan and exit
    #[arg(long)]
    pub plan: bool,

    /// Dry run (don't write output)
    #[arg(long)]
    pub dry_run: bool,

    /// Verbose output (use multiple times for more verbosity)
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Quiet output
    #[arg(short = 'q', long)]
    pub quiet: bool,
}

#[derive(Clone, ValueEnum, Debug, Serialize, Deserialize)]
pub enum OutputFormat {
    Csv,
    Parquet,
}

#[derive(Clone, ValueEnum, Debug, Serialize, Deserialize)]
pub enum Compression {
    None,
    Snappy,
    Gzip,
    Zstd,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Csv => write!(f, "csv"),
            OutputFormat::Parquet => write!(f, "parquet"),
        }
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::None => write!(f, "none"),
            Compression::Snappy => write!(f, "snappy"),
            Compression::Gzip => write!(f, "gzip"),
            Compression::Zstd => write!(f, "zstd"),
        }
    }
}
