mod scan;
mod stream;
mod transform;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "parqonaut",
    version,
    about = "Parquet Analysis, Rewriting, Quality, Orchestration, Navigation, Auditing, Unification & Transformation"
)]
struct Cli {
    #[arg(long, global = true, help = "Emit JSON where supported")]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Forensic scan of local files or directories (Paraclete engine)
    Scan {
        /// File or directory to scan
        path: PathBuf,
        #[arg(short, long, default_value = "standard")]
        profile: String,
    },
    /// Inspect Parquet file metadata (parqknife engine)
    Inspect {
        input: PathBuf,
        #[arg(long, help = "Show detailed column statistics")]
        stats: bool,
    },
    /// Rewrite Parquet with optional recompression (parqknife engine)
    Rewrite {
        input: PathBuf,
        output: PathBuf,
        #[arg(long, help = "Compression codec (snappy, gzip, zstd, uncompressed)")]
        compression: Option<String>,
        #[arg(long)]
        row_group_size_mb: Option<u64>,
        #[arg(long)]
        projection: Option<String>,
        #[arg(long)]
        filter: Option<String>,
        #[arg(long)]
        rebuild_stats: bool,
    },
    /// Stream-convert CSV/Parquet inputs (maw engine)
    Convert {
        /// Input file(s), directories, or globs
        #[arg(required = true)]
        inputs: Vec<String>,
        #[arg(short = 'o', long = "out")]
        out: PathBuf,
        #[arg(long = "out-format")]
        out_format: Option<parqonaut_stream::cli::OutputFormat>,
        #[arg(long, default_value = "none")]
        compression: parqonaut_stream::cli::Compression,
        #[arg(long, default_value_t = 3)]
        zstd_level: u32,
        #[arg(long)]
        plan: bool,
        #[arg(long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("parqonaut=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Scan { path, profile } => scan::run_scan(path, &profile, cli.json).await?,
        Command::Inspect { input, stats } => {
            transform::run_inspect(input, stats).await?;
        }
        Command::Rewrite {
            input,
            output,
            compression,
            row_group_size_mb,
            projection,
            filter,
            rebuild_stats,
        } => {
            transform::run_rewrite(
                input,
                output,
                compression,
                row_group_size_mb,
                projection,
                filter,
                rebuild_stats,
            )
            .await?;
        }
        Command::Convert { inputs, out, out_format, compression, zstd_level, plan, dry_run } => {
            stream::run_convert(inputs, out, out_format, compression, zstd_level, plan, dry_run)
                .await?;
        }
    }
    Ok(())
}
