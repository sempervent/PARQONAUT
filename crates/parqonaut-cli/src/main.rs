mod batch;
mod location;
mod plugin;
mod repair;
mod scan;
mod serve;
mod stream;
mod transform;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "prqnt",
    version,
    about = "PARQONAUT — Parquet Analysis, Rewriting, Quality, Orchestration, Navigation, Auditing, Unification & Transformation"
)]
struct Cli {
    #[arg(long, global = true, help = "Emit JSON where supported")]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Forensic scan of local paths or s3:// dataset prefixes
    Scan {
        /// Local path or s3:// URI to scan
        path: String,
        #[arg(short, long, default_value = "standard")]
        profile: String,
        /// Scan analyzer plugin name (repeatable; explicit opt-in only)
        #[arg(long = "plugin")]
        plugins: Vec<String>,
    },
    /// Discover and validate installed analyzer plugins
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },
    /// Inspect Parquet schema and row-group metadata
    Inspect {
        input: PathBuf,
        #[arg(long, help = "Show detailed column statistics")]
        stats: bool,
    },
    /// Hive-style partition of a Parquet file
    Partition {
        input: PathBuf,
        #[arg(long = "output")]
        output: PathBuf,
        #[arg(long = "by")]
        partition_by: String,
        #[arg(long = "max-open-partitions", default_value_t = 64)]
        max_open_partitions: usize,
    },
    /// Merge Parquet files or a dataset directory
    Merge {
        /// Input file(s), directory, or glob
        input: String,
        #[arg(long = "output")]
        output: PathBuf,
        #[arg(long)]
        row_group_size_mb: Option<u64>,
    },
    /// Split a Parquet file into smaller parts
    Split {
        input: PathBuf,
        #[arg(long = "output")]
        output: PathBuf,
        #[arg(long)]
        target_size_mb: Option<u64>,
        #[arg(long)]
        target_row_groups: Option<usize>,
    },
    /// Declarative multi-step transform workflow
    Transform {
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        check: bool,
        #[arg(long)]
        dry_run: bool,
    },
    /// Rewrite or recompress a Parquet file
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
    /// Diagnose dataset repair opportunities from scan evidence
    Diagnose {
        /// Local path or s3:// dataset URI
        path: String,
    },
    /// Generate an evidence-bound repair plan
    Plan {
        /// Local path or s3:// dataset URI
        path: String,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long, help = "TOML policy file path")]
        policy: Option<PathBuf>,
        #[arg(long, help = "Explicit target schema JSON")]
        target_schema: Option<PathBuf>,
        #[arg(long, help = "Emit canonical deterministic plan JSON")]
        canonical: bool,
    },
    /// Diff two repair plans
    PlanDiff { left: PathBuf, right: PathBuf },
    /// CI-oriented policy compliance check (non-mutating)
    Check {
        /// Local path or s3:// dataset URI
        path: String,
        #[arg(long, help = "TOML CI policy file")]
        policy: Option<PathBuf>,
    },
    /// Execute a repair plan to a separate output directory or s3:// prefix
    Repair {
        /// Source dataset local path or s3:// URI
        path: String,
        #[arg(long)]
        plan: PathBuf,
        /// Output local directory or s3:// dataset prefix
        #[arg(long)]
        output: String,
        #[arg(long = "authorize", help = "Explicitly authorize a ReviewRequired operation ID")]
        authorize: Vec<String>,
    },
    /// Verify repaired dataset against a before scan
    Verify {
        /// Before dataset local path or s3:// URI
        before: String,
        after: PathBuf,
        #[arg(long, help = "Optional execution manifest path")]
        manifest: Option<PathBuf>,
    },
    /// Integrated scan → diagnose → plan (optional safe repair)
    Doctor {
        /// Local path or s3:// dataset URI
        path: String,
        #[arg(long, help = "TOML policy file path (same as plan)")]
        policy: Option<PathBuf>,
        #[arg(long, help = "Execute authorized repairs after showing the plan")]
        repair: bool,
        /// Output local directory or s3:// dataset prefix
        #[arg(long)]
        output: Option<String>,
        #[arg(long = "authorize")]
        authorize: Vec<String>,
    },
    /// Multi-dataset batch orchestration
    Batch {
        #[command(subcommand)]
        command: BatchCommand,
    },
    /// Run the PARQONAUT HTTP application server (`/api/v1`)
    Serve {
        #[command(flatten)]
        args: serve::ServeArgs,
    },
    /// Stream-convert CSV/Parquet inputs
    Convert {
        /// Input file(s), directories, or globs
        #[arg(required = true)]
        inputs: Vec<String>,
        #[arg(short = 'o', long = "out")]
        out: String,
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
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long)]
        resume: bool,
        #[arg(long, default_value = "strict")]
        schema_conflicts: String,
        #[arg(long = "json-progress")]
        json_progress: bool,
    },
}

#[derive(Subcommand, Debug)]
enum PluginCommand {
    /// List discovered plugins
    List,
    /// Show manifest, digest, and host compatibility
    Inspect { name: String },
    /// Validate a plugin root or manifest path (no execution)
    Validate { path: PathBuf },
}

#[derive(Subcommand, Debug)]
enum BatchCommand {
    /// Validate batch configuration without executing
    Check {
        #[arg(long)]
        config: PathBuf,
    },
    /// Produce a durable batch plan
    Plan {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Execute an approved batch plan
    Repair {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        jobs: Option<u32>,
        #[arg(long)]
        dry_run: bool,
        #[arg(long, hide = true, help = "Stop after N successful datasets (demo/tests)")]
        interrupt_after: Option<usize>,
    },
    /// Read run journal status
    Status {
        #[arg(long)]
        run_dir: PathBuf,
    },
    /// Resume an interrupted batch run
    Resume {
        #[arg(long)]
        run_dir: PathBuf,
        #[arg(long)]
        jobs: Option<u32>,
        #[arg(long)]
        dry_run: bool,
    },
    /// Verify batch outputs against the plan
    Verify {
        #[arg(long)]
        run_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("prqnt=info".parse().unwrap()))
        .init();

    let cli = Cli::parse();
    let json_output = cli.json;
    if let Err(e) = run(cli).await {
        if json_output {
            eprintln!("{}", serde_json::json!({ "ok": false, "error": e.to_string() }));
        } else {
            eprintln!("error: {e}");
        }
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Scan { path, profile, plugins } => {
            scan::run_scan(path, &profile, plugins, cli.json).await?
        }
        Command::Plugin { command } => match command {
            PluginCommand::List => plugin::run_list(cli.json)?,
            PluginCommand::Inspect { name } => plugin::run_inspect(&name, cli.json)?,
            PluginCommand::Validate { path } => plugin::run_validate(path, cli.json)?,
        },
        Command::Inspect { input, stats } => {
            transform::run_inspect(input, stats).await?;
        }
        Command::Partition { input, output, partition_by, max_open_partitions } => {
            transform::run_partition(input, output, partition_by, max_open_partitions).await?;
        }
        Command::Merge { input, output, row_group_size_mb } => {
            transform::run_merge(input, output, row_group_size_mb).await?;
        }
        Command::Split { input, output, target_size_mb, target_row_groups } => {
            transform::run_split(input, output, target_size_mb, target_row_groups).await?;
        }
        Command::Transform { spec, check, dry_run } => {
            transform::run_transform_spec(spec, check, dry_run, cli.json)?;
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
        Command::Diagnose { path } => repair::run_diagnose(path, cli.json).await?,
        Command::Plan { path, output, policy, target_schema, canonical } => {
            let policy_toml = policy.map(std::fs::read_to_string).transpose()?;
            repair::run_plan(path, cli.json, canonical, output, policy_toml, target_schema).await?;
        }
        Command::PlanDiff { left, right } => repair::run_plan_diff(left, right, cli.json).await?,
        Command::Check { path, policy } => repair::run_check(path, policy, cli.json).await?,
        Command::Repair { path, plan, output, authorize } => {
            repair::run_repair(path, plan, output, authorize).await?;
        }
        Command::Verify { before, after, manifest } => {
            repair::run_verify(before, after, manifest, cli.json).await?;
        }
        Command::Doctor { path, policy, repair, output, authorize } => {
            let policy_toml = policy.map(std::fs::read_to_string).transpose()?;
            repair::run_doctor(path, policy_toml, repair, output, authorize, cli.json).await?;
        }
        Command::Batch { command } => match command {
            BatchCommand::Check { config } => batch::run_check(config, cli.json).await?,
            BatchCommand::Plan { config, output } => {
                batch::run_plan(config, output, cli.json).await?
            }
            BatchCommand::Repair { plan, jobs, dry_run, interrupt_after } => {
                batch::run_repair(plan, jobs, dry_run, cli.json, interrupt_after).await?
            }
            BatchCommand::Status { run_dir } => batch::run_status(run_dir, cli.json).await?,
            BatchCommand::Resume { run_dir, jobs, dry_run } => {
                batch::run_resume(run_dir, jobs, dry_run, cli.json).await?
            }
            BatchCommand::Verify { run_dir } => batch::run_verify(run_dir, cli.json).await?,
        },
        Command::Convert {
            inputs,
            out,
            out_format,
            compression,
            zstd_level,
            plan,
            dry_run,
            state,
            resume,
            schema_conflicts,
            json_progress,
        } => {
            stream::run_convert(
                inputs,
                out,
                out_format,
                compression,
                zstd_level,
                plan,
                dry_run,
                state,
                resume,
                schema_conflicts,
                json_progress,
            )
            .await?;
        }
        Command::Serve { args } => serve::run(args).await?,
    }
    Ok(())
}
