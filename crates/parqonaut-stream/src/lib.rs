//! Streaming CSV/Parquet conversion engine (maw lineage).
//!
//! Streaming conversion modules; see `docs/migration-analysis.md` for lineage.

pub mod cli;
pub mod coercion;
pub mod csv_in;
pub mod discover;
pub mod error;
pub mod parquet_in;
pub mod pipeline;
pub mod progress;
pub mod schema;
pub mod schema_introspect;
pub mod state;
pub mod writer_csv;
pub mod writer_parquet;

pub use cli::Cli;
pub use error::{MawError, Result};

use tracing::{info, Level};
#[allow(unused_imports)]
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

/// Run the streaming pipeline with logging configured from CLI flags.
pub async fn run(cli: Cli) -> Result<()> {
    init_logging(&cli)?;

    info!("Starting parqonaut-stream v{}", env!("CARGO_PKG_VERSION"));

    if cli.plan {
        info!("Plan mode: would process {} inputs", cli.inputs.len());
        for input in &cli.inputs {
            info!("  - {}", input);
        }
        return Ok(());
    }

    if cli.dry_run {
        info!("Dry run mode: would process inputs without writing output");
        return Ok(());
    }

    let pipeline = pipeline::Pipeline::new(cli);
    pipeline.execute().await
}

fn init_logging(cli: &Cli) -> Result<()> {
    let filter = if cli.verbose > 0 {
        let level = match cli.verbose {
            1 => Level::DEBUG,
            _ => Level::TRACE,
        };
        EnvFilter::new(format!("parqonaut_stream={}", level))
    } else if cli.quiet {
        EnvFilter::new("warn")
    } else {
        EnvFilter::from_default_env()
    };

    if cli.json_logs {
        let _ = fmt().json().with_env_filter(filter).try_init();
    } else {
        let _ = fmt().with_env_filter(filter).try_init();
    }

    Ok(())
}
