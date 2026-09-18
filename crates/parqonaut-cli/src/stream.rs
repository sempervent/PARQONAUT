use parqonaut_stream::{run as run_stream, Cli as StreamCli};
use std::path::PathBuf;

pub async fn run_convert(
    inputs: Vec<String>,
    out: PathBuf,
    out_format: Option<parqonaut_stream::cli::OutputFormat>,
    compression: parqonaut_stream::cli::Compression,
    zstd_level: u32,
    plan: bool,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cli = StreamCli {
        inputs,
        out: Some(out),
        out_format,
        delimiter: None,
        quote: None,
        no_headers: false,
        encoding: "utf8".to_string(),
        na: "NA,null,\\N".to_string(),
        columns: None,
        exclude: None,
        rename: vec![],
        reorder: false,
        stringify_conflicts: false,
        infer_rows: 1000,
        roll_by_bytes: None,
        roll_by_rows: None,
        compression,
        zstd_level,
        concurrency: 4,
        writer_buffer: 64,
        mem_budget: 1024,
        no_recursive: false,
        follow_symlinks: false,
        state: None,
        resume: false,
        verify: false,
        progress: true,
        no_progress: false,
        json_logs: false,
        plan,
        dry_run,
        verbose: 0,
        quiet: false,
    };

    run_stream(cli).await?;
    Ok(())
}
