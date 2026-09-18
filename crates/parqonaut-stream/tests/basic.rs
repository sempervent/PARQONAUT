use parqonaut_stream::{run, Cli};

#[tokio::test]
async fn test_plan_with_nonexistent_file_still_lists_input() {
    let cli = Cli {
        inputs: vec!["nonexistent.csv".to_string()],
        out: None,
        out_format: None,
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
        compression: parqonaut_stream::cli::Compression::None,
        zstd_level: 3,
        concurrency: 1,
        writer_buffer: 64,
        mem_budget: 1024,
        no_recursive: false,
        follow_symlinks: false,
        state: None,
        resume: false,
        verify: false,
        progress: false,
        no_progress: true,
        json_logs: false,
        plan: true,
        dry_run: false,
        verbose: 0,
        quiet: true,
    };

    run(cli).await.unwrap();
}
