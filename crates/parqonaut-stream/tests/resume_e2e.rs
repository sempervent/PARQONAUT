use parqonaut_stream::{run, Cli};
use std::fs;
use tempfile::tempdir;

fn base_cli(
    inputs: Vec<String>,
    out: std::path::PathBuf,
    state: std::path::PathBuf,
    resume: bool,
) -> Cli {
    Cli {
        inputs,
        out: Some(out),
        out_format: Some(parqonaut_stream::cli::OutputFormat::Parquet),
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
        schema_conflicts: "strict".to_string(),
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
        state: Some(state),
        resume,
        verify: false,
        progress: false,
        no_progress: true,
        json_logs: false,
        json_progress: false,
        plan: false,
        dry_run: false,
        verbose: 0,
        quiet: true,
    }
}

#[tokio::test]
async fn resume_scenarios_sequential() {
    unsafe { std::env::remove_var("PARQONAUT_STREAM_INTERRUPT_AFTER") };

    // Interrupt + resume + idempotent second resume
    {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.csv");
        let b = dir.path().join("b.csv");
        fs::write(&a, "x,y\n1,2\n3,4\n").unwrap();
        fs::write(&b, "x,y\n5,6\n7,8\n").unwrap();
        let out = dir.path().join("out.parquet");
        let state = dir.path().join("checkpoint.json");
        let staging = dir.path().join("out.staging");

        unsafe { std::env::set_var("PARQONAUT_STREAM_INTERRUPT_AFTER", "0") };
        let cli = base_cli(
            vec![a.to_string_lossy().into(), b.to_string_lossy().into()],
            out.clone(),
            state.clone(),
            false,
        );
        let err = run(cli).await.unwrap_err();
        assert!(err.to_string().contains("interrupted"));
        assert!(state.exists());
        assert!(staging.exists());
        assert!(!out.exists());

        unsafe { std::env::remove_var("PARQONAUT_STREAM_INTERRUPT_AFTER") };
        let cli = base_cli(
            vec![a.to_string_lossy().into(), b.to_string_lossy().into()],
            out.clone(),
            state.clone(),
            true,
        );
        run(cli).await.unwrap();
        assert!(out.exists());

        let cli = base_cli(
            vec![a.to_string_lossy().into(), b.to_string_lossy().into()],
            out.clone(),
            state.clone(),
            true,
        );
        run(cli).await.unwrap();
    }

    // Stale checkpoint when source changes
    {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.csv");
        fs::write(&a, "x,y\n1,2\n").unwrap();
        let out = dir.path().join("out.parquet");
        let state = dir.path().join("checkpoint.json");

        let cli = base_cli(vec![a.to_string_lossy().into()], out.clone(), state.clone(), false);
        run(cli).await.unwrap();

        fs::write(&a, "x,y\n1,2\n9,9\n").unwrap();
        let cli = base_cli(vec![a.to_string_lossy().into()], out, state, true);
        let err = run(cli).await.unwrap_err();
        assert!(err.to_string().contains("STALE CHECKPOINT"));
    }
}
