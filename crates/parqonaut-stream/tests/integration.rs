use parqonaut_stream::{run, Cli};
use std::fs;
use tempfile::tempdir;

fn base_cli(inputs: Vec<String>, out: Option<std::path::PathBuf>) -> Cli {
    Cli {
        inputs,
        out,
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
        schema_conflicts: "strict".to_string(),
        infer_rows: 1000,
        roll_by_bytes: None,
        roll_by_rows: None,
        compression: parqonaut_stream::cli::Compression::None,
        zstd_level: 3,
        concurrency: 4,
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
        json_progress: false,
        plan: false,
        dry_run: false,
        verbose: 0,
        quiet: true,
    }
}

#[tokio::test]
async fn test_csv_concatenation() {
    let temp_dir = tempdir().unwrap();
    let csv1 = temp_dir.path().join("file1.csv");
    let csv2 = temp_dir.path().join("file2.csv");
    let output = temp_dir.path().join("output.csv");

    fs::write(&csv1, "a,b,c\n1,2,3\n4,5,6\n").unwrap();
    fs::write(&csv2, "a,b,c\n7,8,9\n10,11,12\n").unwrap();

    let cli = base_cli(
        vec![csv1.to_string_lossy().to_string(), csv2.to_string_lossy().to_string()],
        Some(output.clone()),
    );
    run(cli).await.unwrap();

    let content = fs::read_to_string(&output).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 5);
}

#[tokio::test]
async fn test_directory_processing() {
    let temp_dir = tempdir().unwrap();
    let subdir = temp_dir.path().join("subdir");
    fs::create_dir(&subdir).unwrap();

    fs::write(subdir.join("file1.csv"), "x,y\n1,2\n").unwrap();
    fs::write(subdir.join("file2.csv"), "x,y\n3,4\n").unwrap();
    let output = temp_dir.path().join("output.csv");

    let cli = base_cli(vec![subdir.to_string_lossy().to_string()], Some(output.clone()));
    run(cli).await.unwrap();

    let content = fs::read_to_string(&output).unwrap();
    assert!(content.contains("1,2"));
    assert!(content.contains("3,4"));
}

#[tokio::test]
async fn test_plan_mode() {
    let temp_dir = tempdir().unwrap();
    let csv_file = temp_dir.path().join("test.csv");
    fs::write(&csv_file, "a,b\n1,2\n").unwrap();

    let mut cli = base_cli(vec![csv_file.to_string_lossy().to_string()], None);
    cli.plan = true;
    run(cli).await.unwrap();
}

#[tokio::test]
async fn test_dry_run() {
    let temp_dir = tempdir().unwrap();
    let csv_file = temp_dir.path().join("test.csv");
    fs::write(&csv_file, "a,b\n1,2\n").unwrap();

    let mut cli = base_cli(vec![csv_file.to_string_lossy().to_string()], None);
    cli.dry_run = true;
    run(cli).await.unwrap();
}
