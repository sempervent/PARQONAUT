use parqonaut_stream::{run, Cli};
use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_csv_to_csv_identity() {
    let temp_dir = tempdir().unwrap();

    let csv1 = temp_dir.path().join("file1.csv");
    let csv2 = temp_dir.path().join("file2.csv");
    let output = temp_dir.path().join("output.csv");

    fs::write(&csv1, "a,b,c\n1,2,3\n4,5,6\n").unwrap();
    fs::write(&csv2, "a,b,c\n7,8,9\n10,11,12\n").unwrap();

    let cli = Cli {
        inputs: vec![csv1.to_string_lossy().to_string(), csv2.to_string_lossy().to_string()],
        out: Some(output.to_string_lossy().into_owned()),
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
    };

    run(cli).await.unwrap();

    let content = fs::read_to_string(&output).unwrap();
    assert!(content.contains("1,2,3"));
    assert!(content.contains("7,8,9"));
}

#[tokio::test]
async fn test_csv_to_parquet() {
    let temp_dir = tempdir().unwrap();
    let csv = temp_dir.path().join("input.csv");
    let output = temp_dir.path().join("output.parquet");

    fs::write(&csv, "a,b,c\n1,2,3\n4,5,6\n").unwrap();

    let cli = Cli {
        inputs: vec![csv.to_string_lossy().to_string()],
        out: Some(output.to_string_lossy().into_owned()),
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
        compression: parqonaut_stream::cli::Compression::Zstd,
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
        json_progress: false,
        plan: false,
        dry_run: false,
        verbose: 0,
        quiet: true,
    };

    run(cli).await.unwrap();
    assert!(output.exists());

    use parquet::file::reader::FileReader;
    let file = std::fs::File::open(&output).unwrap();
    let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 2);
}
