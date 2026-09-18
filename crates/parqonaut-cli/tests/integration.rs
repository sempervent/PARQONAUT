use assert_cmd::Command;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(name)
}

#[test]
fn scan_parquet_fixture() {
    let path = fixture("phase1/single_parquet/data.parquet");
    Command::cargo_bin("parqonaut")
        .unwrap()
        .args(["scan", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicates::str::contains("Scan complete"));
}

#[test]
fn rewrite_compression() {
    let temp = tempdir().unwrap();
    let input = fixture("phase1/single_parquet/data.parquet");
    let output = temp.path().join("rewritten.parquet");

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "rewrite",
            input.to_str().unwrap(),
            output.to_str().unwrap(),
            "--compression",
            "zstd",
        ])
        .assert()
        .success();

    let file = fs::File::open(&output).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    assert!(reader.metadata().num_row_groups() >= 1);
}

#[test]
fn convert_csv_to_parquet() {
    let temp = tempdir().unwrap();
    let csv = temp.path().join("data.csv");
    let out = temp.path().join("out.parquet");

    fs::write(&csv, "x,y\n1,2\n3,4\n").unwrap();

    Command::cargo_bin("parqonaut")
        .unwrap()
        .args([
            "convert",
            csv.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
            "--out-format",
            "parquet",
        ])
        .assert()
        .success();

    assert!(out.exists());
    let file = fs::File::open(&out).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let batches: Vec<_> = reader.build().unwrap().map(|b| b.unwrap()).collect();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}
