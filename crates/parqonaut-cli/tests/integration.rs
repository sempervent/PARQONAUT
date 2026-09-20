use assert_cmd::Command;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(name)
}

fn data_fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data").join(name)
}

#[test]
fn prqnt_version_and_help() {
    Command::cargo_bin("prqnt")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicates::str::contains("0.9.0"));
    Command::cargo_bin("prqnt")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains("PARQONAUT"));
}

#[test]
fn scan_dummy_parquet_data_fixture() {
    let path = data_fixture("dummy.parquet");
    assert!(path.exists(), "missing committed data fixture: {}", path.display());
    Command::cargo_bin("prqnt")
        .unwrap()
        .args(["scan", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicates::str::contains("Scan complete"));
}

#[test]
fn inspect_dummy_parquet_data_fixture() {
    let path = data_fixture("dummy.parquet");
    Command::cargo_bin("prqnt")
        .unwrap()
        .args(["inspect", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicates::str::contains("region"));
}

#[test]
fn dummy_parquet_has_expected_row_count() {
    let path = data_fixture("dummy.parquet");
    let file = fs::File::open(&path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 100_000);
}

#[test]
fn scan_parquet_fixture() {
    let path = fixture("scan/single_parquet/data.parquet");
    Command::cargo_bin("prqnt")
        .unwrap()
        .args(["scan", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicates::str::contains("Scan complete"));
}

#[test]
fn rewrite_compression() {
    let temp = tempdir().unwrap();
    let input = fixture("scan/single_parquet/data.parquet");
    let output = temp.path().join("rewritten.parquet");

    Command::cargo_bin("prqnt")
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

    Command::cargo_bin("prqnt")
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
