//! Deterministic benchmark fixture generators and RSS measurement helpers.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

/// Returns current process maximum resident set size in KiB (`getrusage(RUSAGE_SELF)`).
pub fn peak_rss_kib() -> u64 {
    #[cfg(unix)]
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
            #[cfg(target_os = "macos")]
            {
                // macOS reports ru_maxrss in bytes.
                return (usage.ru_maxrss as u64) / 1024;
            }
            #[cfg(not(target_os = "macos"))]
            {
                // Linux reports ru_maxrss in KiB.
                return usage.ru_maxrss as u64;
            }
        }
    }
    0
}

pub struct BenchFixtures {
    pub _root: TempDir,
    pub large_parquet: PathBuf,
    pub small_files_dir: PathBuf,
    pub deep_partitioned_dir: PathBuf,
    pub drift_dir: PathBuf,
    pub csv_input: PathBuf,
    pub out_dir: PathBuf,
}

impl BenchFixtures {
    pub fn generate(large_rows: usize, csv_rows: usize) -> Self {
        let root = tempfile::tempdir().expect("bench tempdir");
        let large_parquet = root.path().join("large.parquet");
        let small_files_dir = root.path().join("small_files");
        let deep_partitioned_dir = root.path().join("partitioned");
        let drift_dir = root.path().join("schema_drift");
        let csv_input = root.path().join("input.csv");
        let out_dir = root.path().join("out");

        fs::create_dir_all(&small_files_dir).unwrap();
        fs::create_dir_all(&deep_partitioned_dir).unwrap();
        fs::create_dir_all(&drift_dir).unwrap();
        fs::create_dir_all(&out_dir).unwrap();

        write_parquet_file(&large_parquet, large_rows, 8192, false);

        for i in 0..40 {
            let p = small_files_dir.join(format!("part-{i:03}.parquet"));
            write_parquet_file(&p, 250, 250, false);
        }

        for reg in ["us", "eu", "apac"] {
            for yr in [2024, 2025] {
                for mo in [1, 2, 3, 4] {
                    let dir =
                        deep_partitioned_dir.join(format!("region={reg}/year={yr}/month={mo:02}"));
                    fs::create_dir_all(&dir).unwrap();
                    write_parquet_file(&dir.join("part-000.parquet"), 100, 100, false);
                }
            }
        }

        for i in 0..4 {
            write_parquet_file(&drift_dir.join(format!("a-{i}.parquet")), 250, 250, false);
        }
        for i in 0..4 {
            write_parquet_file(&drift_dir.join(format!("b-{i}.parquet")), 250, 250, true);
        }

        let mut csv = BufWriter::new(File::create(&csv_input).unwrap());
        writeln!(csv, "id,region,score,label").unwrap();
        let regions = ["us", "eu", "apac", "latam"];
        for i in 0..csv_rows {
            writeln!(
                csv,
                "{i},{},{:.2},item_{}",
                regions[i % regions.len()],
                (i as f64) * 0.25,
                i % 50
            )
            .unwrap();
        }
        csv.flush().unwrap();

        Self {
            _root: root,
            large_parquet,
            small_files_dir,
            deep_partitioned_dir,
            drift_dir,
            csv_input,
            out_dir,
        }
    }
}

pub fn write_parquet_file(path: &Path, rows: usize, batch_size: usize, widen_id_i64: bool) {
    let id_field = if widen_id_i64 {
        Field::new("id", DataType::Int64, false)
    } else {
        Field::new("id", DataType::Int32, false)
    };
    let schema = Arc::new(Schema::new(vec![
        id_field,
        Field::new("region", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .set_max_row_group_row_count(Some(batch_size.max(64)))
        .build();
    let file = File::create(path).unwrap();
    let mut writer =
        ArrowWriter::try_new(BufWriter::new(file), schema.clone(), Some(props)).unwrap();

    let regions = ["us", "eu", "apac", "latam"];
    let mut written = 0usize;
    while written < rows {
        let take = (rows - written).min(batch_size.max(1));
        let id_col: Arc<dyn arrow::array::Array> = if widen_id_i64 {
            Arc::new(Int32Array::from_iter_values((written..written + take).map(|v| v as i32)))
        } else {
            Arc::new(Int32Array::from_iter_values((written..written + take).map(|v| v as i32)))
        };
        let id_col = if widen_id_i64 {
            Arc::new(arrow::array::Int64Array::from_iter_values(
                (written..written + take).map(|v| v as i64),
            )) as Arc<dyn arrow::array::Array>
        } else {
            id_col
        };
        let reg_col = Arc::new(StringArray::from_iter_values(
            (written..written + take).map(|v| regions[v % regions.len()]),
        ));
        let score_col = Arc::new(Float64Array::from_iter_values(
            (written..written + take).map(|v| (v as f64) * 1.25),
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![id_col, reg_col, score_col]).unwrap();
        writer.write(&batch).unwrap();
        written += take;
    }
    writer.close().unwrap();
}
