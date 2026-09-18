//! Reproducibly generate Phase 2 pathological Parquet fixtures.
//!
//! Usage: cargo run -p parqonaut-repair --bin generate-phase2-fixtures [output_root]

use std::fs::{self, File};
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fixtures/phase2"));

    fs::create_dir_all(&root)?;

    write_healthy(&root.join("healthy"))?;
    write_small_files(&root.join("small-files"))?;
    write_tiny_row_groups(&root.join("tiny-row-groups"))?;
    write_mixed_compression(&root.join("mixed-compression"))?;
    write_schema_drift(&root.join("schema-drift"))?;
    write_frankenlake(&root.join("frankenlake"))?;

    println!("Phase 2 fixtures written under {}", root.display());
    Ok(())
}

fn write_batch_file(
    path: &PathBuf,
    schema: Arc<Schema>,
    rows: i32,
    compression: Compression,
    max_row_group_size: usize,
    rows_per_batch: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(max_row_group_size)
        .build();
    let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema.clone(), Some(props))?;
    let mut written = 0;
    while written < rows {
        let chunk = rows_per_batch.min(rows - written);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(written..written + chunk))],
        )?;
        writer.write(&batch)?;
        written += chunk;
    }
    writer.close()?;
    Ok(())
}

fn base_schema(name: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]))
}

fn write_healthy(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let schema = base_schema("id");
    write_batch_file(
        &dir.join("data.parquet"),
        schema,
        10_000,
        Compression::ZSTD(ZstdLevel::default()),
        128 * 1024 * 1024,
        10_000,
    )
}

fn write_small_files(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let schema = base_schema("id");
    for i in 0..40 {
        write_batch_file(
            &dir.join(format!("part-{i:03}.parquet")),
            schema.clone(),
            500,
            Compression::ZSTD(ZstdLevel::default()),
            128 * 1024 * 1024,
            500,
        )?;
    }
    Ok(())
}

fn write_tiny_row_groups(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let schema = base_schema("id");
    write_batch_file(
        &dir.join("fragmented.parquet"),
        schema,
        5_000,
        Compression::ZSTD(ZstdLevel::default()),
        512,
        25,
    )
}

fn write_mixed_compression(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let schema = base_schema("id");
    write_batch_file(
        &dir.join("a.snappy.parquet"),
        schema.clone(),
        2_000,
        Compression::SNAPPY,
        128 * 1024 * 1024,
        2_000,
    )?;
    write_batch_file(
        &dir.join("b.zstd.parquet"),
        schema,
        2_000,
        Compression::ZSTD(ZstdLevel::default()),
        128 * 1024 * 1024,
        2_000,
    )?;
    Ok(())
}

fn write_schema_drift(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let s32 = base_schema("value");
    write_batch_file(
        &dir.join("int32.parquet"),
        s32,
        1_000,
        Compression::ZSTD(ZstdLevel::default()),
        128 * 1024 * 1024,
        1_000,
    )?;
    let s64 = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
    let file = File::create(dir.join("int64.parquet"))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(BufWriter::new(file), s64.clone(), Some(props))?;
    let batch = RecordBatch::try_new(s64, vec![Arc::new(Int64Array::from_iter_values(0..1000))])?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_frankenlake(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let s32 = base_schema("metric");
    for i in 0..30 {
        let comp =
            if i % 2 == 0 { Compression::SNAPPY } else { Compression::ZSTD(ZstdLevel::default()) };
        write_batch_file(
            &dir.join(format!("shard-{i:03}.parquet")),
            s32.clone(),
            400,
            comp,
            512,
            1,
        )?;
    }
    let s64 = Arc::new(Schema::new(vec![Field::new("metric", DataType::Int64, false)]));
    let file = File::create(dir.join("schema-outlier.parquet"))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(512)
        .build();
    let mut writer = ArrowWriter::try_new(BufWriter::new(file), s64.clone(), Some(props))?;
    let batch = RecordBatch::try_new(s64, vec![Arc::new(Int64Array::from_iter_values(0..400))])?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
