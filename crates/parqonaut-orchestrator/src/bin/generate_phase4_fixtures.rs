//! Reproducibly generate Phase 4 SHIPWRECK fixture fleet.
//!
//! Usage: cargo run -p parqonaut-orchestrator --bin generate-phase4-fixtures [output_root]

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

#[derive(Debug, Clone, Copy)]
struct DatasetStats {
    files: u32,
    rows: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fixtures/phase4/shipwreck"));

    fs::create_dir_all(&root)?;

    let datasets = [
        ("01-healthy", write_healthy(&root.join("01-healthy"))?),
        ("02-safe-repairs", write_safe_repairs(&root.join("02-safe-repairs"))?),
        ("03-schema-review", write_schema_review(&root.join("03-schema-review"))?),
        ("04-blocked-schema", write_blocked_schema(&root.join("04-blocked-schema"))?),
        ("05-stale-source", write_stale_source(&root.join("05-stale-source"))?),
        ("06-corrupt-input", write_corrupt_input(&root.join("06-corrupt-input"))?),
        ("07-large-streaming", write_large_streaming(&root.join("07-large-streaming"))?),
        (
            "08-interrupt-resume",
            write_interrupt_resume(&root.join("08-interrupt-resume"))?,
        ),
    ];

    println!("Phase 4 SHIPWRECK fixtures written under {}", root.display());
    println!();
    println!("dataset              files    rows");
    println!("-------------------- -------- ------------");
    for (name, stats) in datasets {
        println!("{name:<20} {files:>8} {rows:>12}", files = stats.files, rows = stats.rows);
    }
    Ok(())
}

fn writer_props(
    compression: Compression,
    max_row_group_size: usize,
    stats: bool,
) -> WriterProperties {
    let mut b = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(max_row_group_size);
    b = b.set_statistics_enabled(if stats {
        EnabledStatistics::Chunk
    } else {
        EnabledStatistics::None
    });
    b.build()
}

fn write_batch(
    path: &Path,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    props: WriterProperties,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema, Some(props))?;
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

fn id_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn id_batch(schema: Arc<Schema>, rows: i32, offset: i32) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values(offset..offset + rows))],
    )
    .unwrap()
}

fn write_id_file(
    path: &Path,
    rows: i32,
    offset: i32,
    compression: Compression,
    max_row_group_size: usize,
    rows_per_batch: i32,
    stats: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = id_schema();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let props = writer_props(compression, max_row_group_size, stats);
    let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema.clone(), Some(props))?;
    let mut written = 0;
    while written < rows {
        let chunk = rows_per_batch.min(rows - written);
        writer.write(&id_batch(schema.clone(), chunk, offset + written))?;
        written += chunk;
    }
    writer.close()?;
    Ok(())
}

fn base_sensor_schema(
    temp_type: DataType,
    sensor_type: DataType,
    reading_nullable: bool,
) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("temperature", temp_type, false),
        Field::new("sensor_id", sensor_type, false),
        Field::new("reading", DataType::Int32, reading_nullable),
    ]))
}

fn sensor_batch(schema: Arc<Schema>, rows: i32, temp_offset: i32) -> RecordBatch {
    let n = rows as usize;
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(temp_offset..temp_offset + rows)),
            Arc::new(StringArray::from_iter_values((0..n).map(|i| format!("S{i:04}")))),
            Arc::new(Int32Array::from_iter_values((0..rows).map(|v| v * 10))),
        ],
    )
    .unwrap()
}

fn sensor_batch_int64_temp(schema: Arc<Schema>, rows: i32) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values((0..rows).map(|v| v as i64))),
            Arc::new(StringArray::from_iter_values(
                (0..rows as usize).map(|i| format!("S{i:04}")),
            )),
            Arc::new(Int32Array::from_iter_values((0..rows).map(|v| v * 10))),
        ],
    )
    .unwrap()
}

fn sensor_batch_int64_id(schema: Arc<Schema>, rows: i32) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..rows)),
            Arc::new(Int64Array::from_iter_values((0..rows).map(|v| v as i64 + 9000))),
            Arc::new(Int32Array::from_iter_values((0..rows).map(|v| v * 10))),
        ],
    )
    .unwrap()
}

/// Clean dataset: no Safe or ReviewRequired repair work expected.
fn write_healthy(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    write_id_file(
        &dir.join("data.parquet"),
        2_000,
        0,
        Compression::ZSTD(ZstdLevel::default()),
        128 * 1024 * 1024,
        2_000,
        true,
    )?;
    Ok(DatasetStats { files: 1, rows: 2_000 })
}

/// Small files, compression drift, tiny row groups, missing statistics.
fn write_safe_repairs(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let mut files = 0;
    let mut rows = 0u64;

    for i in 0..12 {
        let comp =
            if i % 2 == 0 { Compression::SNAPPY } else { Compression::ZSTD(ZstdLevel::default()) };
        let stats = i % 3 != 0;
        write_id_file(
            &dir.join(format!("part-{i:03}.parquet")),
            400,
            i * 400,
            comp,
            512,
            25,
            stats,
        )?;
        files += 1;
        rows += 400;
    }

    write_id_file(
        &dir.join("fragmented.parquet"),
        2_000,
        5_000,
        Compression::SNAPPY,
        512,
        50,
        false,
    )?;
    files += 1;
    rows += 2_000;

    Ok(DatasetStats { files, rows })
}

/// Lossless ReviewRequired schema reconciliation (numeric widening).
fn write_schema_review(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let s32 = base_sensor_schema(DataType::Int32, DataType::Utf8, false);
    write_batch(
        &dir.join("baseline.parquet"),
        s32.clone(),
        vec![sensor_batch(s32.clone(), 200, 0)],
        writer_props(Compression::ZSTD(ZstdLevel::default()), 128 * 1024 * 1024, true),
    )?;

    let s64_temp = base_sensor_schema(DataType::Int64, DataType::Utf8, false);
    write_batch(
        &dir.join("widened-temp.parquet"),
        s64_temp.clone(),
        vec![sensor_batch_int64_temp(s64_temp, 200)],
        writer_props(Compression::ZSTD(ZstdLevel::default()), 128 * 1024 * 1024, true),
    )?;

    Ok(DatasetStats { files: 2, rows: 400 })
}

/// Incompatible schema conflict; repair must remain blocked.
fn write_blocked_schema(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let s_utf8 = base_sensor_schema(DataType::Int32, DataType::Utf8, false);
    write_batch(
        &dir.join("a.parquet"),
        s_utf8.clone(),
        vec![sensor_batch(s_utf8.clone(), 150, 0)],
        writer_props(Compression::ZSTD(ZstdLevel::default()), 128 * 1024 * 1024, true),
    )?;

    let s_int = base_sensor_schema(DataType::Int32, DataType::Int64, false);
    write_batch(
        &dir.join("b.parquet"),
        s_int.clone(),
        vec![sensor_batch_int64_id(s_int, 150)],
        writer_props(Compression::ZSTD(ZstdLevel::default()), 128 * 1024 * 1024, true),
    )?;

    Ok(DatasetStats { files: 2, rows: 300 })
}

/// Valid initial snapshot; integration tests mutate after plan generation.
fn write_stale_source(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    for i in 0..3 {
        write_id_file(
            &dir.join(format!("snapshot-{i:02}.parquet")),
            300,
            i * 300,
            Compression::ZSTD(ZstdLevel::default()),
            128 * 1024 * 1024,
            300,
            true,
        )?;
    }
    Ok(DatasetStats { files: 3, rows: 900 })
}

/// One valid file plus deliberately unreadable Parquet payload.
fn write_corrupt_input(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    write_id_file(
        &dir.join("valid.parquet"),
        100,
        0,
        Compression::ZSTD(ZstdLevel::default()),
        128 * 1024 * 1024,
        100,
        true,
    )?;

    let corrupt_path = dir.join("truncated-footer.parquet");
    write_id_file(
        &corrupt_path,
        100,
        100,
        Compression::ZSTD(ZstdLevel::default()),
        128 * 1024 * 1024,
        100,
        true,
    )?;
    let meta = fs::metadata(&corrupt_path)?;
    let truncate_to = meta.len().saturating_sub(32);
    let file = fs::OpenOptions::new().write(true).open(&corrupt_path)?;
    file.set_len(truncate_to)?;
    file.sync_all()?;

    let mut garbage = File::create(dir.join("not-parquet.bin"))?;
    garbage.write_all(b"PAR1this-is-not-a-valid-parquet-file")?;

    Ok(DatasetStats { files: 3, rows: 200 })
}

/// Large enough for multiple streaming batches without huge opaque blobs.
fn write_large_streaming(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    const ROWS_PER_FILE: i32 = 50_000;
    const FILE_COUNT: u32 = 15;
    const TOTAL_ROWS: u64 = ROWS_PER_FILE as u64 * FILE_COUNT as u64;

    for i in 0..FILE_COUNT {
        write_id_file(
            &dir.join(format!("stream-{i:03}.parquet")),
            ROWS_PER_FILE,
            i as i32 * ROWS_PER_FILE,
            Compression::ZSTD(ZstdLevel::default()),
            10_000,
            10_000,
            true,
        )?;
    }

    Ok(DatasetStats {
        files: FILE_COUNT,
        rows: TOTAL_ROWS,
    })
}

/// Medium safe-repair workload for deterministic interrupt/resume injection.
fn write_interrupt_resume(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let mut files = 0;
    let mut rows = 0u64;

    for i in 0..8 {
        let comp =
            if i % 2 == 0 { Compression::SNAPPY } else { Compression::ZSTD(ZstdLevel::default()) };
        write_id_file(
            &dir.join(format!("chunk-{i:03}.parquet")),
            800,
            i * 800,
            comp,
            512,
            40,
            i % 2 == 0,
        )?;
        files += 1;
        rows += 800;
    }

    Ok(DatasetStats { files, rows })
}
