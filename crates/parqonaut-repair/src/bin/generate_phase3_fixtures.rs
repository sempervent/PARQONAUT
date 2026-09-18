//! Reproducibly generate Phase 3 FRANKENLAKE v2 and supporting fixtures.
//!
//! Usage: cargo run -p parqonaut-repair --bin generate-phase3-fixtures [output_root]

use std::fs::{self, File};
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fixtures/phase3"));

    fs::create_dir_all(&root)?;
    write_frankenlake_v2(&root.join("frankenlake-v2"))?;
    write_explicit_target_scenario(&root.join("explicit-target"))?;
    write_rename_map_scenario(&root.join("rename-map"))?;

    println!("Phase 3 fixtures written under {}", root.display());
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
    path: &PathBuf,
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
            Arc::new(StringArray::from_iter_values((0..rows as usize).map(|i| format!("S{i:04}")))),
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

fn write_frankenlake_v2(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;

    let s32 = base_sensor_schema(DataType::Int32, DataType::Utf8, false);

    // 22 tiny shards: mixed compression, tiny row groups, missing statistics on half
    for i in 0..22 {
        let comp =
            if i % 2 == 0 { Compression::SNAPPY } else { Compression::ZSTD(ZstdLevel::default()) };
        let stats = i % 3 != 0;
        write_batch(
            &dir.join(format!("shard-{i:03}.parquet")),
            s32.clone(),
            vec![sensor_batch(s32.clone(), 400, i * 400)],
            writer_props(comp, 512, stats),
        )?;
    }

    // 3 files with int64 temperature (compatible numeric drift)
    let s64_temp = base_sensor_schema(DataType::Int64, DataType::Utf8, false);
    for i in 0..3 {
        write_batch(
            &dir.join(format!("wide-temp-{i:03}.parquet")),
            s64_temp.clone(),
            vec![sensor_batch_int64_temp(s64_temp.clone(), 400)],
            writer_props(Compression::ZSTD(ZstdLevel::default()), 128 * 1024 * 1024, true),
        )?;
    }

    // 2 files with nullable reading drift
    let s_null = base_sensor_schema(DataType::Int32, DataType::Utf8, true);
    for i in 0..2 {
        write_batch(
            &dir.join(format!("nullable-reading-{i:03}.parquet")),
            s_null.clone(),
            vec![sensor_batch(s_null.clone(), 400, 5000 + i * 400)],
            writer_props(Compression::SNAPPY, 512, false),
        )?;
    }

    // 1 incompatible sensor_id type (int64 vs utf8 elsewhere)
    let s_bad = base_sensor_schema(DataType::Int32, DataType::Int64, false);
    write_batch(
        &dir.join("sensor-id-outlier.parquet"),
        s_bad.clone(),
        vec![sensor_batch_int64_id(s_bad, 400)],
        writer_props(Compression::SNAPPY, 512, true),
    )?;

    // 1 oversized file (many rows, single row group)
    let mut big_batches = Vec::new();
    for chunk in 0..40 {
        big_batches.push(sensor_batch(s32.clone(), 5_000, chunk * 5_000));
    }
    write_batch(
        &dir.join("oversized-bulk.parquet"),
        s32.clone(),
        big_batches,
        writer_props(Compression::ZSTD(ZstdLevel::default()), 512 * 1024 * 1024, false),
    )?;

    Ok(())
}

/// Resolvable only with explicit target schema (string sensor_id canonical).
fn write_explicit_target_scenario(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let s_utf8 = base_sensor_schema(DataType::Int32, DataType::Utf8, false);
    write_batch(
        &dir.join("a.parquet"),
        s_utf8.clone(),
        vec![sensor_batch(s_utf8.clone(), 100, 0)],
        writer_props(Compression::ZSTD(ZstdLevel::default()), 128 * 1024 * 1024, true),
    )?;
    let s_int = base_sensor_schema(DataType::Int32, DataType::Int64, false);
    write_batch(
        &dir.join("b.parquet"),
        s_int.clone(),
        vec![sensor_batch_int64_id(s_int, 100)],
        writer_props(Compression::ZSTD(ZstdLevel::default()), 128 * 1024 * 1024, true),
    )?;
    Ok(())
}

fn write_rename_map_scenario(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("cust_id", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));
    for i in 0..2 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(
                    (0..50).map(|v| format!("C{}", v + i * 50)),
                )),
                Arc::new(Int32Array::from_iter_values((0..50).map(|v| v + i * 50))),
            ],
        )
        .unwrap();
        write_batch(
            &dir.join(format!("ledger-{i:02}.parquet")),
            schema.clone(),
            vec![batch],
            writer_props(Compression::ZSTD(ZstdLevel::default()), 128 * 1024 * 1024, true),
        )?;
    }
    Ok(())
}
