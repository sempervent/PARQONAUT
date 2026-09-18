use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use parqonaut_transform::rewrite_parquet_with_cast;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

#[test]
fn cast_rewrite_processes_multiple_batches() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("large.parquet");
    let output = dir.path().join("cast.parquet");

    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
    let file = File::create(&input).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_size(64 * 1024)
        .build();
    let mut writer =
        ArrowWriter::try_new(BufWriter::new(file), schema.clone(), Some(props)).unwrap();
    for chunk in 0..32 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values((0..4096).map(|v| v + chunk * 4096)))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    rewrite_parquet_with_cast(
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "value",
        "INT64",
        None,
        None,
        true,
    )
    .unwrap();

    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        File::open(&output).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
    assert!(batches.len() > 1, "expected streaming batches, got {}", batches.len());
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 32 * 4096);
}
