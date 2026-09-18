use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use parqonaut_transform::engine::pipeline::Pipeline;
use parqonaut_transform::engine::transforms::ProjectionTransform;
use parqonaut_transform::error::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_parquet(path: &std::path::Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]));

    let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["a", "b", "c", "d", "e"]);
    let value_array = Float64Array::from(vec![Some(1.1), Some(2.2), None, Some(4.4), Some(5.5)]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
        ],
    )?;

    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    let props = WriterProperties::builder().build();
    let mut arrow_writer = ArrowWriter::try_new(writer, schema, Some(props))?;
    arrow_writer.write(&batch)?;
    arrow_writer.close()?;

    Ok(())
}

#[test]
fn test_rewrite_with_projection() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("input.parquet");

    create_test_parquet(&input_path)?;

    let file = File::open(&input_path)?;
    let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let batch_reader = arrow_reader.build()?;
    let batches: Vec<RecordBatch> = batch_reader.collect::<std::result::Result<_, _>>()?;

    assert!(!batches.is_empty());
    let original_batch = &batches[0];
    assert_eq!(original_batch.num_columns(), 3);

    let mut pipeline = Pipeline::new();
    pipeline.add_transform(ProjectionTransform::new(vec![
        "id".to_string(),
        "name".to_string(),
    ]));

    let projected = pipeline.execute(original_batch.clone())?;
    assert_eq!(projected.num_columns(), 2);
    assert_eq!(projected.column_by_name("id").unwrap().len(), 5);
    assert_eq!(projected.column_by_name("name").unwrap().len(), 5);

    Ok(())
}

#[test]
fn test_create_and_read_parquet() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("test.parquet");

    create_test_parquet(&path)?;

    let file = File::open(&path)?;
    let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
    assert_eq!(arrow_reader.schema().fields().len(), 3);

    Ok(())
}
