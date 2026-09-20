use parqonaut_transform::columnar_io::ColumnarPipelineIo;
use parqonaut_transform::{
    classify_io, merge_parquet_storage, rewrite_parquet_storage, RemoteIoKind,
};

fn parquet_fixture() -> Vec<u8> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(StringArray::from(vec!["x", "y"]))],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

#[test]
fn local_to_local_matrix_rewrite_and_merge() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.parquet");
    let b = dir.path().join("b.parquet");
    let out = dir.path().join("merged.parquet");
    std::fs::write(&a, parquet_fixture()).unwrap();
    std::fs::write(&b, parquet_fixture()).unwrap();

    let io = ColumnarPipelineIo::for_test();
    assert_eq!(
        classify_io(&a.to_string_lossy(), &out.to_string_lossy()).unwrap(),
        RemoteIoKind::LocalToLocal
    );

    rewrite_parquet_storage(
        &io,
        &a.to_string_lossy(),
        &dir.path().join("copy.parquet").to_string_lossy(),
    )
    .expect("rewrite");

    merge_parquet_storage(
        &io,
        &[a.to_string_lossy().into_owned(), b.to_string_lossy().into_owned()],
        &out.to_string_lossy(),
    )
    .expect("merge");
    assert!(out.exists());
}
