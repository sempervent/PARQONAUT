#![cfg(feature = "storage")]

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parqonaut_columnar::BatchSource;
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::conditional::ConditionalCreate;
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::{S3Config, S3StorageBackend};
use parqonaut_stream::cli::Compression;
use parqonaut_stream::{Cli, Result as StreamResult};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

fn base_cli(inputs: Vec<String>, out: Option<String>) -> Cli {
    Cli {
        inputs,
        out,
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
        compression: Compression::None,
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
    }
}

fn require_endpoint() -> Option<String> {
    std::env::var("PARQONAUT_S3_ENDPOINT").ok()
}

fn bucket() -> String {
    std::env::var("PARQONAUT_S3_BUCKET").unwrap_or_else(|_| "parqonaut-test".into())
}

fn parquet_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

async fn put_object(backend: &S3StorageBackend, bucket: &str, key: &str, body: &[u8]) {
    let loc = ObjectLocation::S3 { bucket: bucket.into(), key: key.into() };
    backend
        .conditional_create(&loc, ConditionalCreate::must_not_exist(), Bytes::copy_from_slice(body))
        .await
        .expect("put");
}

async fn read_parquet_rows(uri: &str) -> (usize, Schema) {
    read_parquet_rows_inner(uri).await.unwrap_or_else(|e| panic!("read {uri}: {e}"))
}

async fn read_parquet_rows_inner(
    uri: &str,
) -> std::result::Result<(usize, Schema), parqonaut_columnar::ColumnarError> {
    let loc = ObjectLocation::parse(uri).unwrap();
    let backend: Arc<dyn StorageBackend> = if loc.is_remote() {
        Arc::new(S3StorageBackend::new(S3Config::from_env()).await)
    } else {
        Arc::new(parqonaut_storage::LocalStorageBackend::direct())
    };
    let source = parqonaut_storage::columnar::StorageParquetBatchSource::new(backend, loc);
    let schema = source.schema()?;
    let mut stream = parqonaut_columnar::BatchSource::into_stream(Box::new(source))?;
    let mut rows = 0usize;
    while let Some(b) = futures::StreamExt::next(&mut stream).await {
        rows += b?.num_rows();
    }
    Ok((rows, schema.as_ref().clone()))
}

#[tokio::test(flavor = "multi_thread")]
async fn convert_local_parquet_to_s3_only() -> StreamResult<()> {
    let Some(_endpoint) = require_endpoint() else {
        panic!("PARQONAUT_S3_ENDPOINT required (run `just s3-up`)");
    };
    let s3 = Arc::new(S3StorageBackend::new(S3Config::from_env()).await);
    let b = bucket();
    let prefix = format!("convert-debug/{}", uuid::Uuid::new_v4());
    let s3_out = format!("s3://{b}/{prefix}/out.parquet");
    let work = TempDir::new().unwrap();
    let local_in = work.path().join("in.parquet");
    std::fs::write(&local_in, parquet_bytes()).unwrap();
    let cli = base_cli(vec![local_in.to_string_lossy().into_owned()], Some(s3_out.clone()));
    parqonaut_stream::run(cli).await?;
    let (rows, _) = read_parquet_rows(&s3_out).await;
    assert_eq!(rows, 3);
    let loc = ObjectLocation::parse(&s3_out).unwrap();
    let _ = s3.delete_owned_object(&loc).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn convert_four_storage_legs() -> StreamResult<()> {
    let Some(endpoint) = require_endpoint() else {
        panic!("PARQONAUT_S3_ENDPOINT required (run `just s3-up`)");
    };
    let _ = endpoint;
    let s3 = Arc::new(S3StorageBackend::new(S3Config::from_env()).await);
    let b = bucket();
    let prefix = format!("convert-matrix/{}", uuid::Uuid::new_v4());
    let s3_in = format!("s3://{b}/{prefix}/in.parquet");
    let s3_out = format!("s3://{b}/{prefix}/out.parquet");
    put_object(&s3, &b, &format!("{prefix}/in.parquet"), &parquet_bytes()).await;

    let work = TempDir::new().unwrap();
    let local_in = work.path().join("in.parquet");
    let local_out = work.path().join("out.parquet");
    std::fs::write(&local_in, parquet_bytes()).unwrap();

    let legs: Vec<(&str, String, String)> = vec![
        ("L2L", local_in.to_string_lossy().into_owned(), local_out.to_string_lossy().into_owned()),
        ("L2S3", local_in.to_string_lossy().into_owned(), s3_out.clone()),
        ("S3L", s3_in.clone(), local_out.to_string_lossy().into_owned()),
        ("S3S3", s3_in.clone(), s3_out.clone()),
    ];

    for (label, input, output) in legs {
        if output.starts_with("s3://") {
            let loc = ObjectLocation::parse(&output).unwrap();
            let _ = s3.delete_owned_object(&loc).await;
        } else {
            let _ = std::fs::remove_file(&output);
        }

        let cli = base_cli(vec![input.clone()], Some(output.clone()));
        parqonaut_stream::run(cli)
            .await
            .map_err(|e| std::io::Error::other(format!("{label} convert: {e}")))?;
        let (rows, schema) = read_parquet_rows(&output).await;
        assert_eq!(rows, 3, "{label} row count");
        assert_eq!(schema.field(0).name(), "id", "{label} schema");
        assert_eq!(schema.field(1).name(), "name", "{label} schema");
    }
    Ok(())
}
