#![cfg(feature = "s3")]
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt;
use parqonaut_columnar::BatchSource;
use parqonaut_storage::columnar::StorageParquetBatchSource;
use parqonaut_storage::location::ObjectLocation;
use parqonaut_transform::ColumnarPipelineIo;

pub fn require_s3_endpoint() -> bool {
    if std::env::var("PARQONAUT_S3_ENDPOINT").is_err()
        && std::env::var("PARQONAUT_S3_INTEGRATION").as_deref() == Ok("1")
    {
        panic!("PARQONAUT_S3_INTEGRATION=1 requires PARQONAUT_S3_ENDPOINT");
    }
    std::env::var("PARQONAUT_S3_ENDPOINT").is_ok()
}

pub fn test_bucket() -> String {
    std::env::var("FOGBANK_BUCKET")
        .or_else(|_| std::env::var("PARQONAUT_S3_BUCKET"))
        .unwrap_or_else(|_| "parqonaut-test".into())
}

pub async fn s3_io() -> (ColumnarPipelineIo, Arc<parqonaut_storage::S3StorageBackend>) {
    let endpoint = std::env::var("PARQONAUT_S3_ENDPOINT").unwrap();
    let s3 = Arc::new(
        parqonaut_storage::S3StorageBackend::new(parqonaut_storage::S3Config::minio(endpoint))
            .await,
    );
    let io = ColumnarPipelineIo::for_test_s3(Arc::clone(&s3));
    (io, s3)
}

pub async fn put_s3_bytes(
    backend: &parqonaut_storage::S3StorageBackend,
    bucket: &str,
    key: &str,
    data: &[u8],
) {
    use bytes::Bytes;
    use parqonaut_storage::backend::StorageBackend;
    use parqonaut_storage::conditional::ConditionalCreate;
    let object = ObjectLocation::S3 { bucket: bucket.into(), key: key.into() };
    backend
        .conditional_create(
            &object,
            ConditionalCreate::must_not_exist(),
            Bytes::copy_from_slice(data),
        )
        .await
        .expect("put fixture");
}

pub fn parquet_fixture(rows: i64) -> Vec<u8> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    let n = rows as usize;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("n{i}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(names))],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

pub fn partition_fixture() -> Vec<u8> {
    std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/transform/partition-basic/input.parquet"),
    )
    .expect("partition fixture")
}

pub fn collect_parquet_outputs(root: &str) -> Vec<String> {
    if root.starts_with("s3://") {
        return vec![root.to_string()];
    }
    let path = std::path::Path::new(root);
    let mut files = Vec::new();
    collect_parquet_local(path, &mut files);
    files.sort();
    files
}

fn collect_parquet_local(path: &std::path::Path, out: &mut Vec<String>) {
    if path.is_file() {
        if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path.to_string_lossy().into_owned());
        }
        return;
    }
    if !path.is_dir() {
        return;
    }
    for entry in std::fs::read_dir(path).expect("read output dir") {
        let entry = entry.expect("entry");
        collect_parquet_local(&entry.path(), out);
    }
}

pub async fn collect_parquet_outputs_async(
    root: &str,
    s3: &Arc<parqonaut_storage::S3StorageBackend>,
) -> Vec<String> {
    if root.starts_with("s3://") {
        use parqonaut_storage::inventory::list_remote_inventory;
        use parqonaut_storage::location::DatasetLocation;
        let prefix = format!("{root}/");
        let dataset = DatasetLocation::parse(prefix.as_str()).expect("dataset");
        let inv = list_remote_inventory(s3.as_ref(), &dataset).await.expect("inventory");
        let mut uris = Vec::new();
        for o in &inv.objects {
            let Some(rel) = inv.relative_key(o) else {
                continue;
            };
            if !rel.ends_with(".parquet") {
                continue;
            }
            if let ObjectLocation::S3 { bucket, key } = &o.location {
                uris.push(format!("s3://{bucket}/{key}"));
            }
        }
        return uris;
    }
    collect_parquet_outputs(root)
}

pub async fn count_parquet_rows(s3: &Arc<parqonaut_storage::S3StorageBackend>, uri: &str) -> usize {
    let loc = ObjectLocation::parse(uri).expect("location");
    let backend: Arc<dyn parqonaut_storage::backend::StorageBackend> = if loc.is_remote() {
        Arc::clone(s3) as Arc<dyn parqonaut_storage::backend::StorageBackend>
    } else {
        Arc::new(parqonaut_storage::LocalStorageBackend::direct())
    };
    let source = StorageParquetBatchSource::new(backend, loc);
    let mut stream = Box::new(source).into_stream().expect("stream");
    let mut rows = 0usize;
    while let Some(b) = stream.next().await {
        rows += b.expect("batch").num_rows();
    }
    rows
}

pub struct LegPaths {
    pub input: String,
    pub output: String,
}

#[derive(Clone, Copy, Debug)]
pub enum Leg {
    LocalLocal,
    LocalS3,
    S3Local,
    S3S3,
}

impl Leg {
    pub const ALL: [Leg; 4] = [Leg::LocalLocal, Leg::LocalS3, Leg::S3Local, Leg::S3S3];

    pub fn output_uri(
        &self,
        work: &tempfile::TempDir,
        bucket: &str,
        cap: &str,
        file: &str,
    ) -> String {
        let id = uuid::Uuid::new_v4();
        match self {
            Leg::LocalLocal | Leg::S3Local => {
                work.path().join(format!("{cap}-{id}-{file}")).to_string_lossy().into_owned()
            }
            Leg::LocalS3 | Leg::S3S3 => format!("s3://{bucket}/matrix/{cap}/{id}/{file}"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn materialize(
        &self,
        work: &std::path::Path,
        s3: &Arc<parqonaut_storage::S3StorageBackend>,
        bucket: &str,
        cap: &str,
        in_name: &str,
        out_name: &str,
        bytes: &[u8],
    ) -> LegPaths {
        let id = uuid::Uuid::new_v4();
        let input = match self {
            Leg::LocalLocal | Leg::LocalS3 => {
                let p = work.join(format!("{cap}-{id}-{in_name}"));
                std::fs::write(&p, bytes).expect("local input");
                p.to_string_lossy().into_owned()
            }
            Leg::S3Local | Leg::S3S3 => {
                let key = format!("matrix/{cap}/{id}/{in_name}");
                put_s3_bytes(s3, bucket, &key, bytes).await;
                format!("s3://{bucket}/{key}")
            }
        };
        let output = self.output_uri_at(work, bucket, cap, out_name);
        LegPaths { input, output }
    }

    fn output_uri_at(&self, work: &std::path::Path, bucket: &str, cap: &str, file: &str) -> String {
        let id = uuid::Uuid::new_v4();
        match self {
            Leg::LocalLocal | Leg::S3Local => {
                work.join(format!("{cap}-{id}-{file}")).to_string_lossy().into_owned()
            }
            Leg::LocalS3 | Leg::S3S3 => format!("s3://{bucket}/matrix/{cap}/{id}/{file}"),
        }
    }
}

pub fn base_stream_cli() -> parqonaut_stream::Cli {
    parqonaut_stream::Cli {
        inputs: vec![],
        out: None,
        out_format: None,
        delimiter: None,
        quote: None,
        no_headers: false,
        encoding: "utf8".into(),
        na: "NA,null,\\N".into(),
        columns: None,
        exclude: None,
        rename: vec![],
        reorder: false,
        stringify_conflicts: false,
        schema_conflicts: "strict".into(),
        infer_rows: 1000,
        roll_by_bytes: None,
        roll_by_rows: None,
        compression: parqonaut_stream::cli::Compression::None,
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
