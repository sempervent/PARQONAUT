//! Integration tests for storage-aware repair execution.

use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use camino::Utf8Path;
use chrono::Utc;
use parqonaut_repair::{
    compute_dataset_fingerprint, requires_storage_execution, scan_directory, EffectivePolicy,
    RepairExecutor, RepairPlan, PARQONAUT_VERSION, PLAN_SCHEMA_VERSION,
};
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::capabilities::StorageCapabilities;
use parqonaut_storage::conditional::ConditionalCreate;
use parqonaut_storage::inventory::list_remote_inventory;
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
use parqonaut_storage::memory::MemoryStorageBackend;
use parqonaut_storage::publication::{
    is_version_committed, read_current_version, PublicationVersionId,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
fn memory_backend() -> MemoryStorageBackend {
    MemoryStorageBackend::new(StorageCapabilities {
        range_reads: true,
        stream_reads: true,
        conditional_create: true,
        conditional_replace: true,
        ..StorageCapabilities::S3
    })
}

async fn put_bytes(backend: &MemoryStorageBackend, bucket: &str, key: &str, data: &[u8]) {
    let object = ObjectLocation::S3 { bucket: bucket.into(), key: key.into() };
    backend
        .conditional_create(
            &object,
            ConditionalCreate::must_not_exist(),
            Bytes::copy_from_slice(data),
        )
        .await
        .unwrap();
}

fn write_small_parquet(path: &std::path::Path) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer =
        ArrowWriter::try_new(BufWriter::new(file), schema.clone(), Some(props)).unwrap();
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(0..128))]).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    std::fs::read(path).unwrap()
}

fn plan_for_local_root(root: &Utf8Path, scan: &paraclete_types::ScanReport) -> RepairPlan {
    let inventory = parqonaut_repair::DatasetInventory::from_scan_report(root, scan).unwrap();
    let fingerprint = compute_dataset_fingerprint(root, &inventory).unwrap();
    RepairPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        plan_id: "test-plan".into(),
        parqonaut_version: PARQONAUT_VERSION.to_string(),
        dataset_root: root.as_str().to_string(),
        dataset_fingerprint: fingerprint,
        policy_fingerprint: EffectivePolicy::default().fingerprint(),
        generated_at: Utc::now(),
        source_scan_id: scan.metadata.scan_id,
        policy: EffectivePolicy::default(),
        operations: vec![],
        expected_outcomes: vec![],
        diagnosis_findings: vec![],
        schema_diff: None,
        schema_conflicts: None,
        target_schema: None,
    }
}

#[test]
fn requires_storage_for_cross_backend_paths() {
    let local = DatasetLocation::parse("/tmp/data").unwrap();
    let remote = DatasetLocation::parse("s3://bucket/out/").unwrap();
    assert!(!requires_storage_execution(&local, &local));
    assert!(requires_storage_execution(&local, &remote));
    assert!(requires_storage_execution(&remote, &local));
    assert!(requires_storage_execution(&remote, &remote));
}

#[tokio::test]
async fn local_to_remote_publishes_committed_version() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    std::fs::create_dir_all(&source).unwrap();
    write_small_parquet(&source.join("part-000.parquet"));

    let root = Utf8Path::from_path(source.as_path()).unwrap();
    let scan = scan_directory(root).unwrap();
    let plan = plan_for_local_root(root, &scan);

    let backend = memory_backend();
    let output = DatasetLocation::parse("s3://repaired-bucket/datasets/demo/").unwrap();
    let report = RepairExecutor::default()
        .execute_storage(&plan, &output, &scan, &backend, &backend, "run-local-to-s3")
        .await
        .unwrap();

    assert!(report.success);
    assert!(report.output_path.starts_with("s3://"));
    assert!(is_version_committed(
        &backend,
        &output,
        &PublicationVersionId::from_run_id("run-local-to-s3")
    )
    .await
    .unwrap());
    assert_eq!(
        read_current_version(&backend, &output).await.unwrap().as_deref(),
        Some("run-local-to-s3")
    );
}

#[tokio::test]
async fn remote_to_local_streams_seed_without_full_dataset_buffer() {
    let backend = memory_backend();
    let dir = tempfile::tempdir().unwrap();
    let bytes = write_small_parquet(&dir.path().join("seed.parquet"));
    put_bytes(&backend, "src-bucket", "datasets/src/part-000.parquet", &bytes).await;

    let source = DatasetLocation::parse("s3://src-bucket/datasets/src/").unwrap();
    let inventory = list_remote_inventory(&backend, &source).await.unwrap();
    assert_eq!(inventory.objects.len(), 1);

    let root = parqonaut_repair::location_root(&source);
    let scan = parqonaut_repair::scan_dataset(&source, &backend).await.unwrap();
    let mut plan = plan_for_local_root(&root, &scan);
    plan.dataset_root = source.display_uri();

    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("repaired");
    let output = DatasetLocation::Local(parqonaut_storage::location::LocalLocation {
        path: Utf8Path::from_path(&out_path).unwrap().to_path_buf(),
    });

    let report = RepairExecutor::default()
        .execute_storage(&plan, &output, &scan, &backend, &backend, "run-s3-to-local")
        .await
        .unwrap();

    assert!(report.success);
    assert!(out_path.join("part-000.parquet").exists());
}

#[tokio::test]
async fn remote_to_remote_repairs_via_staging() {
    let backend = memory_backend();
    let dir = tempfile::tempdir().unwrap();
    let bytes = write_small_parquet(&dir.path().join("seed.parquet"));
    put_bytes(&backend, "src", "in/part-000.parquet", &bytes).await;

    let source = DatasetLocation::parse("s3://src/in/").unwrap();
    let scan = parqonaut_repair::scan_dataset(&source, &backend).await.unwrap();
    let root = parqonaut_repair::location_root(&source);
    let mut plan = plan_for_local_root(&root, &scan);
    plan.dataset_root = source.display_uri();
    let output = DatasetLocation::parse("s3://dst/out/").unwrap();

    RepairExecutor::default()
        .execute_storage(&plan, &output, &scan, &backend, &backend, "run-s3-to-s3")
        .await
        .unwrap();

    assert!(is_version_committed(
        &backend,
        &output,
        &PublicationVersionId::from_run_id("run-s3-to-s3")
    )
    .await
    .unwrap());
    assert_eq!(
        read_current_version(&backend, &output).await.unwrap().as_deref(),
        Some("run-s3-to-s3")
    );
}
