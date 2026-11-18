#![cfg(feature = "s3")]

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use common::matrix_common::{require_s3_endpoint, s3_io, test_bucket, Leg};
use parqonaut_plugin_host::CancelToken;
use parqonaut_transform::{
    compile_plan, execute_plan_with_run, Operation, Spec, Step, TransformRunContext,
};
use parqonaut_workflow::{CollectingProgressObserver, ProgressObserver};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn setup_plugins_env() {
    std::env::set_var(
        "PARQONAUT_PLUGIN_ROOTS",
        repo_root().join("fixtures/plugins").to_string_lossy().to_string(),
    );
    std::env::set_var(
        "PARQONAUT_PLUGIN_SDK_PATH",
        repo_root().join("python/parqonaut_plugins/src").to_string_lossy().to_string(),
    );
}

#[test]
fn s3_plugin_pipeline_cancellation_aborts_upload() {
    if !require_s3_endpoint() {
        return;
    }
    setup_plugins_env();
    std::env::remove_var("PARQONAUT_S3_PART_SIZE_BYTES");

    let work = tempfile::tempdir().expect("tempdir");
    let bytes = std::fs::read(repo_root().join("fixtures/transform/partition-basic/input.parquet"))
        .expect("input bytes");

    let paths = parqonaut_transform::remote::run_io_runtime(move |handle| {
        handle.block_on(async move {
            let (_io, s3) = s3_io().await;
            let bucket = test_bucket();
            Leg::S3S3
                .materialize(
                    work.path(),
                    &s3,
                    &bucket,
                    "plugin-cancel",
                    "in.parquet",
                    "out.parquet",
                    &bytes,
                )
                .await
        })
    });

    let spec = Spec {
        schema_version: 1,
        input: Some(paths.input.clone()),
        output: Some(paths.output.clone()),
        steps: vec![Step {
            operation: Operation::Plugin {
                plugin: "slow-transform".into(),
                config: serde_json::json!({"delay_ms": 300}),
            },
            options: Default::default(),
        }],
        options: parqonaut_transform::Options { overwrite: true, ..Default::default() },
    };
    let plan = compile_plan(&spec).expect("compile");
    let cancel = CancelToken::new();
    let observer = Arc::new(CollectingProgressObserver::new());
    let run = TransformRunContext::with_cancel(
        Arc::clone(&observer) as Arc<dyn ProgressObserver>,
        cancel,
    );
    let run_bg = run.clone();
    let handle = thread::spawn(move || execute_plan_with_run(&plan, &run_bg));
    thread::sleep(Duration::from_millis(450));
    run.request_cancel();
    assert!(handle.join().expect("join").is_err());

    parqonaut_transform::remote::run_io_runtime(move |handle| {
        handle.block_on(async move {
            let (_io, s3) = s3_io().await;
            let loc = parqonaut_storage::location::ObjectLocation::parse(&paths.output).unwrap();
            use parqonaut_storage::backend::StorageBackend;
            assert!(
                s3.head(&loc).await.is_err(),
                "cancelled multipart upload must not leave committed destination object"
            );
        })
    });
}

#[test]
fn s3_mid_stream_plugin_failure_does_not_commit_partial_object() {
    if !require_s3_endpoint() {
        return;
    }
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let work = tempfile::tempdir().expect("tempdir");
    let extra_root = work.path().join("plugins");
    let fail_dir = extra_root.join("s3-fail-second");
    std::fs::create_dir_all(&fail_dir).unwrap();
    std::fs::write(
        fail_dir.join("parqonaut-plugin.json"),
        r#"{
          "protocol_version": 1,
          "name": "s3-fail-second",
          "version": "0.1.0",
          "entrypoint": "s3_fail:transform",
          "capabilities": { "batch_transform": {} }
        }"#,
    )
    .unwrap();
    std::fs::write(
        fail_dir.join("s3_fail.py"),
        "import sys\n_seen = 0\ndef transform(batch, _cfg):\n    global _seen\n    _seen += 1\n    if _seen >= 2:\n        sys.exit(4)\n    return batch\n",
    )
    .unwrap();

    std::env::set_var(
        "PARQONAUT_PLUGIN_ROOTS",
        format!("{}:{}", extra_root.display(), repo_root().join("fixtures/plugins").display()),
    );
    std::env::set_var(
        "PARQONAUT_PLUGIN_SDK_PATH",
        repo_root().join("python/parqonaut_plugins/src").to_string_lossy().to_string(),
    );

    let local_in = work.path().join("multi.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let props = WriterProperties::builder().set_max_row_group_size(1).build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&local_in).unwrap(),
        schema.clone(),
        Some(props),
    )
    .unwrap();
    for i in 0..4 {
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![i]))])
            .unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    let bytes = std::fs::read(&local_in).unwrap();

    let work_path = work.path().to_path_buf();
    let paths = parqonaut_transform::remote::run_io_runtime(move |handle| {
        handle.block_on(async move {
            let (_io, s3) = s3_io().await;
            let bucket = test_bucket();
            Leg::S3S3
                .materialize(
                    &work_path,
                    &s3,
                    &bucket,
                    "plugin-midfail",
                    "in.parquet",
                    "out.parquet",
                    &bytes,
                )
                .await
        })
    });

    let spec = Spec {
        schema_version: 1,
        input: Some(paths.input.clone()),
        output: Some(paths.output.clone()),
        steps: vec![Step {
            operation: Operation::Plugin {
                plugin: "s3-fail-second".into(),
                config: serde_json::json!({}),
            },
            options: Default::default(),
        }],
        options: parqonaut_transform::Options { overwrite: true, ..Default::default() },
    };
    let plan = compile_plan(&spec).expect("compile");
    let res = parqonaut_transform::execute_plan(&plan);
    assert!(res.is_err(), "mid-stream S3 plugin crash must fail");

    parqonaut_transform::remote::run_io_runtime(move |handle| {
        handle.block_on(async move {
            let (_io, s3) = s3_io().await;
            let loc = parqonaut_storage::location::ObjectLocation::parse(&paths.output).unwrap();
            use parqonaut_storage::backend::StorageBackend;
            assert!(
                s3.head(&loc).await.is_err(),
                "mid-stream failed S3 upload must not commit partial destination object"
            );
        })
    });
}
