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
