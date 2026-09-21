use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parqonaut_plugin_host::{
    BatchPluginSession, PluginCatalog, PluginHostError, PluginRuntimeConfig,
};

fn sdk_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/parqonaut_plugins/src")
}

fn plugins_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")
}

#[test]
fn rust_python_batch_ipc_roundtrip() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("batch-passthrough").expect("batch-passthrough");
    let runtime =
        PluginRuntimeConfig { sdk_src_root: Some(sdk_path()), ..PluginRuntimeConfig::default() };
    let mut session =
        BatchPluginSession::start(entry, serde_json::json!({}), "test-exec", &runtime, None)
            .expect("start batch session");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap();
    let out = session.transform(batch.clone()).expect("transform");
    assert_eq!(out.schema(), batch.schema());
    assert_eq!(out.column(0).as_ref(), batch.column(0).as_ref());
    session.finish().expect("finish");
}

#[test]
fn stale_digest_rejected_before_spawn() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("batch-passthrough").unwrap();
    let runtime =
        PluginRuntimeConfig { sdk_src_root: Some(sdk_path()), ..PluginRuntimeConfig::default() };
    let result = BatchPluginSession::start(
        entry,
        serde_json::json!({}),
        "test-exec",
        &runtime,
        Some("deadbeef"),
    );
    assert!(matches!(result, Err(PluginHostError::StalePlugin { .. })));
}
