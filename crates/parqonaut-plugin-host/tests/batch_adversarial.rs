use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::Int32Array;
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
fn batch_schema_mismatch_rejected() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("adv-batch-extra-column").unwrap();
    let runtime =
        PluginRuntimeConfig { sdk_src_root: Some(sdk_path()), ..PluginRuntimeConfig::default() };
    let mut session =
        BatchPluginSession::start(entry, serde_json::json!({}), "test-exec", &runtime, None, None)
            .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let err = session.transform(batch).unwrap_err();
    assert!(matches!(err, PluginHostError::PluginSchemaMismatch(_)));
    let _ = session.finish();
}
