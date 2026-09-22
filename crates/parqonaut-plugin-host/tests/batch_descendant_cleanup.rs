use std::path::PathBuf;
use std::process::Command;
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
fn descendant_processes_reaped_on_timeout() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("adv-batch-spawn-descendant").unwrap();
    let runtime = PluginRuntimeConfig {
        sdk_src_root: Some(sdk_path()),
        batch_policy: parqonaut_plugin_host::BatchResourcePolicy {
            default_timeout_ms: 800,
            ..Default::default()
        },
        ..PluginRuntimeConfig::default()
    };
    let mut session = BatchPluginSession::start(
        entry,
        serde_json::json!({}),
        "descendant-test",
        &runtime,
        None,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let err = session.transform(batch).unwrap_err();
    assert!(matches!(err, PluginHostError::Timeout { .. }));
    let _ = session.finish();

    // Allow a moment for SIGKILL on the process group to propagate.
    std::thread::sleep(std::time::Duration::from_millis(200));
    let orphans = Command::new("pgrep").arg("-f").arg("time.sleep(3600)").output().expect("pgrep");
    if orphans.status.success() {
        let pids = String::from_utf8_lossy(&orphans.stdout);
        panic!("descendant sleep processes still running: {pids}");
    }
}
