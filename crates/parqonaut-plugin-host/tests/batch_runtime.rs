use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parqonaut_plugin_host::{
    BatchPluginBridge, BatchPluginSession, BatchResourcePolicy, CancelToken, PluginCatalog,
    PluginHostError, PluginRuntimeConfig,
};

fn sdk_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/parqonaut_plugins/src")
}

fn plugins_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")
}

fn runtime_with_policy(policy: BatchResourcePolicy) -> PluginRuntimeConfig {
    PluginRuntimeConfig {
        sdk_src_root: Some(sdk_path()),
        batch_policy: policy,
        ..PluginRuntimeConfig::default()
    }
}

#[test]
fn batch_timeout_kills_child() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("adv-batch-timeout").unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy {
        default_timeout_ms: 400,
        ..BatchResourcePolicy::default()
    });
    let mut session = BatchPluginSession::start(
        entry,
        serde_json::json!({}),
        "timeout-test",
        &runtime,
        None,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let err = session.transform(batch).unwrap_err();
    assert!(matches!(err, PluginHostError::Timeout { .. }));
}

#[test]
fn batch_crash_process_failed() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("adv-batch-crash").unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy::default());
    let mut session =
        BatchPluginSession::start(entry, serde_json::json!({}), "crash-test", &runtime, None, None)
            .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let err = session.transform(batch).unwrap_err();
    assert!(matches!(
        err,
        PluginHostError::ProcessFailed { .. } | PluginHostError::ProtocolViolation(_)
    ));
    let _ = session.finish();
}

#[test]
fn batch_stdout_contamination_protocol_violation() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("adv-batch-stdout").unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy::default());
    let mut session = BatchPluginSession::start(
        entry,
        serde_json::json!({}),
        "stdout-test",
        &runtime,
        None,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    assert!(session.transform(batch).is_err());
}

#[test]
fn batch_row_explosion_rejected() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("adv-batch-row-explosion").unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy {
        max_output_expansion_factor: 2.0,
        ..BatchResourcePolicy::default()
    });
    let mut session = BatchPluginSession::start(
        entry,
        serde_json::json!({}),
        "explode-test",
        &runtime,
        None,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let err = session.transform(batch).unwrap_err();
    assert!(matches!(err, PluginHostError::PluginOutputExpansionExceeded(_)));
}

#[test]
fn batch_zero_row_input_cannot_expand() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("adv-batch-zero-expand").unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy::default());
    let mut session =
        BatchPluginSession::start(entry, serde_json::json!({}), "zero-test", &runtime, None, None)
            .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(Vec::<i32>::new()))]).unwrap();
    let err = session.transform(batch).unwrap_err();
    assert!(matches!(err, PluginHostError::PluginOutputExpansionExceeded(_)));
}

#[test]
fn batch_topology_probe_succeeds() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("topology-probe").unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy::default());
    let mut session = BatchPluginSession::start(
        entry,
        serde_json::json!({}),
        "topology-test",
        &runtime,
        None,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    session.transform(batch).expect("topology probe");
    session.finish().unwrap();
}

#[test]
fn batch_secret_env_canary() {
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "canary-secret");
    std::env::set_var("GITHUB_TOKEN", "canary-token");
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("adv-batch-leak-env").unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy::default());
    let mut session =
        BatchPluginSession::start(entry, serde_json::json!({}), "env-test", &runtime, None, None)
            .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    session.transform(batch).expect("batch child must not inherit secrets");
    session.finish().unwrap();
    std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    std::env::remove_var("GITHUB_TOKEN");
}

#[test]
fn batch_bridge_backpressure_peak_bounded() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("slow-transform").unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy {
        max_inflight_batches: 2,
        ..BatchResourcePolicy::default()
    });
    let bridge = BatchPluginBridge::start(
        entry,
        serde_json::json!({"delay_ms": 80}),
        "backpressure",
        &runtime,
        &entry.digest,
        2,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let bridge = Arc::new(Mutex::new(bridge));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let bridge = Arc::clone(&bridge);
            let b = batch.clone();
            std::thread::spawn(move || bridge.lock().unwrap().transform(b))
        })
        .collect();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    let peak = {
        let guard = bridge.lock().unwrap();
        let m = guard.metrics();
        assert_eq!(m.host_to_plugin_capacity, 2);
        m.host_to_plugin_peak()
    };
    assert!(peak <= 2, "peak {peak}");
    bridge.lock().unwrap().finish().unwrap();
}

#[test]
fn batch_cancellation_aborts_worker() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("slow-transform").unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy::default());
    let cancel = CancelToken::new();
    let bridge = BatchPluginBridge::start(
        entry,
        serde_json::json!({"delay_ms": 500}),
        "cancel-test",
        &runtime,
        &entry.digest,
        2,
        Some(cancel.clone()),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let bridge = Arc::new(Mutex::new(bridge));
    let bridge_worker = Arc::clone(&bridge);
    bridge.lock().unwrap().cancel();
    let t = std::thread::spawn(move || bridge_worker.lock().unwrap().transform(batch));
    assert!(t.join().unwrap().is_err(), "cancelled bridge must reject transforms");
    let _ = bridge.lock().unwrap().finish();
}
