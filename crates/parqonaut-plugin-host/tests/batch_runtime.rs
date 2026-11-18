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

#[test]
fn batch_cancel_does_not_wait_for_long_timeout() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("adv-batch-timeout").unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy {
        default_timeout_ms: 30_000,
        ..BatchResourcePolicy::default()
    });
    let cancel = CancelToken::new();
    let mut session = BatchPluginSession::start(
        entry,
        serde_json::json!({}),
        "fast-cancel",
        &runtime,
        None,
        Some(cancel.clone()),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let cancel_bg = cancel.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(150));
        cancel_bg.cancel();
    });
    let start = std::time::Instant::now();
    let err = session.transform(batch).unwrap_err();
    assert!(matches!(err, PluginHostError::Cancelled), "expected Cancelled, got {err:?}");
    assert!(
        start.elapsed() < std::time::Duration::from_secs(3),
        "cancellation waited for timeout: {:?}",
        start.elapsed()
    );
}

#[test]
fn batch_bridge_preserves_underlying_error_not_worker_stopped() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("batch-passthrough").unwrap();
    let runtime = PluginRuntimeConfig {
        python_executable: Some(PathBuf::from("/nonexistent/parqonaut/python3")),
        sdk_src_root: Some(sdk_path()),
        ..PluginRuntimeConfig::default()
    };
    let err = match BatchPluginBridge::start(
        entry,
        serde_json::json!({}),
        "err-preserve-start",
        &runtime,
        &entry.digest,
        2,
        None,
    ) {
        Ok(_) => panic!("expected startup failure"),
        Err(e) => e,
    };
    assert!(matches!(err, PluginHostError::RuntimeUnavailable(_)), "got {err:?}");

    // Also test mid-execution crash preserves ProcessFailed instead of "plugin worker stopped".
    let crash_entry = catalog.get("adv-batch-crash").unwrap();
    let good_runtime = runtime_with_policy(BatchResourcePolicy::default());
    let mut bridge = BatchPluginBridge::start(
        crash_entry,
        serde_json::json!({}),
        "err-preserve-crash",
        &good_runtime,
        &crash_entry.digest,
        2,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let err1 = bridge.transform(batch.clone()).unwrap_err();
    assert!(
        matches!(err1, PluginHostError::ProcessFailed { .. }),
        "expected ProcessFailed, got {err1:?}"
    );
    let err2 = bridge.transform(batch).unwrap_err();
    assert!(
        !err2.to_string().contains("plugin worker stopped"),
        "must not mask error with worker stopped: {err2:?}"
    );
    let _ = bridge.finish();
}

#[test]
fn batch_huge_stderr_drains_without_spinning_and_drop_cleans_up() {
    let work = tempfile::tempdir().unwrap();
    let p_dir = work.path().join("huge-stderr-batch");
    std::fs::create_dir_all(&p_dir).unwrap();
    std::fs::write(
        p_dir.join("parqonaut-plugin.json"),
        r#"{
          "protocol_version": 1,
          "name": "huge-stderr-batch",
          "version": "0.1.0",
          "entrypoint": "huge_err:transform",
          "capabilities": { "batch_transform": {} }
        }"#,
    )
    .unwrap();
    std::fs::write(
        p_dir.join("huge_err.py"),
        "import sys\ndef transform(batch, _cfg):\n    sys.stderr.write('E' * 200000)\n    sys.stderr.flush()\n    return batch\n",
    )
    .unwrap();
    let entry = PluginCatalog::validate_path(&p_dir).unwrap();
    let runtime = runtime_with_policy(BatchResourcePolicy {
        max_stderr_bytes: 1024,
        ..BatchResourcePolicy::default()
    });
    let mut session = BatchPluginSession::start(
        &entry,
        serde_json::json!({}),
        "huge-stderr",
        &runtime,
        None,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let out = session.transform(batch).unwrap();
    assert_eq!(out.num_rows(), 1);
    session.finish().unwrap();
}

#[test]
fn batch_never_reads_stdin_or_truncated_ipc_and_drop_reaps() {
    let work = tempfile::tempdir().unwrap();
    let dummy_entry = {
        let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
        catalog.get("batch-passthrough").unwrap().clone()
    };

    // 1. Plugin never reads stdin (> pipe buffer) -> times out and kills child cleanly.
    let sleeper = work.path().join("ignore_stdin.py");
    std::fs::write(&sleeper, "#!/usr/bin/env python3\nimport time\ntime.sleep(30)\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&sleeper).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&sleeper, perms).unwrap();
    }
    let runtime = PluginRuntimeConfig {
        python_executable: Some(sleeper),
        sdk_src_root: Some(sdk_path()),
        batch_policy: BatchResourcePolicy {
            default_timeout_ms: 250,
            ..BatchResourcePolicy::default()
        },
        ..PluginRuntimeConfig::default()
    };
    let mut session = BatchPluginSession::start(
        &dummy_entry,
        serde_json::json!({}),
        "ignore-stdin",
        &runtime,
        None,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("pad", DataType::Utf8, false),
    ]));
    let n = 4000usize;
    let pad = "Z".repeat(128);
    let big_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from((0..n as i32).collect::<Vec<_>>())),
            Arc::new(arrow::array::StringArray::from(vec![pad.as_str(); n])),
        ],
    )
    .unwrap();
    let start = std::time::Instant::now();
    let err = session.transform(big_batch).unwrap_err();
    assert!(matches!(err, PluginHostError::Timeout { .. }), "got {err:?}");
    assert!(start.elapsed() < std::time::Duration::from_secs(3));
    drop(session);

    // 2. Truncated IPC frame -> ProtocolViolation (not raw UnexpectedEof).
    let trunc_script = work.path().join("trunc_ipc.py");
    std::fs::write(
        &trunc_script,
        "#!/usr/bin/env python3\nimport sys\nsys.stdout.buffer.write((1024).to_bytes(4, 'little'))\nsys.stdout.buffer.write(b'partial')\nsys.stdout.buffer.flush()\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&trunc_script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&trunc_script, perms).unwrap();
    }
    let runtime_trunc = PluginRuntimeConfig {
        python_executable: Some(trunc_script),
        sdk_src_root: Some(sdk_path()),
        ..PluginRuntimeConfig::default()
    };
    let mut sess_trunc = BatchPluginSession::start(
        &dummy_entry,
        serde_json::json!({}),
        "trunc-ipc",
        &runtime_trunc,
        None,
        None,
    )
    .unwrap();
    let small_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let small_batch =
        RecordBatch::try_new(small_schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let err_trunc = sess_trunc.transform(small_batch).unwrap_err();
    assert!(
        matches!(err_trunc, PluginHostError::ProtocolViolation(_)),
        "expected ProtocolViolation, got {err_trunc:?}"
    );
}

#[test]
fn concurrent_batch_bridges_succeed() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("batch-passthrough").unwrap().clone();
    let runtime = runtime_with_policy(BatchResourcePolicy::default());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let handles: Vec<_> = (0..4)
        .map(|worker_idx| {
            let entry = entry.clone();
            let runtime = runtime.clone();
            let schema = schema.clone();
            std::thread::spawn(move || {
                let mut bridge = BatchPluginBridge::start(
                    &entry,
                    serde_json::json!({}),
                    &format!("conc-{worker_idx}"),
                    &runtime,
                    &entry.digest,
                    2,
                    None,
                )
                .unwrap();
                for i in 0..3 {
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int32Array::from(vec![worker_idx * 10 + i]))],
                    )
                    .unwrap();
                    let out = bridge.transform(batch.clone()).unwrap();
                    assert_eq!(out, batch);
                }
                bridge.finish().unwrap();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}
