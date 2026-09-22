use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parqonaut_transform::{
    compile_plan, execute_plan_with_run, plugin_execution_identity, Operation, Spec, Step,
    TransformRunContext,
};
use parqonaut_workflow::{CollectingProgressObserver, ProgressEventKind, ProgressObserver};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::tempdir;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn setup_plugins_env(extra_root: Option<&std::path::Path>) {
    let base = repo_root().join("fixtures/plugins");
    let roots = if let Some(p) = extra_root {
        format!("{}:{}", p.display(), base.display())
    } else {
        base.to_string_lossy().into_owned()
    };
    std::env::set_var("PARQONAUT_PLUGIN_ROOTS", roots);
    std::env::set_var(
        "PARQONAUT_PLUGIN_SDK_PATH",
        repo_root().join("python/parqonaut_plugins/src").to_string_lossy().to_string(),
    );
}

fn write_many_batch_parquet(path: &std::path::Path, batches: usize) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let file = std::fs::File::create(path).expect("create parquet");
    let props = WriterProperties::builder().set_max_row_group_size(1).build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("writer");
    for i in 0..batches {
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![i as i32]))])
                .expect("batch");
        writer.write(&batch).expect("write batch");
    }
    writer.close().expect("close");
}

#[test]
fn plugin_config_same_digest_different_execution_identity() {
    setup_plugins_env(None);
    let input = repo_root().join("fixtures/transform/partition-basic/input.parquet");
    let spec_a = Spec {
        schema_version: 1,
        input: Some(input.to_string_lossy().into_owned()),
        output: Some("/tmp/unused-a.parquet".into()),
        steps: vec![Step {
            operation: Operation::Plugin {
                plugin: "normalize-strings".into(),
                config: serde_json::json!({"columns": ["email"]}),
            },
            options: Default::default(),
        }],
        options: Default::default(),
    };
    let spec_b = Spec {
        schema_version: 1,
        input: Some(input.to_string_lossy().into_owned()),
        output: Some("/tmp/unused-b.parquet".into()),
        steps: vec![Step {
            operation: Operation::Plugin {
                plugin: "normalize-strings".into(),
                config: serde_json::json!({"columns": ["notes"]}),
            },
            options: Default::default(),
        }],
        options: Default::default(),
    };
    let plan_a = compile_plan(&spec_a).expect("compile a");
    let plan_b = compile_plan(&spec_b).expect("compile b");
    let digest_a = match &plan_a.segments[0] {
        parqonaut_transform::CompiledSegment::Fused(f) => match &f.ops[0] {
            parqonaut_transform::FusedOperation::Plugin(p) => p.digest.clone(),
            _ => panic!("plugin"),
        },
        _ => panic!("fused"),
    };
    let digest_b = match &plan_b.segments[0] {
        parqonaut_transform::CompiledSegment::Fused(f) => match &f.ops[0] {
            parqonaut_transform::FusedOperation::Plugin(p) => p.digest.clone(),
            _ => panic!("plugin"),
        },
        _ => panic!("fused"),
    };
    assert_eq!(digest_a, digest_b);
    assert_ne!(plugin_execution_identity(&plan_a), plugin_execution_identity(&plan_b));
}

#[test]
fn two_plugin_transform_report_provenance() {
    setup_plugins_env(None);
    let work = tempdir().expect("tempdir");
    let input = repo_root().join("fixtures/transform/partition-basic/input.parquet");
    let output = work.path().join("out.parquet");
    let spec = Spec {
        schema_version: 1,
        input: Some(input.to_string_lossy().into_owned()),
        output: Some(output.to_string_lossy().into_owned()),
        steps: vec![
            Step {
                operation: Operation::Plugin {
                    plugin: "normalize-strings".into(),
                    config: serde_json::json!({"columns": ["email"]}),
                },
                options: Default::default(),
            },
            Step {
                operation: Operation::Plugin {
                    plugin: "batch-passthrough".into(),
                    config: serde_json::json!({}),
                },
                options: Default::default(),
            },
        ],
        options: parqonaut_transform::Options { overwrite: true, ..Default::default() },
    };
    let plan = compile_plan(&spec).expect("compile");
    let report = execute_plan_with_run(&plan, &TransformRunContext::noop()).expect("execute");
    assert_eq!(report.plugin_executions.len(), 2);
    assert_eq!(report.plugin_executions[0].name, "normalize-strings");
    assert_eq!(report.plugin_executions[1].name, "batch-passthrough");
    assert_eq!(report.plugin_executions[0].status, "completed");
    assert_eq!(report.plugin_executions[1].status, "completed");
    assert!(!report.plugin_executions[0].digest.is_empty());
    assert!(!report.plugin_executions[1].digest.is_empty());
}

#[test]
fn slow_sink_backpressure_e2e_pipeline() {
    setup_plugins_env(None);
    std::env::set_var("PARQONAUT_TEST_SINK_BATCH_DELAY_MS", "60");
    let work = tempdir().expect("tempdir");
    let input = work.path().join("in.parquet");
    write_many_batch_parquet(&input, 10);
    let output = work.path().join("out.parquet");
    let spec = Spec {
        schema_version: 1,
        input: Some(input.to_string_lossy().into_owned()),
        output: Some(output.to_string_lossy().into_owned()),
        steps: vec![Step {
            operation: Operation::Plugin {
                plugin: "batch-passthrough".into(),
                config: serde_json::json!({}),
            },
            options: Default::default(),
        }],
        options: parqonaut_transform::Options { overwrite: true, ..Default::default() },
    };
    let plan = compile_plan(&spec).expect("compile");
    let run = TransformRunContext::noop();
    execute_plan_with_run(&plan, &run).expect("execute");
    std::env::remove_var("PARQONAUT_TEST_SINK_BATCH_DELAY_MS");
    let metrics = run.take_bridge_metrics();
    assert_eq!(metrics.len(), 1);
    let m = metrics[0];
    assert_eq!(m.host_to_plugin_capacity, 4);
    assert!(m.host_to_plugin_peak <= m.host_to_plugin_capacity);
    assert!(m.plugin_to_host_peak <= m.plugin_to_host_capacity);
}

#[test]
fn local_plugin_cancellation_during_pipeline() {
    setup_plugins_env(None);
    let work = tempdir().expect("tempdir");
    let input = work.path().join("in.parquet");
    write_many_batch_parquet(&input, 6);
    let output = work.path().join("out.parquet");
    let spec = Spec {
        schema_version: 1,
        input: Some(input.to_string_lossy().into_owned()),
        output: Some(output.to_string_lossy().into_owned()),
        steps: vec![Step {
            operation: Operation::Plugin {
                plugin: "slow-transform".into(),
                config: serde_json::json!({"delay_ms": 200}),
            },
            options: Default::default(),
        }],
        options: parqonaut_transform::Options { overwrite: true, ..Default::default() },
    };
    let plan = compile_plan(&spec).expect("compile");
    let cancel = parqonaut_plugin_host::CancelToken::new();
    let observer = Arc::new(CollectingProgressObserver::new());
    let run = TransformRunContext::with_cancel(
        Arc::clone(&observer) as Arc<dyn ProgressObserver>,
        cancel.clone(),
    );
    let run_bg = run.clone();
    let handle = thread::spawn(move || execute_plan_with_run(&plan, &run_bg));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let started =
            observer.take_events().iter().any(|e| e.kind == ProgressEventKind::PluginStarted);
        if started {
            run.request_cancel();
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    let result = handle.join().expect("join");
    assert!(result.is_err(), "cancelled pipeline must fail");
    let kinds: Vec<_> = observer.take_events().iter().map(|e| e.kind.clone()).collect();
    if kinds.contains(&ProgressEventKind::PluginStarted) {
        assert!(kinds.contains(&ProgressEventKind::PluginFailed));
        assert_eq!(kinds[0], ProgressEventKind::PluginStarted);
    }
}

#[test]
fn progress_throttling_many_batches() {
    setup_plugins_env(None);
    let work = tempdir().expect("tempdir");
    let input = work.path().join("in.parquet");
    write_many_batch_parquet(&input, 80);
    let output = work.path().join("out.parquet");
    let spec = Spec {
        schema_version: 1,
        input: Some(input.to_string_lossy().into_owned()),
        output: Some(output.to_string_lossy().into_owned()),
        steps: vec![Step {
            operation: Operation::Plugin {
                plugin: "batch-passthrough".into(),
                config: serde_json::json!({}),
            },
            options: Default::default(),
        }],
        options: parqonaut_transform::Options { overwrite: true, ..Default::default() },
    };
    let plan = compile_plan(&spec).expect("compile");
    let observer = Arc::new(CollectingProgressObserver::new());
    let run = TransformRunContext::new(Arc::clone(&observer) as Arc<dyn ProgressObserver>);
    execute_plan_with_run(&plan, &run).expect("execute");
    let kinds: Vec<_> = observer.take_events().iter().map(|e| e.kind.clone()).collect();
    let progress_count = kinds.iter().filter(|k| **k == ProgressEventKind::PluginProgress).count();
    assert!(progress_count < 80, "progress must be throttled, got {progress_count}");
    assert!(kinds.contains(&ProgressEventKind::PluginStarted));
    assert!(kinds.contains(&ProgressEventKind::PluginCompleted));
}
