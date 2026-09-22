use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use parqonaut_plugin_host::CancelToken;
use parqonaut_transform::{
    compile_plan, execute_plan_with_run, parse_spec, Operation, Spec, Step, TransformRunContext,
};
use parqonaut_workflow::{CollectingProgressObserver, ProgressEventKind, ProgressObserver};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
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

fn setup_plugins() {
    std::env::set_var(
        "PARQONAUT_PLUGIN_ROOTS",
        repo_root().join("fixtures/plugins").to_string_lossy().to_string(),
    );
    std::env::set_var(
        "PARQONAUT_PLUGIN_SDK_PATH",
        repo_root().join("python/parqonaut_plugins/src").to_string_lossy().to_string(),
    );
}

fn assert_single_terminal_plugin_events(kinds: &[ProgressEventKind]) {
    let terminal: Vec<_> = kinds
        .iter()
        .filter(|k| {
            matches!(**k, ProgressEventKind::PluginCompleted | ProgressEventKind::PluginFailed)
        })
        .collect();
    assert_eq!(terminal.len(), 1, "expected one terminal plugin event, got {terminal:?}");
    if let Some(last) = kinds.last() {
        assert!(
            matches!(last, ProgressEventKind::PluginCompleted | ProgressEventKind::PluginFailed),
            "terminal event must be last, got {last:?}"
        );
    }
    let started = kinds.iter().position(|k| *k == ProgressEventKind::PluginStarted);
    assert!(started.is_some());
    for (i, k) in kinds.iter().enumerate() {
        if i > started.unwrap()
            && matches!(k, ProgressEventKind::PluginCompleted | ProgressEventKind::PluginFailed)
        {
            for rest in &kinds[i + 1..] {
                assert_ne!(
                    *rest,
                    ProgressEventKind::PluginProgress,
                    "no progress after terminal event"
                );
            }
        }
    }
}

#[test]
fn plugin_progress_lifecycle_success() {
    setup_plugins();
    let spec_path = repo_root().join("fixtures/transform/specs/plugin-normalize-strings.yaml");
    let mut spec = parse_spec(&spec_path).expect("parse");
    let work = tempfile::tempdir().expect("tempdir");
    let inp = repo_root().join("fixtures/transform/partition-basic/input.parquet");
    let out = work.path().join("out.parquet");
    spec.input = Some(inp.to_string_lossy().into_owned());
    spec.output = Some(out.to_string_lossy().into_owned());

    let collector = Arc::new(CollectingProgressObserver::new());
    let run = TransformRunContext::new(Arc::clone(&collector) as Arc<dyn ProgressObserver>);
    let plan = compile_plan(&spec).expect("compile");
    let report = execute_plan_with_run(&plan, &run).expect("execute");
    assert!(!report.plugin_executions.is_empty());
    assert_eq!(report.plugin_executions[0].status, "completed");

    let kinds: Vec<_> = collector.take_events().iter().map(|e| e.kind.clone()).collect();
    assert!(!kinds.is_empty());
    assert_eq!(kinds[0], ProgressEventKind::PluginStarted);
    assert!(kinds.contains(&ProgressEventKind::PluginCompleted));
    assert_single_terminal_plugin_events(&kinds);
}

#[test]
fn plugin_progress_lifecycle_failure() {
    setup_plugins();
    let work = tempfile::tempdir().expect("tempdir");
    let input = repo_root().join("fixtures/transform/partition-basic/input.parquet");
    let output = work.path().join("out.parquet");
    let spec = Spec {
        schema_version: 1,
        input: Some(input.to_string_lossy().into_owned()),
        output: Some(output.to_string_lossy().into_owned()),
        steps: vec![Step {
            operation: Operation::Plugin {
                plugin: "adv-batch-extra-column".into(),
                config: serde_json::json!({}),
            },
            options: Default::default(),
        }],
        options: parqonaut_transform::Options { overwrite: true, ..Default::default() },
    };
    let collector = Arc::new(CollectingProgressObserver::new());
    let run = TransformRunContext::new(Arc::clone(&collector) as Arc<dyn ProgressObserver>);
    let plan = compile_plan(&spec).expect("compile");
    assert!(execute_plan_with_run(&plan, &run).is_err());
    let kinds: Vec<_> = collector.take_events().iter().map(|e| e.kind.clone()).collect();
    if kinds.iter().any(|k| *k == ProgressEventKind::PluginStarted) {
        assert!(kinds.contains(&ProgressEventKind::PluginFailed));
        assert!(!kinds.contains(&ProgressEventKind::PluginCompleted));
        assert_single_terminal_plugin_events(&kinds);
    }
}

#[test]
fn plugin_progress_lifecycle_cancellation() {
    setup_plugins();
    let work = tempfile::tempdir().expect("tempdir");
    let input = work.path().join("in.parquet");
    write_many_batch_parquet(&input, 4);
    let output = work.path().join("out.parquet");
    let spec = Spec {
        schema_version: 1,
        input: Some(input.to_string_lossy().into_owned()),
        output: Some(output.to_string_lossy().into_owned()),
        steps: vec![Step {
            operation: Operation::Plugin {
                plugin: "slow-transform".into(),
                config: serde_json::json!({"delay_ms": 250}),
            },
            options: Default::default(),
        }],
        options: parqonaut_transform::Options { overwrite: true, ..Default::default() },
    };
    let plan = compile_plan(&spec).expect("compile");
    let cancel = CancelToken::new();
    let collector = Arc::new(CollectingProgressObserver::new());
    let run = TransformRunContext::with_cancel(
        Arc::clone(&collector) as Arc<dyn ProgressObserver>,
        cancel,
    );
    let run_bg = run.clone();
    let handle = thread::spawn(move || execute_plan_with_run(&plan, &run_bg));
    thread::sleep(Duration::from_millis(400));
    run.request_cancel();
    assert!(handle.join().expect("join").is_err());
    let kinds: Vec<_> = collector.take_events().iter().map(|e| e.kind.clone()).collect();
    if kinds.iter().any(|k| *k == ProgressEventKind::PluginStarted) {
        assert!(kinds.contains(&ProgressEventKind::PluginFailed));
        assert_single_terminal_plugin_events(&kinds);
    }
}
