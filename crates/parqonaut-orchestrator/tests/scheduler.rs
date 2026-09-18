use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use chrono::Utc;
use parqonaut_orchestrator::{
    build_batch_plan, validate_batch_overlap, BatchConfig, BatchExecutor, BatchExecutorConfig,
    BatchPathSpec, DatasetId, DatasetState, RunHeader, RunId, SqliteRunJournal,
};
use parqonaut_repair::RepairExecutor;
use tempfile::TempDir;

fn write_batch_config(base: &TempDir, ds_paths: &[(&str, &str)]) -> camino::Utf8PathBuf {
    let mut body = String::from(
        r#"
schema_version = 1

[batch]
name = "scheduler-test"
max_concurrency = 2
"#,
    );
    for (id, path) in ds_paths {
        body.push_str(&format!(
            "\n[[datasets]]\nid = \"{id}\"\npath = \"{path}\"\n"
        ));
    }
    let cfg_path = base.path().join("batch.toml");
    fs::write(&cfg_path, body).unwrap();
    cfg_path.try_into().unwrap()
}

#[test]
fn overlap_rejects_shared_output_tree() {
    let err = validate_batch_overlap(&[
        BatchPathSpec {
            dataset_id: DatasetId("left".into()),
            source: "/tmp/a/src".into(),
            output: "/tmp/shared/out".into(),
        },
        BatchPathSpec {
            dataset_id: DatasetId("right".into()),
            source: "/tmp/b/src".into(),
            output: "/tmp/shared/out/nested".into(),
        },
    ])
    .unwrap_err();
    assert!(err.to_string().contains("overlap"));
}

#[tokio::test]
async fn executor_respects_jobs_bound_metric() {
    let tmp = TempDir::new().unwrap();
    let ds1 = tmp.path().join("ds1");
    let ds2 = tmp.path().join("ds2");
    let ds3 = tmp.path().join("ds3");
    fs::create_dir_all(&ds1).unwrap();
    fs::create_dir_all(&ds2).unwrap();
    fs::create_dir_all(&ds3).unwrap();

    let cfg_path = write_batch_config(
        &tmp,
        &[
            ("one", ds1.to_str().unwrap()),
            ("two", ds2.to_str().unwrap()),
            ("three", ds3.to_str().unwrap()),
        ],
    );
    let cfg = BatchConfig::from_toml_path(&cfg_path).unwrap();
    let plan = build_batch_plan(&cfg, &cfg_path).unwrap();

    let mut outputs = HashMap::new();
    outputs.insert(
        DatasetId("one".into()),
        tmp.path().join("out1").try_into().unwrap(),
    );
    outputs.insert(
        DatasetId("two".into()),
        tmp.path().join("out2").try_into().unwrap(),
    );
    outputs.insert(
        DatasetId("three".into()),
        tmp.path().join("out3").try_into().unwrap(),
    );

    let run_id = RunId::new();
    let header = RunHeader {
        run_id: run_id.clone(),
        batch_plan_id: plan.batch_plan_id.clone(),
        batch_name: plan.batch_name.clone(),
        started_at: Utc::now(),
        updated_at: Utc::now(),
        completed_at: None,
    };
    let journal_path: camino::Utf8PathBuf = tmp.path().join("run.db").try_into().unwrap();
    let journal = Arc::new(SqliteRunJournal::open(&journal_path, &header).await.unwrap());

    let executor = BatchExecutor::new(RepairExecutor::default(), BatchExecutorConfig::default());
    let outcome = executor
        .run(&plan, &outputs, &run_id, journal)
        .await
        .unwrap();

    assert!(outcome.peak_concurrent_datasets <= plan.max_concurrency as usize);
    assert!(outcome.peak_concurrent_datasets >= 1);
    assert_eq!(outcome.datasets.len(), 3);
    assert!(outcome
        .datasets
        .iter()
        .all(|d| d.state == DatasetState::Succeeded));
}

#[tokio::test]
async fn failure_isolation_continues_other_datasets() {
    let tmp = TempDir::new().unwrap();
    let good = tmp.path().join("good");
    let blocked = tmp.path().join("blocked");
    fs::create_dir_all(&good).unwrap();
    fs::create_dir_all(&blocked).unwrap();

    let cfg_path = write_batch_config(
        &tmp,
        &[
            ("good", good.to_str().unwrap()),
            ("blocked", blocked.to_str().unwrap()),
        ],
    );
    let cfg = BatchConfig::from_toml_path(&cfg_path).unwrap();
    let plan = build_batch_plan(&cfg, &cfg_path).unwrap();

    let blocked_out: camino::Utf8PathBuf = tmp.path().join("out-blocked").try_into().unwrap();
    fs::create_dir_all(&blocked_out).unwrap();

    let mut outputs = HashMap::new();
    outputs.insert(
        DatasetId("good".into()),
        tmp.path().join("out-good").try_into().unwrap(),
    );
    outputs.insert(DatasetId("blocked".into()), blocked_out);

    let run_id = RunId::new();
    let header = RunHeader {
        run_id: run_id.clone(),
        batch_plan_id: plan.batch_plan_id.clone(),
        batch_name: plan.batch_name.clone(),
        started_at: Utc::now(),
        updated_at: Utc::now(),
        completed_at: None,
    };
    let journal_path: camino::Utf8PathBuf = tmp.path().join("run.db").try_into().unwrap();
    let journal = Arc::new(SqliteRunJournal::open(&journal_path, &header).await.unwrap());

    let executor = BatchExecutor::new(RepairExecutor::default(), BatchExecutorConfig::default());
    let outcome = executor
        .run(&plan, &outputs, &run_id, journal)
        .await
        .unwrap();

    let good_result = outcome
        .datasets
        .iter()
        .find(|d| d.dataset_id.0 == "good")
        .expect("good dataset outcome");
    let blocked_result = outcome
        .datasets
        .iter()
        .find(|d| d.dataset_id.0 == "blocked")
        .expect("blocked dataset outcome");

    assert_eq!(good_result.state, DatasetState::Succeeded);
    assert_eq!(blocked_result.state, DatasetState::FailedPermanent);
    assert_eq!(outcome.datasets.len(), 2);
}
