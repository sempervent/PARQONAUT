use std::fs;
use std::sync::Arc;

use chrono::Utc;
use parqonaut_orchestrator::{
    build_batch_plan_async, validate_batch_overlap, BatchConfig, BatchExecutor,
    BatchExecutorConfig, BatchPathSpec, BatchStorageRuntime, CancelFlag, DatasetId, DatasetState,
    ExecutionMode, RunId, RunIdentity, SqliteRunJournal,
};
use parqonaut_repair::{RepairExecutor, PARQONAUT_VERSION};
use parqonaut_storage::location::DatasetLocation;
use tempfile::TempDir;

fn write_batch_config(base: &TempDir, ds_paths: &[(&str, &str)]) -> camino::Utf8PathBuf {
    let mut body = String::from(
        r#"
schema_version = 1

[batch]
name = "scheduler-test"
max_concurrency = 2
output_root = "outputs"
"#,
    );
    for (id, path) in ds_paths {
        body.push_str(&format!("\n[[datasets]]\nid = \"{id}\"\npath = \"{path}\"\n"));
    }
    let cfg_path = base.path().join("batch.toml");
    fs::write(&cfg_path, body).unwrap();
    cfg_path.try_into().unwrap()
}

fn run_identity(plan: &parqonaut_orchestrator::BatchPlan, run_id: &RunId) -> RunIdentity {
    RunIdentity {
        run_id: run_id.clone(),
        batch_plan_id: plan.batch_plan_id.clone(),
        config_fingerprint: plan.config_fingerprint.clone(),
        batch_name: plan.batch_name.clone(),
        output_root: plan.output_root.clone(),
        parqonaut_version: PARQONAUT_VERSION.to_string(),
        plan_digest: plan.plan_digest(),
        started_at: Utc::now(),
        updated_at: Utc::now(),
        completed_at: None,
        cancelled_at: None,
    }
}

#[test]
fn overlap_rejects_shared_output_tree() {
    let err = validate_batch_overlap(&[
        BatchPathSpec {
            dataset_id: DatasetId("left".into()),
            source: DatasetLocation::parse("/tmp/a/src").unwrap(),
            output: DatasetLocation::parse("/tmp/shared/out").unwrap(),
        },
        BatchPathSpec {
            dataset_id: DatasetId("right".into()),
            source: DatasetLocation::parse("/tmp/b/src").unwrap(),
            output: DatasetLocation::parse("/tmp/shared/out/nested").unwrap(),
        },
    ])
    .unwrap_err();
    assert!(err.to_string().contains("overlap"));
}

#[test]
fn executor_respects_jobs_bound_metric() {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async {
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
        let storage = BatchStorageRuntime::new(cfg.max_storage_requests());
        let plan = build_batch_plan_async(&cfg, &cfg_path, &storage).await.unwrap();

        let run_id = RunId::new();
        let identity = run_identity(&plan, &run_id);
        let journal_path: camino::Utf8PathBuf = tmp.path().join("run.db").try_into().unwrap();
        let journal = Arc::new(SqliteRunJournal::open(&journal_path, &identity).await.unwrap());

        let executor =
            BatchExecutor::new(RepairExecutor::default(), BatchExecutorConfig::default());
        let outcome = executor
            .run(&plan, &run_id, journal, ExecutionMode::Fresh, CancelFlag::new(), false)
            .await
            .unwrap();

        assert!(outcome.peak_concurrent_datasets <= plan.max_concurrency as usize);
        assert!(outcome.peak_concurrent_datasets >= 1);
        assert_eq!(outcome.datasets.len(), 3);
        assert!(outcome.datasets.iter().all(|d| d.state == DatasetState::Succeeded));
    });
}

#[test]
fn failure_isolation_continues_other_datasets() {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let tmp = TempDir::new().unwrap();
        let good = tmp.path().join("good");
        let blocked = tmp.path().join("blocked");
        fs::create_dir_all(&good).unwrap();
        fs::create_dir_all(&blocked).unwrap();

        let cfg_path = write_batch_config(
            &tmp,
            &[("good", good.to_str().unwrap()), ("blocked", blocked.to_str().unwrap())],
        );
        let cfg = BatchConfig::from_toml_path(&cfg_path).unwrap();
        let storage = BatchStorageRuntime::new(cfg.max_storage_requests());
        let mut plan = build_batch_plan_async(&cfg, &cfg_path, &storage).await.unwrap();

        for ds in &mut plan.datasets {
            if ds.dataset_id.0 == "blocked" {
                ds.output_path =
                    tmp.path().join("outputs/blocked-preexisting").to_str().unwrap().to_string();
                fs::create_dir_all(&ds.output_path).unwrap();
            }
        }

        let run_id = RunId::new();
        let identity = run_identity(&plan, &run_id);
        let journal_path: camino::Utf8PathBuf = tmp.path().join("run.db").try_into().unwrap();
        let journal = Arc::new(SqliteRunJournal::open(&journal_path, &identity).await.unwrap());

        let executor =
            BatchExecutor::new(RepairExecutor::default(), BatchExecutorConfig::default());
        let outcome = executor
            .run(&plan, &run_id, journal, ExecutionMode::Fresh, CancelFlag::new(), false)
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
    });
}
