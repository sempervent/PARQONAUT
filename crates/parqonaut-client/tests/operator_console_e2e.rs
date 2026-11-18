//! Full v0.11 operator console E2E: scan, events/SSE, findings, review-required repair,
//! idempotency, batch monitoring, slow-job cancellation, RBAC, and server restart recovery.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use camino::Utf8PathBuf;
use parqonaut_app::{ServerPluginPolicy, ServerPluginState, StoragePolicy};
use parqonaut_client::{
    generate_idempotency_key, OperatorClient, OperatorClientError, RepairReviewState, SseEventItem,
};
use parqonaut_orchestrator::{
    BatchPlan, BatchPlanId, DatasetId, DatasetPlan, BATCH_PLAN_SCHEMA_VERSION,
};
use parqonaut_repair::RepairSafety;
use parqonaut_service::api_types::{JobEventListQuery, JobListQuery, PageQuery, StartScanRequest};
use parqonaut_service::application_http::{
    BatchRepairJobBody, PlanBody, RepairJobBody, VerifyBody,
};
use parqonaut_service::{build_router_with_workers, ParqonautService};
use parqonaut_store::SqliteScanStore;
use parqonaut_types::{AuthRole, JobId, JobStatus, RunId, ScanProfile, ScanTarget};
use parqonaut_workflow::ProgressEventKind;
use parquet::arrow::ArrowWriter;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use uuid::Uuid;

fn sqlite_url(dir: &tempfile::TempDir) -> String {
    let p = dir.path().join("console.sqlite");
    if !p.exists() {
        std::fs::File::create(&p).unwrap();
    }
    format!("sqlite://{}", p.canonicalize().unwrap().display())
}

fn write_int32_parquet(path: &std::path::Path, vals: Vec<i32>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vals))]).unwrap();
    let mut w = ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn write_int64_parquet(path: &std::path::Path, vals: Vec<i64>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))]).unwrap();
    let mut w = ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

async fn start_server(db_url: &str, plugins_enabled: bool) -> (String, JoinHandle<()>) {
    let store = SqliteScanStore::connect(db_url).await.unwrap();
    let plugins = if plugins_enabled {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/parqonaut_plugins");
        std::env::set_var("PARQONAUT_PLUGIN_SDK_PATH", root.join("src"));
        let venv_py = root.join(".venv/bin/python");
        if venv_py.is_file() {
            std::env::set_var("PARQONAUT_PLUGIN_PYTHON", venv_py);
        } else if std::env::var_os("PARQONAUT_PLUGIN_PYTHON").is_none() {
            std::env::set_var("PARQONAUT_PLUGIN_PYTHON", "python3");
        }
        std::env::set_var("PARQONAUT_TEST_SLOW_SCAN_SEC", "8");
        ServerPluginState::bootstrap(ServerPluginPolicy {
            enabled: true,
            roots: vec![PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")],
            allowed: BTreeSet::from(["adv-sleep".into()]),
            runtime: Default::default(),
        })
        .unwrap()
    } else {
        ServerPluginState::disabled()
    };

    let svc = ParqonautService::with_storage_policy_and_plugins(
        store,
        StoragePolicy::cli_unrestricted_local(),
        plugins,
    );
    let router = build_router_with_workers(svc, 1);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (format!("http://{addr}"), handle)
}

async fn wait_terminal(
    client: &OperatorClient,
    jid: JobId,
) -> parqonaut_service::api_types::ScanJobView {
    for _ in 0..400 {
        let view = client.get_job(jid).await.unwrap();
        if matches!(view.status, JobStatus::Succeeded | JobStatus::Failed | JobStatus::Canceled) {
            return view;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("job {} did not reach terminal state in time", jid.0);
}

#[tokio::test]
async fn end_to_end_operator_console_workflow_with_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db_url = sqlite_url(&dir);
    let reader_tok = "e2e-reader";
    let op_tok = "e2e-operator";
    {
        let store = SqliteScanStore::connect(&db_url).await.unwrap();
        store.insert_auth_token("reader", reader_tok, AuthRole::Reader).await.unwrap();
        store.insert_auth_token("operator", op_tok, AuthRole::Operator).await.unwrap();
    }

    let (base_url, server_handle) = start_server(&db_url, true).await;
    let reader = OperatorClient::new(&base_url, Some(reader_tok.into())).unwrap();
    let op = OperatorClient::new(&base_url, Some(op_tok.into())).unwrap();

    // Create a dataset with Int32 + Int64 schema drift -> yields a ReviewRequired widening op.
    let src_dir = dir.path().join("drift_ds");
    std::fs::create_dir_all(&src_dir).unwrap();
    write_int32_parquet(&src_dir.join("part-0.parquet"), vec![1, 2]);
    write_int64_parquet(&src_dir.join("part-1.parquet"), vec![3, 4]);

    // 1. Submit scan and verify RBAC + idempotency + SSE/events.
    let scan_req = StartScanRequest {
        target: ScanTarget::LocalDirectory {
            path: Utf8PathBuf::from_path_buf(src_dir.clone()).unwrap(),
        },
        profile: ScanProfile::Standard,
        options: Default::default(),
        scan_id: None,
        redaction: None,
        plugins: vec![],
    };
    assert!(matches!(
        reader.submit_scan(&scan_req, Some("r-scan")).await.unwrap_err(),
        OperatorClientError::Forbidden(_)
    ));

    let scan_sub =
        op.submit_scan(&scan_req, Some(&generate_idempotency_key("scan"))).await.unwrap();
    let mut rx = op.stream_job_events(JobId(scan_sub.job_id), 0).await.unwrap();
    let mut seen_seqs = Vec::new();
    while let Ok(Some(Ok(item))) = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
        match item {
            SseEventItem::JobEvent(ev) => seen_seqs.push(ev.seq),
            SseEventItem::JobStatus { status, .. } => {
                assert_eq!(status, JobStatus::Succeeded);
                break;
            }
        }
    }
    assert!(!seen_seqs.is_empty());
    let scan_job = op.get_job(JobId(scan_sub.job_id)).await.unwrap();
    let run_id = RunId(scan_job.run_id.expect("scan run_id"));
    let findings = op
        .list_findings(
            run_id,
            &PageQuery {
                limit: 50,
                offset: 0,
                inspection_status: None,
                severity: None,
                code: None,
            },
        )
        .await
        .unwrap();
    assert!(!findings.items.is_empty(), "expected schema drift findings");

    // 2. Generate repair plan, ensure ReviewRequired is NOT auto-authorized, explicitly authorize, submit & verify.
    let policy_toml = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/schema/policy.toml"),
    )
    .unwrap();
    let plan_resp = op
        .generate_plan(&PlanBody {
            location: src_dir.to_str().unwrap().to_string(),
            policy_toml: Some(policy_toml.clone()),
            target_schema_path: None,
        })
        .await
        .unwrap();
    let repaired_out = dir.path().join("repaired_ds");
    let mut review = RepairReviewState::new(
        src_dir.to_str().unwrap(),
        repaired_out.to_str().unwrap(),
        plan_resp.plan,
    );
    assert!(review.authorized_list().is_empty(), "no silent authorization allowed");

    let review_ops: Vec<String> = review
        .plan
        .operations
        .iter()
        .filter(|o| o.safety == RepairSafety::ReviewRequired)
        .map(|o| o.operation_id.clone())
        .collect();
    assert!(!review_ops.is_empty(), "expected at least one ReviewRequired op");
    for id in &review_ops {
        assert_eq!(review.toggle_operation(id), Ok(true));
    }

    let repair_body = RepairJobBody {
        source: review.source.clone(),
        output: review.output.clone(),
        plan: review.plan.clone(),
        authorize: review.authorized_list(),
    };
    let idem_rep = generate_idempotency_key("rep");
    let rep1 = op.submit_repair(&repair_body, Some(&idem_rep)).await.unwrap();
    let rep2 = op.submit_repair(&repair_body, Some(&idem_rep)).await.unwrap();
    assert_eq!(rep1.job_id, rep2.job_id);
    let rep_done = wait_terminal(&op, JobId(rep1.job_id)).await;
    assert_eq!(rep_done.status, JobStatus::Succeeded);

    let verify_resp = op
        .verify(&VerifyBody {
            before: src_dir.to_str().unwrap().to_string(),
            after_local_path: repaired_out.to_str().unwrap().to_string(),
            manifest_path: Some(
                repaired_out.join(".parqonaut-manifest.json").to_str().unwrap().to_string(),
            ),
            policy_toml: Some(policy_toml.clone()),
        })
        .await
        .unwrap();
    assert!(
        matches!(
            verify_resp.report.outcome,
            parqonaut_repair::VerificationOutcome::Verified
                | parqonaut_repair::VerificationOutcome::VerifiedWithWarnings
        ),
        "expected verified outcome, got {:?}",
        verify_resp.report.outcome
    );

    // 3. Batch repair monitoring.
    let batch_out = dir.path().join("batch_out");
    std::fs::create_dir_all(&batch_out).unwrap();
    let batch_plan = BatchPlan {
        schema_version: BATCH_PLAN_SCHEMA_VERSION,
        batch_plan_id: BatchPlanId("bp-e2e".into()),
        config_fingerprint: "cfg".into(),
        batch_name: "e2e-batch".into(),
        max_concurrency: 1,
        max_storage_requests: 4,
        output_root: batch_out.to_str().unwrap().to_string(),
        run_root: batch_out.to_str().unwrap().to_string(),
        parqonaut_version: env!("CARGO_PKG_VERSION").into(),
        datasets: vec![DatasetPlan {
            dataset_id: DatasetId("ds-1".into()),
            source_path: src_dir.to_str().unwrap().to_string(),
            output_path: batch_out.join("ds-1").to_str().unwrap().to_string(),
            repair_plan: review.plan.clone(),
            authorize: review.authorized_list(),
        }],
    };
    let batch_plan_path = dir.path().join("batch-plan.json");
    batch_plan
        .write_json(Utf8PathBuf::from_path_buf(batch_plan_path.clone()).unwrap().as_path())
        .unwrap();
    let batch_sub = op
        .submit_batch_repair(
            &BatchRepairJobBody {
                plan_path: batch_plan_path.to_str().unwrap().to_string(),
                jobs: Some(1),
                dry_run: false,
            },
            Some(&generate_idempotency_key("batch")),
        )
        .await
        .unwrap();
    let batch_done = wait_terminal(&op, JobId(batch_sub.job_id)).await;
    assert_eq!(batch_done.status, JobStatus::Succeeded);
    let batch_status = op.get_job_batch_status(JobId(batch_sub.job_id)).await.unwrap();
    assert!(batch_status.status.completed);
    assert_eq!(batch_status.status.state_counts.succeeded, 1);

    // 4. Slow plugin job cancellation.
    let slow_req = StartScanRequest {
        target: ScanTarget::LocalDirectory {
            path: Utf8PathBuf::from_path_buf(src_dir.clone()).unwrap(),
        },
        profile: ScanProfile::Quick,
        options: Default::default(),
        scan_id: None,
        redaction: None,
        plugins: vec!["adv-sleep".into()],
    };
    let slow_sub =
        op.submit_scan(&slow_req, Some(&generate_idempotency_key("slow-cancel"))).await.unwrap();
    let slow_jid = JobId(slow_sub.job_id);
    for _ in 0..200 {
        let v = op.get_job(slow_jid).await.unwrap();
        if v.status == JobStatus::Running {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(matches!(
        reader.cancel_job(slow_jid).await.unwrap_err(),
        OperatorClientError::Forbidden(_)
    ));
    let _ = op.cancel_job(slow_jid).await.unwrap();
    let canceled = wait_terminal(&op, slow_jid).await;
    assert_eq!(canceled.status, JobStatus::Canceled);
    let cancel_events = op
        .get_job_events(
            slow_jid,
            &JobEventListQuery { after_seq: 0, limit: 50, kind: None, stage: None },
        )
        .await
        .unwrap();
    assert!(cancel_events.items.iter().any(|e| e.kind == ProgressEventKind::JobCancelled));

    // 5. Restart server against the SAME SQLite file and verify durable history survives!
    server_handle.abort();
    let (restarted_url, restarted_handle) = start_server(&db_url, false).await;
    let reader2 = OperatorClient::new(&restarted_url, Some(reader_tok.into())).unwrap();

    let jobs_after = reader2
        .list_jobs(&JobListQuery { status: None, kind: None, limit: 50, offset: 0 })
        .await
        .unwrap();
    assert!(jobs_after.total >= 4);
    let rep_events_after = reader2
        .get_job_events(
            JobId(rep1.job_id),
            &JobEventListQuery { after_seq: 0, limit: 50, kind: None, stage: None },
        )
        .await
        .unwrap();
    assert_eq!(rep_events_after.items.last().unwrap().kind, ProgressEventKind::ExecutionCompleted);
    let runs_after = reader2
        .list_runs(&parqonaut_service::api_types::RunListQuery { limit: 20, offset: 0 })
        .await
        .unwrap();
    assert!(runs_after.total >= 1);
    let _ = Uuid::nil();
    restarted_handle.abort();
}
