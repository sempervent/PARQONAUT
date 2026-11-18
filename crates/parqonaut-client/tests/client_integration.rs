use std::path::PathBuf;
use std::time::Duration;

use camino::Utf8PathBuf;
use parqonaut_app::StoragePolicy;
use parqonaut_client::{
    generate_idempotency_key, OperatorClient, OperatorClientError, RepairReviewState, SseEventItem,
};
use parqonaut_service::api_types::{JobEventListQuery, JobListQuery, PageQuery, StartScanRequest};
use parqonaut_service::application_http::{PlanBody, RepairJobBody};
use parqonaut_service::{build_router_with_workers, ParqonautService};
use parqonaut_store::SqliteScanStore;
use parqonaut_types::{AuthRole, JobId, JobStatus, RunId, ScanProfile, ScanTarget};
use tokio::net::TcpListener;

async fn spawn_test_server() -> (tempfile::TempDir, String, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("client.sqlite");
    std::fs::File::create(&p).unwrap();
    let url = format!("sqlite://{}", p.canonicalize().unwrap().display());
    let store = SqliteScanStore::connect(&url).await.unwrap();
    let reader_tok = "client-reader-tok";
    let op_tok = "client-op-tok";
    store.insert_auth_token("reader", reader_tok, AuthRole::Reader).await.unwrap();
    store.insert_auth_token("operator", op_tok, AuthRole::Operator).await.unwrap();

    let svc = ParqonautService::with_storage_policy(store, StoragePolicy::cli_unrestricted_local());
    let router = build_router_with_workers(svc, 1);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (dir, format!("http://{addr}"), reader_tok.into(), op_tok.into())
}

fn fixture(rel: &str) -> Utf8PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(rel);
    Utf8PathBuf::from_path_buf(p).unwrap()
}

#[tokio::test]
async fn client_end_to_end_scan_sse_findings_plan_repair_and_error_classes() {
    let (dir, base_url, reader_tok, op_tok) = spawn_test_server().await;

    // 1. Connection error on unreachable port.
    let bad_client = OperatorClient::new("http://127.0.0.1:1", Some(op_tok.clone())).unwrap();
    let err = bad_client.health().await.unwrap_err();
    assert!(matches!(err, OperatorClientError::Connection(_)), "got {err:?}");

    // 2. Unauthorized on wrong token.
    let unauth = OperatorClient::new(&base_url, Some("wrong".into())).unwrap();
    assert!(matches!(unauth.whoami().await.unwrap_err(), OperatorClientError::Unauthorized(_)));

    // 3. Reader cannot mutate (403 Forbidden).
    let reader = OperatorClient::new(&base_url, Some(reader_tok)).unwrap();
    let who = reader.whoami().await.unwrap();
    assert_eq!(who.role, AuthRole::Reader);

    let scan_req = StartScanRequest {
        target: ScanTarget::LocalDirectory { path: fixture("scan/single_parquet") },
        profile: ScanProfile::Standard,
        options: Default::default(),
        scan_id: None,
        redaction: None,
        plugins: vec![],
    };
    let ferr = reader.submit_scan(&scan_req, Some("r-key")).await.unwrap_err();
    assert!(matches!(ferr, OperatorClientError::Forbidden(_)), "got {ferr:?}");

    // 4. Operator submits scan with idempotency key and streams SSE events.
    let op = OperatorClient::new(&base_url, Some(op_tok)).unwrap();
    let idem = generate_idempotency_key("scan");
    let sub1 = op.submit_scan(&scan_req, Some(&idem)).await.unwrap();
    let sub2 = op.submit_scan(&scan_req, Some(&idem)).await.unwrap();
    assert_eq!(sub1.job_id, sub2.job_id);

    let mut rx = op.stream_job_events(JobId(sub1.job_id), 0).await.unwrap();
    let mut seqs = Vec::new();
    let mut terminal_status = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(Ok(SseEventItem::JobEvent(ev)))) => seqs.push(ev.seq),
            Ok(Some(Ok(SseEventItem::JobStatus { status, .. }))) => {
                terminal_status = Some(status);
                break;
            }
            Ok(None) => break,
            Ok(Some(Err(e))) => panic!("SSE error: {e}"),
            Err(_) => panic!("SSE timeout"),
        }
    }
    assert_eq!(terminal_status, Some(JobStatus::Succeeded));
    assert!(!seqs.is_empty());
    assert!(seqs.windows(2).all(|w| w[0] < w[1]));

    // 5. Inspect job, overview, runs, findings, and generate/review/submit repair plan.
    let job = op.get_job(JobId(sub1.job_id)).await.unwrap();
    let run_id = job.run_id.expect("run_id");
    let _findings = op
        .list_findings(
            RunId(run_id),
            &PageQuery {
                limit: 20,
                offset: 0,
                inspection_status: None,
                severity: None,
                code: None,
            },
        )
        .await
        .unwrap();
    let _events_page = op
        .get_job_events(
            JobId(sub1.job_id),
            &JobEventListQuery { after_seq: 0, limit: 50, kind: None, stage: None },
        )
        .await
        .unwrap();
    let ov = op.overview().await.unwrap();
    assert_eq!(ov.status, "ok");
    let jobs = op
        .list_jobs(&JobListQuery { status: None, kind: None, limit: 20, offset: 0 })
        .await
        .unwrap();
    assert_eq!(jobs.total, 1);

    let plan_resp = op
        .generate_plan(&PlanBody {
            location: fixture("scan/single_parquet").to_string(),
            policy_toml: None,
            target_schema_path: None,
        })
        .await
        .unwrap();
    let out_dir = dir.path().join("repaired");
    let review = RepairReviewState::new(
        fixture("scan/single_parquet").to_string(),
        out_dir.to_str().unwrap(),
        plan_resp.plan,
    );
    let repair_sub = op
        .submit_repair(
            &RepairJobBody {
                source: review.source.clone(),
                output: review.output.clone(),
                plan: review.plan.clone(),
                authorize: review.authorized_list(),
            },
            Some(&generate_idempotency_key("repair")),
        )
        .await
        .unwrap();
    assert_ne!(repair_sub.job_id, sub1.job_id);
}
