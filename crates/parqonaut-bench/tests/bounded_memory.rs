//! Bounded-memory verification tests for streaming conversion, rewrite, and event coalescing.

use std::time::Duration;

use parqonaut_bench::{peak_rss_kib, write_parquet_file};
use parqonaut_service::events::lifecycle_event;
use parqonaut_service::ParqonautService;
use parqonaut_store::SqliteScanStore;
use parqonaut_stream::pipeline::Pipeline;
use parqonaut_stream::Cli as StreamCli;
use parqonaut_transform::rewrite;
use parqonaut_types::{JobId, JobKind, JobStage, TargetIdentity};
use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};
use std::io::{BufWriter, Write};

#[tokio::test]
async fn streaming_csv_to_parquet_250k_rows_stays_memory_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("big.csv");
    let out = dir.path().join("big.parquet");

    {
        let mut w = BufWriter::new(std::fs::File::create(&csv).unwrap());
        writeln!(w, "id,region,score").unwrap();
        for i in 0..250_000usize {
            writeln!(w, "{i},us,{:.2}", (i as f64) * 0.5).unwrap();
        }
        w.flush().unwrap();
    }

    let rss_before = peak_rss_kib();
    let cli = {
        use clap::Parser;
        StreamCli::parse_from([
            "parqonaut-stream",
            csv.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
            "--mem-budget",
            "64",
            "--writer-buffer",
            "8",
            "--quiet",
        ])
    };
    Pipeline::new(cli).execute().await.unwrap();
    let rss_after = peak_rss_kib();
    let delta_mib = rss_after.saturating_sub(rss_before) as f64 / 1024.0;
    assert!(out.exists());
    // 250k rows with 8k batch + cap 4 should keep additional RSS well under 256 MiB.
    assert!(
        delta_mib < 256.0,
        "streaming CSV->Parquet RSS growth exceeded bound: {delta_mib:.1} MiB"
    );
}

#[tokio::test]
async fn streaming_rewrite_200k_rows_stays_memory_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.parquet");
    let dst = dir.path().join("dst.parquet");
    write_parquet_file(&src, 200_000, 8192, false);

    let rss_before = peak_rss_kib();
    rewrite(
        src.to_str().unwrap(),
        dst.to_str().unwrap(),
        Some("zstd".into()),
        Some(1),
        None,
        None,
        true,
    )
    .await
    .unwrap();
    let rss_after = peak_rss_kib();
    let delta_mib = rss_after.saturating_sub(rss_before) as f64 / 1024.0;
    assert!(dst.exists());
    assert!(delta_mib < 256.0, "streaming rewrite RSS growth exceeded bound: {delta_mib:.1} MiB");
}

#[tokio::test]
async fn high_rate_event_burst_bounds_memory_and_retains_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("burst.sqlite");
    std::fs::File::create(&db).unwrap();
    let store = SqliteScanStore::connect(&format!("sqlite://{}", db.display())).await.unwrap();
    let svc = ParqonautService::new(store);
    let jid = JobId::new();
    let tid = TargetIdentity { target_kind: "local_file".into(), normalized_key: "/b.pq".into() };
    svc.store().insert_application_job(jid, JobKind::Scan, &tid, "{}").await.unwrap();

    for i in 0..10_000u64 {
        svc.events().record_nonblocking(lifecycle_event(
            jid,
            ProgressEventKind::BatchProcessed,
            JobStage::Scan,
            ProgressMetrics { rows: i, ..Default::default() },
            format!("burst {i}"),
        ));
    }
    svc.events().flush().await;
    svc.events()
        .record_lifecycle(lifecycle_event(
            jid,
            ProgressEventKind::ExecutionCompleted,
            JobStage::Scan,
            ProgressMetrics::default(),
            "burst complete",
        ))
        .await
        .unwrap();

    let events = svc.store().list_job_events(jid, 0, 5000, None, None).await.unwrap();
    assert!(events.len() <= 505, "expected retention bound, got {}", events.len());
    assert_eq!(events.last().unwrap().kind, ProgressEventKind::ExecutionCompleted);
    svc.graceful_shutdown(Duration::from_secs(2)).await;
}
