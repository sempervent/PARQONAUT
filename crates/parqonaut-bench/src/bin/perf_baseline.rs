//! Reproducible single-shot baseline profiler emitting Markdown metrics for docs/history/v0.12.0/performance.md.

use std::time::Instant;

use camino::Utf8PathBuf;
use chrono::Utc;
use clap::Parser;
use parqonaut_app::{
    BatchPlanRequest, BatchRepairRequest, DiagnoseRequest, ParqonautApp, PlanRequest,
    RepairRequest, APP_REQUEST_SCHEMA_VERSION,
};
use parqonaut_bench::{peak_rss_kib, BenchFixtures};
use parqonaut_core::ScanEngine;
use parqonaut_orchestrator::{write_plan, CancelFlag};
use parqonaut_repair::EffectivePolicy;
use parqonaut_storage::location::DatasetLocation;
use parqonaut_store::{AppendJobEvent, SqliteScanStore};
use parqonaut_stream::pipeline::Pipeline;
use parqonaut_stream::Cli as StreamCli;
use parqonaut_transform::{compile_plan, execute_plan, parse_spec, rewrite};
use parqonaut_types::{
    JobId, JobKind, JobStage, ScanProfile, ScanRequest, ScanTarget, TargetIdentity,
};
use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};

#[tokio::main]
async fn main() {
    let fx = BenchFixtures::generate(100_000, 100_000);
    let app = ParqonautApp::cli();
    let large_u = Utf8PathBuf::from_path_buf(fx.large_parquet.clone()).unwrap();
    let small_u = Utf8PathBuf::from_path_buf(fx.small_files_dir.clone()).unwrap();
    let part_u = Utf8PathBuf::from_path_buf(fx.deep_partitioned_dir.clone()).unwrap();
    let drift_u = Utf8PathBuf::from_path_buf(fx.drift_dir.clone()).unwrap();

    println!("| Workload | Rows / Items | Elapsed (ms) | Throughput | Peak RSS (MiB) |");
    println!("|---|---:|---:|---:|---:|");

    // 1. Metadata quick scan
    let t0 = Instant::now();
    let rep = ScanEngine::scan(&ScanRequest::new(
        ScanTarget::LocalFile { path: large_u.clone() },
        ScanProfile::Quick,
    ))
    .unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "| Parquet metadata inspection (100k rows) | {} file | {ms:.2} | {:.1} files/s | {:.1} |",
        rep.summary.discovered_assets,
        1000.0 / ms,
        peak_rss_kib() as f64 / 1024.0
    );

    // 2. Large-file standard scan
    let t0 = Instant::now();
    let _rep = ScanEngine::scan(&ScanRequest::new(
        ScanTarget::LocalFile { path: large_u.clone() },
        ScanProfile::Standard,
    ))
    .unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    let sz_mib = std::fs::metadata(&fx.large_parquet).unwrap().len() as f64 / (1024.0 * 1024.0);
    println!(
        "| Large-file standard scan (100k rows, {sz_mib:.2} MiB) | 100,000 rows | {ms:.2} | {:.0} rows/s ({:.1} MiB/s) | {:.1} |",
        100_000.0 / (ms / 1000.0),
        sz_mib / (ms / 1000.0),
        peak_rss_kib() as f64 / 1024.0
    );

    // 3. Small-files scan (40 files)
    let t0 = Instant::now();
    let _ = ScanEngine::scan(&ScanRequest::new(
        ScanTarget::LocalDirectory { path: small_u.clone() },
        ScanProfile::Standard,
    ))
    .unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "| Small-file dataset scan (40 files) | 40 files | {ms:.2} | {:.1} files/s | {:.1} |",
        40.0 / (ms / 1000.0),
        peak_rss_kib() as f64 / 1024.0
    );

    // 4. Deeply partitioned dataset scan (24 partitions)
    let t0 = Instant::now();
    let _ = ScanEngine::scan(&ScanRequest::new(
        ScanTarget::LocalDirectory { path: part_u.clone() },
        ScanProfile::Standard,
    ))
    .unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "| Deeply partitioned scan (3x2x4=24 partitions) | 24 files | {ms:.2} | {:.1} files/s | {:.1} |",
        24.0 / (ms / 1000.0),
        peak_rss_kib() as f64 / 1024.0
    );

    // 5. Schema reconciliation plan
    let t0 = Instant::now();
    let drift_loc = DatasetLocation::parse(drift_u.as_str()).unwrap();
    let _dx = app
        .diagnose(DiagnoseRequest::new(drift_loc.clone(), EffectivePolicy::default()))
        .await
        .unwrap();
    let _plan =
        app.plan(PlanRequest::new(drift_loc, EffectivePolicy::default(), None)).await.unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "| Schema reconciliation (8 files, mixed Int32/Int64) | 8 files | {ms:.2} | {:.1} plans/s | {:.1} |",
        1000.0 / ms,
        peak_rss_kib() as f64 / 1024.0
    );

    // 6. Streaming CSV -> Parquet (100k rows)
    let csv_out = fx.out_dir.join("stream_100k.parquet");
    let csv_mib = std::fs::metadata(&fx.csv_input).unwrap().len() as f64 / (1024.0 * 1024.0);
    let cli = StreamCli::parse_from([
        "parqonaut-stream",
        fx.csv_input.to_str().unwrap(),
        "-o",
        csv_out.to_str().unwrap(),
        "--quiet",
    ]);
    let t0 = Instant::now();
    Pipeline::new(cli).execute().await.unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "| Streaming CSV -> Parquet (100k rows, {csv_mib:.2} MiB CSV) | 100,000 rows | {ms:.2} | {:.0} rows/s ({:.1} MiB/s) | {:.1} |",
        100_000.0 / (ms / 1000.0),
        csv_mib / (ms / 1000.0),
        peak_rss_kib() as f64 / 1024.0
    );

    // 7. Rewrite & recompression (100k rows)
    let rw_out = fx.out_dir.join("rw_100k.parquet");
    let t0 = Instant::now();
    rewrite(
        fx.large_parquet.to_str().unwrap(),
        rw_out.to_str().unwrap(),
        Some("zstd".into()),
        None,
        None,
        None,
        true,
    )
    .await
    .unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "| Rewrite ZSTD + stats rebuild (100k rows) | 100,000 rows | {ms:.2} | {:.0} rows/s | {:.1} |",
        100_000.0 / (ms / 1000.0),
        peak_rss_kib() as f64 / 1024.0
    );

    // 8. Fused multi-step transform (filter + project)
    let fused_out = fx.out_dir.join("fused_100k.parquet");
    let spec_path = fx.out_dir.join("spec.yaml");
    std::fs::write(
        &spec_path,
        format!(
            "schema-version: 1\ninput: \"{}\"\noutput: \"{}\"\noptions:\n  overwrite: true\nsteps:\n  - operation:\n      type: rewrite\n      compression: zstd\n  - operation:\n      type: partition\n      partition-by:\n        - region\n",
            fx.large_parquet.display(),
            fused_out.display()
        ),
    )
    .unwrap();
    let spec = parse_spec(&spec_path).unwrap();
    let plan = compile_plan(&spec).unwrap();
    let t0 = Instant::now();
    let rep = execute_plan(&plan).unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "| Fused multi-step transform (rewrite + partition, 100k rows) | 100,000 rows ({} parts) | {ms:.2} | {:.0} rows/s | {:.1} |",
        rep.output_files.len(),
        100_000.0 / (ms / 1000.0),
        peak_rss_kib() as f64 / 1024.0
    );

    // 9. Repair planning + execution (40 small files)
    let rep_out = fx.out_dir.join("repaired_small");
    let src_loc = DatasetLocation::parse(small_u.as_str()).unwrap();
    let dst_loc = DatasetLocation::parse(rep_out.to_str().unwrap()).unwrap();
    let t0 = Instant::now();
    let _dx = app
        .diagnose(DiagnoseRequest::new(src_loc.clone(), EffectivePolicy::default()))
        .await
        .unwrap();
    let rplan = app
        .plan(PlanRequest::new(src_loc.clone(), EffectivePolicy::default(), None))
        .await
        .unwrap()
        .plan;
    let _exec = app
        .repair(RepairRequest {
            schema_version: APP_REQUEST_SCHEMA_VERSION,
            source: src_loc,
            output: dst_loc,
            plan: rplan,
            authorize: Vec::new(),
        })
        .await
        .unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "| Repair plan + execution (40 small files) | 40 files | {ms:.2} | {:.1} datasets/s | {:.1} |",
        1000.0 / ms,
        peak_rss_kib() as f64 / 1024.0
    );

    // 10. Concurrent batch scheduling (4 datasets, concurrency 2)
    let batch_root = fx.out_dir.join("batch");
    let batch_out = batch_root.join("out");
    std::fs::create_dir_all(&batch_out).unwrap();
    let mut ds_entries = String::new();
    for i in 0..4 {
        let d = batch_root.join(format!("ds-{i}"));
        std::fs::create_dir_all(&d).unwrap();
        parqonaut_bench::write_parquet_file(&d.join("data.parquet"), 1000, 1000, false);
        ds_entries
            .push_str(&format!("\n[[datasets]]\nid = \"ds-{i}\"\npath = \"{}\"\n", d.display()));
    }
    let cfg_path = batch_root.join("batch.toml");
    std::fs::write(
        &cfg_path,
        format!(
            "schema_version = 1\n\n[batch]\nname = \"perf-batch\"\nmax_concurrency = 2\noutput_root = \"{}\"\nrun_root = \"{}\"\n{}",
            batch_out.display(),
            batch_root.display(),
            ds_entries
        ),
    )
    .unwrap();
    let bplan = app
        .batch_plan(BatchPlanRequest {
            schema_version: APP_REQUEST_SCHEMA_VERSION,
            config_path: Utf8PathBuf::from_path_buf(cfg_path).unwrap(),
        })
        .unwrap()
        .plan;
    let bplan_path = Utf8PathBuf::from_path_buf(batch_root.join("batch-plan.json")).unwrap();
    write_plan(&bplan, &bplan_path).unwrap();
    let t0 = Instant::now();
    let _bres = app
        .batch_repair(
            BatchRepairRequest {
                schema_version: APP_REQUEST_SCHEMA_VERSION,
                plan_path: bplan_path,
                jobs: Some(2),
                dry_run: false,
                interrupt_after_completed: None,
            },
            CancelFlag::new(),
        )
        .await
        .unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "| Concurrent batch scheduling (4 datasets, jobs=2) | 4 datasets | {ms:.2} | {:.1} datasets/s | {:.1} |",
        4.0 / (ms / 1000.0),
        peak_rss_kib() as f64 / 1024.0
    );

    // 11. Job/event ingest & listing under 2000-event load
    let db = fx.out_dir.join("perf_events.sqlite");
    std::fs::File::create(&db).unwrap();
    let store = SqliteScanStore::connect(&format!("sqlite://{}", db.display())).await.unwrap();
    let jid = JobId::new();
    let tid = TargetIdentity { target_kind: "local_file".into(), normalized_key: "/p.pq".into() };
    store.insert_application_job(jid, JobKind::Scan, &tid, "{}").await.unwrap();
    let t0 = Instant::now();
    for i in 0..2000u64 {
        store
            .append_job_event(
                AppendJobEvent {
                    job_id: jid,
                    occurred_at: Utc::now(),
                    kind: ProgressEventKind::BatchProcessed,
                    stage: JobStage::Scan,
                    is_terminal: false,
                    metrics: ProgressMetrics { rows: i, ..Default::default() },
                    message: Some(format!("ev {i}")),
                    run_id: None,
                    plugin_name: None,
                    plugin_digest: None,
                },
                2500,
            )
            .await
            .unwrap();
    }
    let ingest_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let t1 = Instant::now();
    let listed = store.list_job_events(jid, 0, 500, None, None).await.unwrap();
    let list_ms = t1.elapsed().as_secs_f64() * 1000.0;
    println!(
        "| SQLite job event ingest (2,000 events) | 2,000 events | {ingest_ms:.2} | {:.0} events/s | {:.1} |",
        2000.0 / (ingest_ms / 1000.0),
        peak_rss_kib() as f64 / 1024.0
    );
    println!(
        "| SQLite job event page list (limit=500) | {} events | {list_ms:.2} | {:.1} queries/s | {:.1} |",
        listed.len(),
        1000.0 / list_ms,
        peak_rss_kib() as f64 / 1024.0
    );
}
