use std::hint::black_box;

use camino::Utf8PathBuf;
use chrono::Utc;
use clap::Parser;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use parqonaut_app::{
    DiagnoseRequest, ParqonautApp, PlanRequest, RepairRequest, APP_REQUEST_SCHEMA_VERSION,
};
use parqonaut_bench::BenchFixtures;
use parqonaut_core::ScanEngine;
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
use tokio::runtime::Runtime;

fn bench_scan_and_inspection(c: &mut Criterion) {
    let fx = BenchFixtures::generate(50_000, 25_000);
    let large_u = Utf8PathBuf::from_path_buf(fx.large_parquet.clone()).unwrap();
    let small_u = Utf8PathBuf::from_path_buf(fx.small_files_dir.clone()).unwrap();
    let part_u = Utf8PathBuf::from_path_buf(fx.deep_partitioned_dir.clone()).unwrap();

    let mut group = c.benchmark_group("scan_and_inspect");
    group.sample_size(20);

    group.bench_function("scan_quick_metadata_50k", |b| {
        b.iter(|| {
            let req = ScanRequest::new(
                ScanTarget::LocalFile { path: large_u.clone() },
                ScanProfile::Quick,
            );
            let rep = ScanEngine::scan(&req).unwrap();
            black_box(rep.summary.discovered_assets);
        });
    });

    group.throughput(Throughput::Elements(50_000));
    group.bench_function("scan_standard_large_50k", |b| {
        b.iter(|| {
            let req = ScanRequest::new(
                ScanTarget::LocalFile { path: large_u.clone() },
                ScanProfile::Standard,
            );
            let rep = ScanEngine::scan(&req).unwrap();
            black_box(rep.summary.inspected_assets);
        });
    });

    group.bench_function("scan_small_files_40", |b| {
        b.iter(|| {
            let req = ScanRequest::new(
                ScanTarget::LocalDirectory { path: small_u.clone() },
                ScanProfile::Standard,
            );
            let rep = ScanEngine::scan(&req).unwrap();
            black_box(rep.summary.discovered_assets);
        });
    });

    group.bench_function("scan_deeply_partitioned_24", |b| {
        b.iter(|| {
            let req = ScanRequest::new(
                ScanTarget::LocalDirectory { path: part_u.clone() },
                ScanProfile::Standard,
            );
            let rep = ScanEngine::scan(&req).unwrap();
            black_box(rep.summary.discovered_assets);
        });
    });

    group.finish();
}

fn bench_streaming_and_transforms(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fx = BenchFixtures::generate(50_000, 25_000);
    let mut group = c.benchmark_group("stream_and_transform");
    group.sample_size(15);

    group.throughput(Throughput::Elements(25_000));
    group.bench_function("csv_to_parquet_25k", |b| {
        b.iter(|| {
            let out = fx.out_dir.join("csv_conv.parquet");
            let cli = StreamCli::parse_from([
                "parqonaut-stream",
                fx.csv_input.to_str().unwrap(),
                "-o",
                out.to_str().unwrap(),
                "--quiet",
            ]);
            rt.block_on(async {
                Pipeline::new(cli).execute().await.unwrap();
            });
            black_box(out);
        });
    });

    group.throughput(Throughput::Elements(50_000));
    group.bench_function("rewrite_zstd_50k", |b| {
        b.iter(|| {
            let out = fx.out_dir.join("rewritten.parquet");
            rt.block_on(async {
                rewrite(
                    &fx.large_parquet.to_string_lossy(),
                    &out.to_string_lossy(),
                    Some("zstd".into()),
                    None,
                    None,
                    None,
                    true,
                )
                .await
                .unwrap();
            });
            black_box(out);
        });
    });

    let spec_dir = tempfile::tempdir().unwrap();
    let spec_path = spec_dir.path().join("spec.yaml");
    let fused_out = fx.out_dir.join("fused.parquet");
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
    group.bench_function("fused_transform_50k", |b| {
        b.iter(|| {
            let plan = compile_plan(&spec).unwrap();
            let rep = execute_plan(&plan).unwrap();
            black_box(rep.rows_written);
        });
    });

    group.finish();
}

fn bench_repair_and_store_events(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fx = BenchFixtures::generate(10_000, 5_000);
    let app = ParqonautApp::cli();
    let mut group = c.benchmark_group("repair_and_store");
    group.sample_size(15);

    group.bench_function("diagnose_plan_and_repair_small_files", |b| {
        b.iter(|| {
            let src = DatasetLocation::parse(&fx.small_files_dir.to_string_lossy()).unwrap();
            let out_dir = tempfile::tempdir().unwrap();
            let out_loc = DatasetLocation::parse(&out_dir.path().to_string_lossy()).unwrap();
            rt.block_on(async {
                let policy = EffectivePolicy::default();
                let _dx =
                    app.diagnose(DiagnoseRequest::new(src.clone(), policy.clone())).await.unwrap();
                let plan =
                    app.plan(PlanRequest::new(src.clone(), policy, None)).await.unwrap().plan;
                let exec = app
                    .repair(RepairRequest {
                        schema_version: APP_REQUEST_SCHEMA_VERSION,
                        source: src,
                        output: out_loc,
                        plan,
                        authorize: Vec::new(),
                    })
                    .await
                    .unwrap();
                black_box(exec.execution.execution_id);
            });
        });
    });

    group.bench_function("store_event_ingest_and_list_500", |b| {
        b.iter(|| {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("events.sqlite");
            std::fs::File::create(&db).unwrap();
            let url = format!("sqlite://{}", db.display());
            rt.block_on(async {
                let store = SqliteScanStore::connect(&url).await.unwrap();
                let jid = JobId::new();
                let tid = TargetIdentity {
                    target_kind: "local_file".into(),
                    normalized_key: "/bench.parquet".into(),
                };
                store.insert_application_job(jid, JobKind::Scan, &tid, "{}").await.unwrap();
                for i in 0..500u64 {
                    store
                        .append_job_event(
                            AppendJobEvent {
                                job_id: jid,
                                occurred_at: Utc::now(),
                                kind: ProgressEventKind::BatchProcessed,
                                stage: JobStage::Scan,
                                is_terminal: false,
                                metrics: ProgressMetrics { rows: i * 100, ..Default::default() },
                                message: Some(format!("batch {i}")),
                                run_id: None,
                                plugin_name: None,
                                plugin_digest: None,
                            },
                            2000,
                        )
                        .await
                        .unwrap();
                }
                let listed = store.list_job_events(jid, 0, 200, None, None).await.unwrap();
                black_box(listed.len());
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_scan_and_inspection,
    bench_streaming_and_transforms,
    bench_repair_and_store_events
);
criterion_main!(benches);
