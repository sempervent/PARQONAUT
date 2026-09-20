//! User-visible local/S3 pipeline matrix (6 capabilities × 4 storage legs).

#![cfg(feature = "s3")]

mod matrix_common;

use std::path::PathBuf;

use matrix_common::{
    base_stream_cli, collect_parquet_outputs_async, count_parquet_rows, parquet_fixture,
    partition_fixture, require_s3_endpoint, s3_io, test_bucket, Leg,
};
use parqonaut_transform::{
    compile_plan, execute_plan, merge_parquet_storage, parse_spec, partition_parquet_routed,
    rewrite_parquet_storage, split_parquet_routed,
};

#[tokio::test(flavor = "multi_thread")]
async fn matrix_rewrite_four_legs() {
    require_s3_endpoint();
    let (io, s3) = s3_io().await;
    let bucket = test_bucket();
    let work = tempfile::tempdir().unwrap();
    let fixture = parquet_fixture(2);
    for leg in Leg::ALL {
        let paths = leg
            .materialize(&work, &s3, &bucket, "rewrite", "in.parquet", "out.parquet", &fixture)
            .await;
        rewrite_parquet_storage(&io, &paths.input, &paths.output).expect("rewrite");
        assert_eq!(count_parquet_rows(&s3, &paths.output).await, 2, "{leg:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_merge_four_legs() {
    require_s3_endpoint();
    let (io, s3) = s3_io().await;
    let bucket = test_bucket();
    let work = tempfile::tempdir().unwrap();
    let fixture = parquet_fixture(2);
    for leg in Leg::ALL {
        let a =
            leg.materialize(&work, &s3, &bucket, "merge", "a.parquet", "a.parquet", &fixture).await;
        let b =
            leg.materialize(&work, &s3, &bucket, "merge", "b.parquet", "b.parquet", &fixture).await;
        let out = leg.output_uri(&work, &bucket, "merge", "merged.parquet");
        merge_parquet_storage(&io, &[a.input, b.input], &out).expect("merge");
        assert_eq!(count_parquet_rows(&s3, &out).await, 4, "{leg:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_split_four_legs() {
    require_s3_endpoint();
    let (io, s3) = s3_io().await;
    let bucket = test_bucket();
    let work = tempfile::tempdir().unwrap();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/transform/split-basic/input.parquet");
    let bytes = std::fs::read(&root).expect("split fixture");
    for leg in Leg::ALL {
        let paths =
            leg.materialize(&work, &s3, &bucket, "split", "in.parquet", "parts", &bytes).await;
        let source_rows = count_parquet_rows(&s3, &paths.input).await;
        let outs = split_parquet_routed(&io, &paths.input, &paths.output, 1).expect("split");
        assert!(outs.len() >= 2, "{leg:?} expected multiple parts");
        let mut total = 0usize;
        for o in &outs {
            total += count_parquet_rows(&s3, o).await;
        }
        assert_eq!(total, source_rows, "{leg:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_partition_four_legs() {
    require_s3_endpoint();
    let (io, s3) = s3_io().await;
    let bucket = test_bucket();
    let work = tempfile::tempdir().unwrap();
    let bytes = partition_fixture();
    for leg in Leg::ALL {
        let paths =
            leg.materialize(&work, &s3, &bucket, "partition", "in.parquet", "out", &bytes).await;
        let source_rows = count_parquet_rows(&s3, &paths.input).await;
        let outs =
            partition_parquet_routed(&io, &paths.input, &paths.output, &["region".into()], 8)
                .expect("partition");
        assert!(!outs.is_empty(), "{leg:?}");
        let mut total = 0usize;
        for o in &outs {
            total += count_parquet_rows(&s3, o).await;
        }
        assert_eq!(total, source_rows, "{leg:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_fused_spec_four_legs() {
    require_s3_endpoint();
    let (_io, s3) = s3_io().await;
    let bucket = test_bucket();
    let work = tempfile::tempdir().unwrap();
    let bytes = partition_fixture();
    let spec_template = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/transform/specs/rewrite-partition.yaml");
    for leg in Leg::ALL {
        let paths =
            leg.materialize(&work, &s3, &bucket, "fused", "in.parquet", "processed", &bytes).await;
        let source_rows = count_parquet_rows(&s3, &paths.input).await;
        let mut spec = parse_spec(&spec_template).expect("parse spec");
        *spec.input.as_mut().unwrap() = paths.input.clone();
        *spec.output.as_mut().unwrap() = paths.output.clone();
        spec.options.overwrite = true;
        let plan = compile_plan(&spec).expect("compile");
        let report = execute_plan(&plan).expect("fused execute");
        assert_eq!(report.intermediate_files_created, 0, "{leg:?}");
        let outs = collect_parquet_outputs_async(&paths.output, &s3).await;
        assert!(!outs.is_empty(), "{leg:?} partition outputs");
        let mut total = 0usize;
        for out in &outs {
            total += count_parquet_rows(&s3, out).await;
        }
        assert_eq!(total, source_rows, "{leg:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_convert_four_legs() {
    require_s3_endpoint();
    let (_io, s3) = s3_io().await;
    let bucket = test_bucket();
    let work = tempfile::tempdir().unwrap();
    let fixture = parquet_fixture(3);
    for leg in Leg::ALL {
        let paths = leg
            .materialize(&work, &s3, &bucket, "convert", "in.parquet", "out.parquet", &fixture)
            .await;
        let mut cli = base_stream_cli();
        cli.inputs = vec![paths.input.clone()];
        cli.out = Some(paths.output.clone());
        parqonaut_stream::run(cli).await.expect("convert");
        assert_eq!(count_parquet_rows(&s3, &paths.output).await, 3, "{leg:?}");
    }
}
