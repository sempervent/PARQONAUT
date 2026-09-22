#![cfg(feature = "s3")]

mod common;

use std::path::PathBuf;

use arrow::array::Array;
use common::matrix_common::{
    count_parquet_rows, partition_fixture, require_s3_endpoint, s3_io, Leg,
};
use parqonaut_transform::{
    compile_plan, execute_plan, CompiledSegment, FusedOperation, Operation, Spec, Step,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn setup_plugins_env() {
    std::env::set_var(
        "PARQONAUT_PLUGIN_ROOTS",
        repo_root().join("fixtures/plugins").to_string_lossy().to_string(),
    );
    std::env::set_var(
        "PARQONAUT_PLUGIN_SDK_PATH",
        repo_root().join("python/parqonaut_plugins/src").to_string_lossy().to_string(),
    );
}

fn plugin_spec(input: &str, output: &str) -> Spec {
    Spec {
        schema_version: 1,
        input: Some(input.into()),
        output: Some(output.into()),
        steps: vec![Step {
            operation: Operation::Plugin {
                plugin: "normalize-strings".into(),
                config: serde_json::json!({"columns": ["email"]}),
            },
            options: Default::default(),
        }],
        options: parqonaut_transform::Options { overwrite: true, ..Default::default() },
    }
}

fn email_fingerprint(path: &str) -> String {
    let file = File::open(path).expect("open parquet");
    let mut reader =
        ParquetRecordBatchReaderBuilder::try_new(file).expect("reader").build().expect("build");
    let batch = reader.next().expect("batch").expect("batch ok");
    let col = batch.column_by_name("email").expect("email");
    let arr = col.as_any().downcast_ref::<arrow::array::StringArray>().expect("utf8");
    let mut out = String::new();
    for i in 0..arr.len().min(8) {
        if arr.is_null(i) {
            out.push('|');
        } else {
            out.push_str(arr.value(i));
            out.push('|');
        }
    }
    out
}

fn run_leg(leg: Leg, bytes: &[u8]) -> Result<(String, String, u64), String> {
    let work = tempfile::tempdir().expect("tempdir");
    let work_path = work.path().to_path_buf();
    let bytes_owned = bytes.to_vec();
    let paths = parqonaut_transform::remote::run_io_runtime(move |handle| {
        handle.block_on(async move {
            let (_io, s3) = s3_io().await;
            let bucket = common::matrix_common::test_bucket();
            leg.materialize(
                work_path.as_path(),
                &s3,
                &bucket,
                "plugin-matrix",
                "in.parquet",
                "out.parquet",
                &bytes_owned,
            )
            .await
        })
    });
    let spec = plugin_spec(&paths.input, &paths.output);
    let plan = compile_plan(&spec).expect("compile");
    let pinned = match &plan.segments[0] {
        CompiledSegment::Fused(f) => match &f.ops[0] {
            FusedOperation::Plugin(p) => p.digest.clone(),
            _ => panic!("expected plugin"),
        },
        _ => panic!("expected fused"),
    };
    // Same sync entry as CLI: no nested tokio runtime around execute_plan.
    let report = execute_plan(&plan).map_err(|e| e.to_string())?;
    assert_eq!(report.intermediate_files_created, 0);
    let out_path = paths.output.clone();
    let verify_work = work.path().to_path_buf();
    let out_path_owned = out_path.clone();
    let (rows, fp) = parqonaut_transform::remote::run_io_runtime(move |handle| {
        handle.block_on(async move {
            let (_io, s3) = s3_io().await;
            let rows = count_parquet_rows(&s3, &out_path_owned).await as u64;
            let fp = if out_path_owned.starts_with("s3://") {
                let local = verify_work.join("dl.parquet");
                let loc =
                    parqonaut_storage::location::ObjectLocation::parse(&out_path_owned).unwrap();
                use parqonaut_storage::backend::{ByteRange, StorageBackend};
                let meta = s3.head(&loc).await.expect("head output");
                let end = meta.size.saturating_sub(1);
                let data = s3
                    .read_range(&loc, ByteRange::new(0, end).expect("range"))
                    .await
                    .expect("read s3 output");
                std::fs::write(&local, &data).expect("write");
                email_fingerprint(local.to_str().unwrap())
            } else {
                email_fingerprint(&out_path_owned)
            };
            (rows, fp)
        })
    });
    Ok((fp, pinned, rows))
}

#[test]
fn batch_plugin_storage_topology_matrix() {
    if !require_s3_endpoint() {
        return;
    }
    setup_plugins_env();
    let bytes = partition_fixture();
    let mut baseline: Option<(String, String, u64)> = None;
    for leg in Leg::ALL {
        let (fp, digest, rows) =
            run_leg(leg, &bytes).unwrap_or_else(|e| panic!("matrix leg {:?} failed: {e}", leg));
        if let Some((bfp, bdig, brows)) = &baseline {
            assert_eq!(&fp, bfp, "fingerprint mismatch");
            assert_eq!(&digest, bdig, "digest mismatch");
            assert_eq!(rows, *brows, "row count mismatch");
        } else {
            baseline = Some((fp, digest, rows));
        }
    }
}
