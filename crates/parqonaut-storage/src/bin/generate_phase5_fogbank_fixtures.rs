//! Generate Phase 5 FOGBANK fixtures (local Parquet + optional MinIO upload).
//!
//! ```text
//! cargo run -p parqonaut-storage --features s3 --bin generate-phase5-fogbank-fixtures -- fixtures/phase5/local
//! cargo run -p parqonaut-storage --features s3 --bin generate-phase5-fogbank-fixtures -- fixtures/phase5/local --upload
//! ```

use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

#[cfg(feature = "s3")]
use parqonaut_storage::backend::StorageBackend;
#[cfg(feature = "s3")]
use parqonaut_storage::conditional::ConditionalCreate;
#[cfg(feature = "s3")]
use parqonaut_storage::fingerprint::compute_remote_fingerprint;
#[cfg(feature = "s3")]
use parqonaut_storage::inventory::list_remote_inventory;
#[cfg(feature = "s3")]
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
#[cfg(feature = "s3")]
use parqonaut_storage::{S3Config, S3StorageBackend};

const HEALTHY_ROWS: i64 = 500;
const LARGE_ROWS: i64 = 250_000;
const STALE_SNAPSHOTS: usize = 3;

#[derive(Debug, Clone, Copy)]
struct DatasetStats {
    files: u32,
    rows: u64,
    bytes: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root =
        args.next().map(PathBuf::from).unwrap_or_else(|| PathBuf::from("fixtures/phase5/local"));
    let upload = args.any(|a| a == "--upload");

    fs::create_dir_all(&root)?;
    let manifest_root = root
        .parent()
        .map(|p| p.join("manifests"))
        .unwrap_or_else(|| PathBuf::from("fixtures/phase5/manifests"));
    fs::create_dir_all(&manifest_root)?;

    let healthy = write_healthy(&root.join("healthy"))?;
    let large = write_large_parquet(&root.join("large-parquet"))?;
    let stale = write_stale_source(&root.join("stale-source"))?;

    write_scenario_readme(&root.join("stale-source"))?;

    println!("Phase 5 FOGBANK local fixtures written under {}", root.display());
    println!();
    println!("scenario             files    rows         bytes");
    println!("-------------------- -------- ------------ ------------");
    for (name, stats) in [("healthy", healthy), ("large-parquet", large), ("stale-source", stale)] {
        println!(
            "{name:<20} {files:>8} {rows:>12} {bytes:>12}",
            files = stats.files,
            rows = stats.rows,
            bytes = stats.bytes
        );
    }

    if upload {
        #[cfg(not(feature = "s3"))]
        {
            return Err("upload requires the `s3` feature".into());
        }
        #[cfg(feature = "s3")]
        {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(upload_fixtures(&root, &manifest_root))?;
        }
    } else {
        println!();
        println!("Local-only generation complete. Upload with --upload after MinIO is up.");
    }

    Ok(())
}

fn writer_props(compression: Compression, max_row_group_size: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(max_row_group_size)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build()
}

fn write_batch(
    path: &Path,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    props: WriterProperties,
) -> Result<u64, Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema, Some(props))?;
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(fs::metadata(path)?.len())
}

fn id_batch(schema: Arc<Schema>, rows: i64, start_id: i64) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + rows).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(vec!["healthy"; rows as usize])),
        ],
    )
    .expect("batch")
}

fn wide_batch(schema: Arc<Schema>, rows: i64, start_id: i64) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + rows).collect();
    let payload: Vec<String> =
        ids.iter().map(|id| format!("payload-{id}-{}", "x".repeat(96))).collect();
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(payload))],
    )
    .expect("batch")
}

fn write_healthy(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("kind", DataType::Utf8, false),
    ]));
    let props = writer_props(Compression::ZSTD(ZstdLevel::default()), 128 * 1024);

    let mut total_bytes = 0u64;
    for (idx, name) in ["events-001.parquet", "events-002.parquet"].iter().enumerate() {
        total_bytes += write_batch(
            &dir.join(name),
            schema.clone(),
            vec![id_batch(schema.clone(), HEALTHY_ROWS, idx as i64 * HEALTHY_ROWS)],
            props.clone(),
        )?;
    }

    let meta = serde_json::json!({
        "scenario": "healthy",
        "description": "Small multi-file remote dataset for inventory + fingerprint smoke tests.",
        "files": ["events-001.parquet", "events-002.parquet"],
    });
    fs::write(dir.join("manifest.json"), serde_json::to_string_pretty(&meta)?)?;

    Ok(DatasetStats { files: 2, rows: (HEALTHY_ROWS * 2) as u64, bytes: total_bytes })
}

fn write_large_parquet(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let props = writer_props(Compression::ZSTD(ZstdLevel::default()), 64 * 1024);
    let chunk: i64 = 50_000;
    let mut batches = Vec::new();
    let mut start = 0i64;
    while start < LARGE_ROWS {
        let take = chunk.min(LARGE_ROWS - start);
        batches.push(wide_batch(schema.clone(), take, start));
        start += take;
    }
    let bytes = write_batch(&dir.join("wide-table.parquet"), schema, batches, props)?;

    Ok(DatasetStats { files: 1, rows: LARGE_ROWS as u64, bytes })
}

fn write_stale_source(dir: &Path) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("kind", DataType::Utf8, false),
    ]));
    let props = writer_props(Compression::ZSTD(ZstdLevel::default()), 128 * 1024);

    let mut total_bytes = 0u64;
    for i in 0..STALE_SNAPSHOTS {
        total_bytes += write_batch(
            &dir.join(format!("snapshot-{i:02}.parquet")),
            schema.clone(),
            vec![id_batch(schema.clone(), 300, i as i64 * 300)],
            props.clone(),
        )?;
    }

    Ok(DatasetStats {
        files: STALE_SNAPSHOTS as u32,
        rows: (300 * STALE_SNAPSHOTS) as u64,
        bytes: total_bytes,
    })
}

fn write_scenario_readme(dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let path = dir.join("MUTATION.md");
    let body = r"# Stale-source mutation (FOGBANK)

Baseline uploads `snapshot-00.parquet` … `snapshot-02.parquet` only.

After capturing a plan bound to the baseline remote fingerprint, add a new object to simulate drift:

```bash
cp fixtures/phase5/local/stale-source/snapshot-02.parquet \
   fixtures/phase5/local/stale-source/snapshot-03.parquet
cargo run -p parqonaut-storage --features s3 --bin generate-phase5-fogbank-fixtures -- \
  fixtures/phase5/local --upload
```

Or upload only the new key with your preferred S3 client. Fingerprint verification should fail once `snapshot-03.parquet` appears remotely.
";
    fs::write(path, body)?;
    Ok(())
}

#[cfg(feature = "s3")]
async fn upload_fixtures(
    local_root: &Path,
    manifest_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    let bucket = std::env::var("FOGBANK_BUCKET").unwrap_or_else(|_| "fogbank".into());
    let prefix = std::env::var("FOGBANK_DATASET_PREFIX").unwrap_or_else(|_| "datasets".into());

    let config = S3Config::minio(endpoint);
    let backend = S3StorageBackend::new(config).await;

    let scenarios = ["healthy", "large-parquet", "stale-source"];
    for scenario in scenarios {
        let scenario_dir = local_root.join(scenario);
        if !scenario_dir.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&scenario_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or_default();
            if !name.ends_with(".parquet") {
                continue;
            }
            let key = format!("{prefix}/{scenario}/{name}");
            let object = ObjectLocation::S3 { bucket: bucket.clone(), key: key.clone() };
            let bytes = fs::read(&path)?;
            let _ = backend.delete_owned_object(&object).await;
            backend
                .conditional_create(
                    &object,
                    ConditionalCreate::must_not_exist(),
                    Bytes::from(bytes),
                )
                .await
                .map_err(|e| format!("upload {key}: {e}"))?;
            println!("uploaded s3://{bucket}/{key}");
        }
    }

    for scenario in ["healthy", "stale-source"] {
        let dataset = DatasetLocation::parse(&format!("s3://{bucket}/{prefix}/{scenario}/"))?;
        let inventory = list_remote_inventory(&backend, &dataset).await?;
        let fingerprint = compute_remote_fingerprint(&inventory);
        let manifest_name = if scenario == "healthy" {
            "healthy-fingerprint.json"
        } else {
            "stale-baseline-fingerprint.json"
        };
        let manifest_path = manifest_root.join(manifest_name);
        fs::write(&manifest_path, serde_json::to_string_pretty(&fingerprint)?)?;
        println!("wrote {}", manifest_path.display());
    }

    Ok(())
}
