//! Convert with independent local/S3 batch sources and sinks.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parqonaut_columnar::BatchSource;
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::columnar::{write_parquet_batch_stream, StorageParquetBatchSource};
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::{LocalStorageBackend, S3Config, S3StorageBackend};
use parqonaut_workflow::{NoOpProgressObserver, SchemaConflictPolicy};

use crate::cli::{Cli, Compression, OutputFormat};
use crate::coercion::BatchAligner;
use crate::csv_in::{CsvConfig, CsvReader};
use crate::discover::{discover_inputs, DiscoveryConfig, FileFormat, InputFile};
use crate::error::{MawError, Result};
use crate::parquet_in::ParquetReader;
use crate::pipeline::compression_from_cli_opts;
use crate::pipeline::Pipeline;
use crate::schema::UnifiedSchema;
use crate::writer_parquet::{ParquetWriter, ParquetWriterConfig};

enum ConvertInput {
    Local(InputFile),
    RemoteParquet(String),
}

pub fn needs_storage_routing(cli: &Cli) -> bool {
    let out_remote = cli.out.as_deref().map(|o| o.starts_with("s3://")).unwrap_or(false);
    cli.inputs.iter().any(|i| i.starts_with("s3://")) || out_remote
}

pub async fn execute(cli: Cli) -> Result<()> {
    tracing::info!(inputs = ?cli.inputs, out = ?cli.out, "storage_convert execute");
    if cli.state.is_some() {
        return Err(MawError::UnsupportedCapability(
            "resumable convert with remote s3:// paths is not supported in v0.9.1".into(),
        ));
    }
    if cli.plan || cli.dry_run {
        return Err(MawError::UnsupportedCapability(
            "plan/dry-run for remote convert uses local stream path only".into(),
        ));
    }

    let pipeline = Pipeline::new(cli.clone());
    let discovery_config = DiscoveryConfig {
        recursive: !cli.no_recursive,
        follow_symlinks: cli.follow_symlinks,
        max_depth: None,
    };

    let inputs = resolve_convert_inputs(&cli.inputs, &discovery_config)?;
    if inputs.is_empty() {
        return Err(MawError::InvalidInput("no inputs".into()));
    }

    for inp in &inputs {
        if matches!(inp, ConvertInput::RemoteParquet(uri) if uri.ends_with(".csv")) {
            return Err(MawError::UnsupportedCapability(
                "remote CSV input is not supported; use local CSV or s3:// Parquet".into(),
            ));
        }
    }

    let policy = pipeline.conflict_policy();
    let unified = build_unified_schema(&inputs, policy).await?;
    let unified_arc = Arc::new(unified);

    let output_spec = cli.out.clone().unwrap_or_else(|| "output.parquet".into());
    let output_format = determine_output_format(&output_spec, cli.out_format.as_ref())?;
    if output_spec.starts_with("s3://") && !matches!(output_format, OutputFormat::Parquet) {
        return Err(MawError::UnsupportedCapability(
            "remote output must be Parquet (s3://…/*.parquet)".into(),
        ));
    }

    run_convert_pipeline(&cli, &inputs, unified_arc, &output_spec, output_format).await
}

fn resolve_convert_inputs(
    inputs: &[String],
    config: &DiscoveryConfig,
) -> Result<Vec<ConvertInput>> {
    let mut out = Vec::new();
    for input in inputs {
        if input.starts_with("s3://") {
            if !input.ends_with(".parquet") {
                return Err(MawError::InvalidInput(format!(
                    "remote convert input must be a Parquet object: {input}"
                )));
            }
            out.push(ConvertInput::RemoteParquet(input.clone()));
            continue;
        }
        let files = discover_inputs(std::slice::from_ref(input), config)?;
        out.extend(files.into_iter().map(ConvertInput::Local));
    }
    Ok(out)
}

async fn build_unified_schema(
    inputs: &[ConvertInput],
    policy: SchemaConflictPolicy,
) -> Result<UnifiedSchema> {
    let inputs = inputs.to_vec();
    tokio::task::spawn_blocking(move || {
        let schemas: Vec<Schema> = inputs
            .iter()
            .map(|inp| match inp {
                ConvertInput::Local(f) => crate::schema_introspect::schema_for_input(f),
                ConvertInput::RemoteParquet(uri) => schema_for_remote_parquet(uri),
            })
            .collect::<Result<Vec<_>>>()?;
        crate::schema::unified_from_schemas(&schemas, policy)
    })
    .await
    .map_err(|e| MawError::InvalidInput(format!("schema task join error: {e}")))?
}

fn schema_for_remote_parquet(uri: &str) -> Result<Schema> {
    let rt = tokio::runtime::Runtime::new().map_err(MawError::Io)?;
    rt.block_on(async {
        let loc = ObjectLocation::parse(uri).map_err(|e| MawError::InvalidInput(e.to_string()))?;
        let local = Arc::new(LocalStorageBackend::direct());
        let s3 = Arc::new(S3StorageBackend::new(S3Config::from_env()).await);
        let backend: Arc<dyn parqonaut_storage::backend::StorageBackend> =
            if loc.is_remote() { s3 } else { local };
        let source = StorageParquetBatchSource::new(backend, loc);
        Ok(source.schema().map_err(|e| MawError::Schema(e.to_string()))?.as_ref().clone())
    })
}

fn determine_output_format(path: &str, explicit: Option<&OutputFormat>) -> Result<OutputFormat> {
    if let Some(format) = explicit {
        return Ok(format.clone());
    }
    if path.ends_with(".parquet") || path.starts_with("s3://") {
        return Ok(OutputFormat::Parquet);
    }
    Ok(OutputFormat::Csv)
}

async fn run_convert_pipeline(
    cli: &Cli,
    inputs: &[ConvertInput],
    unified_schema: Arc<UnifiedSchema>,
    output_spec: &str,
    output_format: OutputFormat,
) -> Result<()> {
    let (tx, rx) = mpsc::sync_channel::<RecordBatch>(8);
    let aligner = Arc::new(BatchAligner::new(
        unified_schema.clone(),
        HashMap::new(),
        cli.columns.as_ref().map(|c| {
            c.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
        }),
        cli.exclude.as_ref().map(|c| {
            c.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
        }),
        cli.stringify_conflicts || cli.schema_conflicts == "stringify",
    ));

    let mut handles = Vec::new();
    for input in inputs {
        let txc = tx.clone();
        let aligner_c = Arc::clone(&aligner);
        let cli_c = cli.clone();
        let input = input.clone();
        handles.push(tokio::task::spawn_blocking(move || read_input(input, cli_c, aligner_c, txc)));
    }
    drop(tx);

    let output_spec = output_spec.to_string();
    let schema = Arc::new(unified_schema.schema.clone());
    let compression = cli.compression.clone();
    let zstd_level = cli.zstd_level;

    let writer = tokio::spawn(async move {
        if output_spec.starts_with("s3://") && matches!(output_format, OutputFormat::Parquet) {
            write_remote_parquet(&output_spec, rx, schema).await
        } else {
            tokio::task::spawn_blocking(move || {
                write_output(&output_spec, output_format, rx, schema, &compression, zstd_level)
            })
            .await
            .map_err(|e| MawError::InvalidInput(format!("writer blocking task: {e}")))?
        }
    });

    let read_side = async {
        for h in handles {
            h.await??;
        }
        Ok::<(), MawError>(())
    };
    let (read_res, write_res) = tokio::join!(read_side, writer);
    read_res?;
    write_res.map_err(|e| MawError::InvalidInput(format!("writer task join: {e}")))??;
    Ok(())
}

async fn write_remote_parquet(
    output_spec: &str,
    rx: Receiver<RecordBatch>,
    schema: Arc<arrow::datatypes::Schema>,
) -> Result<()> {
    let loc =
        ObjectLocation::parse(output_spec).map_err(|e| MawError::InvalidInput(e.to_string()))?;
    let local = Arc::new(LocalStorageBackend::direct());
    let s3 = Arc::new(S3StorageBackend::new(S3Config::from_env()).await);
    let backend: Arc<dyn StorageBackend> = if loc.is_remote() { s3 } else { local };
    let stream = sync_receiver_batch_stream(rx);
    let summary = write_parquet_batch_stream(backend, loc, schema, stream, &NoOpProgressObserver)
        .await
        .map_err(|e| MawError::Parquet(e.to_string()))?;
    if summary.rows_written == 0 {
        return Err(MawError::InvalidInput("remote convert wrote no rows".into()));
    }
    Ok(())
}

fn sync_receiver_batch_stream(rx: Receiver<RecordBatch>) -> parqonaut_columnar::BatchStream {
    let rx = Arc::new(std::sync::Mutex::new(rx));
    Box::pin(futures::stream::unfold(rx, |rx| async move {
        let recv = tokio::task::spawn_blocking({
            let rx = Arc::clone(&rx);
            move || rx.lock().expect("batch rx lock").recv()
        })
        .await
        .ok()?;
        let batch = recv.ok()?;
        Some((Ok(batch), rx))
    }))
}

fn read_input(
    input: ConvertInput,
    cli: Cli,
    aligner: Arc<BatchAligner>,
    tx: SyncSender<RecordBatch>,
) -> Result<()> {
    match input {
        ConvertInput::Local(file) => match file.format {
            FileFormat::Csv => {
                let config = CsvConfig {
                    delimiter: cli.delimiter.map(|c| c as u8),
                    quote: cli.quote.map(|c| c as u8),
                    has_headers: !cli.no_headers,
                    encoding: cli.encoding.clone(),
                    na_values: cli.na.split(',').map(|s| s.trim().to_string()).collect(),
                    batch_size: 64_000,
                };
                let mut reader = CsvReader::new(&file.path, &config)?;
                let source_fields: Vec<String> = reader.get_headers().to_vec();
                while let Some(batch) = reader.read_batch()? {
                    let aligned = aligner.align_batch_with_source(batch, &source_fields)?;
                    if tx.send(aligned).is_err() {
                        break;
                    }
                }
            }
            FileFormat::Parquet => {
                let mut reader = ParquetReader::new(&file.path, 64_000)?;
                let source_fields: Vec<String> =
                    reader.get_schema().fields().iter().map(|f| f.name().to_string()).collect();
                while let Some(batch) = reader.read_batch()? {
                    let aligned = aligner.align_batch_with_source(batch, &source_fields)?;
                    if tx.send(aligned).is_err() {
                        break;
                    }
                }
            }
        },
        ConvertInput::RemoteParquet(uri) => {
            let rt = tokio::runtime::Runtime::new().map_err(MawError::Io)?;
            rt.block_on(async {
                let loc = ObjectLocation::parse(&uri)
                    .map_err(|e| MawError::InvalidInput(e.to_string()))?;
                let local = Arc::new(LocalStorageBackend::direct());
                let s3 = Arc::new(S3StorageBackend::new(S3Config::from_env()).await);
                let backend: Arc<dyn parqonaut_storage::backend::StorageBackend> =
                    if loc.is_remote() { s3 } else { local };
                let source = StorageParquetBatchSource::new(backend, loc);
                let fields: Vec<String> = source
                    .schema()
                    .map_err(|e| MawError::Schema(e.to_string()))?
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();
                let mut stream =
                    Box::new(source).into_stream().map_err(|e| MawError::Schema(e.to_string()))?;
                while let Some(item) = stream.next().await {
                    let batch = item.map_err(|e| MawError::Schema(e.to_string()))?;
                    let aligned = aligner.align_batch_with_source(batch, &fields)?;
                    if tx.send(aligned).is_err() {
                        break;
                    }
                }
                Ok::<(), MawError>(())
            })?;
        }
    }
    Ok(())
}

fn write_output(
    output_spec: &str,
    output_format: OutputFormat,
    rx: Receiver<RecordBatch>,
    schema: Arc<arrow::datatypes::Schema>,
    compression: &Compression,
    zstd_level: u32,
) -> Result<()> {
    match output_format {
        OutputFormat::Csv => {
            if output_spec.starts_with("s3://") {
                return Err(MawError::UnsupportedCapability(
                    "remote CSV output is not supported".into(),
                ));
            }
            let path = PathBuf::from(output_spec);
            let config = crate::writer_csv::CsvWriterConfig::default();
            let mut writer = crate::writer_csv::CsvWriter::new(&path, &config)?;
            while let Ok(batch) = rx.recv() {
                writer.write_batch(&batch)?;
            }
            writer.finish()?;
        }
        OutputFormat::Parquet => {
            if output_spec.starts_with("s3://") {
                return Err(MawError::InvalidInput(
                    "remote parquet write must use async write_remote_parquet".into(),
                ));
            } else {
                let parquet_compression = compression_from_cli_opts(compression);
                let config = ParquetWriterConfig {
                    compression: parquet_compression,
                    zstd_level: zstd_level as i32,
                };
                let mut writer = ParquetWriter::new(output_spec, schema, &config)?;
                while let Ok(batch) = rx.recv() {
                    writer.write_batch(&batch)?;
                }
                writer.finish()?;
            }
        }
    }
    Ok(())
}

impl Clone for ConvertInput {
    fn clone(&self) -> Self {
        match self {
            Self::Local(f) => Self::Local(f.clone()),
            Self::RemoteParquet(u) => Self::RemoteParquet(u.clone()),
        }
    }
}
