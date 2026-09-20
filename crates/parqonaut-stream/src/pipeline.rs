use crate::{
    cli::{Cli, OutputFormat},
    coercion::BatchAligner,
    csv_in::{CsvConfig, CsvReader},
    discover::{discover_inputs, DiscoveryConfig, FileFormat, InputFile},
    error::{MawError, Result},
    parquet_in::ParquetReader,
    schema::UnifiedSchema,
    schema_introspect::schemas_for_inputs,
    state::{
        fingerprint_inputs, verify_sources_unchanged, StateManager, StreamExecutionIdentity,
        CHECKPOINT_SCHEMA_VERSION,
    },
    writer_csv::{CsvWriter, CsvWriterConfig},
    writer_parquet::{ParquetWriter, ParquetWriterConfig},
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parqonaut_workflow::{
    JsonLinesProgressObserver, NoOpProgressObserver, ProgressEvent, ProgressEventKind,
    ProgressObserver, SchemaConflictPolicy, TerminalProgressObserver,
};
use parquet::basic::Compression;
use std::io::Write;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::mpsc;

pub struct Pipeline {
    cli: Cli,
    progress: Arc<dyn ProgressObserver>,
}

impl Pipeline {
    pub fn new(cli: Cli) -> Self {
        let progress: Arc<dyn ProgressObserver> = if cli.json_progress {
            Arc::new(JsonLinesProgressObserver::new(std::io::stderr()))
        } else if cli.progress && !cli.no_progress && !cli.quiet {
            Arc::new(TerminalProgressObserver::new())
        } else {
            Arc::new(NoOpProgressObserver)
        };
        Self { cli, progress }
    }

    fn emit(&self, kind: ProgressEventKind, message: impl Into<String>) {
        self.progress.emit(ProgressEvent {
            kind,
            path: None,
            metrics: Default::default(),
            message: Some(message.into()),
        });
    }

    fn conflict_policy(&self) -> SchemaConflictPolicy {
        if self.cli.stringify_conflicts {
            return SchemaConflictPolicy::Stringify;
        }
        match self.cli.schema_conflicts.as_str() {
            "widen" => SchemaConflictPolicy::Widen,
            "stringify" => SchemaConflictPolicy::Stringify,
            _ => SchemaConflictPolicy::Strict,
        }
    }

    pub async fn execute(&self) -> Result<()> {
        self.emit(ProgressEventKind::ExecutionStarted, "stream conversion started");
        let discovery_config = DiscoveryConfig {
            recursive: !self.cli.no_recursive,
            follow_symlinks: self.cli.follow_symlinks,
            max_depth: None,
        };

        let input_files = discover_inputs(&self.cli.inputs, &discovery_config)?;

        if input_files.is_empty() {
            return Err(MawError::InvalidInput("No input files found".to_string()));
        }

        let policy = self.conflict_policy();
        let unified_schema = self.build_unified_schema(&input_files, policy).await?;
        let unified_arc = Arc::new(unified_schema);

        let output_path = self.cli.out.clone().unwrap_or_else(|| PathBuf::from("output"));
        let output_format = self.determine_output_format(&output_path)?;

        if self.cli.state.is_some() {
            self.execute_resumable(&input_files, unified_arc, output_path, output_format).await?;
            self.emit(ProgressEventKind::ExecutionCompleted, "resumable conversion completed");
            return Ok(());
        }

        self.process_files_concurrently(&input_files, unified_arc, output_path, output_format)
            .await?;
        self.emit(ProgressEventKind::ExecutionCompleted, "stream conversion completed");
        Ok(())
    }

    async fn build_unified_schema(
        &self,
        input_files: &[InputFile],
        policy: SchemaConflictPolicy,
    ) -> Result<UnifiedSchema> {
        let files = input_files.to_vec();
        tokio::task::spawn_blocking(move || {
            let schemas = schemas_for_inputs(&files)?;
            crate::schema::unified_from_schemas(&schemas, policy)
        })
        .await
        .map_err(|e| MawError::InvalidInput(format!("schema task join error: {e}")))?
    }

    fn determine_output_format(&self, path: &std::path::Path) -> Result<OutputFormat> {
        if let Some(format) = &self.cli.out_format {
            return Ok(format.clone());
        }

        match path.extension().and_then(|ext| ext.to_str()) {
            Some("csv") => Ok(OutputFormat::Csv),
            Some("parquet") => Ok(OutputFormat::Parquet),
            _ => Ok(OutputFormat::Csv),
        }
    }

    async fn execute_resumable(
        &self,
        input_files: &[InputFile],
        unified_schema: Arc<UnifiedSchema>,
        output_path: PathBuf,
        output_format: OutputFormat,
    ) -> Result<()> {
        let state_path = self
            .cli
            .state
            .as_ref()
            .ok_or_else(|| MawError::InvalidInput("state path required".into()))?
            .to_string_lossy()
            .into_owned();

        let mut manager = StateManager::new(Some(state_path));
        let format_label = output_format.to_string();
        let input_paths: Vec<String> =
            input_files.iter().map(|f| f.path.to_string_lossy().into_owned()).collect();
        let inputs_fp = fingerprint_inputs(&input_paths)?;
        let policy_label = format!("{:?}", self.conflict_policy()).to_lowercase();
        let compression_label = format!("{:?}", self.cli.compression).to_lowercase();
        let identity = StreamExecutionIdentity {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            inputs_fingerprint: inputs_fp,
            output_path: output_path.to_string_lossy().into_owned(),
            output_format: format_label.clone(),
            schema_policy: policy_label,
            compression: compression_label,
            batch_size: self.cli.infer_rows.max(1) as u64,
        };

        let mut state = if self.cli.resume {
            if let Some(loaded) = manager.load_state()? {
                if loaded.identity != identity {
                    return Err(MawError::State(
                        "STALE CHECKPOINT: execution identity mismatch (output, policy, or inputs changed)".into(),
                    ));
                }
                verify_sources_unchanged(&loaded)?;
                if loaded.committed && loaded.is_complete() && output_path.exists() {
                    return Ok(());
                }
                loaded
            } else {
                return Err(MawError::InvalidInput(
                    "resume requested but no checkpoint found".into(),
                ));
            }
        } else {
            let staging = output_path.with_extension("staging");
            if staging.exists() {
                let _ = std::fs::remove_dir_all(&staging);
            }
            let mut fresh = manager.create_state(identity);
            for file in input_files {
                let meta = std::fs::metadata(&file.path)?;
                fresh.add_file(
                    file.path.to_string_lossy().into_owned(),
                    format!("{:?}", file.format).to_lowercase(),
                    meta.len(),
                    meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                );
            }
            manager.save_state(&fresh)?;
            fresh
        };

        let staging = output_path.with_extension("staging");
        std::fs::create_dir_all(&staging)?;

        let aligner = Arc::new(BatchAligner::new(
            unified_schema.clone(),
            HashMap::new(),
            self.cli.columns.as_ref().map(|c| {
                c.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
            }),
            self.cli.exclude.as_ref().map(|c| {
                c.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
            }),
            matches!(self.conflict_policy(), SchemaConflictPolicy::Stringify),
        ));

        for (idx, file) in input_files.iter().enumerate() {
            let key = file.path.to_string_lossy().into_owned();
            if state.is_file_processed(&key) {
                continue;
            }

            let part_path = staging.join(format!("part-{idx:05}.{}", format_label));
            let rows = Self::process_one_file_blocking(
                file,
                &part_path,
                output_format.clone(),
                &aligner,
                &self.cli,
            )?;

            state.mark_file_processed(&key, 0, rows);
            manager.save_state(&state)?;

            if std::env::var("PARQONAUT_STREAM_INTERRUPT_AFTER")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                == Some(idx)
            {
                return Err(MawError::Config(
                    "execution interrupted (PARQONAUT_STREAM_INTERRUPT_AFTER)".into(),
                ));
            }
        }

        if state.is_complete() {
            Self::finalize_staged_parts(&staging, &output_path, output_format, unified_schema)?;
            state.committed = true;
            manager.save_state(&state)?;
            if staging.exists() {
                let _ = std::fs::remove_dir_all(&staging);
            }
        }

        Ok(())
    }

    fn process_one_file_blocking(
        file: &InputFile,
        output_path: &Path,
        output_format: OutputFormat,
        aligner: &BatchAligner,
        cli: &Cli,
    ) -> Result<u64> {
        let mut rows: u64 = 0;
        match file.format {
            FileFormat::Csv => {
                let config = CsvConfig {
                    delimiter: cli.delimiter.map(|c| c as u8),
                    quote: cli.quote.map(|c| c as u8),
                    has_headers: !cli.no_headers,
                    encoding: cli.encoding.clone(),
                    na_values: cli.na.split(',').map(|s| s.trim().to_string()).collect(),
                    batch_size: cli.infer_rows.max(1),
                };
                let mut reader = CsvReader::new(&file.path, &config)?;
                let source_fields: Vec<String> = reader.get_headers().to_vec();

                match output_format {
                    OutputFormat::Csv => {
                        let config = CsvWriterConfig::default();
                        let mut writer = CsvWriter::new(output_path, &config)?;
                        while let Some(batch) = reader.read_batch()? {
                            let aligned = aligner.align_batch_with_source(batch, &source_fields)?;
                            rows += aligned.num_rows() as u64;
                            writer.write_batch(&aligned)?;
                        }
                        writer.finish()?;
                    }
                    OutputFormat::Parquet => {
                        let schema: SchemaRef = Arc::new(aligner.unified_schema().schema.clone());
                        let parquet_compression = compression_from_cli(cli);
                        let config = ParquetWriterConfig {
                            compression: parquet_compression,
                            zstd_level: cli.zstd_level as i32,
                        };
                        let mut writer: Option<ParquetWriter> = None;
                        while let Some(batch) = reader.read_batch()? {
                            let aligned = aligner.align_batch_with_source(batch, &source_fields)?;
                            rows += aligned.num_rows() as u64;
                            if writer.is_none() {
                                writer =
                                    Some(ParquetWriter::new(output_path, schema.clone(), &config)?);
                            }
                            writer.as_mut().unwrap().write_batch(&aligned)?;
                        }
                        if let Some(writer) = writer {
                            writer.finish()?;
                        }
                    }
                }
            }
            FileFormat::Parquet => {
                let mut reader = ParquetReader::new(&file.path, cli.infer_rows.max(1))?;
                let source_fields: Vec<String> =
                    reader.get_schema().fields().iter().map(|f| f.name().to_string()).collect();

                match output_format {
                    OutputFormat::Parquet => {
                        let schema: SchemaRef = Arc::new(aligner.unified_schema().schema.clone());
                        let parquet_compression = compression_from_cli(cli);
                        let config = ParquetWriterConfig {
                            compression: parquet_compression,
                            zstd_level: cli.zstd_level as i32,
                        };
                        let mut writer: Option<ParquetWriter> = None;
                        while let Some(batch) = reader.read_batch()? {
                            let aligned = aligner.align_batch_with_source(batch, &source_fields)?;
                            rows += aligned.num_rows() as u64;
                            if writer.is_none() {
                                writer =
                                    Some(ParquetWriter::new(output_path, schema.clone(), &config)?);
                            }
                            writer.as_mut().unwrap().write_batch(&aligned)?;
                        }
                        if let Some(writer) = writer {
                            writer.finish()?;
                        }
                    }
                    OutputFormat::Csv => {
                        let config = CsvWriterConfig::default();
                        let mut writer = CsvWriter::new(output_path, &config)?;
                        while let Some(batch) = reader.read_batch()? {
                            let aligned = aligner.align_batch_with_source(batch, &source_fields)?;
                            rows += aligned.num_rows() as u64;
                            writer.write_batch(&aligned)?;
                        }
                        writer.finish()?;
                    }
                }
            }
        }
        Ok(rows)
    }

    fn finalize_staged_parts(
        staging: &Path,
        output_path: &Path,
        output_format: OutputFormat,
        unified_schema: Arc<UnifiedSchema>,
    ) -> Result<()> {
        let mut parts: Vec<PathBuf> = std::fs::read_dir(staging)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();
        parts.sort();

        if parts.is_empty() {
            return Err(MawError::InvalidInput("no staged parts to finalize".into()));
        }

        if parts.len() == 1 {
            std::fs::rename(&parts[0], output_path)?;
            return Ok(());
        }

        match output_format {
            OutputFormat::Parquet => {
                let schema = Arc::new(unified_schema.schema.clone());
                let config = ParquetWriterConfig::default();
                let mut writer = ParquetWriter::new(output_path, schema, &config)?;
                for part in parts {
                    let mut reader = ParquetReader::new(&part, 64_000)?;
                    while let Some(batch) = reader.read_batch()? {
                        writer.write_batch(&batch)?;
                    }
                }
                writer.finish()?;
            }
            OutputFormat::Csv => {
                let mut out = std::fs::File::create(output_path)?;
                let mut first = true;
                for part in parts {
                    let content = std::fs::read(&part)?;
                    if first {
                        out.write_all(&content)?;
                        first = false;
                    } else {
                        let text = String::from_utf8_lossy(&content);
                        if let Some(body) = text.find('\n') {
                            out.write_all(text[body + 1..].as_bytes())?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_files_concurrently(
        &self,
        input_files: &[InputFile],
        unified_schema: Arc<UnifiedSchema>,
        output_path: PathBuf,
        output_format: OutputFormat,
    ) -> Result<()> {
        let (tx, rx) = mpsc::channel::<RecordBatch>(8);

        let aligner = Arc::new(BatchAligner::new(
            unified_schema.clone(),
            HashMap::new(),
            self.cli.columns.as_ref().map(|c| {
                c.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
            }),
            self.cli.exclude.as_ref().map(|c| {
                c.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
            }),
            matches!(self.conflict_policy(), SchemaConflictPolicy::Stringify),
        ));

        let reader_handles = self.spawn_readers(input_files, tx, aligner).await?;

        let writer_handle =
            self.spawn_writer(output_path, output_format, rx, unified_schema).await?;

        for handle in reader_handles {
            handle.await??;
        }

        writer_handle.await??;

        Ok(())
    }

    async fn spawn_readers(
        &self,
        input_files: &[InputFile],
        tx: mpsc::Sender<RecordBatch>,
        aligner: Arc<BatchAligner>,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>> {
        let mut handles = Vec::new();
        let cli = self.cli.clone();

        for file in input_files {
            let tx_clone = tx.clone();
            let file_path = file.path.clone();
            let format = file.format.clone();
            let aligner_clone = aligner.clone();
            let cli = cli.clone();

            let handle = tokio::task::spawn_blocking(move || {
                match format {
                    FileFormat::Csv => {
                        let config = CsvConfig {
                            delimiter: cli.delimiter.map(|c| c as u8),
                            quote: cli.quote.map(|c| c as u8),
                            has_headers: !cli.no_headers,
                            encoding: cli.encoding.clone(),
                            na_values: cli.na.split(',').map(|s| s.trim().to_string()).collect(),
                            batch_size: 64_000,
                        };
                        let mut reader = CsvReader::new(&file_path, &config)?;
                        let source_fields: Vec<String> = reader.get_headers().to_vec();

                        while let Some(batch) = reader.read_batch()? {
                            let aligned =
                                aligner_clone.align_batch_with_source(batch, &source_fields)?;
                            if tx_clone.blocking_send(aligned).is_err() {
                                break;
                            }
                        }
                    }
                    FileFormat::Parquet => {
                        let mut reader = ParquetReader::new(&file_path, 64_000)?;
                        let source_fields: Vec<String> = reader
                            .get_schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().to_string())
                            .collect();

                        while let Some(batch) = reader.read_batch()? {
                            let aligned =
                                aligner_clone.align_batch_with_source(batch, &source_fields)?;
                            if tx_clone.blocking_send(aligned).is_err() {
                                break;
                            }
                        }
                    }
                }
                Ok(())
            });

            handles.push(handle);
        }

        Ok(handles)
    }

    async fn spawn_writer(
        &self,
        output_path: PathBuf,
        output_format: OutputFormat,
        mut rx: mpsc::Receiver<RecordBatch>,
        unified_schema: Arc<UnifiedSchema>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let compression = self.cli.compression.clone();
        let zstd_level = self.cli.zstd_level;

        let handle = tokio::task::spawn_blocking(move || {
            match output_format {
                OutputFormat::Csv => {
                    let config = CsvWriterConfig::default();
                    let mut writer = CsvWriter::new(&output_path, &config)?;

                    while let Some(batch) = rx.blocking_recv() {
                        writer.write_batch(&batch)?;
                    }

                    writer.finish()?;
                }
                OutputFormat::Parquet => {
                    let schema = Arc::new(unified_schema.schema.clone());
                    let parquet_compression = compression_from_cli_opts(&compression);
                    let config = ParquetWriterConfig {
                        compression: parquet_compression,
                        zstd_level: zstd_level as i32,
                    };

                    let mut writer = ParquetWriter::new(&output_path, schema, &config)?;

                    while let Some(batch) = rx.blocking_recv() {
                        writer.write_batch(&batch)?;
                    }

                    writer.finish()?;
                }
            }
            Ok(())
        });

        Ok(handle)
    }
}

fn compression_from_cli(cli: &Cli) -> Compression {
    compression_from_cli_opts(&cli.compression)
}

fn compression_from_cli_opts(compression: &crate::cli::Compression) -> Compression {
    match compression {
        crate::cli::Compression::None => Compression::UNCOMPRESSED,
        crate::cli::Compression::Snappy => Compression::SNAPPY,
        crate::cli::Compression::Gzip => Compression::GZIP(Default::default()),
        crate::cli::Compression::Zstd => Compression::ZSTD(Default::default()),
    }
}

#[cfg(test)]
#[allow(clippy::needless_borrows_for_generic_args, clippy::bool_assert_comparison)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_pipeline_creation() {
        let cli = Cli::parse_from(&["parqonaut-stream", "test.csv"]);
        let pipeline = Pipeline::new(cli);
        assert!(!pipeline.cli.inputs.is_empty());
    }

    #[test]
    fn test_output_format_detection() {
        let cli = Cli::parse_from(&["parqonaut-stream", "test.csv"]);
        let pipeline = Pipeline::new(cli);

        let csv_path = PathBuf::from("test.csv");
        let format = pipeline.determine_output_format(&csv_path).unwrap();
        assert!(matches!(format, OutputFormat::Csv));

        let parquet_path = PathBuf::from("test.parquet");
        let format = pipeline.determine_output_format(&parquet_path).unwrap();
        assert!(matches!(format, OutputFormat::Parquet));
    }
}
