use crate::{
    cli::{Cli, OutputFormat},
    csv_in::{CsvConfig, CsvReader},
    discover::{discover_inputs, DiscoveryConfig, InputFile},
    error::{MawError, Result},
    parquet_in::ParquetReader,
    schema::UnifiedSchema,
    writer_csv::{CsvWriter, CsvWriterConfig},
    writer_parquet::{ParquetWriter, ParquetWriterConfig},
};
use arrow2::{array::Array, chunk::Chunk};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::mpsc;

pub struct Pipeline {
    cli: Cli,
    #[allow(dead_code)]
    unified_schema: Arc<UnifiedSchema>,
}

impl Pipeline {
    pub fn new(cli: Cli) -> Self {
        Self { cli, unified_schema: Arc::new(UnifiedSchema::default()) }
    }

    pub async fn execute(&self) -> Result<()> {
        // Discover input files
        let discovery_config = DiscoveryConfig {
            recursive: !self.cli.no_recursive,
            follow_symlinks: self.cli.follow_symlinks,
            max_depth: None,
        };

        let input_files = discover_inputs(&self.cli.inputs, &discovery_config)?;

        if input_files.is_empty() {
            return Err(MawError::InvalidInput("No input files found".to_string()));
        }

        // Build unified schema from all inputs
        let unified_schema = self.build_unified_schema(&input_files).await?;

        // Create output writer
        let output_path = self.cli.out.clone().unwrap_or_else(|| PathBuf::from("output"));

        let output_format = self.determine_output_format(&output_path)?;

        // Set up concurrent processing
        self.process_files_concurrently(&input_files, &unified_schema, output_path, output_format)
            .await
    }

    async fn build_unified_schema(&self, _input_files: &[InputFile]) -> Result<UnifiedSchema> {
        // For now, create a simple unified schema
        // In a real implementation, we would sample each file and build the schema
        Ok(UnifiedSchema::default())
    }

    fn determine_output_format(&self, path: &std::path::Path) -> Result<OutputFormat> {
        if let Some(format) = &self.cli.out_format {
            return Ok(format.clone());
        }

        match path.extension().and_then(|ext| ext.to_str()) {
            Some("csv") => Ok(OutputFormat::Csv),
            Some("parquet") => Ok(OutputFormat::Parquet),
            _ => Ok(OutputFormat::Csv), // Default to CSV
        }
    }

    async fn process_files_concurrently(
        &self,
        input_files: &[InputFile],
        _unified_schema: &UnifiedSchema,
        output_path: PathBuf,
        output_format: OutputFormat,
    ) -> Result<()> {
        let (tx, rx) = mpsc::channel::<Chunk<Box<dyn Array>>>(8); // Bounded channel

        // Spawn readers
        let reader_handles = self.spawn_readers(input_files, tx).await?;

        // Spawn writer
        let writer_handle = self.spawn_writer(output_path, output_format, rx).await?;

        // Wait for all readers to complete
        for handle in reader_handles {
            handle.await??;
        }

        // Wait for writer to complete
        writer_handle.await??;

        Ok(())
    }

    async fn spawn_readers(
        &self,
        input_files: &[InputFile],
        tx: mpsc::Sender<Chunk<Box<dyn Array>>>,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>> {
        let mut handles = Vec::new();

        for file in input_files {
            let tx_clone = tx.clone();
            let file_path = file.path.clone();
            let format = file.format.clone();
            let batch_size = 64_000; // Default batch size

            let handle = tokio::task::spawn_blocking(move || {
                match format {
                    crate::discover::FileFormat::Csv => {
                        let config = CsvConfig::default();
                        let mut reader = CsvReader::new(&file_path, &config)?;

                        while let Some(batch) = reader.read_batch()? {
                            if tx_clone.blocking_send(batch).is_err() {
                                break;
                            }
                        }
                    }
                    crate::discover::FileFormat::Parquet => {
                        let mut reader = ParquetReader::new(&file_path, batch_size)?;

                        while let Some(batch) = reader.read_batch()? {
                            if tx_clone.blocking_send(batch).is_err() {
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
        mut rx: mpsc::Receiver<Chunk<Box<dyn Array>>>,
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
                    let mut writer: Option<ParquetWriter> = None;
                    let parquet_compression = match compression {
                        crate::cli::Compression::None => {
                            parquet2::compression::Compression::Uncompressed
                        }
                        crate::cli::Compression::Snappy => {
                            parquet2::compression::Compression::Snappy
                        }
                        crate::cli::Compression::Gzip => parquet2::compression::Compression::Gzip,
                        crate::cli::Compression::Zstd => parquet2::compression::Compression::Zstd,
                    };
                    let config = ParquetWriterConfig {
                        compression: parquet_compression,
                        zstd_level,
                        ..ParquetWriterConfig::default()
                    };

                    while let Some(batch) = rx.blocking_recv() {
                        if writer.is_none() {
                            let fields = batch
                                .arrays()
                                .iter()
                                .enumerate()
                                .map(|(idx, array)| {
                                    arrow2::datatypes::Field::new(
                                        format!("col_{}", idx + 1),
                                        array.data_type().clone(),
                                        true,
                                    )
                                })
                                .collect::<Vec<_>>();
                            let schema = Arc::new(arrow2::datatypes::Schema::from(fields));
                            writer = Some(ParquetWriter::new(&output_path, schema, &config)?);
                        }
                        writer.as_mut().unwrap().write_batch(&batch)?;
                    }

                    if let Some(writer) = writer {
                        writer.finish()?;
                    } else {
                        return Err(MawError::InvalidInput(
                            "No data batches received for Parquet output".to_string(),
                        ));
                    }
                }
            }
            Ok(())
        });

        Ok(handle)
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
