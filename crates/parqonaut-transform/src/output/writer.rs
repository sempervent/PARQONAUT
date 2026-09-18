use crate::error::{ParqknifeError, Result};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

pub struct ParquetWriter {
    writer: ArrowWriter<Box<dyn Write + Send>>,
    row_group_size_bytes: Option<usize>,
}

impl ParquetWriter {
    pub fn new<W: Write + Send + 'static>(
        writer: W,
        schema: Arc<arrow::datatypes::Schema>,
        compression: Option<Compression>,
        row_group_size_mb: Option<u64>,
        rebuild_stats: bool,
    ) -> Result<Self> {
        let mut props_builder = WriterProperties::builder()
            .set_write_batch_size(8192)
            .set_max_row_group_size(128 * 1024 * 1024); // 128MB default

        if let Some(comp) = compression {
            props_builder = props_builder.set_compression(comp);
        }

        if let Some(size_mb) = row_group_size_mb {
            props_builder = props_builder.set_max_row_group_size((size_mb * 1024 * 1024) as usize);
        }

        if rebuild_stats {
            props_builder = props_builder
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk);
        }

        let props = props_builder.build();
        let boxed: Box<dyn Write + Send> = Box::new(writer);
        let arrow_writer = ArrowWriter::try_new(boxed, schema, Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            row_group_size_bytes: row_group_size_mb.map(|mb| (mb * 1024 * 1024) as usize),
        })
    }

    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.write(&batch)?;
        Ok(())
    }

    pub fn close(self) -> Result<()> {
        self.writer.close()?;
        Ok(())
    }
}

pub fn compression_from_str(s: &str) -> Result<Compression> {
    match s.to_lowercase().as_str() {
        "uncompressed" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(GzipLevel::default())),
        "lzo" => Ok(Compression::LZO),
        "brotli" => Ok(Compression::BROTLI(BrotliLevel::default())),
        "lz4" => Ok(Compression::LZ4),
        "zstd" => Ok(Compression::ZSTD(ZstdLevel::default())),
        "lz4_raw" => Ok(Compression::LZ4_RAW),
        _ => Err(ParqknifeError::InvalidInput(format!("Unknown compression: {}", s))),
    }
}
