use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Spec {
    pub input: Option<String>,
    pub output: Option<String>,
    pub steps: Vec<Step>,
    #[serde(default)]
    pub options: Options,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Step {
    pub operation: Operation,
    #[serde(default)]
    pub options: StepOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Operation {
    Rewrite {
        #[serde(default)]
        compression: Option<Compression>,
        #[serde(default)]
        row_group_size_mb: Option<u64>,
        #[serde(default)]
        projection: Option<Vec<String>>,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        schema: Option<SchemaOps>,
        #[serde(default)]
        metadata: Option<HashMap<String, String>>,
        #[serde(default)]
        rebuild_stats: bool,
    },
    Partition {
        partition_by: Vec<String>,
    },
    Merge {
        #[serde(default)]
        row_group_size_mb: Option<u64>,
    },
    Split {
        #[serde(default)]
        target_size_mb: Option<u64>,
        #[serde(default)]
        target_row_groups: Option<usize>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SchemaOps {
    #[serde(default)]
    pub rename: Option<HashMap<String, String>>,
    #[serde(default)]
    pub cast: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
    Lz4Raw,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Options {
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default)]
    pub batch_rows: Option<usize>,
    #[serde(default)]
    pub batch_bytes: Option<u64>,
    #[serde(default = "default_fail_fast")]
    pub fail_fast: bool,
    #[serde(default = "default_atomic")]
    pub atomic: bool,
    #[serde(default)]
    pub overwrite: bool,
}

fn default_concurrency() -> usize {
    num_cpus::get()
}

fn default_fail_fast() -> bool {
    false
}

fn default_atomic() -> bool {
    true
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StepOptions {
    #[serde(default)]
    pub dry_run: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            concurrency: default_concurrency(),
            batch_rows: None,
            batch_bytes: None,
            fail_fast: default_fail_fast(),
            atomic: default_atomic(),
            overwrite: false,
        }
    }
}

impl Compression {
    pub fn to_parquet_compression(self) -> parquet::basic::Compression {
        match self {
            Compression::Uncompressed => parquet::basic::Compression::UNCOMPRESSED,
            Compression::Snappy => parquet::basic::Compression::SNAPPY,
            Compression::Gzip => {
                parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default())
            }
            Compression::Lzo => parquet::basic::Compression::LZO,
            Compression::Brotli => {
                parquet::basic::Compression::BROTLI(parquet::basic::BrotliLevel::default())
            }
            Compression::Lz4 => parquet::basic::Compression::LZ4,
            Compression::Zstd => {
                parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default())
            }
            Compression::Lz4Raw => parquet::basic::Compression::LZ4_RAW,
        }
    }
}
