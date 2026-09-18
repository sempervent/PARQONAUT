use camino::Utf8PathBuf;
use paraclete_plugin_protocol::PluginExecutorError;
use thiserror::Error;

/// Library errors for the engine boundary crate.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("target path does not exist: {0}")]
    MissingPath(Utf8PathBuf),
    #[error("unsupported target for local engine: {0}")]
    Unsupported(&'static str),
    #[error(transparent)]
    Plugin(#[from] PluginExecutorError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("path is not valid UTF-8: {0}")]
    NonUtf8Path(String),
    #[error("parquet error: {0}")]
    Parquet(String),
    #[error("shallow inspect error: {0}")]
    Inspect(String),
    #[error("report validation failed: {0}")]
    ReportValidation(String),
}
