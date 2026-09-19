use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;

use crate::error::OrchestratorError;
use crate::ids::DatasetId;
use crate::location::{local_path_for_policy, ConfigLocation};
use crate::storage::default_max_storage_requests;
use crate::BATCH_CONFIG_SCHEMA_VERSION;
use parqonaut_repair::{stable_hex_id, EffectivePolicy};
use parqonaut_storage::location::DatasetLocation;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchConfig {
    pub schema_version: u32,
    pub batch: BatchSection,
    #[serde(default)]
    pub datasets: Vec<DatasetConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchSection {
    pub name: String,
    #[serde(default = "default_concurrency")]
    pub max_concurrency: u32,
    /// Root for repaired dataset outputs (local path or `s3://` URI).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_root: Option<ConfigLocation>,
    /// Local directory for run journals when `output_root` is remote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_root: Option<Utf8PathBuf>,
    /// Upper bound on concurrent storage backend requests (separate from `--jobs`).
    #[serde(default = "default_max_storage_requests")]
    pub max_storage_requests: u32,
}

fn default_concurrency() -> u32 {
    2
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetConfig {
    pub id: String,
    pub path: ConfigLocation,
    #[serde(default)]
    pub policy: Option<Utf8PathBuf>,
    #[serde(default)]
    pub target_schema: Option<Utf8PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorize: Vec<String>,
    /// Output subdirectory under `batch.output_root`, or absolute/`s3://` override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Utf8PathBuf>,
}

impl BatchConfig {
    pub fn from_toml_path(path: &Utf8Path) -> Result<Self, OrchestratorError> {
        let raw = fs::read_to_string(path.as_std_path())?;
        Self::from_toml(&raw)
    }

    pub fn from_toml(raw: &str) -> Result<Self, OrchestratorError> {
        let cfg: Self =
            toml::from_str(raw).map_err(|e| OrchestratorError::InvalidConfig(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if self.schema_version != BATCH_CONFIG_SCHEMA_VERSION {
            return Err(OrchestratorError::UnsupportedConfigVersion {
                found: self.schema_version,
                supported: BATCH_CONFIG_SCHEMA_VERSION,
            });
        }
        if self.batch.max_concurrency == 0 {
            return Err(OrchestratorError::InvalidConfig(
                "batch.max_concurrency must be >= 1".into(),
            ));
        }
        if self.batch.max_storage_requests == 0 {
            return Err(OrchestratorError::InvalidConfig(
                "batch.max_storage_requests must be >= 1".into(),
            ));
        }
        let mut seen = BTreeSet::new();
        for ds in &self.datasets {
            if !seen.insert(ds.id.clone()) {
                return Err(OrchestratorError::DuplicateDatasetId(ds.id.clone()));
            }
        }
        if self.batch.output_root.is_none() {
            return Err(OrchestratorError::InvalidConfig("batch.output_root is required".into()));
        }
        Ok(())
    }

    pub fn config_fingerprint(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_default();
        stable_hex_id("batch-config", &json)
    }

    pub fn dataset_id(&self, id: &str) -> DatasetId {
        DatasetId(id.to_string())
    }

    pub fn resolve_path(base: &Utf8Path, path: &Utf8Path) -> Utf8PathBuf {
        local_path_for_policy(base, path)
    }

    pub fn resolve_policy(
        &self,
        base: &Utf8Path,
        ds: &DatasetConfig,
    ) -> Result<EffectivePolicy, OrchestratorError> {
        let toml = match &ds.policy {
            Some(p) => {
                let path = Self::resolve_path(base, p);
                Some(fs::read_to_string(path.as_std_path())?)
            }
            None => None,
        };
        EffectivePolicy::from_toml(toml.as_deref())
            .map_err(|e| OrchestratorError::InvalidConfig(e.to_string()))
    }

    pub fn resolve_target_schema(
        &self,
        base: &Utf8Path,
        ds: &DatasetConfig,
    ) -> Result<Option<Vec<parqonaut_repair::FieldDescriptor>>, OrchestratorError> {
        let Some(path) = &ds.target_schema else {
            return Ok(None);
        };
        let path = Self::resolve_path(base, path);
        let bytes = fs::read(path.as_std_path())?;
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    pub fn canonicalize_path(
        base: &Utf8Path,
        path: &Utf8Path,
    ) -> Result<Utf8PathBuf, OrchestratorError> {
        if path.is_absolute() {
            return Ok(path.to_path_buf());
        }
        let joined = base.join(path);
        let canonical = fs::canonicalize(joined.as_std_path())
            .map_err(OrchestratorError::Io)?
            .try_into()
            .map_err(|_| OrchestratorError::InvalidConfig("non-UTF8 path".into()))?;
        Ok(canonical)
    }

    pub fn require_output_root(&self) -> Result<&ConfigLocation, OrchestratorError> {
        self.batch.output_root.as_ref().ok_or_else(|| {
            OrchestratorError::InvalidConfig(
                "batch.output_root is required for planning and execution".into(),
            )
        })
    }

    pub fn max_storage_requests(&self) -> u32 {
        self.batch.max_storage_requests
    }
}

impl DatasetConfig {
    pub fn id(&self) -> DatasetId {
        DatasetId(self.id.clone())
    }

    pub fn source_location(&self) -> &DatasetLocation {
        self.path.as_location()
    }
}
