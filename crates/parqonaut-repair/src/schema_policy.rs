use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::stable_id::canonical_json;

/// Schema reconciliation policy (embedded in repair plans).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaPolicy {
    pub allow_numeric_widening: bool,
    pub allow_nullable_widening: bool,
    pub allow_column_reorder: bool,
    pub allow_missing_nullable_columns: bool,
    pub allow_timestamp_unit_widening: bool,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub rename: std::collections::BTreeMap<String, String>,
}

impl Default for SchemaPolicy {
    fn default() -> Self {
        Self {
            allow_numeric_widening: true,
            allow_nullable_widening: true,
            allow_column_reorder: true,
            allow_missing_nullable_columns: false,
            allow_timestamp_unit_widening: false,
            rename: std::collections::BTreeMap::new(),
        }
    }
}

/// File layout policy for split operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilePolicy {
    pub max_file_mb: u64,
    pub split_target_mb: u64,
}

impl Default for FilePolicy {
    fn default() -> Self {
        Self { max_file_mb: 1024, split_target_mb: 512 }
    }
}

impl FilePolicy {
    pub fn max_file_bytes(&self) -> u64 {
        self.max_file_mb * 1024 * 1024
    }

    pub fn split_target_bytes(&self) -> u64 {
        self.split_target_mb * 1024 * 1024
    }
}

/// CI gate policy for `parqonaut check`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CiPolicy {
    pub fail_on_safe_findings: bool,
    pub fail_on_review_required: bool,
    pub fail_on_destructive: bool,
    pub fail_on_unresolvable_schema: bool,
}

impl Default for CiPolicy {
    fn default() -> Self {
        Self {
            fail_on_safe_findings: false,
            fail_on_review_required: true,
            fail_on_destructive: true,
            fail_on_unresolvable_schema: true,
        }
    }
}

/// Combined effective policy bundle recorded in plans.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct EffectivePolicy {
    #[serde(flatten)]
    pub repair: crate::policy::RepairPolicy,
    pub schema: SchemaPolicy,
    pub files: FilePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci: Option<CiPolicy>,
}

impl EffectivePolicy {
    pub fn from_toml(toml: Option<&str>) -> Result<Self, toml::de::Error> {
        let mut policy = Self::default();
        let Some(raw) = toml else {
            return Ok(policy);
        };
        let table: toml::Table = toml::from_str(raw)?;

        if let Some(repair) = table.get("repair").and_then(|v| v.as_table()) {
            let repair_toml = toml::to_string(repair).unwrap_or_default();
            policy.repair = crate::policy::RepairPolicy::from_toml_optional(Some(&repair_toml))?;
        }

        if let Some(schema) = table.get("schema").and_then(|v| v.as_table()) {
            if let Some(v) = schema.get("allow_numeric_widening").and_then(|v| v.as_bool()) {
                policy.schema.allow_numeric_widening = v;
            }
            if let Some(v) = schema.get("allow_nullable_widening").and_then(|v| v.as_bool()) {
                policy.schema.allow_nullable_widening = v;
            }
            if let Some(v) = schema.get("allow_column_reorder").and_then(|v| v.as_bool()) {
                policy.schema.allow_column_reorder = v;
            }
            if let Some(v) = schema.get("allow_missing_nullable_columns").and_then(|v| v.as_bool())
            {
                policy.schema.allow_missing_nullable_columns = v;
            }
            if let Some(v) = schema.get("allow_timestamp_unit_widening").and_then(|v| v.as_bool()) {
                policy.schema.allow_timestamp_unit_widening = v;
            }
            if let Some(rename) = schema.get("rename").and_then(|v| v.as_table()) {
                for (k, v) in rename {
                    if let Some(s) = v.as_str() {
                        policy.schema.rename.insert(k.clone(), s.to_string());
                    }
                }
            }
        }

        if let Some(files) = table.get("files").and_then(|v| v.as_table()) {
            if let Some(v) = files.get("max_file_mb").and_then(|v| v.as_integer()) {
                policy.files.max_file_mb = v as u64;
            }
            if let Some(v) = files.get("split_target_mb").and_then(|v| v.as_integer()) {
                policy.files.split_target_mb = v as u64;
            }
        }

        if let Some(ci) = table.get("ci").and_then(|v| v.as_table()) {
            let mut c = CiPolicy::default();
            if let Some(v) = ci.get("fail_on_safe_findings").and_then(|v| v.as_bool()) {
                c.fail_on_safe_findings = v;
            }
            if let Some(v) = ci.get("fail_on_review_required").and_then(|v| v.as_bool()) {
                c.fail_on_review_required = v;
            }
            if let Some(v) = ci.get("fail_on_destructive").and_then(|v| v.as_bool()) {
                c.fail_on_destructive = v;
            }
            if let Some(v) = ci.get("fail_on_unresolvable_schema").and_then(|v| v.as_bool()) {
                c.fail_on_unresolvable_schema = v;
            }
            policy.ci = Some(c);
        }

        Ok(policy)
    }

    pub fn fingerprint(&self) -> String {
        let json = serde_json::to_value(self).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(b"parqonaut-policy-v1\0");
        hasher.update(canonical_json(&json).as_bytes());
        hex::encode(hasher.finalize())
    }
}
