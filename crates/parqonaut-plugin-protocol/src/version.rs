//! Protocol and manifest versioning.

/// Canonical on-disk manifest name for discovered plugins.
pub const MANIFEST_FILENAME: &str = "parqonaut-plugin.json";

/// Active wire/manifest protocol version for v0.10.
pub const PLUGIN_PROTOCOL_VERSION: u32 = 1;

/// Supported protocol version tag carried on requests/responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct PluginProtocolVersion(pub u32);

impl PluginProtocolVersion {
    pub fn current() -> Self {
        Self(PLUGIN_PROTOCOL_VERSION)
    }

    pub fn validate(v: u32) -> Result<Self, crate::PluginProtocolError> {
        if v == PLUGIN_PROTOCOL_VERSION {
            Ok(Self(v))
        } else {
            Err(crate::PluginProtocolError::ProtocolVersionMismatch {
                expected: PLUGIN_PROTOCOL_VERSION,
                found: v,
            })
        }
    }
}
