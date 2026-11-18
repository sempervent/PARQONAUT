use sha2::{Digest, Sha256};

/// Derives a stable hex id from canonical JSON bytes (deterministic plan content).
pub fn stable_hex_id(prefix: &str, canonical_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prefix.as_bytes());
    hasher.update(b"\0");
    hasher.update(canonical_json.as_bytes());
    let digest = hasher.finalize();
    format!("{}-{}", prefix, hex::encode(&digest[..8]))
}

/// Canonical JSON for hashing: sorted keys via serde_json Value roundtrip.
pub fn canonical_json(value: &serde_json::Value) -> String {
    let sorted = parqonaut_types::sort_json_value(value.clone());
    serde_json::to_string(&sorted).unwrap_or_default()
}
