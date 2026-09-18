//! Deterministic canonical JSON for finding fingerprints (Phase 1+).

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::{Evidence, Finding, FindingLocation};

/// Recursively sorts JSON object keys so serialization order is stable for hashing.
pub fn sort_json_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<String, Value> =
                map.into_iter().map(|(k, v)| (k, sort_json_value(v))).collect();
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sort_json_value).collect()),
        other => other,
    }
}

/// Builds the canonical JSON object used for [`Finding::compute_fingerprint`].
pub fn finding_fingerprint_payload(finding: &Finding) -> Value {
    let evidence: Vec<Value> = finding.evidence.iter().map(evidence_fingerprint_value).collect();
    let locations: Vec<Value> = finding.locations.iter().map(location_fingerprint_value).collect();
    json!({
        "code": finding.code.as_str(),
        "severity": finding.severity,
        "category": finding.category,
        "summary": finding.summary,
        "detail": finding.detail,
        "evidence": evidence,
        "locations": locations,
    })
}

fn evidence_fingerprint_value(e: &Evidence) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("id".into(), json!(e.id));
    m.insert("kind".into(), json!(e.kind));
    m.insert("summary".into(), json!(e.summary));
    if let Some(loc) = &e.location_ref {
        m.insert("location_ref".into(), json!(loc));
    }
    if let Some(p) = &e.payload {
        m.insert("payload".into(), sort_json_value(p.clone()));
    }
    if !e.references.is_empty() {
        m.insert("references".into(), json!(&e.references));
    }
    sort_json_value(Value::Object(m))
}

fn location_fingerprint_value(loc: &FindingLocation) -> Value {
    json!({
        "file": loc.file,
        "columns": loc.columns,
        "partition": loc.partition,
        "row_group": loc.row_group,
    })
}
