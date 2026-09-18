//! JSON helpers for [`paraclete_types::ScanReport`].

use paraclete_types::ScanReport;
use serde_json::Value;

/// Serializes a report to a pretty-printed JSON string.
pub fn to_json_string(report: &ScanReport) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(report)
}

/// Serializes a report to [`serde_json::Value`].
pub fn to_json_value(report: &ScanReport) -> Result<Value, serde_json::Error> {
    serde_json::to_value(report)
}
