use serde_json::Value;

use crate::plan::{RepairPlan, PLAN_SCHEMA_VERSION};
use crate::stable_id::canonical_json;

/// Normalize absolute filesystem paths to stable repo-relative form for golden tests.
fn normalize_portable_path(s: &str) -> String {
    let mut out = s.to_string();
    while let Some(idx) = out.find("/fixtures/") {
        let seg_start = out[..idx].rfind(' ').map(|i| i + 1).unwrap_or(0);
        let fixtures_start = idx + 1;
        out.replace_range(seg_start..fixtures_start, "");
    }
    if out.starts_with('/') {
        if let Some(idx) = out.find("fixtures/") {
            out = out[idx..].to_string();
        }
    }
    out
}

fn normalize_value_paths(value: &mut Value) {
    match value {
        Value::String(s) => {
            if s.contains("/fixtures/") || s.starts_with('/') {
                *s = normalize_portable_path(s);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                normalize_value_paths(v);
            }
        }
        Value::Object(obj) => {
            for v in obj.values_mut() {
                normalize_value_paths(v);
            }
        }
        _ => {}
    }
}

/// Canonical JSON suitable for plan comparison and golden tests.
/// Excludes volatile fields: `generated_at`, scan UUIDs in metadata.
pub fn canonical_plan_json(plan: &RepairPlan) -> Result<String, crate::error::RepairError> {
    if plan.schema_version != PLAN_SCHEMA_VERSION {
        return Err(crate::error::RepairError::UnsupportedPlanVersion {
            found: plan.schema_version,
            supported: PLAN_SCHEMA_VERSION,
        });
    }

    let value = serde_json::to_value(plan)?;
    let mut obj = value.as_object().cloned().unwrap_or_default();
    obj.remove("generated_at");
    obj.remove("source_scan_id");
    obj.remove("diagnosis_findings");
    obj.remove("plan_id");
    if let Some(ops) = obj.get_mut("operations").and_then(|v| v.as_array_mut()) {
        for op in ops {
            if let Some(o) = op.as_object_mut() {
                if let Some(ev) = o.get_mut("evidence").and_then(|v| v.as_array_mut()) {
                    for e in ev {
                        if let Some(eo) = e.as_object_mut() {
                            eo.remove("finding_id");
                        }
                    }
                }
            }
        }
    }
    if let Some(meta) = obj.get_mut("diagnosis_findings").and_then(|v| v.as_array_mut()) {
        for finding in meta {
            if let Some(f) = finding.as_object_mut() {
                f.remove("id");
                f.remove("fingerprint");
                if let Some(ev) = f.get_mut("evidence").and_then(|v| v.as_array_mut()) {
                    for e in ev {
                        if let Some(eo) = e.as_object_mut() {
                            eo.remove("id");
                        }
                    }
                }
            }
        }
    }

    let mut canonical = Value::Object(obj);
    normalize_value_paths(&mut canonical);
    Ok(canonical_json(&canonical))
}
