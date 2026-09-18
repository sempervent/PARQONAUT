//! Shallow, metadata-only inspection for second-class text formats.

use std::fs::File;
use std::io::Read;

use camino::Utf8Path;
use paraclete_types::{ParseConfidence, ProbeDepth, ProbeMetadata};
use serde_json::{json, Value};

use crate::CoreError;

pub const HEAD_BYTES: usize = 64 * 1024;

/// Hints plus bounded probe metadata (execution truth for text assets).
#[derive(Debug, Clone)]
pub struct ShallowInspectOutcome {
    pub hints: Value,
    pub probe: ProbeMetadata,
}

fn read_head_bytes(path: &Utf8Path, max: usize) -> Result<(Vec<u8>, u64), CoreError> {
    let mut f = File::open(path.as_std_path())?;
    let file_size = f.metadata()?.len();
    let mut buf = vec![0u8; max];
    let n = f.read(&mut buf)?;
    buf.truncate(n);
    Ok((buf, file_size))
}

fn probe_common(bytes_read: usize, file_size: u64) -> (ProbeDepth, ParseConfidence) {
    let depth = if bytes_read as u64 >= file_size && file_size <= HEAD_BYTES as u64 {
        ProbeDepth::FullWithinCap
    } else if file_size > HEAD_BYTES as u64 {
        ProbeDepth::PartialHead
    } else {
        ProbeDepth::FullWithinCap
    };
    let conf = if depth == ProbeDepth::PartialHead {
        ParseConfidence::Partial
    } else {
        ParseConfidence::High
    };
    (depth, conf)
}

fn guess_csv_delimiter(line: &str) -> char {
    let mut best = (',', 0usize);
    for (d, pat) in [(',', ','), ('\t', '\t'), (';', ';')] {
        let c = line.matches(pat).count();
        if c > best.1 {
            best = (d, c);
        }
    }
    best.0
}

fn split_csv_line(line: &str, delim: char) -> Vec<String> {
    line.split(delim).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
}

fn looks_like_header(cells: &[String]) -> bool {
    if cells.is_empty() {
        return false;
    }
    cells.iter().all(|c| {
        !c.chars().all(|ch| ch.is_ascii_digit() || ch == '.' || ch == '-')
            && c.chars().next().is_some_and(|ch| ch.is_alphabetic() || ch == '_')
    })
}

/// Best-effort CSV structure hints from the first lines of a file.
pub fn inspect_csv_shallow(path: &Utf8Path) -> Result<ShallowInspectOutcome, CoreError> {
    let (bytes, file_size) = read_head_bytes(path, HEAD_BYTES)?;
    let (probe_depth, mut parse_confidence) = probe_common(bytes.len(), file_size);
    let text = String::from_utf8_lossy(&bytes);
    let mut lines = text.lines();
    let first = lines.next().unwrap_or("").trim_end_matches(['\r', '\n']);
    let hints = if first.is_empty() {
        parse_confidence = ParseConfidence::Low;
        json!({
            "shallow_format": "csv",
            "delimiter": ",",
            "column_count": 0u32,
            "header_names": Value::Null,
            "sample_line_count": 0u32,
        })
    } else {
        let delim = guess_csv_delimiter(first);
        let cells = split_csv_line(first, delim);
        let header_names = if looks_like_header(&cells) { json!(cells) } else { Value::Null };
        let column_count = cells.len().max(1);
        let sample_line_count = 1u32.saturating_add(lines.take(7).count() as u32);
        json!({
            "shallow_format": "csv",
            "delimiter": delim.to_string(),
            "column_count": column_count,
            "header_names": header_names,
            "sample_line_count": sample_line_count,
        })
    };
    let probe = ProbeMetadata {
        bytes_sampled: bytes.len() as u64,
        file_size_bytes: Some(file_size),
        encoding_assumption: "utf-8-lossy".into(),
        probe_depth,
        parse_confidence,
        notes: if probe_depth == ProbeDepth::PartialHead {
            Some("only leading bytes read; file exceeds probe cap".into())
        } else {
            None
        },
    };
    Ok(ShallowInspectOutcome { hints, probe })
}

/// JSON or NDJSON shape hints from a bounded read of the file.
pub fn inspect_json_like_shallow(
    path: &Utf8Path,
    ndjson: bool,
) -> Result<ShallowInspectOutcome, CoreError> {
    let (bytes, file_size) = read_head_bytes(path, HEAD_BYTES)?;
    let (probe_depth, _) = probe_common(bytes.len(), file_size);
    let text = String::from_utf8_lossy(&bytes);
    let mut parse_confidence = ParseConfidence::High;
    let mut notes: Option<String> = None;
    let hints = if ndjson {
        notes = Some("ndjson semantics: only first non-empty line parsed".into());
        parse_confidence = ParseConfidence::Partial;
        let line = text.lines().find(|l| !l.trim().is_empty()).unwrap_or("").trim();
        if line.is_empty() {
            json!({ "shallow_format": "ndjson", "first_line_kind": "empty" })
        } else {
            let v: Value =
                serde_json::from_str(line).map_err(|e| CoreError::Inspect(e.to_string()))?;
            json!({
                "shallow_format": "ndjson",
                "first_line_kind": json_kind(&v),
                "sample_keys": sample_keys(&v),
            })
        }
    } else {
        let v: Value =
            serde_json::from_str(text.trim()).map_err(|e| CoreError::Inspect(e.to_string()))?;
        if file_size > HEAD_BYTES as u64 {
            parse_confidence = ParseConfidence::Partial;
            notes = Some("json parsed from bounded head only; file exceeds probe cap".into());
        }
        json!({
            "shallow_format": "json",
            "top_level_kind": json_kind(&v),
            "sample_keys": sample_keys(&v),
        })
    };
    let probe = ProbeMetadata {
        bytes_sampled: bytes.len() as u64,
        file_size_bytes: Some(file_size),
        encoding_assumption: "utf-8-lossy".into(),
        probe_depth,
        parse_confidence,
        notes,
    };
    Ok(ShallowInspectOutcome { hints, probe })
}

fn json_kind(v: &Value) -> &'static str {
    match v {
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "bool",
        Value::Null => "null",
    }
}

fn sample_keys(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&str> = map.keys().map(String::as_str).collect();
            keys.sort();
            keys.truncate(24);
            json!(keys)
        }
        Value::Array(a) => a.first().map(sample_keys).unwrap_or(json!([])),
        _ => json!([]),
    }
}
