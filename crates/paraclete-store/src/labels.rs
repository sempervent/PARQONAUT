//! Stable string labels for enum projections (must match serde `rename_all = snake_case`).

use paraclete_types::{
    DataFormat, FindingCategory, FindingSeverity, GroupingKind, PartitionLayout,
};

pub(crate) fn data_format_label(f: DataFormat) -> &'static str {
    match f {
        paraclete_types::DataFormat::Parquet => "parquet",
        paraclete_types::DataFormat::Csv => "csv",
        paraclete_types::DataFormat::Json => "json",
        paraclete_types::DataFormat::Ndjson => "ndjson",
        paraclete_types::DataFormat::Unknown => "unknown",
    }
}

pub(crate) fn grouping_kind_label(k: &GroupingKind) -> String {
    serde_json::to_value(k)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".into())
}

pub(crate) fn partition_layout_label(p: &PartitionLayout) -> String {
    serde_json::to_value(p)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".into())
}

pub(crate) fn severity_label(s: FindingSeverity) -> String {
    serde_json::to_value(s)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".into())
}

pub(crate) fn category_label(c: FindingCategory) -> String {
    serde_json::to_value(c)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".into())
}

pub(crate) fn parse_data_format(s: &str) -> Option<DataFormat> {
    match s {
        "parquet" => Some(DataFormat::Parquet),
        "csv" => Some(DataFormat::Csv),
        "json" => Some(DataFormat::Json),
        "ndjson" => Some(DataFormat::Ndjson),
        "unknown" => Some(DataFormat::Unknown),
        _ => None,
    }
}
