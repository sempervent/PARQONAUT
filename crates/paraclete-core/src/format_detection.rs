//! Format detection from paths and extension heuristics.

use camino::Utf8Path;
use paraclete_types::DataFormat;

/// Heuristic mapping from file extensions to [`DataFormat`].
pub fn classify_format_from_path(path: &Utf8Path) -> DataFormat {
    match path.extension() {
        Some(ext) if ext.eq_ignore_ascii_case("parquet") || ext.eq_ignore_ascii_case("pq") => {
            DataFormat::Parquet
        }
        Some(ext) if ext.eq_ignore_ascii_case("csv") => DataFormat::Csv,
        Some(ext) if ext.eq_ignore_ascii_case("json") => DataFormat::Json,
        Some(ext) if ext.eq_ignore_ascii_case("ndjson") || ext.eq_ignore_ascii_case("jsonl") => {
            DataFormat::Ndjson
        }
        _ => DataFormat::Unknown,
    }
}

/// Abstraction for future magic-byte sniffing and adapter-specific detection.
pub trait FormatDetector: Send + Sync {
    /// Best-effort format classification for a single path.
    fn detect_format(&self, path: &Utf8Path) -> DataFormat;
}

/// Extension-only [`FormatDetector`] used in Phase 0 smoke paths.
#[derive(Debug, Default, Clone, Copy)]
pub struct ExtensionFormatDetector;

impl FormatDetector for ExtensionFormatDetector {
    fn detect_format(&self, path: &Utf8Path) -> DataFormat {
        classify_format_from_path(path)
    }
}

#[cfg(test)]
mod tests {
    use camino::Utf8Path;

    use super::classify_format_from_path;
    use paraclete_types::DataFormat;

    #[test]
    fn classifies_common_extensions() {
        assert_eq!(classify_format_from_path(Utf8Path::new("x.parquet")), DataFormat::Parquet);
        assert_eq!(classify_format_from_path(Utf8Path::new("x.pq")), DataFormat::Parquet);
        assert_eq!(classify_format_from_path(Utf8Path::new("x.csv")), DataFormat::Csv);
        assert_eq!(classify_format_from_path(Utf8Path::new("x.json")), DataFormat::Json);
        assert_eq!(classify_format_from_path(Utf8Path::new("x.ndjson")), DataFormat::Ndjson);
        assert_eq!(classify_format_from_path(Utf8Path::new("x.jsonl")), DataFormat::Ndjson);
        assert_eq!(classify_format_from_path(Utf8Path::new("x.unknown")), DataFormat::Unknown);
    }
}
