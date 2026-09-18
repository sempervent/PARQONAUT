//! Map engine errors to typed [`paraclete_types::FailureKind`].

use paraclete_types::FailureKind;

use crate::CoreError;

pub(crate) fn failure_kind(err: &CoreError) -> FailureKind {
    match err {
        CoreError::MissingPath(_) | CoreError::Io(_) => FailureKind::IoError,
        CoreError::NonUtf8Path(_) => FailureKind::IoError,
        CoreError::Parquet(_) => FailureKind::FormatReadError,
        CoreError::Inspect(_) => FailureKind::ProbeDecodeError,
        CoreError::Unsupported(_) => FailureKind::UnsupportedFormat,
        CoreError::ReportValidation(_) | CoreError::Plugin(_) => FailureKind::InternalEngineError,
    }
}
