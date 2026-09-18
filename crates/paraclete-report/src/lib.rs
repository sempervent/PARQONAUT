//! Report serialization helpers and Markdown rendering stubs.

#![forbid(unsafe_code)]

mod error;
mod json;
mod markdown;

pub use error::ReportError;
pub use json::{to_json_string, to_json_value};
pub use markdown::render_markdown_stub;
pub use paraclete_types::validate_report;
