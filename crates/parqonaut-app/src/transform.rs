//! Transform spec execution (including digest-pinned batch plugins).

use std::path::Path;

use parqonaut_transform::{execute_spec, parse_spec, TransformReport};

use crate::error::ApplicationError;
use crate::plugins::default_plugin_roots;

fn ensure_plugin_env() {
    if std::env::var("PARQONAUT_PLUGIN_ROOTS").is_err() {
        let roots: Vec<String> =
            default_plugin_roots().into_iter().map(|p| p.to_string_lossy().into_owned()).collect();
        std::env::set_var("PARQONAUT_PLUGIN_ROOTS", roots.join(":"));
    }
}

pub fn run_transform_spec(spec_path: &Path) -> Result<TransformReport, ApplicationError> {
    ensure_plugin_env();
    let spec =
        parse_spec(spec_path).map_err(|e| ApplicationError::InvalidRequest(e.to_string()))?;
    execute_spec(&spec).map_err(|e| ApplicationError::InvalidRequest(e.to_string()))
}
