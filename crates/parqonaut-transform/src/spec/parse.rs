use crate::error::{Result, TransformError};
use crate::spec::types::Spec;
use std::path::Path;

use parqonaut_workflow::TRANSFORM_SPEC_SCHEMA_VERSION;

pub fn parse_spec<P: AsRef<Path>>(path: P) -> Result<Spec> {
    let path_ref = path.as_ref();
    let content = std::fs::read_to_string(path_ref)?;
    let ext = path_ref.extension().and_then(|s| s.to_str());

    let spec: Spec = match ext {
        Some("yaml") | Some("yml") => serde_yaml::from_str(&content)
            .map_err(|e| TransformError::SpecError(format!("YAML parse error: {}", e)))?,
        Some("json") => serde_json::from_str(&content)
            .map_err(|e| TransformError::SpecError(format!("JSON parse error: {}", e)))?,
        _ => {
            return Err(TransformError::SpecError(
                "Spec file must have .yaml, .yml, or .json extension".to_string(),
            ))
        }
    };

    if spec.schema_version != TRANSFORM_SPEC_SCHEMA_VERSION {
        return Err(TransformError::SpecError(format!(
            "unsupported schema-version {} (expected {})",
            spec.schema_version, TRANSFORM_SPEC_SCHEMA_VERSION
        )));
    }

    Ok(spec)
}

pub fn merge_spec_with_cli(spec: Spec, cli_overrides: Spec) -> Spec {
    // CLI values override spec values
    Spec {
        schema_version: spec.schema_version,
        input: cli_overrides.input.or(spec.input),
        output: cli_overrides.output.or(spec.output),
        steps: if cli_overrides.steps.is_empty() { spec.steps } else { cli_overrides.steps },
        options: Options {
            concurrency: cli_overrides.options.concurrency.max(spec.options.concurrency),
            batch_rows: cli_overrides.options.batch_rows.or(spec.options.batch_rows),
            batch_bytes: cli_overrides.options.batch_bytes.or(spec.options.batch_bytes),
            fail_fast: cli_overrides.options.fail_fast || spec.options.fail_fast,
            atomic: cli_overrides.options.atomic || spec.options.atomic,
            overwrite: cli_overrides.options.overwrite || spec.options.overwrite,
        },
    }
}

use crate::spec::types::Options;
