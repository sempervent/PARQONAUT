//! Refresh or verify `fixtures/api/openapi-v1.json` from the runtime OpenAPI model.

use std::path::PathBuf;

use paraclete_service::openapi_spec;
use serde_json::Value;

fn normalize_openapi_document(v: Value) -> String {
    fn sort_json_value(v: Value) -> Value {
        match v {
            Value::Object(map) => {
                let mut keys: Vec<_> = map.keys().cloned().collect();
                keys.sort();
                let mut out = serde_json::Map::new();
                for k in keys {
                    out.insert(k.clone(), sort_json_value(map[&k].clone()));
                }
                Value::Object(out)
            }
            Value::Array(items) => Value::Array(items.into_iter().map(sort_json_value).collect()),
            other => other,
        }
    }

    let mut sorted = sort_json_value(v);
    if let Some(info) = sorted.get_mut("info").and_then(|i| i.as_object_mut()) {
        info.insert(
            "version".into(),
            Value::String(env!("CARGO_PKG_VERSION").into()),
        );
    }
    serde_json::to_string_pretty(&sorted).expect("normalized openapi serializes")
}

pub fn run(write: bool, check: bool) -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let golden_path = repo_root.join("fixtures/api/openapi-v1.json");
    let live = normalize_openapi_document(serde_json::to_value(openapi_spec())?);

    if check {
        let golden_text = std::fs::read_to_string(&golden_path)?;
        let golden: Value = serde_json::from_str(&golden_text)?;
        let expected = normalize_openapi_document(golden);
        if live != expected {
            return Err("OpenAPI golden drift — run `cargo xtask docs openapi`".into());
        }
        eprintln!("OpenAPI golden OK");
        return Ok(());
    }

    if write {
        if let Some(parent) = golden_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&golden_path, format!("{live}\n"))?;
        eprintln!("wrote {}", golden_path.display());
    }
    Ok(())
}
