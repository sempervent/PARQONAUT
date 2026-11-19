#![no_main]

use libfuzzer_sys::fuzz_target;
use parqonaut_plugin_protocol::{PluginManifest, PluginRequest, PluginResponse};

fuzz_target!(|data: &[u8]| {
    if data.len() > 1024 * 1024 {
        return;
    }
    if let Ok(m) = serde_json::from_slice::<PluginManifest>(data) {
        let _ = m.validate();
    }
    let _ = serde_json::from_slice::<PluginRequest>(data);
    let _ = serde_json::from_slice::<PluginResponse>(data);
});
