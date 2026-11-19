#![no_main]

use libfuzzer_sys::fuzz_target;
use parqonaut_orchestrator::{BatchConfig, BatchPlan};

fuzz_target!(|data: &[u8]| {
    if data.len() > 512 * 1024 {
        return;
    }
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = BatchConfig::from_toml(s);
    }
    let _ = BatchPlan::from_json(data);
});
