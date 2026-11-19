#![no_main]

use libfuzzer_sys::fuzz_target;
use parqonaut_repair::{canonical_plan_json, ExecutionManifest, RepairPlan};

fuzz_target!(|data: &[u8]| {
    if data.len() > 1024 * 1024 {
        return;
    }
    if let Ok(plan) = RepairPlan::from_json(data) {
        let _ = canonical_plan_json(&plan);
    }
    let _ = serde_json::from_slice::<ExecutionManifest>(data);
});
