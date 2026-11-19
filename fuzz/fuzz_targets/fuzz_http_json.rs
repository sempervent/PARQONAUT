#![no_main]

use libfuzzer_sys::fuzz_target;
use parqonaut_service::api_types::{AuthTokenCreateRequest, StartScanRequest};
use parqonaut_service::application_http::{
    BatchConfigBody, BatchRepairJobBody, LocationPolicyBody, PlanBody, RepairJobBody,
};

fuzz_target!(|data: &[u8]| {
    if data.len() > 512 * 1024 {
        return;
    }
    let _ = serde_json::from_slice::<StartScanRequest>(data);
    let _ = serde_json::from_slice::<AuthTokenCreateRequest>(data);
    let _ = serde_json::from_slice::<LocationPolicyBody>(data);
    let _ = serde_json::from_slice::<PlanBody>(data);
    let _ = serde_json::from_slice::<RepairJobBody>(data);
    let _ = serde_json::from_slice::<BatchConfigBody>(data);
    let _ = serde_json::from_slice::<BatchRepairJobBody>(data);
});
