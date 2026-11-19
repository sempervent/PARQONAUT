#![no_main]

use libfuzzer_sys::fuzz_target;
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};

fuzz_target!(|data: &[u8]| {
    if data.len() > 64 * 1024 {
        return;
    }
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = DatasetLocation::parse(s);
        let _ = ObjectLocation::parse(s);
    }
});
