#![no_main]

use libfuzzer_sys::fuzz_target;
use parqonaut_client::sse::SseParser;

fuzz_target!(|data: &[u8]| {
    if data.len() > 2 * 1024 * 1024 {
        return;
    }
    let mut parser = SseParser::new();
    let _ = parser.push_bytes(data);
    let _ = parser.finish();
});
