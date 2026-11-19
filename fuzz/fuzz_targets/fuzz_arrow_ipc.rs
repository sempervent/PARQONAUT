#![no_main]

use libfuzzer_sys::fuzz_target;
use parqonaut_plugin_host::decode_ipc_stream_bytes;

fuzz_target!(|data: &[u8]| {
    let _ = decode_ipc_stream_bytes(data);
});
