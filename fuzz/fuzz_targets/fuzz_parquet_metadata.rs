#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use parquet::file::reader::{FileReader, SerializedFileReader};

fuzz_target!(|data: &[u8]| {
    if data.len() > 2 * 1024 * 1024 {
        return;
    }
    let bytes = Bytes::copy_from_slice(data);
    if let Ok(reader) = SerializedFileReader::new(bytes) {
        let meta = reader.metadata();
        let _ = meta.file_metadata().num_rows();
        let _ = meta.num_row_groups();
    }
});
