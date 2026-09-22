//! Length-framed Arrow IPC records for batch plugin transport (protocol v1).

use std::io::{Read, Write};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::error::PluginHostError;

const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

pub fn write_batch_frame(
    writer: &mut impl Write,
    batch: &RecordBatch,
) -> Result<(), PluginHostError> {
    let mut buf = Vec::new();
    {
        let mut ipc = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
        ipc.write(batch).map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
        ipc.finish().map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
    }
    if buf.len() > MAX_FRAME_BYTES {
        return Err(PluginHostError::ProtocolViolation("batch frame too large".into()));
    }
    let len = u32::try_from(buf.len())
        .map_err(|_| PluginHostError::ProtocolViolation("batch frame length overflow".into()))?;
    writer.write_all(&len.to_le_bytes()).map_err(PluginHostError::Io)?;
    writer.write_all(&buf).map_err(PluginHostError::Io)?;
    writer.flush().map_err(PluginHostError::Io)?;
    Ok(())
}

pub fn read_batch_frame(
    reader: &mut (impl Read + ?Sized),
) -> Result<Option<RecordBatch>, PluginHostError> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(PluginHostError::Io(e)),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len == 0 {
        return Ok(None);
    }
    if len > MAX_FRAME_BYTES {
        return Err(PluginHostError::ResponseTooLarge { size: len, max: MAX_FRAME_BYTES });
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).map_err(PluginHostError::Io)?;
    let mut cursor = std::io::Cursor::new(payload);
    let mut stream = StreamReader::try_new(&mut cursor, None)
        .map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
    let batch = stream
        .next()
        .ok_or_else(|| PluginHostError::ProtocolViolation("empty ipc frame".into()))?
        .map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
    Ok(Some(batch))
}
