//! Length-framed Arrow IPC records for batch plugin transport (protocol v1).

use std::io::Read;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::error::PluginHostError;

const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn encode_batch_frame(batch: &RecordBatch) -> Result<Vec<u8>, PluginHostError> {
    let mut ipc_buf = Vec::new();
    {
        let mut ipc = StreamWriter::try_new(&mut ipc_buf, &batch.schema())
            .map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
        ipc.write(batch).map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
        ipc.finish().map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
    }
    if ipc_buf.len() > MAX_FRAME_BYTES {
        return Err(PluginHostError::ProtocolViolation("batch frame too large".into()));
    }
    let len = u32::try_from(ipc_buf.len())
        .map_err(|_| PluginHostError::ProtocolViolation("batch frame length overflow".into()))?;
    let mut frame = Vec::with_capacity(4 + ipc_buf.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&ipc_buf);
    Ok(frame)
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
    reader.read_exact(&mut payload).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            PluginHostError::ProtocolViolation("truncated ipc batch frame".into())
        } else {
            PluginHostError::Io(e)
        }
    })?;
    let mut cursor = std::io::Cursor::new(payload);
    let mut stream = StreamReader::try_new(&mut cursor, None)
        .map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
    let batch = stream
        .next()
        .ok_or_else(|| PluginHostError::ProtocolViolation("empty ipc frame".into()))?
        .map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
    Ok(Some(batch))
}
