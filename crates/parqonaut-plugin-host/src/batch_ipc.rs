//! Length-framed Arrow IPC records for batch plugin transport (protocol v1).

use std::io::Read;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::error::PluginHostError;

const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

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

/// Validates every Arrow IPC stream message header inside `payload` before handing it to
/// `arrow_ipc::reader::StreamReader` (which otherwise trusts `i32` metadata lengths and
/// `bodyLength` prior to `read_exact` and can panic on negative/oversized values).
pub fn validate_ipc_stream_framing(payload: &[u8]) -> Result<(), PluginHostError> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(PluginHostError::ResponseTooLarge {
            size: payload.len(),
            max: MAX_FRAME_BYTES,
        });
    }
    let mut off = 0usize;
    let mut saw_schema = false;
    while off < payload.len() {
        if payload.len() - off < 4 {
            return Err(PluginHostError::ProtocolViolation(
                "truncated Arrow IPC size prefix".into(),
            ));
        }
        let mut size_bytes = [0u8; 4];
        size_bytes.copy_from_slice(&payload[off..off + 4]);
        off += 4;
        if size_bytes == CONTINUATION_MARKER {
            if payload.len() - off < 4 {
                return Err(PluginHostError::ProtocolViolation(
                    "truncated Arrow IPC continuation length".into(),
                ));
            }
            size_bytes.copy_from_slice(&payload[off..off + 4]);
            off += 4;
        }
        let meta_len = i32::from_le_bytes(size_bytes);
        if meta_len == 0 {
            break;
        }
        if meta_len < 0 {
            return Err(PluginHostError::ProtocolViolation(
                "negative Arrow IPC metadata length".into(),
            ));
        }
        let mlen = meta_len as usize;
        if payload.len() - off < mlen {
            return Err(PluginHostError::ProtocolViolation(
                "Arrow IPC metadata length exceeds frame payload".into(),
            ));
        }
        let msg = arrow::ipc::root_as_message(&payload[off..off + mlen]).map_err(|e| {
            PluginHostError::ProtocolViolation(format!("invalid IPC message: {e:?}"))
        })?;
        off += mlen;
        let blen = msg.bodyLength();
        if blen < 0 {
            return Err(PluginHostError::ProtocolViolation(
                "negative Arrow IPC body length".into(),
            ));
        }
        let blen_usize = usize::try_from(blen).map_err(|_| {
            PluginHostError::ProtocolViolation("Arrow IPC body length overflow".into())
        })?;
        if payload.len() - off < blen_usize {
            return Err(PluginHostError::ProtocolViolation(
                "Arrow IPC body length exceeds frame payload".into(),
            ));
        }
        off += blen_usize;
        saw_schema = true;
    }
    if !saw_schema {
        return Err(PluginHostError::ProtocolViolation("empty Arrow IPC frame".into()));
    }
    Ok(())
}

/// Validates framing and decodes a single Arrow IPC stream payload into a [`RecordBatch`].
pub fn decode_ipc_stream_bytes(payload: &[u8]) -> Result<RecordBatch, PluginHostError> {
    validate_ipc_stream_framing(payload)?;
    let mut cursor = std::io::Cursor::new(payload);
    let mut stream = StreamReader::try_new(&mut cursor, None)
        .map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
    let batch = stream
        .next()
        .ok_or_else(|| PluginHostError::ProtocolViolation("empty ipc frame".into()))?
        .map_err(|e| PluginHostError::ProtocolViolation(e.to_string()))?;
    Ok(batch)
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
    decode_ipc_stream_bytes(&payload).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_negative_and_oversized_ipc_metadata_without_panicking() {
        // 0xfffffffe (-2i32) triggered capacity_overflow panic inside arrow-ipc 54.3.1.
        let neg = [0xfe, 0xff, 0xff, 0xff];
        assert!(decode_ipc_stream_bytes(&neg).is_err());

        // Continuation marker + negative length
        let cont_neg = [0xff, 0xff, 0xff, 0xff, 0xf0, 0xff, 0xff, 0xff];
        assert!(decode_ipc_stream_bytes(&cont_neg).is_err());

        // Huge positive metadata length exceeding payload
        let huge = [0x00, 0x00, 0x00, 0x40, 0x00, 0x00];
        assert!(decode_ipc_stream_bytes(&huge).is_err());
    }
}
