//! Byte-oriented incremental SSE parser with explicit bounds and UTF-8 safety.

use parqonaut_types::{JobEvent, JobStatus};
use serde::Deserialize;
use uuid::Uuid;

use crate::error::OperatorClientError;

pub const MAX_SSE_LINE_BYTES: usize = 512 * 1024;
pub const MAX_SSE_FRAME_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SseEventItem {
    JobEvent(Box<JobEvent>),
    JobStatus { job_id: Uuid, status: JobStatus, last_seq: i64 },
}

#[derive(Debug, Deserialize)]
struct SseStatusPayload {
    job_id: Uuid,
    status: JobStatus,
    last_seq: i64,
}

#[derive(Debug, Default)]
pub struct SseParser {
    buf: Vec<u8>,
    event_name: Option<String>,
    event_id: Option<String>,
    last_event_id: Option<String>,
    last_seq: i64,
    data_lines: Vec<String>,
    frame_bytes: usize,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cursor(last_seq: i64) -> Self {
        Self { last_seq, ..Self::default() }
    }

    pub fn last_seq(&self) -> i64 {
        self.last_seq
    }

    pub fn last_event_id(&self) -> Option<&str> {
        self.last_event_id.as_deref()
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<Vec<SseEventItem>, OperatorClientError> {
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        while let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
            if pos > MAX_SSE_LINE_BYTES {
                self.buf.clear();
                self.reset_frame();
                return Err(OperatorClientError::InvalidResponse(format!(
                    "SSE line exceeds max size ({MAX_SSE_LINE_BYTES} bytes)"
                )));
            }
            let line_end = if pos > 0 && self.buf[pos - 1] == b'\r' { pos - 1 } else { pos };
            let line_bytes = self.buf[..line_end].to_vec();
            self.buf.drain(..=pos);

            let line = std::str::from_utf8(&line_bytes).map_err(|e| {
                self.reset_frame();
                OperatorClientError::InvalidResponse(format!("SSE invalid UTF-8: {e}"))
            })?;

            if line.is_empty() {
                if let Some(item) = self.flush_frame()? {
                    out.push(item);
                }
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            self.frame_bytes = self.frame_bytes.saturating_add(line.len());
            if self.frame_bytes > MAX_SSE_FRAME_BYTES {
                self.reset_frame();
                return Err(OperatorClientError::InvalidResponse(format!(
                    "SSE frame exceeds max size ({MAX_SSE_FRAME_BYTES} bytes)"
                )));
            }
            if let Some(rest) = line.strip_prefix("event:") {
                let val = rest.strip_prefix(' ').unwrap_or(rest).trim();
                self.event_name = Some(val.to_string());
            } else if let Some(rest) = line.strip_prefix("id:") {
                let val = rest.strip_prefix(' ').unwrap_or(rest).trim();
                if !val.contains('\0') {
                    self.event_id = Some(val.to_string());
                    self.last_event_id = Some(val.to_string());
                }
            } else if let Some(rest) = line.strip_prefix("data:") {
                let data = rest.strip_prefix(' ').unwrap_or(rest);
                self.data_lines.push(data.to_string());
            }
        }
        if self.buf.len() > MAX_SSE_LINE_BYTES {
            self.buf.clear();
            self.reset_frame();
            return Err(OperatorClientError::InvalidResponse(format!(
                "SSE unterminated line exceeds max size ({MAX_SSE_LINE_BYTES} bytes)"
            )));
        }
        Ok(out)
    }

    /// Call on stream EOF; errors if the connection terminated mid-frame.
    pub fn finish(&mut self) -> Result<(), OperatorClientError> {
        if !self.buf.is_empty() || !self.data_lines.is_empty() {
            self.buf.clear();
            self.reset_frame();
            return Err(OperatorClientError::Transient("SSE stream terminated mid-frame".into()));
        }
        Ok(())
    }

    fn reset_frame(&mut self) {
        self.event_name = None;
        self.event_id = None;
        self.data_lines.clear();
        self.frame_bytes = 0;
    }

    fn flush_frame(&mut self) -> Result<Option<SseEventItem>, OperatorClientError> {
        let name = self.event_name.take();
        let _id = self.event_id.take();
        self.frame_bytes = 0;
        if self.data_lines.is_empty() {
            return Ok(None);
        }
        let payload = self.data_lines.join("\n");
        self.data_lines.clear();
        match name.as_deref() {
            Some("job_event") | None => {
                let ev: JobEvent = serde_json::from_str(&payload).map_err(|e| {
                    OperatorClientError::InvalidResponse(format!("invalid job_event JSON: {e}"))
                })?;
                if ev.seq <= self.last_seq {
                    return Ok(None);
                }
                self.last_seq = ev.seq;
                Ok(Some(SseEventItem::JobEvent(Box::new(ev))))
            }
            Some("job_status") => {
                let st: SseStatusPayload = serde_json::from_str(&payload).map_err(|e| {
                    OperatorClientError::InvalidResponse(format!("invalid job_status JSON: {e}"))
                })?;
                if st.last_seq > self.last_seq {
                    self.last_seq = st.last_seq;
                }
                Ok(Some(SseEventItem::JobStatus {
                    job_id: st.job_id,
                    status: st.status,
                    last_seq: st.last_seq,
                }))
            }
            Some(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event_json(seq: i64, msg: &str) -> String {
        serde_json::json!({
            "event_id": Uuid::nil(),
            "job_id": Uuid::nil(),
            "seq": seq,
            "schema_version": 1,
            "occurred_at": "2026-11-17T12:00:00Z",
            "kind": "job_queued",
            "stage": "queue",
            "is_terminal": false,
            "metrics": { "files_total": 0, "files_done": 0, "rows": 0, "bytes": 0, "elapsed_ms": 0 },
            "message": msg
        })
        .to_string()
    }

    #[test]
    fn parses_split_multibyte_utf8_at_every_byte_boundary() {
        let msg = "résumé 🦀 café — naïve";
        let ev_json = sample_event_json(1, msg);
        let full = format!("id: 1\nevent: job_event\ndata: {ev_json}\n\n");
        let bytes = full.as_bytes();

        for split in 1..bytes.len() {
            let mut p = SseParser::new();
            let mut items = p.push_bytes(&bytes[..split]).unwrap();
            items.extend(p.push_bytes(&bytes[split..]).unwrap());
            assert_eq!(items.len(), 1, "split={split}");
            match &items[0] {
                SseEventItem::JobEvent(ev) => {
                    assert_eq!(ev.seq, 1);
                    assert_eq!(ev.message.as_deref(), Some(msg));
                }
                other => panic!("expected JobEvent, got {other:?}"),
            }
            assert!(p.finish().is_ok());
        }

        // Also test 1-byte-at-a-time streaming.
        let mut p_one = SseParser::new();
        let mut collected = Vec::new();
        for b in bytes {
            collected.extend(p_one.push_bytes(&[*b]).unwrap());
        }
        assert_eq!(collected.len(), 1);
    }

    #[test]
    fn handles_multiple_frames_crlf_multiline_comments_unknown_and_dedup() {
        let e1 = sample_event_json(1, "first");
        let e2 = sample_event_json(2, "second");
        // Split e2 across two data: lines (with JSON newline inside string or split across lines—here use pretty JSON)
        let e2_pretty =
            serde_json::to_string_pretty(&serde_json::from_str::<serde_json::Value>(&e2).unwrap())
                .unwrap();
        let multiline_data =
            e2_pretty.lines().map(|l| format!("data: {l}\r\n")).collect::<String>();

        let stream = format!(
            ": keepalive comment\r\n\r\n\
             event: custom_ignored\r\ndata: {{\"ignored\":true}}\r\n\r\n\
             id: 1\r\nevent: job_event\r\ndata: {e1}\r\n\r\n\
             id: 1\r\nevent: job_event\r\ndata: {e1}\r\n\r\n\
             id: 2\r\nevent: job_event\r\n{multiline_data}\r\n"
        );
        let mut p = SseParser::new();
        let items = p.push_bytes(stream.as_bytes()).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(p.last_seq(), 2);
        assert_eq!(p.last_event_id(), Some("2"));
    }

    #[test]
    fn rejects_malformed_utf8_oversized_and_mid_frame_eof() {
        let mut p_utf8 = SseParser::new();
        let err = p_utf8.push_bytes(b"data: \xff\xfe\n\n").unwrap_err();
        assert!(matches!(err, OperatorClientError::InvalidResponse(_)));

        let mut p_big = SseParser::new();
        let huge = vec![b'a'; MAX_SSE_LINE_BYTES + 16];
        let err_big = p_big.push_bytes(&huge).unwrap_err();
        assert!(matches!(err_big, OperatorClientError::InvalidResponse(_)));

        let mut p_frame = SseParser::new();
        let chunk = format!("data: {}\n", "x".repeat(64 * 1024));
        let mut hit_err = false;
        for _ in 0..40 {
            if let Err(e) = p_frame.push_bytes(chunk.as_bytes()) {
                assert!(matches!(e, OperatorClientError::InvalidResponse(_)));
                hit_err = true;
                break;
            }
        }
        assert!(hit_err, "expected oversized frame rejection");

        let mut p_eof = SseParser::new();
        assert!(p_eof.push_bytes(b"event: job_event\ndata: {\"partial\":1").unwrap().is_empty());
        assert!(matches!(p_eof.finish().unwrap_err(), OperatorClientError::Transient(_)));
    }
}
