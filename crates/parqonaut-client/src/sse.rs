//! Incremental SSE parser for `/api/v1/jobs/{job_id}/events/stream`.

use parqonaut_types::{JobEvent, JobStatus};
use serde::Deserialize;
use uuid::Uuid;

use crate::error::OperatorClientError;

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
    buf: String,
    event_name: Option<String>,
    event_id: Option<String>,
    data_lines: Vec<String>,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<Vec<SseEventItem>, OperatorClientError> {
        let chunk = std::str::from_utf8(bytes)
            .map_err(|e| OperatorClientError::InvalidResponse(format!("SSE non-UTF8: {e}")))?;
        self.buf.push_str(chunk);
        let mut out = Vec::new();
        while let Some(pos) = self.buf.find('\n') {
            let mut line = self.buf[..pos].to_string();
            self.buf.drain(..=pos);
            if line.ends_with('\r') {
                line.pop();
            }
            if line.is_empty() {
                if let Some(item) = self.flush_frame()? {
                    out.push(item);
                }
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            if let Some(rest) = line.strip_prefix("event:") {
                self.event_name = Some(rest.trim().to_string());
            } else if let Some(rest) = line.strip_prefix("id:") {
                self.event_id = Some(rest.trim().to_string());
            } else if let Some(rest) = line.strip_prefix("data:") {
                let data = rest.strip_prefix(' ').unwrap_or(rest);
                self.data_lines.push(data.to_string());
            }
        }
        Ok(out)
    }

    fn flush_frame(&mut self) -> Result<Option<SseEventItem>, OperatorClientError> {
        let name = self.event_name.take();
        let _id = self.event_id.take();
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
                Ok(Some(SseEventItem::JobEvent(Box::new(ev))))
            }
            Some("job_status") => {
                let st: SseStatusPayload = serde_json::from_str(&payload).map_err(|e| {
                    OperatorClientError::InvalidResponse(format!("invalid job_status JSON: {e}"))
                })?;
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

    #[test]
    fn parses_split_sse_chunks_and_comments() {
        let mut p = SseParser::new();
        let ev_json = serde_json::json!({
            "event_id": Uuid::nil(),
            "job_id": Uuid::nil(),
            "seq": 3,
            "schema_version": 1,
            "occurred_at": "2026-11-17T12:00:00Z",
            "kind": "job_queued",
            "stage": "queue",
            "is_terminal": false,
            "metrics": { "files_total": 0, "files_done": 0, "rows": 0, "bytes": 0, "elapsed_ms": 0 },
            "message": "queued"
        })
        .to_string();
        let full = format!(": keepalive\n\nid: 3\nevent: job_event\ndata: {ev_json}\n\n");
        let mid = full.len() / 2;
        assert!(p.push_bytes(&full.as_bytes()[..mid]).unwrap().is_empty());
        let items = p.push_bytes(&full.as_bytes()[mid..]).unwrap();
        assert_eq!(items.len(), 1);
        assert!(matches!(&items[0], SseEventItem::JobEvent(ev) if ev.seq == 3));
    }
}
