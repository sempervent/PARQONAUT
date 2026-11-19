//! Property-based tests for incremental UTF-8 SSE framing & cursor deduplication.

use chrono::Utc;
use parqonaut_client::sse::{SseEventItem, SseParser};
use parqonaut_types::{JobEvent, JobId, JobStage};
use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};
use proptest::prelude::*;
use uuid::Uuid;

fn make_event_json(job_id: JobId, seq: i64, msg: &str) -> String {
    let ev = JobEvent {
        event_id: Uuid::new_v4(),
        job_id,
        seq,
        schema_version: 1,
        occurred_at: Utc::now(),
        kind: ProgressEventKind::BatchProcessed,
        stage: JobStage::Scan,
        is_terminal: false,
        metrics: ProgressMetrics::default(),
        message: Some(msg.to_string()),
        run_id: None,
        plugin_name: None,
        plugin_digest: None,
    };
    serde_json::to_string(&ev).unwrap()
}

proptest! {
    #[test]
    fn chunking_valid_utf8_sse_stream_at_arbitrary_offsets_is_invariant(
        msgs in prop::collection::vec("[a-zA-Z0-9 é🚀日本]{1,32}", 1..6),
        chunk_sizes in prop::collection::vec(1usize..17, 1..40),
        use_crlf in any::<bool>(),
    ) {
        let sep = if use_crlf { "\r\n" } else { "\n" };
        let job_id = JobId::new();
        let mut wire = String::new();
        for (idx, msg) in msgs.iter().enumerate() {
            let seq = (idx + 1) as i64;
            let data = make_event_json(job_id, seq, msg);
            wire.push_str(&format!(
                ": keepalive{sep}id: {seq}{sep}event: job_event{sep}data: {data}{sep}{sep}"
            ));
        }

        // Reference unchunked parse
        let mut ref_parser = SseParser::new();
        let ref_frames = ref_parser.push_bytes(wire.as_bytes()).unwrap();
        ref_parser.finish().unwrap();

        // Arbitrary chunked parse
        let mut chunk_parser = SseParser::new();
        let mut chunked_frames = Vec::new();
        let bytes = wire.as_bytes();
        let mut pos = 0usize;
        let mut ci = 0usize;
        while pos < bytes.len() {
            let step = chunk_sizes[ci % chunk_sizes.len()];
            ci += 1;
            let end = (pos + step).min(bytes.len());
            chunked_frames.extend(chunk_parser.push_bytes(&bytes[pos..end]).unwrap());
            pos = end;
        }
        chunk_parser.finish().unwrap();

        prop_assert_eq!(chunked_frames.len(), ref_frames.len());
        for (a, b) in chunked_frames.iter().zip(ref_frames.iter()) {
            prop_assert_eq!(a, b);
            if let SseEventItem::JobEvent(ea) = a {
                prop_assert!(ea.seq >= 1);
            }
        }
    }

    #[test]
    fn sse_cursor_deduplication_never_emits_stale_seq(
        total in 1i64..12,
        cursor in 0i64..15,
    ) {
        let job_id = JobId::new();
        let mut wire = String::new();
        for seq in 1..=total {
            let data = make_event_json(job_id, seq, "step");
            wire.push_str(&format!("id: {seq}\nevent: job_event\ndata: {data}\n\n"));
        }
        let mut parser = SseParser::with_cursor(cursor);
        let frames = parser.push_bytes(wire.as_bytes()).unwrap();
        parser.finish().unwrap();
        let expected_count = (total - cursor).max(0) as usize;
        prop_assert_eq!(frames.len(), expected_count);
    }
}

#[test]
fn sse_disconnect_mid_frame_triggers_transient_error_and_recovers_on_reconnect() {
    let jid = JobId::new();
    let ev1 = make_event_json(jid, 1, "one");
    let ev2 = make_event_json(jid, 2, "two");

    let mut parser = SseParser::new();
    let partial = format!(
        "id: 1\nevent: job_event\ndata: {ev1}\n\nid: 2\nevent: job_event\ndata: {{\"seq\":2"
    );
    let f1 = parser.push_bytes(partial.as_bytes()).unwrap();
    assert_eq!(f1.len(), 1);
    assert_eq!(parser.last_seq(), 1);

    let err = parser.finish().unwrap_err();
    assert!(matches!(err, parqonaut_client::error::OperatorClientError::Transient(_)));

    let mut reconnected = SseParser::with_cursor(parser.last_seq());
    let full =
        format!("id: 1\nevent: job_event\ndata: {ev1}\n\nid: 2\nevent: job_event\ndata: {ev2}\n\n");
    let f2 = reconnected.push_bytes(full.as_bytes()).unwrap();
    assert_eq!(f2.len(), 1);
    assert_eq!(reconnected.last_seq(), 2);
}
