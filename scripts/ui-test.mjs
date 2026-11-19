import test from "node:test";
import assert from "node:assert/strict";
import { SseFrameParser } from "../crates/parqonaut-service/src/ui/sse.js";
import { ParqonautApi, generateIdempotencyKey } from "../crates/parqonaut-service/src/ui/api.js";

test("SseFrameParser handles chunked frames, ids, comments, and job_status", () => {
  const parser = new SseFrameParser();
  const raw =
    ": keepalive\n\n" +
    "id: 1\n" +
    "event: job_event\n" +
    'data: {"seq":1,"kind":"job_queued"}\n\n' +
    "event: job_status\n" +
    'data: {"job_id":"00000000-0000-0000-0000-000000000000","status":"succeeded","last_seq":1}\n\n';
  const half = Math.floor(raw.length / 2);
  const f1 = parser.pushText(raw.slice(0, half));
  const f2 = parser.pushText(raw.slice(half));
  const all = [...f1, ...f2];
  assert.equal(all.length, 2);
  assert.equal(all[0].event, "job_event");
  assert.equal(all[0].id, "1");
  assert.equal(JSON.parse(all[0].data).seq, 1);
  assert.equal(all[1].event, "job_status");
});

test("ParqonautApi keeps token strictly in memory and attaches Authorization header only", () => {
  const api = new ParqonautApi();
  assert.equal(api.hasToken(), false);
  api.setToken("  prqnt_test_secret  ");
  assert.equal(api.hasToken(), true);
  assert.deepEqual(api.authHeaders(), { Authorization: "Bearer prqnt_test_secret" });
  api.clearToken();
  assert.equal(api.hasToken(), false);
  assert.deepEqual(api.authHeaders(), {});
  const k1 = generateIdempotencyKey("ui-repair");
  const k2 = generateIdempotencyKey("ui-repair");
  assert.notEqual(k1, k2);
});

test("TextDecoder({stream:true}) + SseFrameParser survives split multibyte UTF-8 at every byte offset", () => {
  const payload =
    'event: job_event\nid: 7\ndata: {"seq":7,"message":"héllo 🚀 日本語"}\n\n';
  const bytes = new TextEncoder().encode(payload);
  for (let split = 0; split <= bytes.length; split++) {
    const decoder = new TextDecoder();
    const parser = new SseFrameParser();
    const t1 = decoder.decode(bytes.subarray(0, split), { stream: true });
    const t2 = decoder.decode(bytes.subarray(split), { stream: true }) + decoder.decode();
    const frames = [...parser.pushText(t1), ...parser.pushText(t2)];
    assert.equal(frames.length, 1, `failed at split=${split}`);
    assert.equal(JSON.parse(frames[0].data).message, "héllo 🚀 日本語");
  }
});
