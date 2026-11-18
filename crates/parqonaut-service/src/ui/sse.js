// Authenticated SSE client over fetch() with Authorization: Bearer header & polling fallback.

export class SseFrameParser {
  constructor() {
    this.buf = "";
    this.eventName = null;
    this.eventId = null;
    this.dataLines = [];
  }

  pushText(chunk) {
    this.buf += chunk;
    const out = [];
    let nl;
    while ((nl = this.buf.indexOf("\n")) !== -1) {
      let line = this.buf.slice(0, nl);
      this.buf = this.buf.slice(nl + 1);
      if (line.endsWith("\r")) line = line.slice(0, -1);
      if (line === "") {
        if (this.dataLines.length > 0) {
          const payload = this.dataLines.join("\n");
          const event = this.eventName || "message";
          const id = this.eventId;
          this.eventName = null;
          this.eventId = null;
          this.dataLines = [];
          out.push({ event, id, data: payload });
        } else {
          this.eventName = null;
          this.eventId = null;
        }
        continue;
      }
      if (line.startsWith(":")) continue;
      if (line.startsWith("event:")) {
        this.eventName = line.slice(6).trim();
      } else if (line.startsWith("id:")) {
        this.eventId = line.slice(3).trim();
      } else if (line.startsWith("data:")) {
        const rest = line.slice(5);
        this.dataLines.push(rest.startsWith(" ") ? rest.slice(1) : rest);
      }
    }
    return out;
  }
}

export function subscribeJobEvents(api, jobId, afterSeq, callbacks) {
  const controller = new AbortController();
  let stopped = false;
  let cursor = afterSeq || 0;

  async function run() {
    const url = `/api/v1/jobs/${encodeURIComponent(jobId)}/events/stream?after_seq=${encodeURIComponent(String(cursor))}`;
    try {
      const resp = await fetch(url, {
        method: "GET",
        headers: api.authHeaders({ "Last-Event-ID": String(cursor), Accept: "text/event-stream" }),
        signal: controller.signal,
      });
      if (!resp.ok || !resp.body || typeof resp.body.getReader !== "function") {
        throw new Error(`SSE fallback (status ${resp.status})`);
      }
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      const parser = new SseFrameParser();
      while (!stopped) {
        const { value, done } = await reader.read();
        if (done) break;
        const frames = parser.pushText(decoder.decode(value, { stream: true }));
        for (const frame of frames) {
          if (frame.event === "job_event") {
            const ev = JSON.parse(frame.data);
            if (ev.seq > cursor) {
              cursor = ev.seq;
              callbacks.onEvent(ev);
            }
          } else if (frame.event === "job_status") {
            const st = JSON.parse(frame.data);
            callbacks.onTerminal(st);
            return;
          }
        }
      }
    } catch (err) {
      if (stopped || controller.signal.aborted) return;
      if (callbacks.onFallback) callbacks.onFallback(err);
      await pollLoop();
    }
  }

  async function pollLoop() {
    while (!stopped) {
      try {
        const page = await api.getJobEvents(jobId, cursor, 100);
        for (const ev of page.items || []) {
          if (ev.seq > cursor) {
            cursor = ev.seq;
            callbacks.onEvent(ev);
          }
        }
        const job = await api.getJob(jobId);
        if (["succeeded", "failed", "canceled"].includes(job.status)) {
          callbacks.onTerminal({ job_id: jobId, status: job.status, last_seq: cursor });
          return;
        }
      } catch {
        // Retry quietly until canceled.
      }
      await new Promise((r) => setTimeout(r, 750));
    }
  }

  run();
  return () => {
    stopped = true;
    controller.abort();
  };
}
