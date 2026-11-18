// Same-origin API wrapper for /api/v1 (bearer token held in memory only).

export class ApiError extends Error {
  constructor(status, code, message, category) {
    super(message);
    this.status = status;
    this.code = code;
    this.category = category;
  }
}

export function generateIdempotencyKey(prefix) {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}-${crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export class ParqonautApi {
  constructor() {
    this._token = null;
  }

  setToken(token) {
    const trimmed = typeof token === "string" ? token.trim() : "";
    this._token = trimmed.length > 0 ? trimmed : null;
  }

  clearToken() {
    this._token = null;
  }

  hasToken() {
    return this._token !== null;
  }

  authHeaders(extra = {}) {
    const h = { ...extra };
    if (this._token) {
      h["Authorization"] = `Bearer ${this._token}`;
    }
    return h;
  }

  async request(method, path, body = null, idempotencyKey = null) {
    const headers = this.authHeaders();
    if (body !== null) {
      headers["Content-Type"] = "application/json";
    }
    if (idempotencyKey) {
      headers["Idempotency-Key"] = idempotencyKey;
    }
    let resp;
    try {
      resp = await fetch(path, {
        method,
        headers,
        body: body !== null ? JSON.stringify(body) : undefined,
      });
    } catch (err) {
      throw new ApiError(0, "connection_error", `Network error: ${err.message}`, "connection");
    }
    const text = await resp.text();
    let data = null;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        data = null;
      }
    }
    if (!resp.ok) {
      const env = data && data.error ? data.error : null;
      const code = env ? env.code : "http_error";
      const msg = env ? env.message : (text || `HTTP ${resp.status}`);
      let category = "operation";
      if (resp.status === 401) category = "unauthorized";
      else if (resp.status === 403) category = "forbidden";
      else if (resp.status >= 502 || resp.status === 408 || resp.status === 429) category = "transient";
      throw new ApiError(resp.status, code, msg, category);
    }
    return data;
  }

  health() { return this.request("GET", "/api/v1/health"); }
  whoami() { return this.request("GET", "/api/v1/whoami"); }
  overview() { return this.request("GET", "/api/v1/overview"); }
  listPlugins() { return this.request("GET", "/api/v1/plugins"); }

  listJobs({ status = "", kind = "", limit = 50, offset = 0 } = {}) {
    const p = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    if (status) p.set("status", status);
    if (kind) p.set("kind", kind);
    return this.request("GET", `/api/v1/jobs?${p.toString()}`);
  }

  getJob(jobId) { return this.request("GET", `/api/v1/jobs/${encodeURIComponent(jobId)}`); }

  getJobEvents(jobId, afterSeq = 0, limit = 100) {
    const p = new URLSearchParams({ after_seq: String(afterSeq), limit: String(limit) });
    return this.request("GET", `/api/v1/jobs/${encodeURIComponent(jobId)}/events?${p.toString()}`);
  }

  getJobBatch(jobId) {
    return this.request("GET", `/api/v1/jobs/${encodeURIComponent(jobId)}/batch`);
  }

  listRuns(limit = 30, offset = 0) {
    const p = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    return this.request("GET", `/api/v1/runs?${p.toString()}`);
  }

  getRun(runId) { return this.request("GET", `/api/v1/runs/${encodeURIComponent(runId)}`); }

  listFindings(runId, { severity = "", code = "", limit = 50, offset = 0 } = {}) {
    const p = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    if (severity) p.set("severity", severity);
    if (code) p.set("code", code);
    return this.request("GET", `/api/v1/runs/${encodeURIComponent(runId)}/findings?${p.toString()}`);
  }

  listAssets(runId, limit = 50, offset = 0) {
    const p = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    return this.request("GET", `/api/v1/runs/${encodeURIComponent(runId)}/assets?${p.toString()}`);
  }

  submitScan(body, idempotencyKey) {
    return this.request("POST", "/api/v1/jobs/scans", body, idempotencyKey);
  }

  cancelJob(jobId) {
    return this.request("POST", `/api/v1/jobs/${encodeURIComponent(jobId)}/cancel`);
  }

  generatePlan(body) {
    return this.request("POST", "/api/v1/plans", body);
  }

  submitRepair(body, idempotencyKey) {
    return this.request("POST", "/api/v1/repairs", body, idempotencyKey);
  }

  verifyRepair(body) {
    return this.request("POST", "/api/v1/verifications", body);
  }

  submitBatchRepair(body, idempotencyKey) {
    return this.request("POST", "/api/v1/batches/repairs", body, idempotencyKey);
  }

  submitBatchResume(runDir, body, idempotencyKey) {
    return this.request("POST", `/api/v1/batches/${encodeURIComponent(runDir)}/resume`, body, idempotencyKey);
  }
}
