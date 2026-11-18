ALTER TABLE application_jobs ADD COLUMN IF NOT EXISTS last_event_seq BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS application_job_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    occurred_at TEXT NOT NULL,
    kind TEXT NOT NULL,
    stage TEXT NOT NULL,
    is_terminal INTEGER NOT NULL DEFAULT 0,
    metrics_json TEXT NOT NULL,
    message TEXT,
    run_id TEXT,
    plugin_name TEXT,
    plugin_digest TEXT,
    FOREIGN KEY (job_id) REFERENCES application_jobs (job_id) ON DELETE CASCADE,
    UNIQUE (job_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_seq ON application_job_events (job_id, seq ASC);
CREATE INDEX IF NOT EXISTS idx_job_events_occurred ON application_job_events (occurred_at DESC);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    job_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (scope, idempotency_key),
    FOREIGN KEY (job_id) REFERENCES application_jobs (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_keys (expires_at);
