PRAGMA foreign_keys = ON;

CREATE TABLE scan_jobs (
    job_id TEXT PRIMARY KEY NOT NULL,
    submitted_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    status TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    normalized_target_key TEXT NOT NULL,
    request_json TEXT NOT NULL,
    failure_code TEXT,
    failure_message TEXT,
    run_id TEXT,
    FOREIGN KEY (run_id) REFERENCES scan_runs (run_id) ON DELETE SET NULL
);

CREATE INDEX idx_scan_jobs_status_submitted ON scan_jobs (status, submitted_at DESC);
