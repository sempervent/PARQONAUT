-- Paraclete Postgres schema (Phase 17): TEXT timestamps (RFC 3339) for parity with SQLite rows.

CREATE TABLE scan_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    request_scan_id TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    normalized_target_key TEXT NOT NULL,
    target_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    run_outcome TEXT NOT NULL,
    engine_revision TEXT,
    contract_schema_version TEXT NOT NULL,
    report_format_version TEXT NOT NULL,
    report_sha256 TEXT NOT NULL,
    summary_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE scan_reports (
    run_id TEXT PRIMARY KEY NOT NULL,
    report_json TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES scan_runs (run_id) ON DELETE CASCADE
);

CREATE TABLE scan_assets (
    run_id TEXT NOT NULL,
    path TEXT NOT NULL,
    format TEXT NOT NULL,
    inspection_status TEXT NOT NULL,
    failure_kind TEXT,
    failure_message TEXT,
    dataset_id TEXT,
    PRIMARY KEY (run_id, path),
    FOREIGN KEY (run_id) REFERENCES scan_runs (run_id) ON DELETE CASCADE
);

CREATE TABLE scan_datasets (
    run_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    grouping_kind TEXT NOT NULL,
    partition_layout TEXT NOT NULL,
    file_count INTEGER NOT NULL,
    PRIMARY KEY (run_id, dataset_id),
    FOREIGN KEY (run_id) REFERENCES scan_runs (run_id) ON DELETE CASCADE
);

CREATE TABLE scan_findings (
    run_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    code TEXT NOT NULL,
    severity TEXT NOT NULL,
    category TEXT NOT NULL,
    asset_path TEXT,
    dataset_id TEXT,
    PRIMARY KEY (run_id, fingerprint),
    FOREIGN KEY (run_id) REFERENCES scan_runs (run_id) ON DELETE CASCADE
);

CREATE INDEX idx_scan_runs_target_time ON scan_runs (target_kind, normalized_target_key, completed_at DESC);

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
    worker_id TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    heartbeat_at TEXT,
    leased_until TEXT,
    recovery_note TEXT,
    FOREIGN KEY (run_id) REFERENCES scan_runs (run_id) ON DELETE SET NULL
);

CREATE INDEX idx_scan_jobs_status_submitted ON scan_jobs (status, submitted_at DESC);

CREATE TABLE auth_tokens (
  token_id TEXT PRIMARY KEY NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL,
  role TEXT NOT NULL,
  created_at TEXT NOT NULL,
  disabled_at TEXT,
  note TEXT,
  token_prefix TEXT,
  last_used_at TEXT,
  replaced_by_token_id TEXT
);

CREATE INDEX idx_auth_tokens_hash ON auth_tokens(token_hash);
