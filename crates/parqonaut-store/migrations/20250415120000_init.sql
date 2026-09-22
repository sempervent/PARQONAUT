PRAGMA foreign_keys = ON;

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
    report_sha256 TEXT NOT NULL
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
