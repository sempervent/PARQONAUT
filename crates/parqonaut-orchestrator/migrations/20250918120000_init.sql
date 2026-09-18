CREATE TABLE IF NOT EXISTS parqonaut_journal_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_runs (
    run_id TEXT PRIMARY KEY,
    batch_plan_id TEXT NOT NULL,
    config_fingerprint TEXT NOT NULL,
    batch_name TEXT NOT NULL,
    output_root TEXT NOT NULL,
    parqonaut_version TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    cancelled_at TEXT
);

CREATE TABLE IF NOT EXISTS dataset_runs (
    run_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    output_path TEXT NOT NULL,
    repair_plan_id TEXT NOT NULL,
    policy_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    source_fingerprint TEXT,
    output_fingerprint TEXT,
    error_class TEXT,
    error_message TEXT,
    started_at TEXT,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY (run_id, dataset_id),
    FOREIGN KEY (run_id) REFERENCES batch_runs(run_id)
);
