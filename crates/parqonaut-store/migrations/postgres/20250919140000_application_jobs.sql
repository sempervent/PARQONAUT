ALTER TABLE scan_jobs ADD COLUMN IF NOT EXISTS job_kind TEXT NOT NULL DEFAULT 'scan';
ALTER TABLE scan_jobs ADD COLUMN IF NOT EXISTS payload_schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE scan_jobs ADD COLUMN IF NOT EXISTS cancel_requested INTEGER NOT NULL DEFAULT 0;
ALTER TABLE scan_jobs ADD COLUMN IF NOT EXISTS result_ref_json TEXT;

ALTER TABLE scan_jobs RENAME TO application_jobs;

DROP INDEX IF EXISTS idx_scan_jobs_status_submitted;
CREATE INDEX IF NOT EXISTS idx_application_jobs_status_submitted ON application_jobs (status, submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_application_jobs_kind_status ON application_jobs (job_kind, status, submitted_at DESC);
