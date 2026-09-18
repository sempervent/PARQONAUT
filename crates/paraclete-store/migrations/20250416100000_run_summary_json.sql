-- Denormalized scan summary for read-heavy APIs without loading full report_json.
ALTER TABLE scan_runs ADD COLUMN summary_json TEXT NOT NULL DEFAULT '{}';
