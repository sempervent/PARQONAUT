-- Lease, heartbeat, and retry metadata for stale-job recovery (Phase 8).

ALTER TABLE scan_jobs ADD COLUMN worker_id TEXT;
ALTER TABLE scan_jobs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE scan_jobs ADD COLUMN heartbeat_at TEXT;
ALTER TABLE scan_jobs ADD COLUMN leased_until TEXT;
ALTER TABLE scan_jobs ADD COLUMN recovery_note TEXT;
