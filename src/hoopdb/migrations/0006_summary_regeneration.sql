-- Summary regeneration, push A (self-host Bob Costas): the session_summaries coverage and retry columns.
-- Nullable or constant-defaulted, no CHECK and no backfill, so no table rewrite or validation scan.
-- Every reader and writer is behind default-off flags and feature-detects these columns. The audit table
-- sessions.summary_regeneration_audit is created once, by upstream 20261105062040, on the same database;
-- this step does not depend on it (a 96bulls lane may complete before a later upstream prefix is applied),
-- and the audit writer feature-detects the table, so the two lanes never drift or block each other.
ALTER TABLE sessions.session_summaries
 ADD COLUMN covered_turns integer,
 ADD COLUMN failure_code text,
 ADD COLUMN degraded boolean NOT NULL DEFAULT false,
 ADD COLUMN attempts smallint NOT NULL DEFAULT 0,
 ADD COLUMN next_retry_at timestamptz,
 ADD COLUMN config_hash text,
 ADD COLUMN pipeline_version text;
