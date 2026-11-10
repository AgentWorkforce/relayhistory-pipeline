-- Remove the read surface and recorded-title storage; generated briefs and pointers
-- are retained. Recorded fallback display copies are cleared, not kept without source
-- provenance; ordinary pointer/model projection may rebuild authoritative metadata.
-- Guard, function removal and ledger removal are one transaction under the
-- migrator lock. On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996, 1);
DO $rollback_guard$
DECLARE v_sha text;
BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief summary: migration ledger required';
 END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations
   WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105044100_session_brief_summary_read.sql') THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief summary: a later migration is applied; roll it back first';
 END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations
  WHERE name='upstream:20261105044100_session_brief_summary_read.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'7a437ddba3600f2261f5436caa78f4daa8312f95298b58720c94638dfb038ccd' THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief summary: own applied checksum required';
 END IF;
END
$rollback_guard$;
DROP FUNCTION sessions.session_brief_summary_for(text,text,text);
DROP TRIGGER session_directory_recorded_title_invalidate ON sessions.session_directory;
DROP FUNCTION sessions.session_directory_recorded_title_invalidate();
UPDATE sessions.session_directory AS d
 SET title=NULL, search_text=sessions.session_directory_search_text(
   NULL,d.task_title,d.agent_name,d.project_id,d.git_repo,d.git_branch,d.model,d.short_summary)
 WHERE d.recorded_catalog_title IS NOT NULL AND d.title IS NOT DISTINCT FROM d.recorded_catalog_title;
ALTER TABLE sessions.session_directory DROP COLUMN recorded_catalog_title;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105044100_session_brief_summary_read.sql';
COMMIT;
