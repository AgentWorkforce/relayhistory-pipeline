-- Down path for supabase/migrations/20261030020000_session_directory_short_summary.sql.
--
-- Lives outside supabase/migrations/ on purpose: the tests apply every file in that
-- directory in name order, and a down file there would undo the up. Exercised by
-- src/hoopdb/__tests__/session-directory-short-summary.test.ts (up, down, up).
--
-- Rolling back removes every copy of summary text from the directory: search_text
-- is rebuilt without the summary before the columns are dropped. btree_gin is left
-- installed (CREATE EXTENSION IF NOT EXISTS in the up; other objects may use it),
-- matching how 20261030010000 treats pg_trgm.
--
-- Code only: running it against a hosted database needs Khaliq's go.

BEGIN;

DROP INDEX IF EXISTS sessions.session_directory_summary_trgm_idx;
DROP TRIGGER IF EXISTS session_directory_tombstone_clears_summary ON sessions.session_directory;
DROP FUNCTION IF EXISTS sessions.session_directory_tombstone_clears_summary();

UPDATE sessions.session_directory
   SET search_text = sessions.session_directory_search_text(
         title, task_title, agent_name, project_id, git_repo, git_branch, model, NULL)
 WHERE short_summary IS NOT NULL;

DROP FUNCTION IF EXISTS sessions.session_directory_search_text(
  text, text, text, text, text, text, text, text);

ALTER TABLE sessions.session_directory
  DROP CONSTRAINT IF EXISTS session_directory_short_summary_not_on_tombstone,
  DROP CONSTRAINT IF EXISTS session_directory_short_summary_provenance,
  DROP COLUMN IF EXISTS short_summary_generated_at,
  DROP COLUMN IF EXISTS short_summary_source,
  DROP COLUMN IF EXISTS short_summary_scrub_version,
  DROP COLUMN IF EXISTS short_summary_scrub_ruleset,
  DROP COLUMN IF EXISTS short_summary;

COMMENT ON COLUMN sessions.session_directory.search_text IS
  'Metadata-only search document; MUST NOT contain transcript or event content.';

COMMIT;
