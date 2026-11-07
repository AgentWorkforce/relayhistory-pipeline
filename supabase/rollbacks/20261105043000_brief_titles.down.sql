-- Down path for supabase/migrations/20261105043000_brief_titles.sql.
-- Drops session_brief_titles_for, the provenance trigger and its function, and the two
-- columns, and restores 20261102000000's session_briefs_title_check and
-- session_briefs_text_provenance and 20261102020000's session_briefs_state_text. Revert the code first:
-- the read API's GET /v1/sessions/titles answers 501 brief_titles_not_migrated once the
-- function is gone. Exercised by src/session-brief/__tests__/brief-titles.test.ts (up,
-- down, up). Code only: running it against a hosted database needs the user's OK.
--
-- Refused (55000), before anything is dropped:
--   - while a later migration is applied (roll it back first), as 20261105034000's down;
--   - while any backfilled brief remains. Dropping `origin` would make a Neon title
--     indistinguishable from a native one, and a legacy row without a state would violate
--     the restored CHECK. Delete them first, deliberately:
--       DELETE FROM sessions.session_briefs WHERE origin = 'neon_backfill';
--
-- Like 20261105034000's named down, it removes this file's ledger row so the next migrate
-- reapplies it cleanly. No explicit BEGIN/COMMIT, as in the sibling window downs: run it as
-- one transaction (psql -1 -v ON_ERROR_STOP=1).
DO $rollback_guard$ DECLARE v_later boolean; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C" > ''upstream:20261105043000_brief_titles.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'brief titles: a later migration is applied; roll it back first'; END IF;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'sessions.session_briefs'::regclass AND attname = 'origin' AND NOT attisdropped) THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.session_briefs WHERE origin = ''neon_backfill'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'brief titles: backfilled briefs remain; delete them first'; END IF;
 END IF;
END $rollback_guard$;
DROP FUNCTION IF EXISTS sessions.session_brief_titles_for(text, text[], text[]);
DROP TRIGGER IF EXISTS session_briefs_provenance ON sessions.session_briefs;
DROP FUNCTION IF EXISTS sessions.session_briefs_provenance();
ALTER TABLE sessions.session_briefs DROP CONSTRAINT IF EXISTS session_briefs_text_provenance;
ALTER TABLE sessions.session_briefs ADD CONSTRAINT session_briefs_text_provenance CHECK (
  (title IS NULL) = (description IS NULL) AND
  (title IS NULL) = (brief_revision IS NULL) AND
  (title IS NULL) = (brief_source_revisions IS NULL) AND
  (title IS NULL) = (brief_fingerprint IS NULL) AND
  (title IS NULL) = (scrub_ruleset IS NULL) AND
  (title IS NULL) = (scrub_version IS NULL) AND
  (title IS NULL) = (prompt_version IS NULL) AND
  (title IS NULL) = (engine_version IS NULL) AND
  (title IS NULL) = (model IS NULL) AND
  (title IS NULL) = (generated_at IS NULL)
);
ALTER TABLE sessions.session_briefs DROP CONSTRAINT IF EXISTS session_briefs_title_check;
ALTER TABLE sessions.session_briefs ADD CONSTRAINT session_briefs_title_check CHECK (
  char_length(title) BETWEEN 1 AND 90 AND title !~ '[\r\n]');
ALTER TABLE sessions.session_briefs DROP CONSTRAINT IF EXISTS session_briefs_state_text;
ALTER TABLE sessions.session_briefs ADD CONSTRAINT session_briefs_state_text CHECK (
  (title IS NOT NULL OR state IS NULL) AND
  (title IS NULL OR state IS NOT NULL OR prompt_version = 'session-summary-v2'));
ALTER TABLE sessions.session_briefs
  DROP CONSTRAINT IF EXISTS session_briefs_origin_provenance,
  DROP CONSTRAINT IF EXISTS session_briefs_legacy_job_valid,
  DROP CONSTRAINT IF EXISTS session_briefs_origin_valid,
  DROP COLUMN IF EXISTS legacy_job_id,
  DROP COLUMN IF EXISTS origin;
DO $ledger$ BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105043000_brief_titles.sql';
 END IF;
END $ledger$;
