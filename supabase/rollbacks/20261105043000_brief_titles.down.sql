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
-- Restore 20261105026000's session_brief body exactly (CREATE OR REPLACE keeps its ACL).
CREATE OR REPLACE FUNCTION sessions.session_brief(
  p_workspace_id text, p_harness text, p_session_id text
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text,
  status text, stale boolean, title text, description text, state text,
  revision bigint, current_revision bigint, prompt_version text, engine_version text,
  model text, message_count integer, shortened_messages integer,
  input_tokens bigint, output_tokens bigint, error text,
  generated_at timestamptz, attempted_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $brief$
DECLARE
  v_org text;
  v_live jsonb;
  v_current bigint;
  v_queued boolean;
  b sessions.session_briefs%ROWTYPE;
  v_fresh boolean;
  v_status text;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session brief: workspace is required';
  END IF;
  SELECT s.org_id INTO v_org FROM sessions.session_directory_worker_scope(p_workspace_id) AS s;
  IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session brief: harness is invalid';
  END IF;
  IF p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session brief: session id is invalid';
  END IF;
  SELECT jsonb_object_agg(p.machine_id, p.revision), max(p.revision)
    INTO v_live, v_current
    FROM sessions.session_index AS p
   WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
     AND p.harness = p_harness AND p.session_id = p_session_id
     AND p.deleted_at IS NULL;
  IF v_live IS NULL THEN
    RETURN;
  END IF;
  -- Raw revision equality cannot attest a globally moved record's brief.
  -- Preserve absent/unmanaged behavior; managed text requires qualified input.
  IF sessions.ingest_history_live()AND EXISTS(SELECT 1 FROM sessions.ingest_history_scope_current r WHERE r.org_id=v_org AND r.workspace_id=p_workspace_id AND r.harness=p_harness
    AND(r.session_id=p_session_id OR r.session_id IN(SELECT coalesce(i.canonical_session_id,i.session_id)FROM sessions.session_index i
      WHERE i.org_id=v_org AND i.workspace_id=p_workspace_id AND i.harness=p_harness AND i.session_id=p_session_id)))THEN
   RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='history: qualified brief metadata not ready';END IF;
  SELECT * INTO b FROM sessions.session_briefs AS x
   WHERE x.org_id = v_org AND x.workspace_id = p_workspace_id
     AND x.harness = p_harness AND x.session_id = p_session_id;
  v_queued := EXISTS (SELECT 1 FROM sessions.session_brief_wakeups AS w
                       WHERE w.org_id = v_org AND w.workspace_id = p_workspace_id
                         AND w.harness = p_harness AND w.session_id = p_session_id);
  v_fresh := b.title IS NOT NULL AND b.brief_source_revisions = v_live;
  v_status := CASE
    WHEN v_fresh THEN 'ready'
    -- A rate-limited wakeup waits for the UTC reset: it is not in flight.
    WHEN b.status = 'rate_limited' THEN 'rate_limited'
    WHEN v_queued THEN CASE WHEN b.title IS NULL THEN 'pending' ELSE 'refreshing' END
    WHEN b.status IN ('failed', 'ineligible', 'quarantined', 'empty') THEN 'failed'
    WHEN b.title IS NOT NULL THEN 'stale'
    ELSE 'missing' END;
  org_id := v_org;
  workspace_id := p_workspace_id;
  harness := p_harness;
  session_id := p_session_id;
  status := v_status;
  stale := b.title IS NOT NULL AND NOT v_fresh;
  title := b.title;
  description := b.description;
  state := b.state;
  revision := b.brief_revision;
  current_revision := v_current;
  prompt_version := b.prompt_version;
  engine_version := b.engine_version;
  model := b.model;
  message_count := CASE WHEN b.title IS NOT NULL THEN b.message_count END;
  shortened_messages := CASE WHEN b.title IS NOT NULL THEN b.shortened_messages END;
  input_tokens := CASE WHEN b.title IS NOT NULL THEN b.input_tokens END;
  output_tokens := CASE WHEN b.title IS NOT NULL THEN b.output_tokens END;
  error := CASE WHEN v_fresh THEN NULL
                WHEN b.status = 'ineligible' THEN coalesce(b.error, 'session_analysis_ineligible')
                WHEN b.status = 'quarantined' THEN coalesce(b.error, 'session_quarantined')
                WHEN b.status = 'empty' THEN coalesce(b.error, 'session_empty')
                ELSE b.error END;
  generated_at := b.generated_at;
  attempted_at := b.attempted_at;
  RETURN NEXT;
END
$brief$;
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
