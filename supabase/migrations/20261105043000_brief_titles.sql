-- Session-list brief titles off Neon (feature F2, registry slot 043000). Cloud's session
-- list reads each row's cached brief title. Today that is direct SQL on Neon
-- (sessions.session_analysis_requests JOIN session_analysis_jobs, kind = 'brief'); after
-- this, Cloud asks the 96Bulls read API (GET /v1/sessions/titles), which reads
-- sessions.session_briefs here. Two pieces:
--
--   1. Provenance on sessions.session_briefs: `origin` ('native' | 'neon_backfill') and
--      `legacy_job_id` (the Neon session_analysis_jobs.id a backfilled title came from).
--      `origin` is DERIVED by a BEFORE trigger, never trusted from a writer: a row is
--      'neon_backfill' exactly while its brief text carries the backfill engine version
--      ('neon-backfill:%'). So the native writer (src/session-brief/store.ts) is
--      unchanged: a native brief replaces the engine version and the row becomes 'native'
--      (legacy_job_id cleared); a quarantine clear drops the text and the row becomes
--      'native'; a failed native attempt keeps the previous text, so a backfilled title
--      keeps its provenance. The backfill (src/migration/neon-brief-titles) writes as the
--      owner and never overwrites a native title.
--      session_briefs_state_text (20261102020000) is restated to also admit a backfilled
--      brief without a state: legacy Neon briefs from prompts before session-summary-v3
--      have none.
--   2. sessions.session_brief_titles_for(workspace, harnesses[], session_ids[]): the titles
--      for 1 to 100 (harness, session id) pairs, for the read API role bulls96_read_api.
--      The tenant comes from sessions.session_directory_read_api_scope (20261105034000):
--      the caller must be bulls96_read_api with the verified assertion's tenant bound in
--      the transaction. A requested id matches a brief by its physical session id, or by
--      the logical id a live pointer of that brief carries (canonical_session_id), so a
--      directory (logical) id and a relayhistory (physical) id both resolve. Only sessions
--      with a live pointer return anything, the erasure rule of sessions.session_brief.
--
-- Every row keeps org_id and a time column (generated_at), so a per-plan TTL can be added
-- later. Additive except the restated CHECK. The ADD COLUMN default is a constant
-- (no rewrite); the CHECKs scan session_briefs once (bounded: one row per briefed session).
--
-- Code only; nothing here is applied by this repository. It changes the migration-set
-- generation, so it ships only in a reviewed generation window, after 040000-042000.
-- Down path: supabase/rollbacks/20261105043000_brief_titles.down.sql.

ALTER TABLE sessions.session_briefs
  ADD COLUMN origin text NOT NULL DEFAULT 'native',
  ADD COLUMN legacy_job_id text;
ALTER TABLE sessions.session_briefs
  ADD CONSTRAINT session_briefs_origin_valid CHECK (origin IN ('native', 'neon_backfill')),
  ADD CONSTRAINT session_briefs_legacy_job_valid
    CHECK (legacy_job_id IS NULL OR legacy_job_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
  ADD CONSTRAINT session_briefs_origin_provenance CHECK (
    (origin = 'neon_backfill') = (legacy_job_id IS NOT NULL) AND
    (origin = 'native' OR title IS NOT NULL));
ALTER TABLE sessions.session_briefs DROP CONSTRAINT session_briefs_state_text;
ALTER TABLE sessions.session_briefs ADD CONSTRAINT session_briefs_state_text CHECK (
  (title IS NOT NULL OR state IS NULL) AND
  (title IS NULL OR state IS NOT NULL OR prompt_version = 'session-summary-v2'
   OR origin = 'neon_backfill'));
COMMENT ON COLUMN sessions.session_briefs.origin IS
  'Derived (session_briefs_provenance): neon_backfill while the brief text is a Neon backfill (engine_version neon-backfill:*), else native.';
COMMENT ON COLUMN sessions.session_briefs.legacy_job_id IS
  'The Neon sessions.session_analysis_jobs.id of a backfilled brief; NULL for a native one.';

-- Runs before the CHECKs, so a writer cannot mislabel a row: the label follows the text.
CREATE FUNCTION sessions.session_briefs_provenance()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $provenance$
BEGIN
  IF NEW.title IS NOT NULL AND NEW.engine_version LIKE 'neon-backfill:%' THEN
    NEW.origin := 'neon_backfill';
  ELSE
    NEW.origin := 'native';
    NEW.legacy_job_id := NULL;
  END IF;
  RETURN NEW;
END
$provenance$;

CREATE TRIGGER session_briefs_provenance
  BEFORE INSERT OR UPDATE ON sessions.session_briefs
  FOR EACH ROW EXECUTE FUNCTION sessions.session_briefs_provenance();

-- The cached brief title per requested (harness, session id), for the read API's
-- GET /v1/sessions/titles. Zero rows for a key: no title (none generated, none
-- backfilled, or no live pointer). Errors:
--   42501  caller is not bulls96_read_api; or the tenant binding is missing or names
--          another workspace (session_directory_read_api_scope)
--   22023  invalid arguments
CREATE FUNCTION sessions.session_brief_titles_for(
  p_workspace_id text, p_harnesses text[], p_session_ids text[]
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text,
  title text, origin text, generated_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $titles$
DECLARE
  v_org text;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'brief titles: workspace is required';
  END IF;
  -- Role guard and tenant pin; refuses (42501) before any value is read.
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);
  IF p_harnesses IS NULL OR p_session_ids IS NULL
     OR cardinality(p_session_ids) NOT BETWEEN 1 AND 100
     OR cardinality(p_harnesses) <> cardinality(p_session_ids)
     OR EXISTS (SELECT 1 FROM unnest(p_harnesses, p_session_ids) AS k(h, s)
                 WHERE h IS NULL OR h !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
                    OR s IS NULL OR char_length(s) NOT BETWEEN 1 AND 255) THEN
    RAISE EXCEPTION USING ERRCODE = '22023',
      MESSAGE = 'brief titles: session keys must be 1 to 100 (harness, session id) pairs';
  END IF;
  RETURN QUERY
    WITH wanted AS (
      SELECT DISTINCT k.h AS harness, k.s AS session_id
        FROM unnest(p_harnesses, p_session_ids) AS k(h, s)
    )
    SELECT v_org, p_workspace_id, w.harness, w.session_id, hit.title, hit.origin, hit.generated_at
      FROM wanted AS w
      CROSS JOIN LATERAL (
        -- Candidate physical ids, both index-backed: the id itself when it has a live
        -- pointer (session_index primary key), and every physical id whose live pointer
        -- names it as its logical id (session_index_canonical_live).
        SELECT b.title, b.origin, b.generated_at, b.session_id
          FROM (
            SELECT p.session_id FROM sessions.session_index AS p
             WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
               AND p.harness = w.harness AND p.session_id = w.session_id
               AND p.deleted_at IS NULL
            UNION
            SELECT p.session_id FROM sessions.session_index AS p
             WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
               AND p.harness = w.harness AND p.canonical_session_id = w.session_id
               AND p.deleted_at IS NULL AND p.canonical_session_id IS NOT NULL
          ) AS c
          JOIN sessions.session_briefs AS b
            ON b.org_id = v_org AND b.workspace_id = p_workspace_id
           AND b.harness = w.harness AND b.session_id = c.session_id
         WHERE b.title IS NOT NULL
         -- The exact physical id first, then the newest brief among the logical id's.
         ORDER BY (b.session_id = w.session_id) DESC, b.generated_at DESC, b.session_id
         LIMIT 1
      ) AS hit
     ORDER BY w.harness, w.session_id;
END
$titles$;

REVOKE ALL ON FUNCTION sessions.session_briefs_provenance()
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_brief_titles_for(text, text[], text[])
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_brief_titles_for(text, text[], text[])
  TO bulls96_read_api;

-- As in 20261105034000: strip EXECUTE from every grantee present at creation (default
-- privileges, a pre-existing runtime role), whatever the order. A grant added later is
-- still refused at call time by the role guard in the scope helper.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.session_briefs_provenance()'::regprocedure,
    'sessions.session_brief_titles_for(text, text[], text[])'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'bulls96_read_api'
                  AND v_fn = 'sessions.session_brief_titles_for(text, text[], text[])'::regprocedure)
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
