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
--      Three CHECKs are restated to admit every title Cloud shows today (parity): for a
--      backfilled row only, the state may be NULL (prompts before session-summary-v3 have
--      none; session_briefs_state_text), the description may be NULL (a title-only legacy
--      brief; session_briefs_text_provenance), and the title may carry a line break
--      (session_briefs_title_check; the 1-90 bound stays: relayhistory's brief schema had
--      maxLength 90). Native rows keep exactly the 20261102000000/020000 rules.
--   2. sessions.session_brief (relayhistory-cloud's /brief) is contained: a backfilled brief
--      reads as no brief there (26000's body plus one IF block).
--   3. sessions.session_brief_titles_for(workspace, harnesses[], session_ids[]): the titles
--      for 1 to 100 (harness, session id) pairs, for the read API role bulls96_read_api.
--      The tenant comes from sessions.session_directory_read_api_scope (20261105034000):
--      the caller must be bulls96_read_api with the verified assertion's tenant bound in
--      the transaction. A requested id is the RelayHistory (logical) session id, which is
--      coalesce(canonical_session_id, session_id) of a live pointer (20261102010000: a NULL
--      canonical id means session_id is that id; an object-layout-encoded pointer carries it
--      in canonical_session_id). The brief of any physical session resolving to it counts;
--      a native brief wins over a backfilled one, then the newest. Titles are trimmed and a
--      blank one is never served. Only sessions with a live pointer return anything, the
--      erasure rule of sessions.session_brief. The backfill resolves keys the same way.
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
-- 20261102000000's column CHECK on title, plus: a backfilled title may hold a line break.
ALTER TABLE sessions.session_briefs DROP CONSTRAINT session_briefs_title_check;
ALTER TABLE sessions.session_briefs ADD CONSTRAINT session_briefs_title_check CHECK (
  char_length(title) BETWEEN 1 AND 90 AND (title !~ '[\r\n]' OR origin = 'neon_backfill'));
-- 20261102000000's all-or-nothing brief, plus: a backfilled brief may have no description.
ALTER TABLE sessions.session_briefs DROP CONSTRAINT session_briefs_text_provenance;
ALTER TABLE sessions.session_briefs ADD CONSTRAINT session_briefs_text_provenance CHECK (
  ((title IS NULL) = (description IS NULL) OR (origin = 'neon_backfill' AND description IS NULL)) AND
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

-- sessions.session_brief (last replaced by 20261105026000), contained: a backfilled brief
-- reads as no brief there, so relayhistory-cloud's GET /v1/sessions/:id/brief never serves a
-- Neon title, description-less brief or line-break title. Body: 026000's, plus the one
-- IF block after the brief row is read. CREATE OR REPLACE keeps the function's ACL.
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
  -- F2 containment (20261105043000): a Neon-backfilled brief (engine_version 'neon-backfill:*',
  -- origin 'neon_backfill') is served only to Cloud's session list through
  -- sessions.session_brief_titles_for, never as this session's brief: it reads as no brief.
  IF b.title IS NOT NULL AND b.engine_version LIKE 'neon-backfill:%' THEN
    b.title := NULL; b.description := NULL; b.state := NULL; b.brief_revision := NULL;
    b.brief_source_revisions := NULL; b.prompt_version := NULL; b.engine_version := NULL;
    b.model := NULL; b.generated_at := NULL;
  END IF;
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
        -- The physical sessions whose live pointer resolves to the requested logical id,
        -- both index-backed: an unencoded pointer (session_id is the id, canonical NULL;
        -- session_index primary key) and an encoded one (session_index_canonical_live).
        SELECT btrim(b.title) AS title, b.origin, b.generated_at
          FROM (
            SELECT p.session_id FROM sessions.session_index AS p
             WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
               AND p.harness = w.harness AND p.session_id = w.session_id
               AND p.canonical_session_id IS NULL AND p.deleted_at IS NULL
            UNION
            SELECT p.session_id FROM sessions.session_index AS p
             WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
               AND p.harness = w.harness AND p.canonical_session_id = w.session_id
               AND p.deleted_at IS NULL
          ) AS c
          JOIN sessions.session_briefs AS b
            ON b.org_id = v_org AND b.workspace_id = p_workspace_id
           AND b.harness = w.harness AND b.session_id = c.session_id
         WHERE b.title IS NOT NULL AND btrim(b.title) <> ''
         -- A native brief over a backfilled one, then the newest: a stale backfill on one
         -- physical id never hides the current native title on another.
         ORDER BY (b.origin = 'native') DESC, b.generated_at DESC, b.session_id
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
