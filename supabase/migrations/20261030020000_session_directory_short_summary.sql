-- G6: a bounded, scrubbed short summary on the logical-session directory, and the
-- one tenant-bound search path over it.
--
-- Provenance of scope: the helm-chart-chief coordinator recorded the user's direct
-- approval to implement bounded short_summary work in its controlling session.
-- Draft only; applying or merging needs the user's OK on the exact head.
--
-- Long summaries and transcripts stay in object storage. No embeddings, pgvector,
-- tsvector/FTS, or project/model/reasoning search here: those are V2
-- (ClickHouse/ObsessionDB).
--
-- Invariants enforced here (the TypeScript writer in
-- src/hoopdb/session-directory-summary.ts enforces the same ones first):
--   - hard length bound: char_length(short_summary) <= 1000;
--   - a stored summary carries scrub provenance (ruleset, version), source and
--     generation time, all or none. The database checks those labels; only the
--     writer can actually run the scrubber, so it alone proves the text is scrubbed;
--   - a tombstoned row never holds a summary or a search document (trigger + CHECK).
--
-- Search: the summary is part of search_text. #38's metadata trigram index is not
-- tenant-scoped, so it is replaced by session_directory_tenant_search_trgm_idx
-- (org_id, workspace_id, search_text) via btree_gin, and the only search path is
-- sessions.search_session_directory: SECURITY DEFINER, fixed SQL, tenant taken from
-- the signed JWT claims, callable only by the NOLOGIN Worker role, which has no
-- table privileges and no BYPASSRLS. Direct search_text/short_summary reads by
-- `authenticated` are revoked so a PostgREST table ILIKE cannot bypass it.
--
-- Code only; nothing here is applied by this repository. The down path is
-- supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql.

CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;

ALTER TABLE sessions.session_directory
  ADD COLUMN short_summary text
    CONSTRAINT session_directory_short_summary_length
    CHECK (char_length(short_summary) BETWEEN 1 AND 1000),
  -- Which lib/scrub.ts rules the text went through (raw-chunks SCRUB_RULESET and
  -- SCRUB_RULESET_VERSION; the same pair the manifest records as raw.scrub).
  ADD COLUMN short_summary_scrub_ruleset text
    CONSTRAINT session_directory_short_summary_scrub_ruleset
    CHECK (short_summary_scrub_ruleset = 'relayhistory-scrub'),
  ADD COLUMN short_summary_scrub_version smallint
    CONSTRAINT session_directory_short_summary_scrub_version
    CHECK (short_summary_scrub_version >= 1),
  -- The producer and its version, e.g. 'bob-costas/extractive/v1' (the same shape as
  -- session_summaries.model_id and session_verdicts.model_id).
  ADD COLUMN short_summary_source text
    CONSTRAINT session_directory_short_summary_source
    CHECK (short_summary_source ~ '^[a-z0-9][a-z0-9._/-]{0,63}$'),
  ADD COLUMN short_summary_generated_at timestamptz;

ALTER TABLE sessions.session_directory
  ADD CONSTRAINT session_directory_short_summary_provenance CHECK (
    (short_summary IS NULL) = (short_summary_scrub_ruleset IS NULL) AND
    (short_summary IS NULL) = (short_summary_scrub_version IS NULL) AND
    (short_summary IS NULL) = (short_summary_source IS NULL) AND
    (short_summary IS NULL) = (short_summary_generated_at IS NULL)
  ),
  ADD CONSTRAINT session_directory_short_summary_not_on_tombstone CHECK (
    deleted_at IS NULL OR short_summary IS NULL
  );

COMMENT ON COLUMN sessions.session_directory.short_summary IS
  'Scrubbed short summary, at most 1000 characters. Long summaries and transcripts stay in object storage. Cleared on tombstone; removed with the row on erasure.';
COMMENT ON COLUMN sessions.session_directory.search_text IS
  'Search document built by sessions.session_directory_search_text: metadata plus the scrubbed short_summary. MUST NOT contain transcript or event content. Searched only through sessions.search_session_directory.';

-- The one definition of the search document. The summary writer uses it, and the
-- future projector must too, so short_summary is always part of search_text.
CREATE FUNCTION sessions.session_directory_search_text(
  p_title text, p_task_title text, p_agent_name text, p_project_id text,
  p_git_repo text, p_git_branch text, p_model text, p_short_summary text
) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, pg_temp AS $search_text$
  SELECT left(concat_ws(' ',
    nullif(btrim(p_title), ''), nullif(btrim(p_task_title), ''),
    nullif(btrim(p_agent_name), ''), nullif(btrim(p_project_id), ''),
    nullif(btrim(p_git_repo), ''), nullif(btrim(p_git_branch), ''),
    nullif(btrim(p_model), ''), nullif(btrim(p_short_summary), '')), 8192)
$search_text$;

-- Deletion propagation backstop: whatever writer sets deleted_at, the row keeps no
-- summary text, in the column or in the search document that embedded it.
CREATE FUNCTION sessions.session_directory_tombstone_clears_summary()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $tombstone$
BEGIN
  IF NEW.deleted_at IS NOT NULL THEN
    NEW.short_summary := NULL;
    NEW.short_summary_scrub_ruleset := NULL;
    NEW.short_summary_scrub_version := NULL;
    NEW.short_summary_source := NULL;
    NEW.short_summary_generated_at := NULL;
    NEW.search_text := '';
  END IF;
  RETURN NEW;
END
$tombstone$;

CREATE TRIGGER session_directory_tombstone_clears_summary
  BEFORE INSERT OR UPDATE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_tombstone_clears_summary();

-- Tenant-scoped search index. Replaces #38's session_directory_metadata_trgm_idx,
-- whose GIN over search_text alone mixed every tenant's text in one posting list.
DROP INDEX sessions.session_directory_metadata_trgm_idx;
CREATE INDEX session_directory_tenant_search_trgm_idx
  ON sessions.session_directory
  USING gin (org_id, workspace_id, search_text public.gin_trgm_ops)
  WHERE search_text <> '' AND deleted_at IS NULL;

-- The dedicated Worker identity. NOLOGIN: the authenticator/credential binding is a
-- separately reviewed deployment step. No table privileges, no BYPASSRLS; it can
-- only execute the search RPC. PostgREST (>= v12) applies role-level settings, so
-- the timeout bounds every request made as this role.
DO $role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'session_directory_search_worker') THEN
    CREATE ROLE session_directory_search_worker NOLOGIN NOINHERIT NOBYPASSRLS;
  END IF;
END
$role$;
ALTER ROLE session_directory_search_worker SET statement_timeout = '2s';
GRANT USAGE ON SCHEMA sessions TO session_directory_search_worker;

-- The only search path. Fixed SQL, fixed ORDER BY, fixed LIMIT 51 (a page of 50
-- plus one to signal more). The organization comes only from the signed claims;
-- the workspace must be one the claims list. Errors:
--   42501  claims missing, malformed, for another role, or not covering the workspace
--   22023  query shorter than 3 or longer than 200 characters after trim + NFC
CREATE FUNCTION sessions.search_session_directory(p_workspace_id text, p_query text)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text,
  created_at timestamptz, last_activity_at timestamptz,
  title text, task_title text, agent_name text, project_id text,
  git_repo text, git_branch text, model text, state text,
  event_count bigint, input_tokens bigint, output_tokens bigint, cost_usd_micros bigint,
  machine_ids text[], user_ids text[], short_summary text, projected_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $search$
DECLARE
  v_claims jsonb;
  v_org text;
  v_workspaces text[];
  v_query text;
BEGIN
  BEGIN
    v_claims := nullif(current_setting('request.jwt.claims', true), '')::jsonb;
  EXCEPTION WHEN others THEN
    v_claims := NULL;
  END;
  IF v_claims IS NULL OR jsonb_typeof(v_claims) <> 'object'
     OR (v_claims ->> 'role') IS DISTINCT FROM 'session_directory_search_worker'
     OR jsonb_typeof(v_claims -> 'app_metadata') IS DISTINCT FROM 'object'
     OR jsonb_typeof(v_claims -> 'app_metadata' -> 'org_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(v_claims -> 'app_metadata' -> 'workspace_ids') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session search: tenant claims missing or invalid';
  END IF;
  v_org := v_claims -> 'app_metadata' ->> 'org_id';
  SELECT coalesce(array_agg(w.value #>> '{}'), '{}') INTO v_workspaces
    FROM jsonb_array_elements(v_claims -> 'app_metadata' -> 'workspace_ids') AS w
   WHERE jsonb_typeof(w.value) = 'string';
  IF v_org = '' OR p_workspace_id IS NULL OR NOT (p_workspace_id = ANY (v_workspaces)) THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session search: tenant claims missing or invalid';
  END IF;

  v_query := normalize(btrim(coalesce(p_query, '')), NFC);
  IF char_length(v_query) < 3 OR char_length(v_query) > 200 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session search: query must be 3 to 200 characters';
  END IF;

  RETURN QUERY EXECUTE $sql$
SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.created_at, d.last_activity_at,
       d.title, d.task_title, d.agent_name, d.project_id, d.git_repo, d.git_branch, d.model, d.state,
       d.event_count, d.input_tokens, d.output_tokens, d.cost_usd_micros,
       d.machine_ids, d.user_ids, d.short_summary, d.projected_at
  FROM sessions.session_directory AS d
 WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.deleted_at IS NULL
   AND d.search_text <> '' AND d.search_text ILIKE $3 ESCAPE '\'
 ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC
 LIMIT 51$sql$
  USING v_org, p_workspace_id,
        '%' || replace(replace(replace(v_query, '\', '\\'), '%', '\%'), '_', '\_') || '%';
END
$search$;

REVOKE ALL ON FUNCTION sessions.search_session_directory(text, text)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.search_session_directory(text, text)
  TO session_directory_search_worker;

-- No direct search by end-user tokens: `authenticated` keeps RLS-scoped reads of the
-- listing columns but not search_text or short_summary, so a PostgREST
-- `search_text=ilike.*q*` table filter is refused instead of sequentially scanning.
REVOKE SELECT ON sessions.session_directory FROM authenticated;
GRANT SELECT (
  org_id, workspace_id, harness, session_id, created_at, last_activity_at,
  title, task_title, agent_name, agent_name_normalized, project_id, git_repo, git_branch,
  model, state, event_count, input_tokens, output_tokens, cost_usd_micros,
  machine_ids, user_ids, projection_version, projected_at, deleted_at
) ON sessions.session_directory TO authenticated;

REVOKE ALL ON FUNCTION sessions.session_directory_search_text(
  text, text, text, text, text, text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_directory_tombstone_clears_summary() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.session_directory_search_text(
  text, text, text, text, text, text, text, text) TO service_role;
