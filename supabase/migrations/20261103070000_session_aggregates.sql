-- 20261103070000_session_aggregates: compact aggregate metadata + read RPCs.
--
-- Supabase carries pointers and compact metadata only; transcript bytes live in
-- S3 and text search in ClickHouse. The session-output/activity answers therefore
-- read DERIVED candidates published from the canonical S3 fold — never
-- convergence_events/conversation_turns rows (those stay empty here).
--
-- Candidates are the minimal facts the oracle needs, and nothing else:
--   assistant_message  (source, session_id, dedup message_id, ts)
--   legacy_turn        (source, session_id, turn row id, ts)
--   explicit_commit    (sha, ts, normalized line counts, provenance method)
--   tool_commit        (sha, ts, normalized stats, provenance call key)
-- No content, record payloads, args, or transcript text crosses into Supabase.
--
-- Candidates are ALL-TIME for the scope (publication folds the whole canonical
-- fold, not the caller's window); window semantics live in the read.
--
-- Publication is gated: each published batch carries the source epoch and the
-- union content hash (CAS) it was folded from. session_output/session_activity
-- return 42501 when the tenant's window has no ready publication — a missing or
-- stale fold must never surface as false zeros.

CREATE TABLE sessions.session_aggregate_publications (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source_epoch timestamptz NOT NULL,
  union_cas text NOT NULL,
  published_at timestamptz NOT NULL DEFAULT now(),
  ready boolean NOT NULL DEFAULT false,
  superseded_by bigint REFERENCES sessions.session_aggregate_publications(id)
);
CREATE TABLE sessions.session_aggregate_candidates (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  candidate_type text NOT NULL CHECK (candidate_type IN
    ('assistant_message','legacy_turn','explicit_commit','tool_commit')),
  dedup_key text NOT NULL,
  ts timestamptz NOT NULL,
  -- Normalized counters only: added/removed/files for commits; empty otherwise.
  stats jsonb NOT NULL DEFAULT '{}'::jsonb,
  -- Fold provenance (method/call key/epoch), never payload text.
  provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
  publication_id bigint NOT NULL
    REFERENCES sessions.session_aggregate_publications(id) ON DELETE CASCADE,
  PRIMARY KEY (publication_id, candidate_type, dedup_key)
);

CREATE INDEX session_aggregate_candidates_scope
  ON sessions.session_aggregate_candidates (org_id, workspace_id, ts);
CREATE INDEX session_aggregate_publications_scope
  ON sessions.session_aggregate_publications (org_id, workspace_id, ready)
  WHERE ready AND superseded_by IS NULL;

-- The claim-bound readers bind the search-worker JWT via worker_scope (the
-- RHC/session-read-Worker transport), not the bulls96_read.* node settings.

CREATE FUNCTION sessions.session_output(
  p_workspace_id text, p_since timestamptz, p_until timestamptz
) RETURNS TABLE (
  agent_messages bigint, commits bigint,
  lines_added bigint, lines_removed bigint, commits_without_line_counts bigint
) LANGUAGE plpgsql SECURITY DEFINER
SET search_path = sessions, pg_catalog, public AS $fn$
DECLARE
  v_org text;
BEGIN
  SELECT s.org_id INTO v_org FROM sessions.session_directory_worker_scope(p_workspace_id) AS s;
  IF NOT EXISTS (SELECT 1 FROM sessions.session_aggregate_publications p
                  WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
                    AND p.ready AND p.superseded_by IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'session output: no ready aggregate publication for this workspace';
  END IF;
  RETURN QUERY
    -- Only the current ready publication counts; staged/superseded rows are
    -- invisible to the read (the EXISTS gate above is existence, not scoping).
    WITH scoped AS MATERIALIZED (
      SELECT c.* FROM sessions.session_aggregate_candidates c
      JOIN sessions.session_aggregate_publications p ON p.id = c.publication_id
      WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
        AND p.ready AND p.superseded_by IS NULL
        AND c.ts >= p_since AND c.ts <= p_until
    ), commits AS (
      -- One row per sha, explicit links/outcomes preferred over tool-derived;
      -- earliest ts wins inside a type (the oracle's prefix+min(ts) semantics).
      SELECT DISTINCT ON (dedup_key) dedup_key, stats
      FROM scoped WHERE candidate_type IN ('explicit_commit','tool_commit')
      ORDER BY dedup_key,
        CASE candidate_type WHEN 'explicit_commit' THEN 0 ELSE 1 END, ts
    )
    SELECT
      (count(*) FILTER (WHERE candidate_type = 'assistant_message')
       -- Legacy turns count only when their session produced no in-window
       -- assistant message (the oracle's window-scoped suppression — the fold
       -- itself is all-time, so this cannot live in deriveCandidates).
       + count(*) FILTER (WHERE candidate_type = 'legacy_turn' AND NOT EXISTS (
           SELECT 1 FROM scoped m
            WHERE m.candidate_type = 'assistant_message'
              AND m.source = scoped.source AND m.session_id = scoped.session_id)))::bigint,
      (SELECT count(*) FROM commits)::bigint,
      (SELECT COALESCE(sum((stats->>'added')::bigint), 0) FROM commits WHERE stats ? 'added')::bigint,
      (SELECT COALESCE(sum((stats->>'removed')::bigint), 0) FROM commits WHERE stats ? 'removed')::bigint,
      (SELECT count(*) FROM commits WHERE NOT (stats ? 'added' AND stats ? 'removed'))::bigint
    FROM scoped;
END
$fn$;

CREATE FUNCTION sessions.session_activity(
  p_workspace_id text, p_since timestamptz, p_until timestamptz
) RETURNS TABLE (day text, source text, sessions bigint)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = sessions, pg_catalog, public AS $fn$
DECLARE
  v_org text;
BEGIN
  SELECT s.org_id INTO v_org FROM sessions.session_directory_worker_scope(p_workspace_id) AS s;
  IF NOT EXISTS (SELECT 1 FROM sessions.session_aggregate_publications p
                  WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
                    AND p.ready AND p.superseded_by IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'session activity: no ready aggregate publication for this workspace';
  END IF;
  RETURN QUERY
    -- Compact metadata answers non-q activity: distinct sessions per UTC day and
    -- source from event_times (20261103010000), not transcript rows.
    SELECT to_char(e.ts AT TIME ZONE 'UTC', 'YYYY-MM-DD')::text,
           e.source, count(DISTINCT e.session_id)::bigint
    FROM sessions.session_directory_event_times e
    WHERE e.org_id = v_org AND e.workspace_id = p_workspace_id
      AND e.ts >= p_since AND e.ts <= p_until
    GROUP BY 1, e.source ORDER BY 1, e.source;
END
$fn$;

REVOKE ALL ON FUNCTION sessions.session_output(text, timestamptz, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_activity(text, timestamptz, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_output(text, timestamptz, timestamptz)
  TO session_directory_search_worker;
GRANT EXECUTE ON FUNCTION sessions.session_activity(text, timestamptz, timestamptz)
  TO session_directory_search_worker;
