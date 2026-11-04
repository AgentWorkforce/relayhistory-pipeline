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
  id bigint GENERATED ALWAYS AS IDENTITY,
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
  PRIMARY KEY (id)
);
-- Message and turn rows dedup by their dedup key; commit candidates pool every
-- row per sha (the read's GROUP sha min(ts)/max stats needs link+outcome+tool
-- rows for the same sha to all arrive).
CREATE UNIQUE INDEX session_aggregate_candidates_message_dedup
  ON sessions.session_aggregate_candidates (publication_id, candidate_type, dedup_key)
  WHERE candidate_type IN ('assistant_message', 'legacy_turn');

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
    -- Oracle-exact over the compact publication: explicit links/outcomes pool
    -- all-history; tool-derived candidates are the publisher's windowed set
    -- (fold supplies every tool_commit row it derived; the read re-applies the
    -- window so published rows outside it never count). Short-sha prefixes
    -- canonicalize against the same-window unique 40-char set.
    WITH scoped AS MATERIALIZED (
      SELECT c.* FROM sessions.session_aggregate_candidates c
      JOIN sessions.session_aggregate_publications p ON p.id = c.publication_id
      WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
        AND p.ready AND p.superseded_by IS NULL
    ), msg AS (
      SELECT DISTINCT source, session_id, dedup_key FROM scoped
      WHERE candidate_type = 'assistant_message' AND ts >= p_since AND ts <= p_until
    ), legacy AS (
      SELECT c.dedup_key FROM scoped c
      WHERE c.candidate_type = 'legacy_turn' AND c.ts >= p_since AND c.ts <= p_until
        AND NOT EXISTS (SELECT 1 FROM msg m WHERE m.source = c.source AND m.session_id = c.session_id)
    ), commit_candidates AS (
      SELECT dedup_key AS sha, ts, stats FROM scoped WHERE candidate_type = 'explicit_commit'
      UNION ALL
      SELECT dedup_key, ts, stats FROM scoped
      WHERE candidate_type = 'tool_commit' AND ts >= p_since AND ts <= p_until
    ), commit_hashes AS MATERIALIZED (
      SELECT DISTINCT sha FROM commit_candidates
    ), short_lengths AS (
      SELECT DISTINCT length(sha) AS size FROM commit_hashes WHERE length(sha) BETWEEN 1 AND 39
    ), unique_prefixes AS MATERIALIZED (
      SELECT left(full_id.sha, short.size) AS prefix, min(full_id.sha) AS sha
      FROM commit_hashes full_id CROSS JOIN short_lengths short
      WHERE length(full_id.sha) = 40
      GROUP BY left(full_id.sha, short.size) HAVING count(*) = 1
    ), canonical_commits AS (
      SELECT COALESCE(prefix.sha, c.sha) AS sha, c.ts, c.stats
      FROM commit_candidates c LEFT JOIN unique_prefixes prefix ON prefix.prefix = c.sha
    ), stat_entries AS (
      SELECT sha, ts, CASE
        WHEN jsonb_typeof(stats) = 'array' THEN stats
        WHEN jsonb_typeof(stats) = 'object' AND EXISTS (
          SELECT 1 FROM jsonb_each(stats) field
          WHERE field.key IN ('added', 'deleted', 'removed', 'additions', 'deletions')
            AND jsonb_typeof(field.value) NOT IN ('object', 'array')
        ) THEN jsonb_build_array(stats)
        WHEN jsonb_typeof(stats) = 'object' THEN (SELECT jsonb_agg(value) FROM jsonb_each(stats))
      END AS entries
      FROM canonical_commits
    ), normalized_stats AS (
      SELECT sha, ts,
        CASE WHEN entries IS NOT NULL THEN (
          SELECT CASE WHEN COALESCE(bool_and(COALESCE(COALESCE(value->>'additions', value->>'added') ~ '^[0-9]{1,12}$' OR COALESCE(value->>'additions', value->>'added') = '-' OR (jsonb_typeof(value->'path') = 'string' AND value->'additions' = 'null'::jsonb AND value->'deletions' = 'null'::jsonb), false)), true)
            THEN COALESCE(sum(CASE WHEN COALESCE(value->>'additions', value->>'added') ~ '^[0-9]{1,12}$' THEN COALESCE(value->>'additions', value->>'added')::bigint ELSE 0 END), 0) END
          FROM jsonb_array_elements(entries)
        ) END AS added,
        CASE WHEN entries IS NOT NULL THEN (
          SELECT CASE WHEN COALESCE(bool_and(COALESCE(COALESCE(value->>'deletions', value->>'removed', value->>'deleted') ~ '^[0-9]{1,12}$' OR COALESCE(value->>'deletions', value->>'removed', value->>'deleted') = '-' OR (jsonb_typeof(value->'path') = 'string' AND value->'additions' = 'null'::jsonb AND value->'deletions' = 'null'::jsonb), false)), true)
            THEN COALESCE(sum(CASE WHEN COALESCE(value->>'deletions', value->>'removed', value->>'deleted') ~ '^[0-9]{1,12}$' THEN COALESCE(value->>'deletions', value->>'removed', value->>'deleted')::bigint ELSE 0 END), 0) END
          FROM jsonb_array_elements(entries)
        ) END AS removed
      FROM stat_entries
    ), commits AS (
      SELECT sha, min(ts) AS ts, max(added) AS added, max(removed) AS removed
      FROM normalized_stats GROUP BY sha
    )
    SELECT ((SELECT count(*) FROM msg) + (SELECT count(*) FROM legacy))::bigint,
      count(*)::bigint, COALESCE(sum(added), 0)::bigint, COALESCE(sum(removed), 0)::bigint,
      count(*) FILTER (WHERE added IS NULL OR removed IS NULL)::bigint
    FROM commits WHERE ts >= p_since AND ts <= p_until;
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
