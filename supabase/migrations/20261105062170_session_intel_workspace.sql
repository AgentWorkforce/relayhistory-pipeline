-- Slot 20261105062170 (PROVISIONAL: the lander assigns the slot). Session Intelligence M3-4a, the workspace view's
-- two read-API readers (docs/session-intel-workspace-api.md): GET /v1/session-intel/workspace reads the workspace's
-- open advisories and its sessions' current intent and decision titles through them. Enables nothing: the route is
-- off unless READ_API_SESSION_INTEL_ENABLED is exactly "true". Code only: applying this to any real database needs
-- Khaliq's separate, explicit OK.
--
-- Two SECURITY DEFINER functions, read-only (STABLE), search_path pinned, PUBLIC EXECUTE revoked, EXECUTE for
-- bulls96_read_api alone. Each refuses (42501) unless the org is the read API's request scope
-- (session_directory_read_api_scope, as coordination_advisories_pending_for_v1 enforces it). No table, index, grant
-- on a table, or change to an existing function: the 062000 advisories table and the 062080 and 062150 claims
-- tables are read as their owner, and the directory is read through its existing read-API functions.
-- No transcript text is returned: advisory rows hold ids, kinds, artifacts and timestamps only; claims return their
-- title (cut to 120 characters), never the statement or the evidence claim text.

-- The workspace's open advisories, one row per advisory id: an advisory is open while at least one of its
-- recipient rows is pending or delivered and younger than the 7-day retention. Acked rows of an open advisory
-- are kept for the per-recipient acked flags; expired rows are never read. Newest first (the earliest created_at
-- of its rows, then advisory_id), at most p_limit (1 to 100). pair is the distinct recipient and peer
-- sessions of its rows (two for a pair; at most 4), recipients one entry per live recipient row, evidence the
-- distinct entries of its rows (at most 20).
CREATE FUNCTION sessions.session_intel_workspace_alerts_v1(p_org_id text, p_workspace_id text, p_limit integer)
RETURNS TABLE (
  advisory_id text, urgency text, kind text, artifact text, pair jsonb, recipients jsonb,
  created_at timestamptz, delivered_at timestamptz, evidence jsonb
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $session_intel_workspace_alerts$
BEGIN
  IF sessions.session_directory_read_api_scope(p_workspace_id) IS DISTINCT FROM p_org_id THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session intelligence: org outside the read scope';
  END IF;
  IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session intelligence: limit must be 1 to 100';
  END IF;
  RETURN QUERY
    WITH live AS (
      SELECT a.advisory_id AS l_id, a.harness AS l_harness, a.session_id AS l_session, a.peer_harness AS l_peer_harness,
             a.peer_session_id AS l_peer_session, a.urgency AS l_urgency, a.kind AS l_kind, a.artifact AS l_artifact,
             a.evidence AS l_evidence, a.created_at AS l_created, a.delivered_at AS l_delivered, a.status AS l_status
        FROM sessions.coordination_advisories AS a
       WHERE a.org_id = p_org_id AND a.workspace_id = p_workspace_id
         AND a.status IN ('pending','delivered','acked') AND a.created_at >= now() - interval '7 days'
    ), open_ids AS (
      SELECT l.l_id AS o_id, min(l.l_created) AS o_created
        FROM live AS l
       GROUP BY l.l_id
      HAVING bool_or(l.l_status IN ('pending','delivered'))
       ORDER BY min(l.l_created) DESC, l.l_id
       LIMIT p_limit
    )
    SELECT o.o_id,
           CASE WHEN bool_or(l.l_urgency = 'high') THEN 'high' ELSE 'low' END,
           min(l.l_kind),
           min(l.l_artifact),
           (SELECT coalesce(jsonb_agg(jsonb_build_object('source', p.s_source, 'sessionId', p.s_session)
                                      ORDER BY p.s_source COLLATE "C", p.s_session COLLATE "C"), '[]'::jsonb)
              FROM (SELECT DISTINCT x.s_source, x.s_session
                      FROM live AS l2
                     CROSS JOIN LATERAL (VALUES (l2.l_harness, l2.l_session), (l2.l_peer_harness, l2.l_peer_session))
                           AS x(s_source, s_session)
                     WHERE l2.l_id = o.o_id
                     ORDER BY x.s_source, x.s_session
                     LIMIT 4) AS p),
           jsonb_agg(jsonb_build_object('source', l.l_harness, 'sessionId', l.l_session, 'acked', l.l_status = 'acked')
                     ORDER BY l.l_harness COLLATE "C", l.l_session COLLATE "C"),
           o.o_created,
           min(l.l_delivered),
           (SELECT coalesce(jsonb_agg(e.e_entry ORDER BY e.e_ts, e.e_session, e.e_event), '[]'::jsonb)
              FROM (SELECT DISTINCT ON (v.value ->> 'sessionId', v.value ->> 'eventId', v.value ->> 'kind')
                           jsonb_build_object('sessionId', v.value ->> 'sessionId', 'eventId', v.value ->> 'eventId',
                                              'ts', v.value ->> 'ts', 'kind', v.value ->> 'kind') AS e_entry,
                           v.value ->> 'ts' AS e_ts, v.value ->> 'sessionId' AS e_session, v.value ->> 'eventId' AS e_event
                      FROM live AS l3 CROSS JOIN LATERAL jsonb_array_elements(l3.l_evidence) AS v(value)
                     WHERE l3.l_id = o.o_id
                     ORDER BY v.value ->> 'sessionId', v.value ->> 'eventId', v.value ->> 'kind'
                     LIMIT 20) AS e)
      FROM open_ids AS o
      JOIN live AS l ON l.l_id = o.o_id
     GROUP BY o.o_id, o.o_created
     ORDER BY o.o_created DESC, o.o_id;
END
$session_intel_workspace_alerts$;

-- The current (not superseded) intent claims (062150 relayfacts_claims_v1) and decision records (062080
-- relayfacts_v1) whose evidence names one of the given sessions: per session at most 3 intents and 5 decisions,
-- newest first (updated_at desc, id). p_session_keys holds 1 to 300 distinct "<source>:<sessionId>" keys; the
-- source is a harness name (no colon), so the first colon splits it. Titles only, cut to 120 characters; at most
-- p_limit rows (1 to 2400).
CREATE FUNCTION sessions.session_intel_workspace_claims_v1(
  p_org_id text, p_workspace_id text, p_session_keys text[], p_limit integer
) RETURNS TABLE (source text, session_id text, kind text, title text, updated_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $session_intel_workspace_claims$
BEGIN
  IF sessions.session_directory_read_api_scope(p_workspace_id) IS DISTINCT FROM p_org_id THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session intelligence: org outside the read scope';
  END IF;
  IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 2400
     OR p_session_keys IS NULL OR coalesce(cardinality(p_session_keys), 0) NOT BETWEEN 1 AND 300
     OR EXISTS (SELECT 1 FROM unnest(p_session_keys) AS u(k)
                 WHERE k IS NULL OR char_length(k) > 545 OR k !~ '^[a-z][a-z0-9_-]{0,31}:[^\r\n]+$') THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session intelligence: 1 to 300 source:sessionId keys and limit 1 to 2400';
  END IF;
  RETURN QUERY
    WITH keys AS (
      SELECT DISTINCT split_part(u.k, ':', 1) AS k_source, substr(u.k, strpos(u.k, ':') + 1) AS k_session
        FROM unnest(p_session_keys) AS u(k)
    ), hits AS (
      SELECT k.k_source AS h_source, k.k_session AS h_session, 'intent'::text AS h_kind, c.id AS h_id,
             left(c.title, 120) AS h_title, c.updated_at AS h_updated
        FROM sessions.relayfacts_claims_v1 AS c
        JOIN sessions.relayfacts_claim_evidence_v1 AS e ON e.claim_id = c.id
        JOIN keys AS k ON k.k_source = e.source AND k.k_session = e.session_id
       WHERE c.org_id = p_org_id AND c.workspace_id = p_workspace_id AND c.kind = 'intent' AND c.status <> 'superseded'
       GROUP BY k.k_source, k.k_session, c.id, c.title, c.updated_at
      UNION ALL
      SELECT k.k_source, k.k_session, 'decision'::text, t.id, left(t.title, 120), t.updated_at
        FROM sessions.relayfacts_v1 AS t
        JOIN sessions.relayfacts_evidence_v1 AS e ON e.decision_id = t.id
        JOIN keys AS k ON k.k_source = e.source AND k.k_session = e.session_id
       WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.status <> 'superseded'
       GROUP BY k.k_source, k.k_session, t.id, t.title, t.updated_at
    ), ranked AS (
      SELECT h.*, row_number() OVER (PARTITION BY h.h_source, h.h_session, h.h_kind ORDER BY h.h_updated DESC, h.h_id) AS h_rank
        FROM hits AS h
    )
    SELECT r.h_source, r.h_session, r.h_kind, r.h_title, r.h_updated
      FROM ranked AS r
     WHERE r.h_rank <= CASE r.h_kind WHEN 'intent' THEN 3 ELSE 5 END
     ORDER BY r.h_source COLLATE "C", r.h_session COLLATE "C", r.h_kind, r.h_rank
     LIMIT p_limit;
END
$session_intel_workspace_claims$;

REVOKE ALL ON FUNCTION sessions.session_intel_workspace_alerts_v1(text,text,integer),
  sessions.session_intel_workspace_claims_v1(text,text,text[],integer)
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api, bulls96_session_ingest,
       bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_intel_workspace_alerts_v1(text,text,integer),
  sessions.session_intel_workspace_claims_v1(text,text,text[],integer)
  TO bulls96_read_api;
