-- Slot 20261105062080. Relayfacts, slice 1: per-topic decision records with session evidence, and
-- their SECURITY DEFINER writers and readers (docs/relayfacts.md). Enables nothing: no extractor, no
-- route, no flag. Code only: applying this to any real database needs Khaliq's separate, explicit OK.
--
-- No transcript content. A record is a topic slug, a title (at most 200), a statement (at most 1000) and a
-- status. Evidence is a history source, a session id, a turn range, a claim (at most 300) and an optional
-- GitHub commit, pull or issue URL. No stored string holds a line break.
--
-- Deduplication: one record per (org, workspace, topic, title_key); title_key is the title lowercased,
-- whitespace collapsed and trailing punctuation removed. Inserting a known record adds only its new evidence.
-- A record holds at most 50 evidence rows.
--
-- Status: open | decided -> superseded (supersede only). A superseded record is never reopened.
--
-- Roles: the worker (service_role, and the runtime role through RUNTIME_APPLICATION_FUNCTIONS) gets insert
-- and supersede; the read API (bulls96_read_api) gets topics and list, pinned to its request scope
-- (session_directory_read_api_scope). Neither gets any table privilege.

CREATE TABLE sessions.relayfacts_v1 (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512),
  topic text NOT NULL CHECK (topic ~ '^[a-z0-9][a-z0-9-]{0,63}$'),
  title text NOT NULL CHECK (char_length(btrim(title)) BETWEEN 1 AND 200 AND title !~ '[\r\n]'),
  title_key text GENERATED ALWAYS AS (
    regexp_replace(regexp_replace(lower(btrim(title)), '\s+', ' ', 'g'), '[[:punct:][:space:]]+$', '')
  ) STORED,
  statement text NOT NULL CHECK (char_length(btrim(statement)) BETWEEN 1 AND 1000 AND statement !~ '[\r\n]'),
  status text NOT NULL DEFAULT 'decided' CHECK (status IN ('open','decided','superseded')),
  supersedes uuid REFERENCES sessions.relayfacts_v1 (id) ON DELETE SET NULL,
  decided_at timestamptz NOT NULL DEFAULT now(),
  implemented_at timestamptz,
  implemented_ref text CHECK (implemented_ref IS NULL OR (char_length(implemented_ref) BETWEEN 1 AND 500 AND implemented_ref !~ '[\r\n]')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (supersedes IS DISTINCT FROM id)
);
CREATE UNIQUE INDEX relayfacts_v1_dedupe ON sessions.relayfacts_v1 (org_id, workspace_id, topic, title_key);
-- The topic page (oldest first) and the topic list.
CREATE INDEX relayfacts_v1_topic ON sessions.relayfacts_v1 (org_id, workspace_id, topic, created_at, id);
CREATE INDEX relayfacts_v1_updated ON sessions.relayfacts_v1 (org_id, workspace_id, updated_at);

CREATE TABLE sessions.relayfacts_evidence_v1 (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  decision_id uuid NOT NULL REFERENCES sessions.relayfacts_v1 (id) ON DELETE CASCADE,
  source text NOT NULL CHECK (source ~ '^[a-z][a-z0-9_-]{0,31}$'),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 512 AND session_id !~ '[\r\n]'),
  turn_start integer NOT NULL CHECK (turn_start BETWEEN 0 AND 1000000),
  turn_end integer NOT NULL CHECK (turn_end BETWEEN 0 AND 1000000),
  claim text NOT NULL CHECK (char_length(btrim(claim)) BETWEEN 1 AND 300 AND claim !~ '[\r\n]'),
  ref text CHECK (ref IS NULL OR (char_length(ref) <= 500
    AND ref ~ '^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/(commit/[0-9a-f]{7,40}|pull/[0-9]{1,10}|issues/[0-9]{1,10})$')),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (turn_start <= turn_end)
);
CREATE UNIQUE INDEX relayfacts_evidence_v1_dedupe ON sessions.relayfacts_evidence_v1 (decision_id, source, session_id, turn_start, turn_end);

ALTER TABLE sessions.relayfacts_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.relayfacts_evidence_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.relayfacts_v1, sessions.relayfacts_evidence_v1
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;

-- Worker: insert or merge a batch of records for one workspace. p_records is a JSON array (at most 100) of
-- {topic, title, statement, status?, decided_at?, evidence: [{source, session_id, turn_start, turn_end,
-- claim, ref?}] (at most 10)}. status is open or decided (default decided). A record that already exists
-- in the same workspace (same topic and title_key) is kept as it is; only evidence it does not yet hold is
-- added, up to 50 rows per record, and then its updated_at moves. Returns counts only.
CREATE FUNCTION sessions.relayfacts_insert_v1(p_org_id text, p_workspace_id text, p_records jsonb)
RETURNS TABLE (inserted integer, merged integer, evidence_added integer)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_insert$
DECLARE r jsonb; v_id uuid; v_fresh boolean; v_have integer; v_add integer;
        v_ins integer := 0; v_merged integer := 0; v_ev integer := 0;
BEGIN
  IF p_org_id IS NULL OR char_length(p_org_id) NOT BETWEEN 1 AND 512
     OR p_workspace_id IS NULL OR char_length(p_workspace_id) NOT BETWEEN 1 AND 512 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'decision records: invalid workspace';
  END IF;
  IF p_records IS NULL OR jsonb_typeof(p_records) <> 'array' OR jsonb_array_length(p_records) > 100 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'decision records: records must be an array of at most 100';
  END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(p_records) AS a(v)
              WHERE jsonb_typeof(a.v) <> 'object'
                 OR jsonb_typeof(a.v -> 'evidence') IS DISTINCT FROM 'array'
                 OR jsonb_array_length(a.v -> 'evidence') NOT BETWEEN 1 AND 10
                 OR coalesce(a.v ->> 'status', 'decided') NOT IN ('open','decided')
                 OR EXISTS (SELECT 1 FROM jsonb_array_elements(a.v -> 'evidence') AS e(v) WHERE jsonb_typeof(e.v) <> 'object')) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'decision records: each record needs status open or decided and 1 to 10 evidence objects';
  END IF;
  FOR r IN SELECT a.v FROM jsonb_array_elements(p_records) AS a(v) LOOP
    INSERT INTO sessions.relayfacts_v1 AS t (org_id, workspace_id, topic, title, statement, status, decided_at)
    VALUES (p_org_id, p_workspace_id, r ->> 'topic', r ->> 'title', r ->> 'statement',
            coalesce(r ->> 'status', 'decided'), coalesce((r ->> 'decided_at')::timestamptz, now()))
    ON CONFLICT (org_id, workspace_id, topic, title_key) DO NOTHING
    RETURNING t.id INTO v_id;
    v_fresh := v_id IS NOT NULL;
    IF NOT v_fresh THEN
      SELECT t.id INTO v_id FROM sessions.relayfacts_v1 AS t
       WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.topic = r ->> 'topic'
         AND t.title_key = regexp_replace(regexp_replace(lower(btrim(r ->> 'title')), '\s+', ' ', 'g'), '[[:punct:][:space:]]+$', '')
       FOR UPDATE;
      v_merged := v_merged + 1;
    ELSE
      v_ins := v_ins + 1;
    END IF;
    SELECT count(*)::integer INTO v_have FROM sessions.relayfacts_evidence_v1 AS e WHERE e.decision_id = v_id;
    WITH src AS (
      SELECT e.source, e.session_id, e.turn_start, e.turn_end, e.claim, e.ref, e.ord
        FROM ROWS FROM (jsonb_to_recordset(r -> 'evidence')
               AS (source text, session_id text, turn_start integer, turn_end integer, claim text, ref text))
             WITH ORDINALITY AS e(source, session_id, turn_start, turn_end, claim, ref, ord)
       WHERE NOT EXISTS (SELECT 1 FROM sessions.relayfacts_evidence_v1 AS d
                          WHERE d.decision_id = v_id AND d.source = e.source AND d.session_id = e.session_id
                            AND d.turn_start = e.turn_start AND d.turn_end = e.turn_end)
       ORDER BY e.ord
       LIMIT greatest(0, 50 - v_have)
    ), ins AS (
      INSERT INTO sessions.relayfacts_evidence_v1 (decision_id, source, session_id, turn_start, turn_end, claim, ref)
      SELECT v_id, s.source, s.session_id, s.turn_start, s.turn_end, s.claim, s.ref FROM src AS s
      ON CONFLICT (decision_id, source, session_id, turn_start, turn_end) DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::integer INTO v_add FROM ins;
    v_ev := v_ev + v_add;
    IF v_add > 0 AND NOT v_fresh THEN
      UPDATE sessions.relayfacts_v1 AS t SET updated_at = now() WHERE t.id = v_id;
    END IF;
  END LOOP;
  RETURN QUERY SELECT v_ins, v_merged, v_ev;
END
$relayfacts_insert$;

-- Worker: p_new supersedes p_old. Both must be in the workspace and differ; neither may be superseded, and
-- p_new must not already supersede another record (a link is never overwritten). p_old becomes superseded;
-- p_new records what it supersedes. Returns whether the change was made.
CREATE FUNCTION sessions.relayfacts_supersede_v1(p_org_id text, p_workspace_id text, p_old uuid, p_new uuid)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_supersede$
DECLARE v_n integer;
BEGIN
  IF p_old IS NULL OR p_new IS NULL OR p_old = p_new THEN RETURN false; END IF;
  SELECT count(*)::integer INTO v_n FROM (
    SELECT 1 FROM sessions.relayfacts_v1 AS t
     WHERE t.id IN (p_old, p_new) AND t.org_id = p_org_id AND t.workspace_id = p_workspace_id
       AND t.status <> 'superseded' AND (t.id = p_old OR t.supersedes IS NULL)
     FOR UPDATE) AS locked;
  IF v_n <> 2 THEN RETURN false; END IF;
  UPDATE sessions.relayfacts_v1 AS t SET status = 'superseded', updated_at = now() WHERE t.id = p_old;
  UPDATE sessions.relayfacts_v1 AS t SET supersedes = p_old, updated_at = now() WHERE t.id = p_new;
  RETURN true;
END
$relayfacts_supersede$;

-- Read API: the workspace's topics with record counts, by topic, at most 1000. The org must match the read
-- API's request scope.
CREATE FUNCTION sessions.relayfacts_topics_v1(p_org_id text, p_workspace_id text)
RETURNS TABLE (topic text, records integer, open integer, decided integer, superseded integer, updated_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_topics$
BEGIN
  IF sessions.session_directory_read_api_scope(p_workspace_id) IS DISTINCT FROM p_org_id THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'decision records: org outside the read scope';
  END IF;
  RETURN QUERY
    SELECT t.topic, count(*)::integer,
           (count(*) FILTER (WHERE t.status = 'open'))::integer,
           (count(*) FILTER (WHERE t.status = 'decided'))::integer,
           (count(*) FILTER (WHERE t.status = 'superseded'))::integer,
           max(t.updated_at)
      FROM sessions.relayfacts_v1 AS t
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id
     GROUP BY t.topic
     ORDER BY t.topic
     LIMIT 1000;
END
$relayfacts_topics$;

-- Read API: one topic's records, oldest first (created_at, id), after the (p_after_created_at, p_after_id)
-- cursor when given, at most p_limit (1 to 200). Each record carries its evidence (at most 50) as a JSON
-- array ordered by session id and turn range. The org must match the read API's request scope.
CREATE FUNCTION sessions.relayfacts_list_v1(
  p_org_id text, p_workspace_id text, p_topic text, p_after_created_at timestamptz, p_after_id uuid, p_limit integer
) RETURNS TABLE (
  id uuid, topic text, title text, statement text, status text, supersedes uuid, decided_at timestamptz,
  implemented_at timestamptz, implemented_ref text, created_at timestamptz, updated_at timestamptz, evidence jsonb
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_list$
BEGIN
  IF sessions.session_directory_read_api_scope(p_workspace_id) IS DISTINCT FROM p_org_id THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'decision records: org outside the read scope';
  END IF;
  IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 200 OR (p_after_created_at IS NULL) <> (p_after_id IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'decision records: limit 1 to 200 and a complete cursor';
  END IF;
  RETURN QUERY
    SELECT t.id, t.topic, t.title, t.statement, t.status, t.supersedes, t.decided_at,
           t.implemented_at, t.implemented_ref, t.created_at, t.updated_at,
           coalesce((SELECT jsonb_agg(jsonb_build_object(
                       'source', e.source, 'session_id', e.session_id, 'turn_start', e.turn_start,
                       'turn_end', e.turn_end, 'claim', e.claim, 'ref', e.ref)
                     ORDER BY e.session_id, e.turn_start, e.turn_end, e.id)
                       FROM sessions.relayfacts_evidence_v1 AS e WHERE e.decision_id = t.id), '[]'::jsonb)
      FROM sessions.relayfacts_v1 AS t
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.topic = p_topic
       AND (p_after_created_at IS NULL OR (t.created_at, t.id) > (p_after_created_at, p_after_id))
     ORDER BY t.created_at, t.id
     LIMIT p_limit;
END
$relayfacts_list$;

REVOKE ALL ON FUNCTION sessions.relayfacts_insert_v1(text,text,jsonb),
  sessions.relayfacts_supersede_v1(text,text,uuid,uuid),
  sessions.relayfacts_topics_v1(text,text),
  sessions.relayfacts_list_v1(text,text,text,timestamptz,uuid,integer)
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.relayfacts_insert_v1(text,text,jsonb),
  sessions.relayfacts_supersede_v1(text,text,uuid,uuid)
  TO service_role;
GRANT EXECUTE ON FUNCTION sessions.relayfacts_topics_v1(text,text),
  sessions.relayfacts_list_v1(text,text,text,timestamptz,uuid,integer)
  TO bulls96_read_api;
