-- Slot 20261105062150. Relayfacts claims (Session Intelligence M1-1, docs/relayfacts.md, Claims): intent and
-- constraint claims of a session, with a scope (session, project or repo) and the time the claim was made, in two
-- NEW tables next to the 062080 decision records. sessions.relayfacts_v1, sessions.relayfacts_evidence_v1 and every
-- 062080 function are untouched: decisions, GET /v1/relayfacts and everything built on it stay as shipped.
-- Enables nothing: the extractor writes claims only with SESSION_INTEL_CLAIMS_ENABLED=true inside
-- SUMMARY_DECISION_RECORDS_ENABLED=true. Code only: applying this to any real database needs Khaliq's separate,
-- explicit OK.
--
-- Both tables are created empty here, so their indexes are built on empty tables (as 062080 did); nothing touches
-- a populated table except the two replaced erase functions (no lock, a catalog change only).
--
-- Deduplication: one claim per (org, workspace, kind, topic, title_key), title_key as in 062080; the unique index
-- relayfacts_claims_v1_dedupe enforces it and the writer merges ON CONFLICT. A known claim keeps its title and
-- statement and gains only evidence it does not hold (at most 50 rows); only when it gains a row, its scope widens
-- (session < project < repo) and its observed_at moves earlier. The writer refuses (RH016) a batch any of whose
-- evidence sessions is fenced by either erase, before any write, and an observed_at that is not a finite ISO 8601
-- timestamp (22023).
--
-- Status: open | decided (current) -> superseded (relayfacts_claims_supersede_v1 only, same kind and workspace).
--
-- Erase: the 062090 per-session erase and the 062100 per-user erase are replaced (same signatures, results and ACL)
-- so they also delete claim evidence and claims, and rewrite a kept claim's statement from its oldest remaining
-- evidence with observed_at NULL and scope 'session'; claim rows are counted in the same totals. Their fences reach
-- the new evidence table: relayfacts_claims_fence_guard_v1 refuses (RH016) claim evidence for a session fenced by
-- either erase, and the 062100 statement lock triggers serialize claim writes against the per-user erase.
--
-- Roles: the worker (service_role, and the runtime role through RUNTIME_APPLICATION_FUNCTIONS) gets insert,
-- supersede and the claims reader. No role gets a table privilege.

CREATE TABLE sessions.relayfacts_claims_v1 (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512),
  kind text NOT NULL CHECK (kind IN ('intent','constraint')),
  scope text NOT NULL DEFAULT 'session' CHECK (scope IN ('session','project','repo')),
  topic text NOT NULL CHECK (topic ~ '^[a-z0-9][a-z0-9-]{0,63}$'),
  title text NOT NULL CHECK (char_length(btrim(title)) BETWEEN 1 AND 200 AND title !~ '[\r\n]'),
  title_key text GENERATED ALWAYS AS (
    regexp_replace(regexp_replace(lower(btrim(title)), '\s+', ' ', 'g'), '[[:punct:][:space:]]+$', '')
  ) STORED,
  statement text NOT NULL CHECK (char_length(btrim(statement)) BETWEEN 1 AND 1000 AND statement !~ '[\r\n]'),
  status text NOT NULL DEFAULT 'decided' CHECK (status IN ('open','decided','superseded')),
  supersedes uuid REFERENCES sessions.relayfacts_claims_v1 (id) ON DELETE SET NULL,
  observed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (supersedes IS DISTINCT FROM id)
);
CREATE UNIQUE INDEX relayfacts_claims_v1_dedupe ON sessions.relayfacts_claims_v1 (org_id, workspace_id, kind, topic, title_key);
-- The claims reader (newest first).
CREATE INDEX relayfacts_claims_v1_updated ON sessions.relayfacts_claims_v1 (org_id, workspace_id, updated_at);

CREATE TABLE sessions.relayfacts_claim_evidence_v1 (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  claim_id uuid NOT NULL REFERENCES sessions.relayfacts_claims_v1 (id) ON DELETE CASCADE,
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
CREATE UNIQUE INDEX relayfacts_claim_evidence_v1_dedupe ON sessions.relayfacts_claim_evidence_v1 (claim_id, source, session_id, turn_start, turn_end);

ALTER TABLE sessions.relayfacts_claims_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.relayfacts_claim_evidence_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.relayfacts_claims_v1, sessions.relayfacts_claim_evidence_v1
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api, bulls96_session_ingest,
       bulls96_session_ingest_seeder;

-- Fences: claim evidence for a session fenced by the 062090 per-session erase (session_erasure_fences) or the 062100
-- per-user erase (relayfacts_erasure_fences) is refused, so a late analysis write fails as a whole batch.
CREATE FUNCTION sessions.relayfacts_claims_fence_guard_v1() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_claims_fence_guard$
BEGIN
  IF EXISTS (SELECT 1 FROM sessions.relayfacts_claims_v1 AS c JOIN sessions.session_erasure_fences AS f
              ON f.org_id = c.org_id AND f.workspace_id = c.workspace_id
             WHERE c.id = NEW.claim_id AND f.source = NEW.source AND f.session_id = NEW.session_id)
     OR EXISTS (SELECT 1 FROM sessions.relayfacts_claims_v1 AS c JOIN sessions.relayfacts_erasure_fences AS f
              ON f.org_id = c.org_id AND f.workspace_id = c.workspace_id
             WHERE c.id = NEW.claim_id AND f.source = NEW.source AND f.session_id = NEW.session_id) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH016', MESSAGE = 'claim records: evidence session is erased';
  END IF;
  RETURN NEW;
END
$relayfacts_claims_fence_guard$;
CREATE TRIGGER relayfacts_claims_fence_guard BEFORE INSERT OR UPDATE ON sessions.relayfacts_claim_evidence_v1
  FOR EACH ROW EXECUTE FUNCTION sessions.relayfacts_claims_fence_guard_v1();
-- The 062100 shared lock, at statement level before any row lock, as on the decision tables.
CREATE TRIGGER relayfacts_claims_lock_records BEFORE INSERT OR UPDATE ON sessions.relayfacts_claims_v1
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.relayfacts_user_erase_lock_v1();
CREATE TRIGGER relayfacts_claims_lock_evidence BEFORE INSERT OR UPDATE ON sessions.relayfacts_claim_evidence_v1
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.relayfacts_user_erase_lock_v1();

-- Worker: insert or merge a batch of intent and constraint claims for one workspace. p_records is a JSON array (at
-- most 100) of {kind, scope?, observed_at?, topic, title, statement, status?, evidence: [{source, session_id,
-- turn_start, turn_end, claim, ref?}] (1 to 10)}: kind intent or constraint; scope session, project or repo
-- (default session); observed_at a timestamp or null; status open or decided (default decided). Returns counts only.
CREATE FUNCTION sessions.relayfacts_claims_insert_v1(p_org_id text, p_workspace_id text, p_records jsonb)
RETURNS TABLE (inserted integer, merged integer, evidence_added integer)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_claims_insert$
DECLARE r jsonb; v_id uuid; v_fresh boolean; v_have integer; v_add integer; v_scope text; v_observed timestamptz;
        v_ins integer := 0; v_merged integer := 0; v_ev integer := 0;
BEGIN
  IF p_org_id IS NULL OR char_length(p_org_id) NOT BETWEEN 1 AND 512
     OR p_workspace_id IS NULL OR char_length(p_workspace_id) NOT BETWEEN 1 AND 512 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'claim records: invalid workspace';
  END IF;
  IF p_records IS NULL OR jsonb_typeof(p_records) <> 'array' OR jsonb_array_length(p_records) > 100 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'claim records: records must be an array of at most 100';
  END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(p_records) AS a(v)
              WHERE jsonb_typeof(a.v) <> 'object'
                 OR jsonb_typeof(a.v -> 'evidence') IS DISTINCT FROM 'array'
                 OR jsonb_array_length(a.v -> 'evidence') NOT BETWEEN 1 AND 10
                 OR coalesce(a.v ->> 'status', 'decided') NOT IN ('open','decided')
                 OR (a.v ->> 'kind') IS NULL OR (a.v ->> 'kind') NOT IN ('intent','constraint')
                 OR coalesce(a.v ->> 'scope', 'session') NOT IN ('session','project','repo')
                 OR coalesce(jsonb_typeof(a.v -> 'observed_at'), 'null') NOT IN ('string','null')
                 OR EXISTS (SELECT 1 FROM jsonb_array_elements(a.v -> 'evidence') AS e(v) WHERE jsonb_typeof(e.v) <> 'object')) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'claim records: each record needs kind intent or constraint, scope session, project or repo, status open or decided, observed_at a timestamp or null and 1 to 10 evidence objects';
  END IF;
  -- observed_at: an ISO 8601 timestamp with a zone, finite; never a keyword ('now', 'infinity', 'epoch').
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(p_records) AS a(v)
              WHERE jsonb_typeof(a.v -> 'observed_at') = 'string'
                AND CASE WHEN (a.v ->> 'observed_at') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$'
                         THEN NOT isfinite((a.v ->> 'observed_at')::timestamptz) ELSE true END) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'claim records: observed_at must be a finite ISO 8601 timestamp with a zone';
  END IF;
  -- The 062100 shared lock first (the per-user erase takes it exclusively), so the fence read below sees every
  -- fence committed before it, and no erase can commit a new one until this batch commits.
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('96bulls:relayfacts-user-erase-v1', 0));
  -- Every incoming evidence session, against both fences (062090 per-session, 062100 per-user), before any write:
  -- a merge that adds no evidence row (at the 50-row cap) never reaches the row guard, so it is refused here.
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(p_records) AS a(v)
              CROSS JOIN LATERAL jsonb_to_recordset(a.v -> 'evidence') AS e(source text, session_id text)
              WHERE EXISTS (SELECT 1 FROM sessions.session_erasure_fences AS f
                             WHERE f.org_id = p_org_id AND f.workspace_id = p_workspace_id
                               AND f.source = e.source AND f.session_id = e.session_id)
                 OR EXISTS (SELECT 1 FROM sessions.relayfacts_erasure_fences AS f
                             WHERE f.org_id = p_org_id AND f.workspace_id = p_workspace_id
                               AND f.source = e.source AND f.session_id = e.session_id)) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH016', MESSAGE = 'claim records: evidence session is erased';
  END IF;
  FOR r IN SELECT a.v FROM jsonb_array_elements(p_records) AS a(v) LOOP
    v_scope := coalesce(r ->> 'scope', 'session');
    v_observed := (r ->> 'observed_at')::timestamptz;
    INSERT INTO sessions.relayfacts_claims_v1 AS t (org_id, workspace_id, kind, scope, topic, title, statement, status, observed_at)
    VALUES (p_org_id, p_workspace_id, r ->> 'kind', v_scope, r ->> 'topic', r ->> 'title', r ->> 'statement',
            coalesce(r ->> 'status', 'decided'), v_observed)
    ON CONFLICT (org_id, workspace_id, kind, topic, title_key) DO NOTHING
    RETURNING t.id INTO v_id;
    v_fresh := v_id IS NOT NULL;
    IF NOT v_fresh THEN
      SELECT t.id INTO v_id FROM sessions.relayfacts_claims_v1 AS t
       WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.kind = r ->> 'kind' AND t.topic = r ->> 'topic'
         AND t.title_key = regexp_replace(regexp_replace(lower(btrim(r ->> 'title')), '\s+', ' ', 'g'), '[[:punct:][:space:]]+$', '')
       FOR UPDATE;
      v_merged := v_merged + 1;
    ELSE
      v_ins := v_ins + 1;
    END IF;
    SELECT count(*)::integer INTO v_have FROM sessions.relayfacts_claim_evidence_v1 AS e WHERE e.claim_id = v_id;
    WITH src AS (
      SELECT e.source, e.session_id, e.turn_start, e.turn_end, e.claim, e.ref, e.ord
        FROM ROWS FROM (jsonb_to_recordset(r -> 'evidence')
               AS (source text, session_id text, turn_start integer, turn_end integer, claim text, ref text))
             WITH ORDINALITY AS e(source, session_id, turn_start, turn_end, claim, ref, ord)
       WHERE NOT EXISTS (SELECT 1 FROM sessions.relayfacts_claim_evidence_v1 AS d
                          WHERE d.claim_id = v_id AND d.source = e.source AND d.session_id = e.session_id
                            AND d.turn_start = e.turn_start AND d.turn_end = e.turn_end)
       ORDER BY e.ord
       LIMIT greatest(0, 50 - v_have)
    ), ins AS (
      INSERT INTO sessions.relayfacts_claim_evidence_v1 (claim_id, source, session_id, turn_start, turn_end, claim, ref)
      SELECT v_id, s.source, s.session_id, s.turn_start, s.turn_end, s.claim, s.ref FROM src AS s
      ON CONFLICT (claim_id, source, session_id, turn_start, turn_end) DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::integer INTO v_add FROM ins;
    v_ev := v_ev + v_add;
    -- A merge moves scope and observed_at only together with new evidence (rows the fence guard has checked).
    IF NOT v_fresh AND v_add > 0 THEN
      UPDATE sessions.relayfacts_claims_v1 AS t
         SET scope = CASE WHEN array_position(ARRAY['session','project','repo'], v_scope)
                               > array_position(ARRAY['session','project','repo'], t.scope) THEN v_scope ELSE t.scope END,
             observed_at = least(t.observed_at, v_observed),
             updated_at = now()
       WHERE t.id = v_id;
    END IF;
  END LOOP;
  RETURN QUERY SELECT v_ins, v_merged, v_ev;
END
$relayfacts_claims_insert$;

-- Worker: p_new supersedes p_old. Both must be claims of the workspace of the same kind and differ; neither may be
-- superseded, and p_new must not already supersede another claim. Returns whether the change was made.
CREATE FUNCTION sessions.relayfacts_claims_supersede_v1(p_org_id text, p_workspace_id text, p_old uuid, p_new uuid)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_claims_supersede$
DECLARE v_n integer; v_kinds integer;
BEGIN
  IF p_old IS NULL OR p_new IS NULL OR p_old = p_new THEN RETURN false; END IF;
  -- The 062100 shared lock before the row locks (its UPDATEs would otherwise take it after them, and a per-user
  -- erase holding it exclusively while waiting for these rows would deadlock).
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('96bulls:relayfacts-user-erase-v1', 0));
  SELECT count(*)::integer, count(DISTINCT locked.kind)::integer INTO v_n, v_kinds FROM (
    SELECT t.kind FROM sessions.relayfacts_claims_v1 AS t
     WHERE t.id IN (p_old, p_new) AND t.org_id = p_org_id AND t.workspace_id = p_workspace_id
       AND t.status <> 'superseded' AND (t.id = p_old OR t.supersedes IS NULL)
     FOR UPDATE) AS locked;
  IF v_n <> 2 OR v_kinds <> 1 THEN RETURN false; END IF;
  UPDATE sessions.relayfacts_claims_v1 AS t SET status = 'superseded', updated_at = now() WHERE t.id = p_old;
  UPDATE sessions.relayfacts_claims_v1 AS t SET supersedes = p_old, updated_at = now() WHERE t.id = p_new;
  RETURN true;
END
$relayfacts_claims_supersede$;

-- Worker and advisor: the workspace's current claims (status open or decided): intents and constraints from
-- relayfacts_claims_v1, and decisions from the 062080 relayfacts_v1 (kind 'decision', scope NULL, observed_at
-- NULL). Optionally of one topic and one kind, updated at or after p_since; newest first (updated_at desc, id), at
-- most p_limit (1 to 500). statement is the stored scrub-clean statement cut to 200 characters. Evidence (at most
-- 50 per record) carries pointers only: id, source, session id, reference range, ref and created_at.
CREATE FUNCTION sessions.relayfacts_claims_v1(
  p_org_id text, p_workspace_id text, p_topic text, p_kind text, p_since timestamptz, p_limit integer
) RETURNS TABLE (
  id uuid, kind text, scope text, topic text, title text, statement text, status text, supersedes uuid,
  observed_at timestamptz, created_at timestamptz, updated_at timestamptz, evidence jsonb
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_claims$
BEGIN
  IF p_org_id IS NULL OR char_length(p_org_id) NOT BETWEEN 1 AND 512
     OR p_workspace_id IS NULL OR char_length(p_workspace_id) NOT BETWEEN 1 AND 512 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'claim records: invalid workspace';
  END IF;
  IF (p_topic IS NOT NULL AND p_topic !~ '^[a-z0-9][a-z0-9-]{0,63}$')
     OR (p_kind IS NOT NULL AND p_kind NOT IN ('decision','intent','constraint'))
     OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 500 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'claim records: topic slug, kind decision, intent or constraint, and limit 1 to 500';
  END IF;
  RETURN QUERY
    SELECT u.id, u.kind, u.scope, u.topic, u.title, u.statement, u.status, u.supersedes, u.observed_at,
           u.created_at, u.updated_at, u.evidence
      FROM (
        SELECT c.id, c.kind, c.scope, c.topic, c.title, left(c.statement, 200) AS statement, c.status, c.supersedes,
               c.observed_at, c.created_at, c.updated_at,
               coalesce((SELECT jsonb_agg(jsonb_build_object(
                           'id', e.id, 'source', e.source, 'session_id', e.session_id, 'turn_start', e.turn_start,
                           'turn_end', e.turn_end, 'ref', e.ref, 'created_at', e.created_at)
                         ORDER BY e.created_at, e.id)
                           FROM sessions.relayfacts_claim_evidence_v1 AS e WHERE e.claim_id = c.id), '[]'::jsonb) AS evidence
          FROM sessions.relayfacts_claims_v1 AS c
         WHERE c.org_id = p_org_id AND c.workspace_id = p_workspace_id AND c.status <> 'superseded'
           AND (p_topic IS NULL OR c.topic = p_topic)
           AND (p_kind IS NULL OR c.kind = p_kind)
           AND (p_since IS NULL OR c.updated_at >= p_since)
        UNION ALL
        SELECT t.id, 'decision'::text, NULL::text, t.topic, t.title, left(t.statement, 200), t.status, t.supersedes,
               NULL::timestamptz, t.created_at, t.updated_at,
               coalesce((SELECT jsonb_agg(jsonb_build_object(
                           'id', e.id, 'source', e.source, 'session_id', e.session_id, 'turn_start', e.turn_start,
                           'turn_end', e.turn_end, 'ref', e.ref, 'created_at', e.created_at)
                         ORDER BY e.created_at, e.id)
                           FROM sessions.relayfacts_evidence_v1 AS e WHERE e.decision_id = t.id), '[]'::jsonb)
          FROM sessions.relayfacts_v1 AS t
         WHERE (p_kind IS NULL OR p_kind = 'decision')
           AND t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.status <> 'superseded'
           AND (p_topic IS NULL OR t.topic = p_topic)
           AND (p_since IS NULL OR t.updated_at >= p_since)
      ) AS u
     ORDER BY u.updated_at DESC, u.id
     LIMIT p_limit;
END
$relayfacts_claims$;

-- 062090's per-session erase, plus the claims tables (same signature, result keys and ACL).
CREATE OR REPLACE FUNCTION sessions.relayfacts_evidence_erase_session_v1(p_org text, p_ws text, p_source text, p_session text, p_lease uuid)
RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_evidence_erase$
DECLARE v_ids text[]; v_touched uuid[]; v_evidence integer; v_deleted integer; v_rewritten integer;
        v_claims uuid[]; v_n integer;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  IF NOT sessions.ingest_operator_runtime_authorize(p_org, p_ws, p_source, p_session, p_lease) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH014', MESSAGE = 'decision records erase: writer or lease still pending';
  END IF;
  SELECT array_agg(s.target_session_id) INTO v_ids FROM sessions.session_erasure_scopes AS s
   WHERE s.org_id = p_org AND s.workspace_id = p_ws AND s.source = p_source AND s.session_id = p_session;
  -- Lock the affected records in id order, so a concurrent merge cannot add evidence between the delete
  -- and the record decision below.
  SELECT coalesce(array_agg(l.id ORDER BY l.id), '{}') INTO v_touched FROM (
    SELECT t.id FROM sessions.relayfacts_v1 AS t
     WHERE t.org_id = p_org AND t.workspace_id = p_ws
       AND EXISTS (SELECT 1 FROM sessions.relayfacts_evidence_v1 AS e
                    WHERE e.decision_id = t.id AND e.source = p_source AND e.session_id = ANY (v_ids))
     ORDER BY t.id FOR UPDATE) AS l;
  DELETE FROM sessions.relayfacts_evidence_v1 AS e
   WHERE e.decision_id = ANY (v_touched) AND e.source = p_source AND e.session_id = ANY (v_ids);
  GET DIAGNOSTICS v_evidence = ROW_COUNT;
  DELETE FROM sessions.relayfacts_v1 AS t
   WHERE t.id = ANY (v_touched)
     AND NOT EXISTS (SELECT 1 FROM sessions.relayfacts_evidence_v1 AS e WHERE e.decision_id = t.id);
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  UPDATE sessions.relayfacts_v1 AS t
     SET statement = (SELECT e.claim FROM sessions.relayfacts_evidence_v1 AS e
                       WHERE e.decision_id = t.id ORDER BY e.created_at, e.id LIMIT 1),
         updated_at = now()
   WHERE t.id = ANY (v_touched);
  GET DIAGNOSTICS v_rewritten = ROW_COUNT;
  -- 20261105062150: the same rules for intent and constraint claims, counted in the same totals. A kept claim
  -- also loses its observed_at and falls back to scope 'session' (either may have come from the erased session).
  SELECT coalesce(array_agg(l.id ORDER BY l.id), '{}') INTO v_claims FROM (
    SELECT c.id FROM sessions.relayfacts_claims_v1 AS c
     WHERE c.org_id = p_org AND c.workspace_id = p_ws
       AND EXISTS (SELECT 1 FROM sessions.relayfacts_claim_evidence_v1 AS e
                    WHERE e.claim_id = c.id AND e.source = p_source AND e.session_id = ANY (v_ids))
     ORDER BY c.id FOR UPDATE) AS l;
  DELETE FROM sessions.relayfacts_claim_evidence_v1 AS e
   WHERE e.claim_id = ANY (v_claims) AND e.source = p_source AND e.session_id = ANY (v_ids);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_evidence := v_evidence + v_n;
  DELETE FROM sessions.relayfacts_claims_v1 AS c
   WHERE c.id = ANY (v_claims)
     AND NOT EXISTS (SELECT 1 FROM sessions.relayfacts_claim_evidence_v1 AS e WHERE e.claim_id = c.id);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_deleted := v_deleted + v_n;
  UPDATE sessions.relayfacts_claims_v1 AS c
     SET statement = (SELECT e.claim FROM sessions.relayfacts_claim_evidence_v1 AS e
                       WHERE e.claim_id = c.id ORDER BY e.created_at, e.id LIMIT 1),
         observed_at = NULL, scope = 'session', updated_at = now()
   WHERE c.id = ANY (v_claims);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_rewritten := v_rewritten + v_n;
  IF EXISTS (SELECT 1 FROM sessions.relayfacts_claim_evidence_v1 AS e JOIN sessions.relayfacts_claims_v1 AS c ON c.id = e.claim_id
              WHERE c.org_id = p_org AND c.workspace_id = p_ws AND e.source = p_source AND e.session_id = ANY (v_ids)) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH015', MESSAGE = 'decision records erase: claim evidence remains';
  END IF;
  IF EXISTS (SELECT 1 FROM sessions.relayfacts_evidence_v1 AS e JOIN sessions.relayfacts_v1 AS t ON t.id = e.decision_id
              WHERE t.org_id = p_org AND t.workspace_id = p_ws AND e.source = p_source AND e.session_id = ANY (v_ids)) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH015', MESSAGE = 'decision records erase: evidence remains';
  END IF;
  RETURN jsonb_build_object('evidenceRows', v_evidence, 'recordsDeleted', v_deleted, 'recordsRewritten', v_rewritten);
END
$relayfacts_evidence_erase$;

-- 062100's per-user erase, plus the claims tables (same signature, result keys and ACL: owner only).
CREATE OR REPLACE FUNCTION sessions.relayfacts_user_erase_v1(p_request text, p_org text, p_ws text, p_user text)
RETURNS jsonb
LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_user_erase$
DECLARE v_request text; v_org text; v_ws text; v_user text; v_src text[]; v_ids text[]; v_touched uuid[];
        v_sessions integer; v_fences integer; v_evidence integer; v_deleted integer; v_rewritten integer;
        v_claims uuid[]; v_n integer;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'relayfacts user erase: read-committed transaction required';
  END IF;
  v_request := sessions.ingest_identifier(to_jsonb(p_request), false);
  v_org := sessions.ingest_identifier(to_jsonb(p_org), false);
  v_ws := sessions.ingest_identifier(to_jsonb(p_ws), true);
  v_user := sessions.ingest_identifier(to_jsonb(p_user), false);
  -- relayfacts_v1 has no record with an empty workspace.
  IF v_ws = '' THEN
    RETURN jsonb_build_object('sessions', 0, 'fencesAdded', 0, 'evidenceRows', 0, 'recordsDeleted', 0, 'recordsRewritten', 0);
  END IF;
  -- Same rule as operator_user_erase_begin: a request id names one (org, workspace, user).
  IF EXISTS (SELECT 1 FROM sessions.operator_user_erasures AS u
              WHERE u.request_id = v_request AND (u.org_id, u.workspace_id, u.user_id) IS DISTINCT FROM (v_org, v_ws, v_user)) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'relayfacts user erase: request id belongs to another user';
  END IF;
  -- The fences already carry this request id under another org or workspace.
  IF EXISTS (SELECT 1 FROM sessions.relayfacts_erasure_fences AS f
              WHERE f.request_id = v_request AND (f.org_id, f.workspace_id) IS DISTINCT FROM (v_org, v_ws)) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'relayfacts user erase: request id belongs to another workspace';
  END IF;
  -- Every session of the user, soft-deleted and tombstoned rows included, under both id forms. Collected once
  -- without the lock: a user with none returns zeros without stopping every relayfacts writer.
  SELECT coalesce(array_agg(p.harness ORDER BY p.harness, p.id), '{}'), coalesce(array_agg(p.id ORDER BY p.harness, p.id), '{}')
    INTO v_src, v_ids
    FROM (SELECT DISTINCT si.harness, x.id FROM sessions.session_index AS si
           CROSS JOIN LATERAL (VALUES (si.session_id), (coalesce(si.canonical_session_id, si.session_id))) AS x(id)
          WHERE si.org_id = v_org AND si.workspace_id = v_ws AND si.user_id = v_user) AS p;
  IF cardinality(v_ids) = 0 THEN
    RETURN jsonb_build_object('sessions', 0, 'fencesAdded', 0, 'evidenceRows', 0, 'recordsDeleted', 0, 'recordsRewritten', 0);
  END IF;
  -- Exclusive: waits for in-flight relayfacts writers (they hold it shared) and blocks new ones until commit.
  PERFORM pg_advisory_xact_lock(hashtextextended('96bulls:relayfacts-user-erase-v1', 0));
  -- Collected again under the lock (a new statement, so a new read-committed snapshot): a session published
  -- after the first read is fenced too.
  SELECT count(DISTINCT (si.harness, si.session_id))::integer INTO v_sessions FROM sessions.session_index AS si
   WHERE si.org_id = v_org AND si.workspace_id = v_ws AND si.user_id = v_user;
  SELECT coalesce(array_agg(p.harness ORDER BY p.harness, p.id), '{}'), coalesce(array_agg(p.id ORDER BY p.harness, p.id), '{}')
    INTO v_src, v_ids
    FROM (SELECT DISTINCT si.harness, x.id FROM sessions.session_index AS si
           CROSS JOIN LATERAL (VALUES (si.session_id), (coalesce(si.canonical_session_id, si.session_id))) AS x(id)
          WHERE si.org_id = v_org AND si.workspace_id = v_ws AND si.user_id = v_user) AS p;
  INSERT INTO sessions.relayfacts_erasure_fences (org_id, workspace_id, source, session_id, request_id)
  SELECT v_org, v_ws, u.s, u.i, v_request FROM unnest(v_src, v_ids) AS u(s, i)
  ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_fences = ROW_COUNT;
  -- Lock the affected records in id order, so no merge adds evidence between the delete and the record decision.
  SELECT coalesce(array_agg(l.id ORDER BY l.id), '{}') INTO v_touched FROM (
    SELECT t.id FROM sessions.relayfacts_v1 AS t
     WHERE t.org_id = v_org AND t.workspace_id = v_ws
       AND EXISTS (SELECT 1 FROM sessions.relayfacts_evidence_v1 AS e JOIN unnest(v_src, v_ids) AS u(s, i)
                     ON e.source = u.s AND e.session_id = u.i
                    WHERE e.decision_id = t.id)
     ORDER BY t.id FOR UPDATE) AS l;
  DELETE FROM sessions.relayfacts_evidence_v1 AS e USING unnest(v_src, v_ids) AS u(s, i)
   WHERE e.decision_id = ANY (v_touched) AND e.source = u.s AND e.session_id = u.i;
  GET DIAGNOSTICS v_evidence = ROW_COUNT;
  DELETE FROM sessions.relayfacts_v1 AS t
   WHERE t.id = ANY (v_touched)
     AND NOT EXISTS (SELECT 1 FROM sessions.relayfacts_evidence_v1 AS e WHERE e.decision_id = t.id);
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  UPDATE sessions.relayfacts_v1 AS t
     SET statement = (SELECT e.claim FROM sessions.relayfacts_evidence_v1 AS e
                       WHERE e.decision_id = t.id ORDER BY e.created_at, e.id LIMIT 1),
         updated_at = now()
   WHERE t.id = ANY (v_touched);
  GET DIAGNOSTICS v_rewritten = ROW_COUNT;
  -- 20261105062150: the same rules for intent and constraint claims, counted in the same totals. A kept claim
  -- also loses its observed_at and falls back to scope 'session'.
  SELECT coalesce(array_agg(l.id ORDER BY l.id), '{}') INTO v_claims FROM (
    SELECT c.id FROM sessions.relayfacts_claims_v1 AS c
     WHERE c.org_id = v_org AND c.workspace_id = v_ws
       AND EXISTS (SELECT 1 FROM sessions.relayfacts_claim_evidence_v1 AS e JOIN unnest(v_src, v_ids) AS u(s, i)
                     ON e.source = u.s AND e.session_id = u.i
                    WHERE e.claim_id = c.id)
     ORDER BY c.id FOR UPDATE) AS l;
  DELETE FROM sessions.relayfacts_claim_evidence_v1 AS e USING unnest(v_src, v_ids) AS u(s, i)
   WHERE e.claim_id = ANY (v_claims) AND e.source = u.s AND e.session_id = u.i;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_evidence := v_evidence + v_n;
  DELETE FROM sessions.relayfacts_claims_v1 AS c
   WHERE c.id = ANY (v_claims)
     AND NOT EXISTS (SELECT 1 FROM sessions.relayfacts_claim_evidence_v1 AS e WHERE e.claim_id = c.id);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_deleted := v_deleted + v_n;
  UPDATE sessions.relayfacts_claims_v1 AS c
     SET statement = (SELECT e.claim FROM sessions.relayfacts_claim_evidence_v1 AS e
                       WHERE e.claim_id = c.id ORDER BY e.created_at, e.id LIMIT 1),
         observed_at = NULL, scope = 'session', updated_at = now()
   WHERE c.id = ANY (v_claims);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_rewritten := v_rewritten + v_n;
  IF EXISTS (SELECT 1 FROM sessions.relayfacts_claim_evidence_v1 AS e JOIN sessions.relayfacts_claims_v1 AS c ON c.id = e.claim_id
              JOIN unnest(v_src, v_ids) AS u(s, i) ON e.source = u.s AND e.session_id = u.i
              WHERE c.org_id = v_org AND c.workspace_id = v_ws) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH015', MESSAGE = 'relayfacts user erase: claim evidence remains';
  END IF;
  IF EXISTS (SELECT 1 FROM sessions.relayfacts_evidence_v1 AS e JOIN sessions.relayfacts_v1 AS t ON t.id = e.decision_id
              JOIN unnest(v_src, v_ids) AS u(s, i) ON e.source = u.s AND e.session_id = u.i
              WHERE t.org_id = v_org AND t.workspace_id = v_ws) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH015', MESSAGE = 'relayfacts user erase: evidence remains';
  END IF;
  RETURN jsonb_build_object('sessions', v_sessions, 'fencesAdded', v_fences, 'evidenceRows', v_evidence,
                            'recordsDeleted', v_deleted, 'recordsRewritten', v_rewritten);
END
$relayfacts_user_erase$;

REVOKE ALL ON FUNCTION sessions.relayfacts_claims_insert_v1(text,text,jsonb),
  sessions.relayfacts_claims_supersede_v1(text,text,uuid,uuid),
  sessions.relayfacts_claims_v1(text,text,text,text,timestamptz,integer),
  sessions.relayfacts_claims_fence_guard_v1()
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api, bulls96_session_ingest,
       bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.relayfacts_claims_insert_v1(text,text,jsonb),
  sessions.relayfacts_claims_supersede_v1(text,text,uuid,uuid),
  sessions.relayfacts_claims_v1(text,text,text,text,timestamptz,integer)
  TO service_role;
