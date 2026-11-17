-- Slot 20261105062100. Relayfacts follow-up: the hosted operator per-user erase reaches decision records
-- (docs/relayfacts.md, Retention; docs/operator-user-erase.md, Relayfacts step). Enables nothing: the per-user
-- erase job calls the erase function only when SUMMARY_DECISION_RECORDS_ENABLED=true on its process. Code only:
-- applying this to any real database needs Khaliq's separate, explicit OK.
--
-- relayfacts_user_erase_v1 is owner-only (no EXECUTE grant, not SECURITY DEFINER), like every 20261105058010
-- function: the per-user erase job connects as the sessions schema owner and calls it in its own read-committed
-- transaction BEFORE operator_user_erase_begin (begin refuses RH040 while any session_index row of the user
-- exists, soft-deleted included, so this step cannot run after it). It resolves the user's sessions from
-- session_index (org, workspace, user), soft-deleted and tombstoned rows included, as (harness, id) pairs for both
-- the session id and the canonical session id, fences every pair in sessions.relayfacts_erasure_fences, then does
-- what 20261105062090 does for one session, over all the pairs:
--   * the pairs' evidence rows in the workspace are deleted;
--   * a record left with no evidence is deleted (its title and statement came only from erased sessions);
--   * a record that keeps evidence from other sessions is kept, and its statement is replaced by the claim of
--     its oldest remaining evidence row. The title is kept: every remaining row was merged under the same title_key.
-- Idempotent: a rerun adds no fence and deletes nothing, except for sessions of the user published since the last
-- run; it returns zero for every count except `sessions`, which is always the user's session count. A user with no
-- session_index row returns all zeros without taking the advisory lock below. A workspace id of '' (allowed by the
-- per-user erase) has no relayfacts: all zeros.
-- Request ids: refused (22023) when the id belongs to another (org, workspace, user) in operator_user_erasures, or
-- already fences sessions under another org or workspace (fences carry no user id, so a reuse for another user in
-- the same workspace is not detectable here).
--
-- Fences: sessions.relayfacts_erasure_fences rows are permanent (only the rollback removes them).
-- session_erasure_fences cannot be reused: a row there makes the session unreadable and refuses its uploads.
-- relayfacts_user_erase_guard_v1 refuses (RH016) an evidence row whose (workspace, source, session) is fenced
-- here; the whole insert batch rolls back, so no record is left without evidence, and the analysis hook only
-- counts the failure (decision_records_fenced). Known limitation: one fenced reference fails the whole batch.
--
-- Serialization: every INSERT or UPDATE statement on relayfacts_v1 and relayfacts_evidence_v1 takes the shared
-- advisory lock hashtextextended('96bulls:relayfacts-user-erase-v1',0) (statement-level triggers, which fire
-- even for INSERT ... ON CONFLICT DO NOTHING); the erase takes it exclusively. So the erase waits for in-flight
-- relayfacts writers and blocks new ones until it commits; a blocked writer's guard then sees the new fences.
-- Statement level, not row level: relayfacts_insert_v1 locks an existing record FOR UPDATE before it inserts
-- evidence, and a row trigger on the evidence table would deadlock against the erase's FOR UPDATE on records.
-- Known deadlock, resolved by Postgres: 20261105062090's per-session erase locks a record FOR UPDATE before its
-- UPDATE takes the shared lock, while this erase holds the lock exclusively and waits FOR UPDATE on the same record.
-- Postgres aborts one side with 40P01; both are retried (this job exits 4, the per-session pass records
-- failed_retryable), and the aborted transaction leaves nothing behind.

CREATE TABLE sessions.relayfacts_erasure_fences (
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512),
  source text NOT NULL CHECK (char_length(source) BETWEEN 1 AND 64),
  -- Same rule as session_index.session_id (1..255, no character class), so every row there can be fenced.
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 255),
  request_id text NOT NULL CHECK (char_length(request_id) BETWEEN 1 AND 512),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (org_id, workspace_id, source, session_id)
);
ALTER TABLE sessions.relayfacts_erasure_fences ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.relayfacts_erasure_fences
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api, bulls96_session_ingest,
       bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.relayfacts_user_erase_lock_v1() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_user_erase_lock$
BEGIN
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('96bulls:relayfacts-user-erase-v1', 0));
  RETURN NULL;
END
$relayfacts_user_erase_lock$;
CREATE TRIGGER relayfacts_user_erase_lock_records BEFORE INSERT OR UPDATE ON sessions.relayfacts_v1
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.relayfacts_user_erase_lock_v1();
CREATE TRIGGER relayfacts_user_erase_lock_evidence BEFORE INSERT OR UPDATE ON sessions.relayfacts_evidence_v1
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.relayfacts_user_erase_lock_v1();

CREATE FUNCTION sessions.relayfacts_user_erase_guard_v1() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_user_erase_guard$
BEGIN
  IF EXISTS (SELECT 1 FROM sessions.relayfacts_v1 AS t JOIN sessions.relayfacts_erasure_fences AS f
              ON f.org_id = t.org_id AND f.workspace_id = t.workspace_id
             WHERE t.id = NEW.decision_id AND f.source = NEW.source AND f.session_id = NEW.session_id) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH016', MESSAGE = 'decision records: evidence session is erased';
  END IF;
  RETURN NEW;
END
$relayfacts_user_erase_guard$;
CREATE TRIGGER relayfacts_user_erase_guard BEFORE INSERT OR UPDATE ON sessions.relayfacts_evidence_v1
  FOR EACH ROW EXECUTE FUNCTION sessions.relayfacts_user_erase_guard_v1();

CREATE FUNCTION sessions.relayfacts_user_erase_v1(p_request text, p_org text, p_ws text, p_user text)
RETURNS jsonb
LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_user_erase$
DECLARE v_request text; v_org text; v_ws text; v_user text; v_src text[]; v_ids text[]; v_touched uuid[];
        v_sessions integer; v_fences integer; v_evidence integer; v_deleted integer; v_rewritten integer;
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
  IF EXISTS (SELECT 1 FROM sessions.relayfacts_evidence_v1 AS e JOIN sessions.relayfacts_v1 AS t ON t.id = e.decision_id
              JOIN unnest(v_src, v_ids) AS u(s, i) ON e.source = u.s AND e.session_id = u.i
              WHERE t.org_id = v_org AND t.workspace_id = v_ws) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH015', MESSAGE = 'relayfacts user erase: evidence remains';
  END IF;
  RETURN jsonb_build_object('sessions', v_sessions, 'fencesAdded', v_fences, 'evidenceRows', v_evidence,
                            'recordsDeleted', v_deleted, 'recordsRewritten', v_rewritten);
END
$relayfacts_user_erase$;

REVOKE ALL ON FUNCTION sessions.relayfacts_user_erase_v1(text,text,text,text),
  sessions.relayfacts_user_erase_lock_v1(), sessions.relayfacts_user_erase_guard_v1()
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api, bulls96_session_ingest,
       bulls96_session_ingest_seeder;
