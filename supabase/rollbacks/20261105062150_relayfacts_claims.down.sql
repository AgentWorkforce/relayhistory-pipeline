-- Rolls back 20261105062150 relayfacts claims: restores 20261105062090's relayfacts_evidence_erase_session_v1 and
-- 20261105062100's relayfacts_user_erase_v1 bodies exactly (CREATE OR REPLACE keeps their owner and grants), drops
-- relayfacts_claims_insert_v1, relayfacts_claims_supersede_v1, relayfacts_claims_v1 and relayfacts_claims_fence_guard_v1,
-- the tables sessions.relayfacts_claim_evidence_v1 and sessions.relayfacts_claims_v1 (their triggers and indexes go
-- with them), and the ledger row. sessions.relayfacts_v1 and sessions.relayfacts_evidence_v1 are not touched: 062150
-- never changed them, so decision records stay exactly as they are and no index is rebuilt.
-- Intent and constraint rows: while any row exists in relayfacts_claims_v1 this refuses (55000) and changes nothing.
-- To drop them, set the override in the same session before running this file (it opens its own transaction, so
-- SET LOCAL cannot precede it), for example
--   psql -v ON_ERROR_STOP=1 -c "SET relayfacts.rollback_drop_claims = 'true'" -f <this file>
-- Anything but exactly 'true' (unset included) refuses. With it, every intent and constraint claim and its evidence
-- is DELETED with the tables and is not restored by re-applying 062150; later analyses write them again only while
-- SESSION_INTEL_CLAIMS_ENABLED=true. Turn SESSION_INTEL_CLAIMS_ENABLED off first: without 062150 the claims writer
-- and reader are gone and the analysis hook only counts failures (claim_records_failed).
-- Guard, restore and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts claims: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062150_relayfacts_claims.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts claims: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062150_relayfacts_claims.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'252fc5bec7af162f07022f89581e6c015ec21f360c60e2144edcec1e234bb735'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts claims: own applied checksum required';END IF;
 LOCK TABLE sessions.relayfacts_claims_v1, sessions.relayfacts_claim_evidence_v1 IN ACCESS EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.relayfacts_claims_v1)
    AND coalesce(current_setting('relayfacts.rollback_drop_claims',true),'')<>'true'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts claims: intent or constraint claims exist; set relayfacts.rollback_drop_claims=true to delete them';END IF;
END $rollback_guard$;
CREATE OR REPLACE FUNCTION sessions.relayfacts_evidence_erase_session_v1(p_org text, p_ws text, p_source text, p_session text, p_lease uuid)
RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $relayfacts_evidence_erase$
DECLARE v_ids text[]; v_touched uuid[]; v_evidence integer; v_deleted integer; v_rewritten integer;
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
  IF EXISTS (SELECT 1 FROM sessions.relayfacts_evidence_v1 AS e JOIN sessions.relayfacts_v1 AS t ON t.id = e.decision_id
              WHERE t.org_id = p_org AND t.workspace_id = p_ws AND e.source = p_source AND e.session_id = ANY (v_ids)) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH015', MESSAGE = 'decision records erase: evidence remains';
  END IF;
  RETURN jsonb_build_object('evidenceRows', v_evidence, 'recordsDeleted', v_deleted, 'recordsRewritten', v_rewritten);
END
$relayfacts_evidence_erase$;

CREATE OR REPLACE FUNCTION sessions.relayfacts_user_erase_v1(p_request text, p_org text, p_ws text, p_user text)
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

DROP FUNCTION sessions.relayfacts_claims_insert_v1(text,text,jsonb),
 sessions.relayfacts_claims_supersede_v1(text,text,uuid,uuid),
 sessions.relayfacts_claims_v1(text,text,text,text,timestamptz,integer);
DROP TABLE sessions.relayfacts_claim_evidence_v1;
DROP TABLE sessions.relayfacts_claims_v1;
DROP FUNCTION sessions.relayfacts_claims_fence_guard_v1();
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062150_relayfacts_claims.sql';
COMMIT;
