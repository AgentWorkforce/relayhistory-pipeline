-- Slot 20261105062090. Memory documents follow-up: the operator per-session erase reaches decision records
-- (docs/memory-documents.md, Retention). Enables nothing: the self-host operator erase calls the erase function
-- only when SUMMARY_DECISION_RECORDS_ENABLED=true on its process. Code only: applying this to any real database
-- needs Khaliq's separate, explicit OK.
--
-- decision_evidence_erase_session_v1 runs inside the operator erase lease (ingest_operator_runtime_authorize),
-- before the metadata purge, as bulls96_session_ingest. For the erase key's scope (session_erasure_scopes: the
-- key session and its alias ids) it deletes every evidence row of that history source in the workspace. Then:
--   * a record left with no evidence is deleted (its title and statement came only from erased sessions);
--   * a record that keeps evidence from other sessions is kept, and its statement is replaced by the claim of
--     its oldest remaining evidence row, so no statement text written from the erased session survives. The
--     title is kept: every remaining row was merged under the same title_key.
-- Idempotent: a retry deletes nothing more and returns zero counts.
--
-- decision_evidence_fence_guard_v1 refuses (RH016) an evidence row whose (workspace, source, session) is
-- fenced by an operator erase, so a late analysis write cannot restore it. The whole insert batch rolls back,
-- so no record is left without evidence; the analysis hook counts the failure and the analysis is unaffected.

CREATE FUNCTION sessions.decision_evidence_erase_session_v1(p_org text, p_ws text, p_source text, p_session text, p_lease uuid)
RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $decision_evidence_erase$
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
    SELECT t.id FROM sessions.decision_records_v1 AS t
     WHERE t.org_id = p_org AND t.workspace_id = p_ws
       AND EXISTS (SELECT 1 FROM sessions.decision_evidence_v1 AS e
                    WHERE e.decision_id = t.id AND e.source = p_source AND e.session_id = ANY (v_ids))
     ORDER BY t.id FOR UPDATE) AS l;
  DELETE FROM sessions.decision_evidence_v1 AS e
   WHERE e.decision_id = ANY (v_touched) AND e.source = p_source AND e.session_id = ANY (v_ids);
  GET DIAGNOSTICS v_evidence = ROW_COUNT;
  DELETE FROM sessions.decision_records_v1 AS t
   WHERE t.id = ANY (v_touched)
     AND NOT EXISTS (SELECT 1 FROM sessions.decision_evidence_v1 AS e WHERE e.decision_id = t.id);
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  UPDATE sessions.decision_records_v1 AS t
     SET statement = (SELECT e.claim FROM sessions.decision_evidence_v1 AS e
                       WHERE e.decision_id = t.id ORDER BY e.created_at, e.id LIMIT 1),
         updated_at = now()
   WHERE t.id = ANY (v_touched);
  GET DIAGNOSTICS v_rewritten = ROW_COUNT;
  IF EXISTS (SELECT 1 FROM sessions.decision_evidence_v1 AS e JOIN sessions.decision_records_v1 AS t ON t.id = e.decision_id
              WHERE t.org_id = p_org AND t.workspace_id = p_ws AND e.source = p_source AND e.session_id = ANY (v_ids)) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH015', MESSAGE = 'decision records erase: evidence remains';
  END IF;
  RETURN jsonb_build_object('evidenceRows', v_evidence, 'recordsDeleted', v_deleted, 'recordsRewritten', v_rewritten);
END
$decision_evidence_erase$;

CREATE FUNCTION sessions.decision_evidence_fence_guard_v1() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $decision_evidence_fence_guard$
BEGIN
  IF EXISTS (SELECT 1 FROM sessions.decision_records_v1 AS t JOIN sessions.session_erasure_fences AS f
              ON f.org_id = t.org_id AND f.workspace_id = t.workspace_id
             WHERE t.id = NEW.decision_id AND f.source = NEW.source AND f.session_id = NEW.session_id) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH016', MESSAGE = 'decision records: evidence session is erased';
  END IF;
  RETURN NEW;
END
$decision_evidence_fence_guard$;
CREATE TRIGGER decision_evidence_fence_guard BEFORE INSERT OR UPDATE ON sessions.decision_evidence_v1
  FOR EACH ROW EXECUTE FUNCTION sessions.decision_evidence_fence_guard_v1();

REVOKE ALL ON FUNCTION sessions.decision_evidence_erase_session_v1(text,text,text,text,uuid),
  sessions.decision_evidence_fence_guard_v1()
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.decision_evidence_erase_session_v1(text,text,text,text,uuid) TO bulls96_session_ingest;
