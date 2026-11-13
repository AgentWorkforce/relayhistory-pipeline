-- SLOT 057030. Forward fix to 20261105044070 (privacy, #566). sessions.ingest_selfhost_set_session_uploads (the
-- self-host per-session erase) tombstones the caller's pointer and, with no live pointer left, the session_directory
-- row, but never touched the directory's membership companions. session_directory_users and
-- session_directory_machines (and the machine_ids/user_ids arrays) kept the erased srv_ machine and the caller, so
-- the list's user=/machine= filters still matched an erased copy, and the native catalog activation audit
-- (src/hoopdb/native-agent-catalog-activation.ts) counted each such row in orphanMachines, which keeps activation
-- ineligible (every count but liveRows must be 0).
--
-- Inside the per-pair loop, after the existing pointer and directory writes, for each directory row the erase already
-- targets (d.session_id IN(pair session_id, its canonical id)), the function now applies the projector's membership
-- rule (src/hoopdb/session-directory-projector.ts, tombstone and membership branches; the same rows the operator purge
-- in 20261105050020 deletes):
--   * tombstoned row: delete every users/machines row and set machine_ids='{}' and user_ids='{}';
--   * live row: delete the erased pointer's machine (coalesce(canonical_machine_id,machine_id)) unless another live
--     pointer of the logical session still carries it, and the erased pointer's user unless that user still has a
--     live pointer there; array_remove the same ids from machine_ids/user_ids.
-- "Live pointer of the logical session" is coalesce(canonical_session_id,session_id)=d.session_id AND deleted_at IS
-- NULL, the rule this function already uses to tombstone the row and the audit uses for orphanMachines.
--
-- No flag: it restores the membership state the projector already produces for the same pointer tombstone event.
--
-- Replacement only: the body is 044070's byte for byte plus one DECLARE line and the membership block above;
-- signature, VOLATILE, SECURITY DEFINER, search_path, owner and ACL are unchanged (CREATE OR REPLACE keeps them). The
-- return shape {updated} is unchanged. sessions.ingest_publication_retire_fence is not replaced.
--
-- Does not change: membership rows left behind by erases before this migration stay until the projector re-projects
-- the session or an operator cleans them up; that is an operator step, not applied here.
--
-- Code only: applying this to any real database needs Khaliq's separate, explicit OK.
CREATE OR REPLACE FUNCTION sessions.ingest_selfhost_set_session_uploads(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $selfhost_selection$
DECLARE
 v_org text;v_ws text;v_user text;v_named text;v_origin text;v_sessions jsonb;v_machine text;v_pair record;v_canonical text;
 v_updated bigint:=0;
 v_member_machine text;v_member_user text;v_dir record;
 c_erased_revision CONSTANT bigint:=9007199254740991;
 c_turn_origin CONSTANT text:='relayhistory:conversation_turns:v1';
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','sessions','enabled']);
 IF p->'enabled' IS DISTINCT FROM 'false'::jsonb THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: only enabled:false is served here';
 END IF;
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);
 v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);
 v_named:=sessions.ingest_identifier(p->'originId',false);
 -- The claim's lock (ingest_lock_fences), held before the ownership read below. A claim that holds it
 -- first finishes, and its ownership row is visible to that read; one that comes later waits for this erase.
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 -- Listed before any fence: the selection deletes the ownership rows of the origin it fences.
 FOR v_origin,v_sessions IN
  WITH pairs AS (SELECT DISTINCT f.source,f.session_id FROM sessions.ingest_fence_pairs(p->'sessions') AS f),
  targets AS (
   SELECT o.origin_id,x.source,x.session_id FROM pairs AS x JOIN sessions.ingest_session_ownership AS o
    ON o.org_id=v_org AND o.workspace_id=v_ws AND o.user_id=v_user AND o.source=x.source AND o.session_id=x.session_id
   UNION SELECT v_named,x.source,x.session_id FROM pairs AS x
   UNION SELECT c_turn_origin,x.source,x.session_id FROM pairs AS x)
  SELECT t.origin_id,jsonb_agg(jsonb_build_array(t.source,t.session_id)) FROM targets AS t GROUP BY t.origin_id ORDER BY t.origin_id COLLATE "C"
 LOOP
  v_updated:=v_updated+(sessions.ingest_set_session_uploads(jsonb_build_object('tenant',p->'tenant','originId',v_origin,'sessions',v_sessions,'enabled',false))->>'updated')::bigint;
  -- src/session-ingest/writer.ts liveMachineId: the machine this origin's manifests are written under.
  v_machine:='srv_'||left(encode(sha256(convert_to(v_ws||chr(31)||v_origin||chr(31)||v_user,'UTF8')),'hex'),32);
  FOR v_pair IN SELECT f.source,f.session_id FROM sessions.ingest_fence_pairs(v_sessions) AS f LOOP
   v_canonical:=NULL;
   UPDATE sessions.session_index AS s SET deleted_at=coalesce(s.deleted_at,clock_timestamp()),indexed_at=clock_timestamp(),
     revision=greatest(s.revision,c_erased_revision)
    WHERE s.org_id=v_org AND s.workspace_id=v_ws AND s.harness=v_pair.source AND s.session_id=v_pair.session_id AND s.machine_id=v_machine
    RETURNING coalesce(s.canonical_session_id,s.session_id) INTO v_canonical;
   -- Only a session this caller actually has a pointer for: a selection naming someone else's session id
   -- must not touch that session's directory row.
   IF NOT FOUND THEN CONTINUE;END IF;
   -- The retire trigger's directory write: clear derived text, tombstone only with no live pointer left.
   UPDATE sessions.session_directory AS d SET short_summary=NULL,short_summary_scrub_ruleset=NULL,short_summary_scrub_version=NULL,
     short_summary_source=NULL,short_summary_generated_at=NULL,
     search_text=sessions.session_directory_search_text(d.title,d.task_title,d.agent_name,d.project_id,d.git_repo,d.git_branch,d.model,NULL),
     deleted_at=CASE WHEN EXISTS(SELECT 1 FROM sessions.session_index AS q WHERE q.org_id=d.org_id AND q.workspace_id=d.workspace_id
       AND q.harness=d.harness AND coalesce(q.canonical_session_id,q.session_id)=d.session_id AND q.deleted_at IS NULL)
      THEN d.deleted_at ELSE coalesce(d.deleted_at,clock_timestamp()) END
    WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.harness=v_pair.source AND d.session_id IN(v_pair.session_id,v_canonical);
   -- 057030 (#566): the erased pointer's directory membership, kept as src/hoopdb/session-directory-projector.ts keeps
   -- it: machines are coalesce(canonical_machine_id,machine_id) and users are user_id of the LIVE pointers of the logical
   -- session; a tombstoned row has empty arrays and no membership rows at all.
   SELECT coalesce(s.canonical_machine_id,s.machine_id),s.user_id INTO v_member_machine,v_member_user FROM sessions.session_index AS s
    WHERE s.org_id=v_org AND s.workspace_id=v_ws AND s.harness=v_pair.source AND s.session_id=v_pair.session_id AND s.machine_id=v_machine;
   FOR v_dir IN SELECT d.session_id,d.deleted_at IS NOT NULL AS gone,
      EXISTS(SELECT 1 FROM sessions.session_index AS q WHERE q.org_id=d.org_id AND q.workspace_id=d.workspace_id AND q.harness=d.harness
        AND coalesce(q.canonical_session_id,q.session_id)=d.session_id AND q.deleted_at IS NULL
        AND coalesce(q.canonical_machine_id,q.machine_id)=v_member_machine) AS machine_live,
      EXISTS(SELECT 1 FROM sessions.session_index AS q WHERE q.org_id=d.org_id AND q.workspace_id=d.workspace_id AND q.harness=d.harness
        AND coalesce(q.canonical_session_id,q.session_id)=d.session_id AND q.deleted_at IS NULL
        AND q.user_id=v_member_user) AS user_live
     FROM sessions.session_directory AS d
     WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.harness=v_pair.source AND d.session_id IN(v_pair.session_id,v_canonical)
     ORDER BY d.session_id COLLATE "C"
   LOOP
    DELETE FROM sessions.session_directory_machines AS m WHERE m.org_id=v_org AND m.workspace_id=v_ws AND m.harness=v_pair.source
      AND m.session_id=v_dir.session_id AND (v_dir.gone OR (m.machine_id=v_member_machine AND NOT v_dir.machine_live));
    DELETE FROM sessions.session_directory_users AS u WHERE u.org_id=v_org AND u.workspace_id=v_ws AND u.harness=v_pair.source
      AND u.session_id=v_dir.session_id AND (v_dir.gone OR (u.user_id=v_member_user AND NOT v_dir.user_live));
    IF v_dir.gone OR NOT v_dir.machine_live OR NOT v_dir.user_live THEN
     UPDATE sessions.session_directory AS d SET
       machine_ids=CASE WHEN v_dir.gone THEN '{}'::text[] WHEN v_dir.machine_live THEN d.machine_ids ELSE array_remove(d.machine_ids,v_member_machine) END,
       user_ids=CASE WHEN v_dir.gone THEN '{}'::text[] WHEN v_dir.user_live THEN d.user_ids ELSE array_remove(d.user_ids,v_member_user) END
      WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.harness=v_pair.source AND d.session_id=v_dir.session_id;
    END IF;
   END LOOP;
  END LOOP;
 END LOOP;
 RETURN jsonb_build_object('updated',v_updated);
END $selfhost_selection$;
