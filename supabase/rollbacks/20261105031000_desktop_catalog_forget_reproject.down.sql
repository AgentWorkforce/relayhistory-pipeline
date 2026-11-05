-- Rollback for registry slot 20261105031000 (desktop catalog forget/reproject).
-- Operator-only: first turn SESSION_DESKTOP_CATALOG_ENABLED off (the desktop catalog writer
-- writes catalog_forgotten_child_edges) and stop every desktop catalog reader; the down file is
-- not an authorization to change production schema. Restores the 20261105022000 read RPC body
-- (its ACL is kept by CREATE OR REPLACE) and drops only what 20261105031000 added.
DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105031000_desktop_catalog_forget_reproject.sql'')'
      INTO v_later;
    IF v_later THEN
      RAISE EXCEPTION USING ERRCODE='55000',
        MESSAGE='desktop catalog forget/reproject: a later migration is applied; roll it back first';
    END IF;
  END IF;
END
$rollback_guard$;
CREATE OR REPLACE FUNCTION sessions.session_directory_desktop_catalog(p_workspace_id text,p_harness text,p_session_id text)
RETURNS SETOF jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,sessions,pg_temp AS $catalog$
DECLARE
 v_org text; d sessions.session_directory%ROWTYPE; v_models text[];
 v_parents jsonb;v_children jsonb;v_body jsonb;v_truncated boolean;
BEGIN
 v_org:=sessions.session_directory_events_require_ready(p_workspace_id);
 IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
 OR p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='desktop catalog: invalid identity';
 END IF;
 -- An unprojected parent's edge can be absent entirely. A scoped lookup alone
 -- cannot prove a child has no parent, so require the whole metadata edge index.
 IF EXISTS(SELECT 1 FROM sessions.session_directory WHERE org_id=v_org AND workspace_id=p_workspace_id AND deleted_at IS NULL
   AND (catalog_read_version<>1 OR catalog_read_epoch IS DISTINCT FROM event_projection_epoch
        OR catalog_read_projected_at IS DISTINCT FROM catalog_projected_at OR NOT catalog_relationships_complete)) THEN
  RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='desktop catalog: relationship_metadata_not_ready';
 END IF;
 SELECT * INTO d FROM sessions.session_directory WHERE org_id=v_org AND workspace_id=p_workspace_id
  AND harness=p_harness AND session_id=p_session_id AND deleted_at IS NULL;
 IF NOT FOUND THEN
  v_body:=jsonb_build_object('version',1,'exists',false,'identity',jsonb_build_object('orgId',v_org,'workspaceId',p_workspace_id,'source',p_harness,'sessionId',p_session_id));
  RETURN NEXT v_body||jsonb_build_object('fingerprint',encode(sha256(convert_to(v_body::text,'UTF8')),'hex'));RETURN;
 END IF;
 IF NOT d.catalog_models_complete THEN
  RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='desktop catalog: model_metadata_not_complete';
 END IF;
 SELECT coalesce(array_agg(model ORDER BY model COLLATE "C"),'{}') INTO v_models
 FROM (SELECT DISTINCT value AS model FROM unnest(d.catalog_models||d.event_models) value WHERE value IS NOT NULL AND value<>'') all_models;
 SELECT coalesce(jsonb_agg(edge ORDER BY parent_id,uid),'[]'::jsonb),count(*)>500 INTO v_parents,v_truncated
 FROM (SELECT r.parent_session_id parent_id,r.relationship_uid uid,jsonb_build_object(
   'relationshipUid',r.relationship_uid,'parentSessionId',r.parent_session_id,'childSessionId',r.child_session_id,
   'childModel',r.child_model,'childAgentName',r.child_agent_name,'childAgentType',r.child_agent_type) edge
  FROM sessions.session_directory_relationships r WHERE r.org_id=v_org AND r.workspace_id=p_workspace_id
    AND r.harness=p_harness AND r.child_session_id=p_session_id ORDER BY r.parent_session_id,r.relationship_uid LIMIT 501) parents;
 IF jsonb_array_length(v_parents)>500 THEN v_parents:=v_parents-500; END IF;
 SELECT coalesce(jsonb_agg(edge ORDER BY uid),'[]'::jsonb),v_truncated OR count(*)>500 INTO v_children,v_truncated
 FROM (SELECT r.relationship_uid uid,jsonb_build_object(
   'relationshipUid',r.relationship_uid,'parentSessionId',r.parent_session_id,'childSessionId',r.child_session_id,
   'childModel',r.child_model,'childAgentName',r.child_agent_name,'childAgentType',r.child_agent_type) edge
  FROM sessions.session_directory_relationships r WHERE r.org_id=v_org AND r.workspace_id=p_workspace_id
    AND r.harness=p_harness AND r.parent_session_id=p_session_id ORDER BY r.relationship_uid LIMIT 501) children;
 IF jsonb_array_length(v_children)>500 THEN v_children:=v_children-500; END IF;
 v_body:=jsonb_build_object('version',1,'exists',true,
  'identity',jsonb_build_object('orgId',v_org,'workspaceId',p_workspace_id,'source',p_harness,'sessionId',p_session_id),
  'epoch',d.event_projection_epoch::text,'catalogProjectedAt',d.catalog_read_projected_at,
  'modelsComplete',true,'catalog',jsonb_build_object(
    'title',d.title,'gitBranch',d.git_branch,'repoUrl',d.git_repo,'initialCommit',d.initial_commit,
    'projectKey',d.project_key,'projectKeyMethod',d.project_key_method,'models',to_jsonb(v_models),
    'originator',d.originator,'agentVersion',d.agent_version),
  'parents',v_parents,'children',v_children,'truncated',v_truncated);
 RETURN NEXT v_body||jsonb_build_object('fingerprint',encode(sha256(convert_to(v_body::text,'UTF8')),'hex'));RETURN;
END $catalog$;
DROP TRIGGER session_directory_catalog_projected ON sessions.session_directory;
DROP TRIGGER session_directory_catalog_requeue_on_purge ON sessions.session_directory;
DROP TRIGGER session_directory_catalog_requeue_on_restore ON sessions.session_directory;
DROP TRIGGER session_directory_catalog_mark_parents_on_delete ON sessions.session_directory;
DROP TRIGGER session_directory_catalog_mark_parents_on_tombstone ON sessions.session_directory;
DROP FUNCTION sessions.session_desktop_catalog_reproject_page(text,text,text,text,integer);
DROP FUNCTION sessions.session_desktop_catalog_projected();
DROP FUNCTION sessions.session_desktop_catalog_requeue_watchers();
DROP FUNCTION sessions.session_desktop_catalog_mark_parents();
DROP FUNCTION sessions.set_session_desktop_catalog_forget_tracking(boolean);
DROP TABLE sessions.session_desktop_catalog_reproject;
DROP TABLE sessions.session_desktop_catalog_control;
ALTER TABLE sessions.session_directory DROP COLUMN catalog_forgotten_child_edges;
DO $ledger$
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    DELETE FROM sessions.__96bulls_migrations
      WHERE name='upstream:20261105031000_desktop_catalog_forget_reproject.sql';
  END IF;
END
$ledger$;
