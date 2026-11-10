-- Registry slot 20261105031000 (reserved follow-up to 20261105022000; never renamed).
-- Closes the desktop catalog activation blockers that need SQL (docs/session-desktop-catalog.md):
-- a forgotten child re-queues every live parent that named it, a restored child re-queues the
-- parents whose projection dropped a forgotten child, the read RPC refuses while any live parent
-- in the workspace waits for reprojection, and >500 incoming parents (or children) refuse
-- instead of returning a truncated success.
--
-- Dormant on apply: two empty tables, one control row (off), one constant-default column, gated
-- triggers whose first operation reads the control row, and owner-only functions. Applying this
-- file creates no derived state and takes no per-row lock in any live write path. Nothing runs
-- until the owner calls sessions.set_session_desktop_catalog_forget_tracking(true).
-- Fail fast instead of queueing live readers/writers behind a blocked DDL lock (migrate.ts applies
-- each file in one transaction, so SET LOCAL lasts exactly as long as this file).
SET LOCAL lock_timeout='5s';

-- Written only by the 20261105022000 desktop catalog writer (SESSION_DESKTOP_CATALOG_ENABLED=1):
-- true when its last projection dropped at least one edge because the child is a tombstone.
-- The legacy writer never touches it. Constant default: no table rewrite.
ALTER TABLE sessions.session_directory
 ADD COLUMN catalog_forgotten_child_edges boolean NOT NULL DEFAULT false;

CREATE TABLE sessions.session_desktop_catalog_control(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 forget_tracking boolean NOT NULL DEFAULT false
);
INSERT INTO sessions.session_desktop_catalog_control(singleton,forget_tracking) VALUES(true,false);
-- Parent identities only: an erased child's id is never stored here.
--   pending: the parent may lack an edge (a child it named was forgotten, or a forgotten child
--            came back); the read RPC refuses its workspace until it is reprojected.
--   watch:   the parent's last projection dropped a forgotten child; it becomes pending when a
--            tombstone in its (org, workspace, harness) is restored or purged.
CREATE TABLE sessions.session_desktop_catalog_reproject(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 state text NOT NULL CHECK(state IN('pending','watch')),
 marked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,harness,session_id)
);
CREATE INDEX session_desktop_catalog_reproject_pending_idx
 ON sessions.session_desktop_catalog_reproject(org_id,workspace_id) WHERE state='pending';
CREATE INDEX session_desktop_catalog_reproject_watch_idx
 ON sessions.session_desktop_catalog_reproject(org_id,workspace_id,harness,session_id) WHERE state='watch';
ALTER TABLE sessions.session_desktop_catalog_control ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_desktop_catalog_reproject ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_desktop_catalog_control,sessions.session_desktop_catalog_reproject
 FROM PUBLIC,anon,authenticated,service_role;

-- Owner-only switch (no grants). Waits out directory writers (SHARE lock) so no tombstone,
-- restore or catalog write straddles the flip. Enabling invalidates every desktop catalog
-- readiness marker and empties the queue: forgets and restores during a disabled interval were
-- not tracked, so the catalog stays refused until a complete reprojection after this call
-- (which also rebuilds the watch rows). Disabling makes the read RPC refuse.
CREATE FUNCTION sessions.set_session_desktop_catalog_forget_tracking(p_enabled boolean)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp SET lock_timeout='5s' AS $set_tracking$
DECLARE v_enabled boolean;
BEGIN
 IF p_enabled IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='desktop catalog: forget tracking must be boolean'; END IF;
 SELECT forget_tracking INTO v_enabled FROM sessions.session_desktop_catalog_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='desktop catalog: control row missing'; END IF;
 IF v_enabled=p_enabled THEN RETURN v_enabled; END IF;
 LOCK TABLE sessions.session_directory IN SHARE MODE;
 IF p_enabled THEN
  UPDATE sessions.session_directory SET catalog_read_version=0 WHERE catalog_read_version<>0;
  DELETE FROM sessions.session_desktop_catalog_reproject;
 END IF;
 UPDATE sessions.session_desktop_catalog_control SET forget_tracking=p_enabled WHERE singleton;
 RETURN p_enabled;
END $set_tracking$;

-- Forget (tombstone or hard delete) of a session: every LIVE parent holding an edge to it becomes
-- pending, and the session's own queue row goes. Bounded by the incoming edges that
-- session_directory_forget_catalog deletes in the same statement (one index lookup, one queue row
-- per parent, no directory row lock, no workspace scan). The trigger names sort before
-- session_directory_forget_catalog_on_*, so the edges still exist when this runs.
CREATE FUNCTION sessions.session_desktop_catalog_mark_parents()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $mark_parents$
DECLARE k sessions.session_directory%ROWTYPE;
BEGIN
 -- Must stay first: while forget tracking is off this trigger reads one row and returns.
 IF NOT coalesce((SELECT forget_tracking FROM sessions.session_desktop_catalog_control WHERE singleton),false) THEN RETURN NULL; END IF;
 IF TG_OP='DELETE' THEN k:=OLD; ELSE k:=NEW; END IF;
 INSERT INTO sessions.session_desktop_catalog_reproject AS q(org_id,workspace_id,harness,session_id,state)
 SELECT DISTINCT r.org_id,r.workspace_id,r.harness,r.parent_session_id,'pending'
   FROM sessions.session_directory_relationships r
   JOIN sessions.session_directory p ON p.org_id=r.org_id AND p.workspace_id=r.workspace_id
    AND p.harness=r.harness AND p.session_id=r.parent_session_id AND p.deleted_at IS NULL
  WHERE r.org_id=k.org_id AND r.workspace_id=k.workspace_id AND r.harness=k.harness
    AND r.child_session_id=k.session_id AND r.parent_session_id<>k.session_id
  ORDER BY r.parent_session_id
 ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET state='pending',marked_at=clock_timestamp();
 DELETE FROM sessions.session_desktop_catalog_reproject q
  WHERE q.org_id=k.org_id AND q.workspace_id=k.workspace_id AND q.harness=k.harness AND q.session_id=k.session_id;
 RETURN NULL;
END $mark_parents$;
CREATE TRIGGER session_directory_catalog_mark_parents_on_tombstone
 AFTER INSERT OR UPDATE OF deleted_at ON sessions.session_directory
 FOR EACH ROW WHEN (NEW.deleted_at IS NOT NULL)
 EXECUTE FUNCTION sessions.session_desktop_catalog_mark_parents();
CREATE TRIGGER session_directory_catalog_mark_parents_on_delete
 AFTER DELETE ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.session_desktop_catalog_mark_parents();

-- A tombstone restored (deleted_at back to NULL) or purged (hard delete of a tombstone): the
-- parents whose projection dropped a forgotten child in this (org, workspace, harness) become
-- pending. The erased child's id was never kept, so this cannot narrow further; it touches only
-- watch rows (locked in key order, the same order the forget trigger uses), never directory rows.
CREATE FUNCTION sessions.session_desktop_catalog_requeue_watchers()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $requeue_watchers$
DECLARE k sessions.session_directory%ROWTYPE;
BEGIN
 -- Must stay first: while forget tracking is off this trigger reads one row and returns.
 IF NOT coalesce((SELECT forget_tracking FROM sessions.session_desktop_catalog_control WHERE singleton),false) THEN RETURN NULL; END IF;
 IF TG_OP='DELETE' THEN k:=OLD; ELSE k:=NEW; END IF;
 WITH w AS MATERIALIZED(
  SELECT q.org_id,q.workspace_id,q.harness,q.session_id FROM sessions.session_desktop_catalog_reproject q
   WHERE q.org_id=k.org_id AND q.workspace_id=k.workspace_id AND q.harness=k.harness AND q.state='watch'
   ORDER BY q.session_id FOR UPDATE)
 UPDATE sessions.session_desktop_catalog_reproject q SET state='pending',marked_at=clock_timestamp()
  FROM w WHERE q.org_id=w.org_id AND q.workspace_id=w.workspace_id AND q.harness=w.harness AND q.session_id=w.session_id
   AND q.state='watch';
 RETURN NULL;
END $requeue_watchers$;
CREATE TRIGGER session_directory_catalog_requeue_on_restore
 AFTER UPDATE OF deleted_at ON sessions.session_directory
 FOR EACH ROW WHEN (OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL)
 EXECUTE FUNCTION sessions.session_desktop_catalog_requeue_watchers();
CREATE TRIGGER session_directory_catalog_requeue_on_purge
 AFTER DELETE ON sessions.session_directory
 FOR EACH ROW WHEN (OLD.deleted_at IS NOT NULL)
 EXECUTE FUNCTION sessions.session_desktop_catalog_requeue_watchers();

-- A desktop catalog projection of a live row (only the 20261105022000 writer sets
-- catalog_read_projected_at) settles that row's queue entry: watch if it dropped a forgotten
-- child, otherwise none. The writer holds the row FOR UPDATE and FOR SHARE locks on every child
-- it names, so a concurrent forget or restore of one of those children either committed before
-- the writer read it or re-queues this parent after the writer commits.
CREATE FUNCTION sessions.session_desktop_catalog_projected()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $projected$
BEGIN
 -- Must stay first: while forget tracking is off this trigger reads one row and returns.
 IF NOT coalesce((SELECT forget_tracking FROM sessions.session_desktop_catalog_control WHERE singleton),false) THEN RETURN NULL; END IF;
 IF NEW.catalog_forgotten_child_edges THEN
  INSERT INTO sessions.session_desktop_catalog_reproject AS q(org_id,workspace_id,harness,session_id,state)
  VALUES(NEW.org_id,NEW.workspace_id,NEW.harness,NEW.session_id,'watch')
  ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET state='watch',marked_at=clock_timestamp();
 ELSE
  DELETE FROM sessions.session_desktop_catalog_reproject q
   WHERE q.org_id=NEW.org_id AND q.workspace_id=NEW.workspace_id AND q.harness=NEW.harness AND q.session_id=NEW.session_id;
 END IF;
 RETURN NULL;
END $projected$;
CREATE TRIGGER session_directory_catalog_projected
 AFTER UPDATE OF catalog_read_projected_at ON sessions.session_directory
 FOR EACH ROW WHEN (NEW.deleted_at IS NULL)
 EXECUTE FUNCTION sessions.session_desktop_catalog_projected();

-- Owner-only (no grants), read-only page of live pending parents in (org, workspace, harness,
-- session) "C" order after an exclusive cursor. It takes no row lock: the caller reprojects each
-- parent (network reads included) with no transaction open, and the writer's completion settles
-- the queue row. Resume with the last returned key; restart from NULL to retry failures.
CREATE FUNCTION sessions.session_desktop_catalog_reproject_page(p_after_org text,p_after_workspace text,p_after_harness text,p_after_session text,p_limit integer DEFAULT 100)
RETURNS TABLE(org_id text,workspace_id text,harness text,session_id text,event_projection_epoch text)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $reproject_page$
BEGIN
 IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 500 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='desktop catalog reproject: invalid page size';
 END IF;
 IF num_nulls(p_after_org,p_after_workspace,p_after_harness,p_after_session) NOT IN(0,4) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='desktop catalog reproject: invalid cursor';
 END IF;
 IF NOT coalesce((SELECT c.forget_tracking FROM sessions.session_desktop_catalog_control c WHERE c.singleton),false) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='desktop catalog reproject: forget tracking is disabled';
 END IF;
 RETURN QUERY
 SELECT q.org_id,q.workspace_id,q.harness,q.session_id,d.event_projection_epoch::text
   FROM sessions.session_desktop_catalog_reproject q
   JOIN sessions.session_directory d ON d.org_id=q.org_id AND d.workspace_id=q.workspace_id
    AND d.harness=q.harness AND d.session_id=q.session_id AND d.deleted_at IS NULL
  WHERE q.state='pending'
    AND (p_after_org IS NULL OR (q.org_id COLLATE "C",q.workspace_id COLLATE "C",q.harness COLLATE "C",q.session_id COLLATE "C")
         >(p_after_org COLLATE "C",p_after_workspace COLLATE "C",p_after_harness COLLATE "C",p_after_session COLLATE "C"))
  ORDER BY q.org_id COLLATE "C",q.workspace_id COLLATE "C",q.harness COLLATE "C",q.session_id COLLATE "C"
  LIMIT p_limit;
END $reproject_page$;

-- The read RPC from 20261105022000 (same signature, owner, SECURITY DEFINER and ACL), now also
-- refusing (0A000) while forget tracking is off, while any live parent in the workspace is
-- pending reprojection, and when a session has more than 500 parents or children. The response
-- shape is unchanged ('truncated' is always false). The rollback restores the 022000 body.
CREATE OR REPLACE FUNCTION sessions.session_directory_desktop_catalog(p_workspace_id text,p_harness text,p_session_id text)
RETURNS SETOF jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,sessions,pg_temp AS $catalog$
DECLARE
 v_org text; d sessions.session_directory%ROWTYPE; v_models text[];
 v_parents jsonb;v_children jsonb;v_body jsonb;
BEGIN
 v_org:=sessions.session_directory_events_require_ready(p_workspace_id);
 IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
 OR p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='desktop catalog: invalid identity';
 END IF;
 -- Without forget tracking a restored child's parent edges cannot be trusted.
 IF NOT coalesce((SELECT c.forget_tracking FROM sessions.session_desktop_catalog_control c WHERE c.singleton),false) THEN
  RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='desktop catalog: forget_tracking_not_enabled';
 END IF;
 -- An unprojected parent's edge can be absent entirely. A scoped lookup alone
 -- cannot prove a child has no parent, so require the whole metadata edge index.
 IF EXISTS(SELECT 1 FROM sessions.session_directory WHERE org_id=v_org AND workspace_id=p_workspace_id AND deleted_at IS NULL
   AND (catalog_read_version<>1 OR catalog_read_epoch IS DISTINCT FROM event_projection_epoch
        OR catalog_read_projected_at IS DISTINCT FROM catalog_projected_at OR NOT catalog_relationships_complete)) THEN
  RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='desktop catalog: relationship_metadata_not_ready';
 END IF;
 -- A live parent that lost (or may have lost) an edge to a forgotten or restored child.
 IF EXISTS(SELECT 1 FROM sessions.session_desktop_catalog_reproject q JOIN sessions.session_directory p
   ON p.org_id=q.org_id AND p.workspace_id=q.workspace_id AND p.harness=q.harness AND p.session_id=q.session_id AND p.deleted_at IS NULL
   WHERE q.org_id=v_org AND q.workspace_id=p_workspace_id AND q.state='pending') THEN
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
 SELECT coalesce(jsonb_agg(edge ORDER BY parent_id,uid),'[]'::jsonb) INTO v_parents
 FROM (SELECT r.parent_session_id parent_id,r.relationship_uid uid,jsonb_build_object(
   'relationshipUid',r.relationship_uid,'parentSessionId',r.parent_session_id,'childSessionId',r.child_session_id,
   'childModel',r.child_model,'childAgentName',r.child_agent_name,'childAgentType',r.child_agent_type) edge
  FROM sessions.session_directory_relationships r WHERE r.org_id=v_org AND r.workspace_id=p_workspace_id
    AND r.harness=p_harness AND r.child_session_id=p_session_id ORDER BY r.parent_session_id,r.relationship_uid LIMIT 501) parents;
 SELECT coalesce(jsonb_agg(edge ORDER BY uid),'[]'::jsonb) INTO v_children
 FROM (SELECT r.relationship_uid uid,jsonb_build_object(
   'relationshipUid',r.relationship_uid,'parentSessionId',r.parent_session_id,'childSessionId',r.child_session_id,
   'childModel',r.child_model,'childAgentName',r.child_agent_name,'childAgentType',r.child_agent_type) edge
  FROM sessions.session_directory_relationships r WHERE r.org_id=v_org AND r.workspace_id=p_workspace_id
    AND r.harness=p_harness AND r.parent_session_id=p_session_id ORDER BY r.relationship_uid LIMIT 501) children;
 -- Refuse rather than acknowledge a partial edge list.
 IF jsonb_array_length(v_parents)>500 OR jsonb_array_length(v_children)>500 THEN
  RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='desktop catalog: relationship_fanout_exceeded';
 END IF;
 v_body:=jsonb_build_object('version',1,'exists',true,
  'identity',jsonb_build_object('orgId',v_org,'workspaceId',p_workspace_id,'source',p_harness,'sessionId',p_session_id),
  'epoch',d.event_projection_epoch::text,'catalogProjectedAt',d.catalog_read_projected_at,
  'modelsComplete',true,'catalog',jsonb_build_object(
    'title',d.title,'gitBranch',d.git_branch,'repoUrl',d.git_repo,'initialCommit',d.initial_commit,
    'projectKey',d.project_key,'projectKeyMethod',d.project_key_method,'models',to_jsonb(v_models),
    'originator',d.originator,'agentVersion',d.agent_version),
  'parents',v_parents,'children',v_children,'truncated',false);
 RETURN NEXT v_body||jsonb_build_object('fingerprint',encode(sha256(convert_to(v_body::text,'UTF8')),'hex'));RETURN;
END $catalog$;

REVOKE ALL ON FUNCTION sessions.set_session_desktop_catalog_forget_tracking(boolean),
 sessions.session_desktop_catalog_mark_parents(),sessions.session_desktop_catalog_requeue_watchers(),
 sessions.session_desktop_catalog_projected(),
 sessions.session_desktop_catalog_reproject_page(text,text,text,text,integer)
 FROM PUBLIC,anon,authenticated,service_role;
