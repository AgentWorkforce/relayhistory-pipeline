-- Bob/Referee metadata follows an authorized physical-pointer tombstone/delete.
-- Installation changes no existing row. Scope is the complete physical tenant
-- key, so another machine, harness, workspace or org keeps its own derivatives.
-- This function is trigger-only: no app role can call an erasure definer RPC.
CREATE FUNCTION sessions.forget_pointer_derivatives()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $forget$
BEGIN
  DELETE FROM sessions.session_summaries AS s
   WHERE s.org_id = OLD.org_id AND s.workspace_id = OLD.workspace_id
     AND s.harness = OLD.harness AND s.session_id = OLD.session_id
     AND s.machine_id = OLD.machine_id;
  DELETE FROM sessions.session_verdicts AS v
   WHERE v.org_id = OLD.org_id AND v.workspace_id = OLD.workspace_id
     AND v.harness = OLD.harness AND v.session_id = OLD.session_id
     AND v.machine_id = OLD.machine_id;
  RETURN NULL;
END
$forget$;

REVOKE ALL ON FUNCTION sessions.forget_pointer_derivatives()
  FROM PUBLIC, anon, authenticated, service_role;

CREATE TRIGGER session_index_forget_tombstoned_derivatives
  AFTER UPDATE OF deleted_at ON sessions.session_index
  FOR EACH ROW WHEN (NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.forget_pointer_derivatives();
CREATE TRIGGER session_index_forget_deleted_derivatives
  AFTER DELETE ON sessions.session_index
  FOR EACH ROW EXECUTE FUNCTION sessions.forget_pointer_derivatives();
