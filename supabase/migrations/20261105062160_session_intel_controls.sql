-- M3-1: default-deny controls for optional session intelligence. No rows are
-- seeded and no feature is enabled. Operators, never runtime roles, write rows.
CREATE TABLE sessions.session_intel_controls (
  scope text NOT NULL CHECK (scope IN ('global', 'workspace')),
  org_id text,
  workspace_id text,
  feature text NOT NULL CHECK (feature IN ('advisor', 'conflicts', 'dm', 'claims', 'regen')),
  enabled boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by text NOT NULL CHECK (char_length(updated_by) BETWEEN 1 AND 128),
  CONSTRAINT session_intel_controls_scope CHECK (
    (scope = 'global' AND org_id IS NULL AND workspace_id IS NULL) OR
    (scope = 'workspace' AND org_id IS NOT NULL AND char_length(org_id) BETWEEN 1 AND 512
      AND workspace_id IS NOT NULL AND char_length(workspace_id) BETWEEN 1 AND 512)
  ),
  UNIQUE NULLS NOT DISTINCT (scope, org_id, workspace_id, feature)
);
ALTER TABLE sessions.session_intel_controls ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_intel_controls FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;

CREATE FUNCTION sessions.session_intel_controls_v1(p_org_id text, p_workspace_id text)
RETURNS TABLE (feature text, enabled boolean)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $controls$
BEGIN
  IF p_org_id IS NULL OR char_length(p_org_id) NOT BETWEEN 1 AND 512
     OR p_workspace_id IS NULL OR char_length(p_workspace_id) NOT BETWEEN 1 AND 512 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session intelligence controls: invalid tenant';
  END IF;
  RETURN QUERY
    SELECT f.name, coalesce(g.enabled, false) AND coalesce(w.enabled, false)
    FROM (VALUES ('advisor'), ('conflicts'), ('dm'), ('claims'), ('regen')) AS f(name)
    LEFT JOIN sessions.session_intel_controls AS g
      ON g.scope = 'global' AND g.feature = f.name AND g.org_id IS NULL AND g.workspace_id IS NULL
    LEFT JOIN sessions.session_intel_controls AS w
      ON w.scope = 'workspace' AND w.feature = f.name AND w.org_id = p_org_id AND w.workspace_id = p_workspace_id
    ORDER BY f.name;
END $controls$;
REVOKE ALL ON FUNCTION sessions.session_intel_controls_v1(text,text)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.session_intel_controls_v1(text,text) TO service_role;
