-- Proposed 062290 component. Compose under the release migrator transaction; no live activation.
CREATE TABLE sessions.publication_scheduler_fleets (
  fleet text PRIMARY KEY CHECK (fleet ~ '^[a-f0-9]{64}$'),
  login_oid oid NOT NULL UNIQUE,
  login_name name NOT NULL UNIQUE
);
CREATE TABLE sessions.publication_scheduler_scopes (
  scope text PRIMARY KEY CHECK (scope ~ '^[a-f0-9]{64}$'),
  fleet text NOT NULL REFERENCES sessions.publication_scheduler_fleets(fleet)
);

CREATE TABLE sessions.publication_retry_checkpoints (
  scope text PRIMARY KEY CHECK (scope ~ '^[a-f0-9]{64}$'),
  version bigint NOT NULL CHECK (version BETWEEN 1 AND 9007199254740991),
  scan jsonb NOT NULL CHECK (jsonb_typeof(scan)='object'),
  retry_turn boolean NOT NULL
);
CREATE TABLE sessions.publication_receipt_retries (
  scope text NOT NULL REFERENCES sessions.publication_retry_checkpoints(scope),
  stage_token uuid NOT NULL,
  attempts integer NOT NULL CHECK (attempts BETWEEN 1 AND 6),
  state text NOT NULL CHECK (state='audit' OR (state='exhausted' AND attempts=6) OR (state='retry' AND attempts<6)),
  due_ms bigint NOT NULL CHECK (due_ms BETWEEN 0 AND 9007199254740991),
  PRIMARY KEY (scope,stage_token)
);
CREATE INDEX publication_receipt_retries_due ON sessions.publication_receipt_retries(scope,due_ms,stage_token)
  WHERE state='retry';

CREATE TABLE sessions.publication_fleet_cursors (
  fleet text PRIMARY KEY CHECK (fleet ~ '^[a-f0-9]{64}$'),
  last_org text NOT NULL CHECK (last_org ~ '^[a-f0-9]{64}$'),
  origins jsonb NOT NULL CHECK (jsonb_typeof(origins)='object'),
  last_scope text NOT NULL CHECK (last_scope ~ '^[a-f0-9]{64}$'),
  lease_token uuid,
  lease_until timestamptz,
  CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);

-- Owner controls both allowlists. Removing a binding immediately hides its state;
-- checkpoint/retry/lease rows intentionally have no cascading FK to these grants.
-- Remove provider-specific default grants, including column privileges inherited
-- through a table-wide grant. Only the separately reviewed login receives access.
DO $isolate$
DECLARE t text; grantee_oid oid; grantee_sql text;
BEGIN
  FOREACH t IN ARRAY ARRAY['publication_scheduler_fleets','publication_scheduler_scopes',
      'publication_fleet_cursors','publication_retry_checkpoints','publication_receipt_retries'] LOOP
    FOR grantee_oid IN
      SELECT DISTINCT a.grantee FROM pg_catalog.pg_class c
      CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
      WHERE c.oid=pg_catalog.to_regclass('sessions.'||t) AND a.grantee<>c.relowner
    LOOP
      grantee_sql := CASE WHEN grantee_oid=0 THEN 'PUBLIC'
        ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(grantee_oid)) END;
      EXECUTE format('REVOKE ALL ON TABLE sessions.%I FROM %s',t,grantee_sql);
    END LOOP;
    EXECUTE format('ALTER TABLE sessions.%I ENABLE ROW LEVEL SECURITY',t);
  END LOOP;
END $isolate$;

-- Use the authenticated login and its OID, never a user-settable GUC or a
-- caller-supplied tenant. A role switch cannot gain another login's bindings.
CREATE POLICY publication_own_fleet ON sessions.publication_scheduler_fleets FOR SELECT
  USING (current_user=session_user AND login_name=session_user
    AND login_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user));
CREATE POLICY publication_own_scopes ON sessions.publication_scheduler_scopes FOR SELECT
  USING (EXISTS (SELECT FROM sessions.publication_scheduler_fleets f WHERE f.fleet=publication_scheduler_scopes.fleet));

CREATE POLICY publication_fleet_cursor ON sessions.publication_fleet_cursors
  USING (EXISTS (SELECT FROM sessions.publication_scheduler_fleets f WHERE f.fleet=publication_fleet_cursors.fleet))
  WITH CHECK (EXISTS (SELECT FROM sessions.publication_scheduler_scopes s
      WHERE s.fleet=publication_fleet_cursors.fleet AND s.scope=publication_fleet_cursors.last_scope));
CREATE POLICY publication_scoped_checkpoint ON sessions.publication_retry_checkpoints
  USING (EXISTS (SELECT FROM sessions.publication_scheduler_scopes s WHERE s.scope=publication_retry_checkpoints.scope))
  WITH CHECK (EXISTS (SELECT FROM sessions.publication_scheduler_scopes s WHERE s.scope=publication_retry_checkpoints.scope));
CREATE POLICY publication_retry_read ON sessions.publication_receipt_retries FOR SELECT
  USING (EXISTS (SELECT FROM sessions.publication_scheduler_scopes s WHERE s.scope=publication_receipt_retries.scope));
CREATE POLICY publication_retry_insert ON sessions.publication_receipt_retries FOR INSERT
  WITH CHECK (EXISTS (SELECT FROM sessions.publication_scheduler_scopes s WHERE s.scope=publication_receipt_retries.scope));
CREATE POLICY publication_retry_update ON sessions.publication_receipt_retries FOR UPDATE
  USING (state='retry' AND EXISTS (SELECT FROM sessions.publication_scheduler_scopes s WHERE s.scope=publication_receipt_retries.scope))
  WITH CHECK (EXISTS (SELECT FROM sessions.publication_scheduler_scopes s WHERE s.scope=publication_receipt_retries.scope));
CREATE POLICY publication_retry_delete ON sessions.publication_receipt_retries FOR DELETE
  USING (state='retry' AND EXISTS (SELECT FROM sessions.publication_scheduler_scopes s WHERE s.scope=publication_receipt_retries.scope));
