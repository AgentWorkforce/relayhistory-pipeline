-- Separately approved OWNER bootstrap component. The caller supplies the
-- reviewed direct login, its one fleet digest and one exact authorized scope
-- digest with SET LOCAL in the surrounding transaction. Never a scheduler RPC.
SET LOCAL search_path = pg_catalog, pg_temp;
DO $bind$
DECLARE
  actor_name text := current_setting('publication_scheduler.login');
  fleet_key text := current_setting('publication_scheduler.fleet');
  scope_key text := current_setting('publication_scheduler.scope');
  actor oid;
BEGIN
  SELECT r.oid INTO actor FROM pg_catalog.pg_roles r WHERE r.rolname::text=actor_name
    AND r.rolcanlogin AND NOT (r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication);
  IF actor IS NULL OR EXISTS (SELECT FROM pg_catalog.pg_auth_members WHERE member=actor)
     OR pg_catalog.has_schema_privilege(actor,'sessions','CREATE')
     OR EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname=current_database() AND datdba=actor)
     OR EXISTS (SELECT FROM pg_catalog.pg_class WHERE relnamespace='sessions'::regnamespace AND relowner=actor)
     OR fleet_key !~ '^[a-f0-9]{64}$' OR scope_key !~ '^[a-f0-9]{64}$'
  THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='publication scheduler: dedicated unprivileged direct login and exact digests required'; END IF;

  INSERT INTO sessions.publication_scheduler_fleets(fleet,login_oid,login_name)
    VALUES(fleet_key,actor,actor_name) ON CONFLICT DO NOTHING;
  INSERT INTO sessions.publication_scheduler_scopes(scope,fleet)
    VALUES(scope_key,fleet_key) ON CONFLICT DO NOTHING;
  IF NOT EXISTS (SELECT FROM sessions.publication_scheduler_fleets f JOIN sessions.publication_scheduler_scopes s USING(fleet)
      WHERE f.fleet=fleet_key AND f.login_oid=actor AND f.login_name::text=actor_name AND s.scope=scope_key)
  THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='publication scheduler: binding conflict'; END IF;

  EXECUTE format('GRANT USAGE ON SCHEMA sessions TO %I', actor_name);
  EXECUTE format('GRANT SELECT ON sessions.publication_scheduler_fleets,sessions.publication_scheduler_scopes TO %I',actor_name);
  EXECUTE format('GRANT SELECT,INSERT,UPDATE(last_org,last_scope,origins,lease_token,lease_until) ON sessions.publication_fleet_cursors TO %I',actor_name);
  EXECUTE format('GRANT SELECT,INSERT,UPDATE(version,scan,retry_turn) ON sessions.publication_retry_checkpoints TO %I',actor_name);
  EXECUTE format('GRANT SELECT,INSERT,UPDATE(attempts,state,due_ms),DELETE ON sessions.publication_receipt_retries TO %I',actor_name);
END $bind$;
