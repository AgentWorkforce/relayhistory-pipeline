-- Bare search_consumer_runtime role: created before every migrate, like the
-- other session_*_roles bootstraps. A migration never creates or alters a
-- role (a CNPG/Cloud SQL migration owner has no CREATEROLE), so this file
-- provisions the bare role wherever the migrator may; on self-host the
-- deploy step runs it as the owner (CREATEROLE suffices on PG15 and below,
-- PG16+; role creation needs no other grant), on Cloud SQL and RDS as the
-- constrained owner, and on Supabase the runbook runs it as postgres.
-- The role is NOLOGIN with no grants here: the dedicated LOGIN and the
-- grants come later from the owner-approved consumer bootstrap
-- (supabase/bootstrap/session_consumer_role.sql), which verifies the bare
-- shape before promoting it. Never alter an existing role here (no LOGIN,
-- no password, no settings): once the consumer bootstrap has claimed it,
-- this file must not touch it, only fail closed when the spec drifts.
-- Changing this file also changes the checked generation hash.
DO $consumer_roles$
DECLARE
  v_role text := 'search_consumer_runtime';
  v_count integer;
BEGIN
  IF current_setting('server_version_num', true)::integer < 150000 THEN
    RAISE EXCEPTION USING ERRCODE = '0A000',
      MESSAGE = 'consumer roles bootstrap: Postgres 15+ required, refusing to provision ' || v_role;
  END IF;
  SELECT count(*) INTO v_count FROM pg_roles WHERE rolname = v_role;
  IF v_count = 0 THEN
    EXECUTE format('CREATE ROLE %I NOLOGIN NOINHERIT', v_role);
  END IF;
  -- Bare-role spec: non-login, non-superuser, non-replication, no bypass,
  -- no createdb/createrole, no inherit. Owner and membership stay whatever
  -- the creator set (a NOLOGIN role has no login to hijack and no grants to
  -- inherit through yet).
  PERFORM 1 FROM pg_roles
   WHERE rolname = v_role
     AND rolcanlogin IS FALSE
     AND rolsuper IS FALSE
     AND rolreplication IS FALSE
     AND rolbypassrls IS FALSE
     AND rolcreatedb IS FALSE
     AND rolcreaterole IS FALSE
     AND rolinherit IS FALSE;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '28000',
      MESSAGE = 'consumer roles bootstrap: refusing to run with an elevated ' || v_role;
  END IF;
END
$consumer_roles$;
