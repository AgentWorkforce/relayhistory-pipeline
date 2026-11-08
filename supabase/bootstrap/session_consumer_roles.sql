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
--
-- Steady state across migrates: the first migrate provisions the bare role
-- (NOLOGIN). The operator bootstrap then promotes it to the dedicated LOGIN
-- and records the claim as the role's statement_timeout (60s). Every later
-- migrate must accept exactly those two shapes — bare, or claimed with the
-- timeout marker — and refuse anything else, so an install running the
-- writer can keep migrating. Memberships are checked in both directions in
-- every state: at most the precise creator edge the bootstrap superuser or
-- CREATEROLE owner unavoidably gains (member WITH ADMIN, PG16+: no INHERIT
-- and no SET; PG15: admin flag only), and the role itself is never a member
-- of another role. The grant migration runs after this step, so a forged or
-- borrowed membership can never silently inherit the twelve EXECUTEs.
DO $consumer_roles$
DECLARE
  v_role text := 'search_consumer_runtime';
  v_count integer;
  v_version integer := current_setting('server_version_num', true)::integer;
BEGIN
  IF v_version < 150000 THEN
    RAISE EXCEPTION USING ERRCODE = '0A000',
      MESSAGE = 'consumer roles bootstrap: Postgres 15+ required, refusing to provision ' || v_role;
  END IF;
  SELECT count(*) INTO v_count FROM pg_roles WHERE rolname = v_role;
  IF v_count = 0 THEN
    EXECUTE format('CREATE ROLE %I NOLOGIN NOINHERIT', v_role);
  END IF;
  -- Memberships in both directions, before any grant migration: tolerate at
  -- most the precise creator edge (the creator is a member of the role WITH
  -- ADMIN; the writer itself is a member of nothing). Mirrors the operator
  -- bootstrap's edge tolerance, including the PG15 admin-flag-only branch.
  IF v_version >= 160000 THEN
    IF EXISTS (SELECT 1 FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member
               WHERE (a.roleid = (SELECT oid FROM pg_roles WHERE rolname = v_role)
                  OR a.member = (SELECT oid FROM pg_roles WHERE rolname = v_role))
                 AND NOT (a.roleid = (SELECT oid FROM pg_roles WHERE rolname = v_role)
                          AND m.rolname = current_user AND a.admin_option
                          AND NOT a.inherit_option AND NOT a.set_option)) THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = 'consumer roles bootstrap: role has memberships beyond the creator edge',
        HINT = 'Nothing was changed. Revoke the membership by hand (an operator decision), then rerun.';
    END IF;
  ELSE
    IF EXISTS (SELECT 1 FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member
               WHERE (a.roleid = (SELECT oid FROM pg_roles WHERE rolname = v_role)
                  OR a.member = (SELECT oid FROM pg_roles WHERE rolname = v_role))
                 AND NOT (a.roleid = (SELECT oid FROM pg_roles WHERE rolname = v_role)
                          AND m.rolname = current_user AND a.admin_option)) THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = 'consumer roles bootstrap: role has memberships beyond the creator edge',
        HINT = 'Nothing was changed. Revoke the membership by hand (an operator decision), then rerun.';
    END IF;
  END IF;
  -- Bare-role spec: non-login, non-superuser, non-replication, no bypass,
  -- no createdb/createrole, no inherit. Owner and membership stay whatever
  -- the creator set (a NOLOGIN role has no login to hijack and no grants to
  -- inherit through yet).
  -- Claimed-role spec: the operator bootstrap promotes this same role to the
  -- dedicated LOGIN and records the claim as statement_timeout = 60s. Accept
  -- exactly that shape (every other attribute still false, setting present)
  -- so later migrates converge; refuse a LOGIN without the claim marker,
  -- which was never promoted by the operator bootstrap.
  PERFORM 1 FROM pg_roles
   WHERE rolname = v_role
     AND rolsuper IS FALSE
     AND rolreplication IS FALSE
     AND rolbypassrls IS FALSE
     AND rolcreatedb IS FALSE
     AND rolcreaterole IS FALSE
     AND rolinherit IS FALSE
     AND ((rolcanlogin IS FALSE)
          OR (rolcanlogin IS TRUE
              AND EXISTS (SELECT 1 FROM pg_db_role_setting s
                           WHERE s.setrole = pg_roles.oid AND s.setdatabase = 0
                             AND 'statement_timeout=60s' = ANY (s.setconfig))));
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '28000',
      MESSAGE = 'consumer roles bootstrap: refusing to run with an unexpected ' || v_role || ' shape (want bare NOLOGIN, or operator-claimed LOGIN with the 60s timeout marker)';
  END IF;
END
$consumer_roles$;
