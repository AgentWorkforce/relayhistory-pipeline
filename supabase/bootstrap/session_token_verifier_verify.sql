-- Read-only verification of the auth token verifier role
-- (supabase/bootstrap/session_token_verifier.sql; the 20261101000100 migration).
-- Raises, listing every problem, unless the role has exactly the approved shape:
-- NOLOGIN and no elevated attributes, member of no role, members limited to the
-- separately approved `authenticator` edge (never ADMIN) and the Supabase PG16+
-- `postgres` creator edge, USAGE on schema sessions, no CREATE, no table or
-- column privilege anywhere in schema sessions, and EXECUTE on exactly
-- sessions.auth_session_verify(text, timestamptz) among sessions functions.
DO $verify$
DECLARE
  v_role pg_roles%ROWTYPE;
  v_problems text[] := ARRAY[]::text[];
  v_row record;
  v_fn oid := 'sessions.auth_session_verify(text,timestamptz)'::regprocedure;
  v_version int := current_setting('server_version_num')::int;
BEGIN
  SELECT * INTO v_role FROM pg_roles WHERE rolname = 'session_token_verifier';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'session token verifier: session_token_verifier does not exist';
  END IF;
  IF v_role.rolcanlogin THEN v_problems := array_append(v_problems, ('role can log in')::text); END IF;
  IF v_role.rolinherit THEN v_problems := array_append(v_problems, ('role has INHERIT')::text); END IF;
  IF v_role.rolbypassrls THEN v_problems := array_append(v_problems, ('role has BYPASSRLS')::text); END IF;
  IF v_role.rolsuper OR v_role.rolcreatedb OR v_role.rolcreaterole OR v_role.rolreplication THEN
    v_problems := array_append(v_problems, ('role has SUPERUSER, CREATEDB, CREATEROLE or REPLICATION')::text);
  END IF;
  FOR v_row IN SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid WHERE m.member = v_role.oid LOOP
    v_problems := array_append(v_problems, (format('member of %I', v_row.rolname))::text);
  END LOOP;
  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT u.rolname, g.rolname AS grantor, g.rolsuper AS grantor_super, m.admin_option, m.inherit_option, m.set_option
         FROM pg_auth_members m JOIN pg_roles u ON u.oid = m.member JOIN pg_roles g ON g.oid = m.grantor
        WHERE m.roleid = $1' USING v_role.oid LOOP
      IF v_row.rolname = 'postgres' AND v_row.grantor = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL; -- Supabase PG16+ creator edge
      ELSIF v_row.rolname = 'authenticator' AND NOT v_row.admin_option THEN
        NULL; -- separately approved PostgREST switch-to edge
      ELSE
        v_problems := array_append(v_problems, (format('unexpected member %I', v_row.rolname))::text);
      END IF;
    END LOOP;
  ELSE
    FOR v_row IN SELECT u.rolname, m.admin_option FROM pg_auth_members m
                   JOIN pg_roles u ON u.oid = m.member WHERE m.roleid = v_role.oid LOOP
      IF NOT (v_row.rolname = 'authenticator' AND NOT v_row.admin_option) THEN
        v_problems := array_append(v_problems, (format('unexpected member %I', v_row.rolname))::text);
      END IF;
    END LOOP;
  END IF;

  -- No direct table, view or column privilege of any kind in sessions. Direct
  -- grants only (aclexplode): PUBLIC-derived effective rights are not this
  -- role's grants and are the sessions function boundary's domain.
  FOR v_row IN
    SELECT c.oid::regclass::text AS rel, a.privilege_type AS priv
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace,
           aclexplode(c.relacl) a
     WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
       AND a.grantee = v_role.oid
  LOOP
    v_problems := array_append(v_problems, (format('%s on %s', v_row.priv, v_row.rel))::text);
  END LOOP;
  FOR v_row IN
    SELECT format('%I.%I', c.relname, at.attname) AS col, a.privilege_type AS priv
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute at ON at.attrelid = c.oid AND at.attnum > 0 AND NOT at.attisdropped,
           aclexplode(at.attacl) a
     WHERE n.nspname = 'sessions' AND a.grantee = v_role.oid
  LOOP
    v_problems := array_append(v_problems, (format('%s on %s', v_row.priv, v_row.col))::text);
  END LOOP;
  -- Schema sessions: exactly one direct USAGE grant, nothing else.
  IF NOT EXISTS (SELECT 1 FROM pg_namespace n, aclexplode(n.nspacl) a
                  WHERE n.nspname = 'sessions' AND a.grantee = v_role.oid
                    AND a.privilege_type = 'USAGE') THEN
    v_problems := array_append(v_problems, ('missing USAGE on schema sessions')::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_namespace n, aclexplode(n.nspacl) a
              WHERE n.nspname = 'sessions' AND a.grantee = v_role.oid
                AND (a.privilege_type <> 'USAGE' OR a.is_grantable)) THEN
    v_problems := array_append(v_problems, ('schema sessions: privileges other than USAGE')::text);
  END IF;

  -- EXECUTE on exactly the one approved RPC: direct grants via proacl only.
  -- PUBLIC-executable functions are not this role's grants (the boundary and
  -- the live gate own that check). Function identity is by OID.
  IF NOT EXISTS (SELECT 1 FROM pg_proc p, aclexplode(p.proacl) a
                  WHERE p.oid = v_fn AND a.grantee = v_role.oid
                    AND a.privilege_type = 'EXECUTE' AND NOT a.is_grantable) THEN
    v_problems := array_append(v_problems, (format('missing EXECUTE on %s', v_fn::regprocedure))::text);
  END IF;
  FOR v_row IN
    SELECT p.oid::regprocedure::text AS fn
      FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace,
           aclexplode(p.proacl) a
     WHERE n.nspname = 'sessions' AND a.grantee = v_role.oid
       AND (p.oid <> v_fn OR a.privilege_type <> 'EXECUTE' OR a.is_grantable)
  LOOP
    v_problems := array_append(v_problems, (format('%s: unapproved direct privilege', v_row.fn))::text);
  END LOOP;
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE oid = v_fn AND prosecdef) THEN
    v_problems := array_append(v_problems, ('sessions.auth_session_verify(text,timestamptz) is missing or not SECURITY DEFINER')::text);
  END IF;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION 'session token verifier is not in the approved shape: %', array_to_string(v_problems, '; ');
  END IF;
  RAISE NOTICE 'session token verifier: PASS';
END
$verify$;
