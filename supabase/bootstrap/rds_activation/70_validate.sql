-- rds-activation/v1 fragment 70: validate the final role, membership, ownership, ACL and
-- default-ACL graph. Read-only.
--
-- hoopdb_activation.phase = 'activate': last fragment of the transaction, run as the
--   provisioner with no role set. A failure rolls the whole activation back.
-- hoopdb_activation.phase = 'verify': run alone in a READ ONLY transaction as hoop,
--   after the commit and on every later verify. From then on the sessions schema ACL
--   and hoop's own extra default ACLs belong to the migrator's contract, so this phase
--   does not compare them.
--
-- Target graph: the empty set. No pg_auth_members row may have a HoopDb role (or the
-- executor) as role, member or grantor. So hoop has no ADMIN over anything and is a
-- member of nothing (not rds_superuser), nobody can SET ROLE to or inherit any HoopDb
-- role, the provisioner keeps no edge, and the executor is gone.
DO $rds_activation_70$
DECLARE
  v_phase text := current_setting('hoopdb_activation.phase');
  v_provisioner text := current_setting('hoopdb_activation.provisioner');
  v_spec jsonb := current_setting('hoopdb_activation.role_spec')::jsonb;
  v_ours text[];
  v_bad text[] := '{}';
  v_row record;
  v_acl text[];
  v_want text[];
BEGIN
  SELECT array_agg(e->>'name' ORDER BY e->>'name') INTO v_ours FROM jsonb_array_elements(v_spec) e;
  IF v_phase = 'activate' THEN
    IF current_user::text <> v_provisioner OR session_user::text <> v_provisioner THEN
      v_bad := v_bad || 'activate phase: not the provisioner with no role set'::text;
    END IF;
  ELSIF v_phase = 'verify' THEN
    IF current_user::text <> 'hoop' OR session_user::text <> 'hoop' THEN
      v_bad := v_bad || 'verify phase: not connected as hoop without SET ROLE'::text;
    END IF;
  ELSE
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'rds-activation: unknown phase';
  END IF;
  IF current_database() <> 'hoopdb' THEN
    v_bad := v_bad || 'not connected to hoopdb'::text;
  END IF;
  IF NOT v_ours @> ARRAY['hoop', 'sessions_pipeline_runtime'] THEN
    v_bad := v_bad || 'role spec lacks hoop or sessions_pipeline_runtime'::text;
  END IF;

  -- No helper and no leftover capability.
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'hoopdb_activation_executor') THEN
    v_bad := v_bad || 'executor hoopdb_activation_executor still exists'::text;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m
              WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.oid = m.grantor)
                 OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.oid = m.member)
                 OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.oid = m.roleid)) THEN
    v_bad := v_bad || 'a membership edge references a dropped role'::text;
  END IF;
  FOR v_row IN
    SELECT pg_get_userbyid(m.roleid) AS role, pg_get_userbyid(m.member) AS member,
           pg_get_userbyid(m.grantor) AS grantor, m.admin_option, m.inherit_option, m.set_option
      FROM pg_catalog.pg_auth_members m
     WHERE m.roleid IN (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = ANY (v_ours))
        OR m.member IN (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = ANY (v_ours))
        OR m.grantor IN (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = ANY (v_ours))
     ORDER BY 1, 2, 3
  LOOP
    v_bad := v_bad || format('edge %I <- %I (grantor %I, ADMIN=%s INHERIT=%s SET=%s)', v_row.role, v_row.member,
                             v_row.grantor, v_row.admin_option, v_row.inherit_option, v_row.set_option)::text;
  END LOOP;

  -- Attributes and exact role settings, from the spec.
  IF (SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = ANY (v_ours)) <> cardinality(v_ours) THEN
    v_bad := v_bad || 'a HoopDb role is missing'::text;
  END IF;
  FOR v_row IN
    SELECT r.*, (e->>'login')::boolean AS want_login,
           coalesce((SELECT array_agg(s ORDER BY s) FROM jsonb_array_elements_text(e->'settings') s), '{}') AS want_settings,
           coalesce((SELECT array_agg(s ORDER BY s) FROM pg_catalog.pg_db_role_setting d, unnest(d.setconfig) s
                      WHERE d.setrole = r.oid AND d.setdatabase = 0), '{}') AS have_settings,
           EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting d WHERE d.setrole = r.oid AND d.setdatabase <> 0) AS db_settings
      FROM jsonb_array_elements(v_spec) e JOIN pg_catalog.pg_roles r ON r.rolname = e->>'name'
  LOOP
    IF v_row.rolcanlogin IS DISTINCT FROM v_row.want_login OR v_row.rolinherit OR v_row.rolsuper
       OR v_row.rolcreaterole OR v_row.rolcreatedb OR v_row.rolreplication OR v_row.rolbypassrls THEN
      v_bad := v_bad || format('%I has unexpected attributes', v_row.rolname)::text;
    END IF;
    IF v_row.have_settings IS DISTINCT FROM v_row.want_settings OR v_row.db_settings THEN
      v_bad := v_bad || format('%I settings %s, expected %s', v_row.rolname, v_row.have_settings, v_row.want_settings)::text;
    END IF;
  END LOOP;

  -- Ownership and database ACL: hoop owns hoopdb; each LOGIN role has CONNECT only;
  -- PUBLIC and the provisioner have nothing.
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'hoopdb' AND datdba = 'hoop'::regrole) THEN
    v_bad := v_bad || 'hoopdb is not owned by hoop'::text;
  END IF;
  SELECT coalesce(array_agg(x.e ORDER BY x.e), '{}') INTO v_acl
    FROM (SELECT format('%s:%s:%s:%s', coalesce(pg_get_userbyid(nullif(a.grantee, 0)), 'PUBLIC'),
                        a.privilege_type, pg_get_userbyid(a.grantor), a.is_grantable::text) AS e
            FROM pg_catalog.pg_database d, aclexplode(d.datacl) a WHERE d.datname = 'hoopdb') x;
  SELECT array_agg(x ORDER BY x) INTO v_want FROM (
    SELECT unnest(ARRAY['hoop:CONNECT:hoop:false', 'hoop:CREATE:hoop:false', 'hoop:TEMPORARY:hoop:false']) AS x
    UNION ALL
    SELECT format('%s:CONNECT:hoop:false', e->>'name') FROM jsonb_array_elements(v_spec) e
     WHERE (e->>'login')::boolean AND e->>'name' <> 'hoop') w;
  IF v_acl IS DISTINCT FROM v_want THEN
    v_bad := v_bad || format('hoopdb ACL is %s, expected %s', v_acl, v_want)::text;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'sessions' AND nspowner = 'hoop'::regrole) THEN
    v_bad := v_bad || 'schema sessions is missing or not owned by hoop'::text;
  ELSIF v_phase = 'activate' AND (SELECT nspacl IS NOT NULL FROM pg_catalog.pg_namespace WHERE nspname = 'sessions') THEN
    v_bad := v_bad || 'schema sessions has an ACL at activation'::text;
  END IF;

  -- Default ACLs in hoopdb: the three global function defaults are exactly
  -- {<self>:EXECUTE}. At activation they are the only rows. Later, hoop's own rows
  -- belong to the migrator's gate, but no other role may gain a row.
  FOR v_row IN SELECT unnest(ARRAY['hoop', 'rds_superuser', v_provisioner]) AS role LOOP
    SELECT coalesce(array_agg(x.e ORDER BY x.e), '{}') INTO v_acl
      FROM (SELECT format('%s:%s:%s', CASE WHEN a.grantee = d.defaclrole THEN '<self>'
                          ELSE coalesce(pg_get_userbyid(nullif(a.grantee, 0)), 'PUBLIC') END,
                          a.privilege_type, a.is_grantable::text) AS e
              FROM pg_catalog.pg_default_acl d, aclexplode(d.defaclacl) a
             WHERE d.defaclrole = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = v_row.role)
               AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f') x;
    IF v_acl IS DISTINCT FROM ARRAY['<self>:EXECUTE:false'] THEN
      v_bad := v_bad || format('%I global function default is not exactly {<self>:EXECUTE}: %s', v_row.role, v_acl)::text;
    END IF;
  END LOOP;
  FOR v_row IN
    SELECT pg_get_userbyid(d.defaclrole) AS role, d.defaclnamespace AS ns, d.defaclobjtype AS t
      FROM pg_catalog.pg_default_acl d
     WHERE NOT (d.defaclnamespace = 0 AND d.defaclobjtype = 'f'
                AND d.defaclrole IN (SELECT oid FROM pg_catalog.pg_roles WHERE rolname IN ('hoop', 'rds_superuser', v_provisioner)))
       AND NOT (v_phase = 'verify' AND d.defaclrole = 'hoop'::regrole)
  LOOP
    v_bad := v_bad || format('unexpected default ACL row for %I (%s %s)', v_row.role, v_row.ns, v_row.t)::text;
  END LOOP;

  -- No database-level session defaults in hoopdb (ALTER DATABASE hoopdb SET ..., or
  -- ALTER ROLE ... IN DATABASE hoopdb SET ...): a search_path or similar default set by
  -- the database owner must not steer this or any later session.
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting s
              WHERE s.setdatabase = (SELECT d.oid FROM pg_catalog.pg_database d WHERE d.datname = 'hoopdb')) THEN
    v_bad := v_bad || 'hoopdb has database-level role settings (ALTER DATABASE or ALTER ROLE ... IN DATABASE)'::text;
  END IF;
  -- No HoopDb role other than hoop holds CREATE or TEMPORARY on hoopdb or CREATE on sessions.
  FOR v_row IN SELECT unnest(v_ours) AS role LOOP
    CONTINUE WHEN v_row.role = 'hoop';
    IF has_database_privilege(v_row.role, 'hoopdb', 'CREATE')
       OR has_database_privilege(v_row.role, 'hoopdb', 'TEMPORARY')
       OR has_schema_privilege(v_row.role, 'sessions', 'CREATE') THEN
      v_bad := v_bad || format('%I holds CREATE or TEMPORARY', v_row.role)::text;
    END IF;
  END LOOP;

  IF cardinality(v_bad) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('rds-activation validation failed (%s phase): %s', v_phase, array_to_string(v_bad, '; '));
  END IF;
END
$rds_activation_70$;
