-- rds-activation/v1 fragment 00: read-only preconditions. Runs as the provisioning
-- login (the RDS master), connected to hoopdb, inside the driver's one transaction.
-- It changes nothing. Every failure is 0A000, 42501 or 55000, and the driver rolls back.
--
-- Fresh instance only. Every role in hoopdb_activation.role_spec (hoop,
-- sessions_pipeline_runtime and the complete fixed service-role set) and the executor
-- must be absent, and so must the schema sessions. Roles are instance-wide, and
-- after an activation nobody but rdsadmin holds ADMIN on them. So a role that already
-- exists, whether from an earlier activation, a restore of this instance or another
-- tool, can be neither adopted nor repaired here. It is refused with its problems
-- listed (SUPERUSER, memberships, members). The driver detects an activated install
-- from the shared catalogs and sends it to verify, because the provisioner can no
-- longer CONNECT to hoopdb.
DO $rds_activation_00$
DECLARE
  v_provisioner text := current_setting('hoopdb_activation.provisioner');
  v_spec jsonb := current_setting('hoopdb_activation.role_spec')::jsonb;
  v_verifiers jsonb := current_setting('hoopdb_activation.verifiers')::jsonb;
  v_me record;
  v_rds oid := (SELECT oid FROM pg_roles WHERE rolname = 'rds_superuser');
  v_row record;
  v_problems text[] := '{}';
  v_details text[];
BEGIN
  IF current_setting('server_version_num')::int < 160000 THEN
    -- PostgreSQL 15 records no creator ADMIN edge, and any CREATEROLE role there may
    -- alter any non-superuser role, so the grantor graph below does not hold. Refused.
    RAISE EXCEPTION USING ERRCODE = '0A000',
      MESSAGE = 'rds-activation/v1 requires PostgreSQL 16 or later (PostgreSQL 15 CREATEROLE semantics differ)';
  END IF;
  IF current_setting('hoopdb_activation.phase') <> 'activate' THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'rds-activation: fragment 00 runs only in the activate phase';
  END IF;
  IF session_user::text <> v_provisioner OR current_user::text <> v_provisioner THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = format('rds-activation: connect as the named provisioner %I without SET ROLE (session_user %I, current_user %I)',
                       v_provisioner, session_user, current_user);
  END IF;
  IF current_database() <> 'hoopdb' THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('rds-activation: connected to %I, not hoopdb', current_database());
  END IF;
  SELECT * INTO v_me FROM pg_roles WHERE rolname = current_user;
  IF v_me.rolsuper OR NOT v_me.rolcreaterole OR NOT v_me.rolcreatedb THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'rds-activation: the provisioner must be a non-superuser with CREATEROLE and CREATEDB (the RDS master)';
  END IF;
  IF v_rds IS NULL OR (SELECT rolsuper OR rolcanlogin FROM pg_roles WHERE oid = v_rds)
     OR NOT pg_has_role(v_me.oid, v_rds, 'USAGE') THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'rds-activation: rds_superuser must exist (NOLOGIN, not SUPERUSER) and the provisioner must inherit it';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rdsadmin' AND rolsuper AND oid <> v_me.oid) THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'rds-activation: the fixed rdsadmin SUPERUSER was not proven';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'hoopdb' AND datdba = v_me.oid) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'rds-activation: hoopdb must still be owned by the provisioner (fresh path)',
      HINT = 'Nothing was changed. An activated hoopdb is owned by hoop: run verify instead.';
  END IF;

  -- The role spec itself: hoop and the runtime are LOGIN, names are plain identifiers,
  -- and no reserved or platform name is present.
  IF jsonb_typeof(v_spec) <> 'array'
     OR NOT v_spec @> '[{"name": "hoop", "login": true}, {"name": "sessions_pipeline_runtime", "login": true}]'::jsonb
     OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_spec) e
                 WHERE NOT (e->>'name' ~ '^[a-z_][a-z0-9_]{0,62}$')
                    OR e->>'name' IN ('rdsadmin', 'rds_superuser', 'hoopdb_activation_executor', v_provisioner)
                    OR e->>'name' LIKE 'pg\_%' OR e->>'name' LIKE 'rds%'
                    OR jsonb_typeof(e->'login') <> 'boolean' OR jsonb_typeof(e->'settings') <> 'array')
     OR (SELECT count(DISTINCT e->>'name') FROM jsonb_array_elements(v_spec) e) <> jsonb_array_length(v_spec) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'rds-activation: malformed role spec';
  END IF;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hoopdb_activation_executor') THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'rds-activation: reserved executor role hoopdb_activation_executor already exists',
      HINT = 'Nothing was changed. This executor never leaves it behind; inspect and remove it as a separate operator action.';
  END IF;
  FOR v_row IN
    SELECT r.* FROM pg_roles r
     WHERE r.rolname IN (SELECT e->>'name' FROM jsonb_array_elements(v_spec) e)
     ORDER BY r.rolname
  LOOP
    v_details := '{}';
    IF v_row.rolsuper THEN v_details := v_details || 'SUPERUSER'::text; END IF;
    IF v_row.rolcreaterole THEN v_details := v_details || 'CREATEROLE'::text; END IF;
    IF v_row.rolcreatedb THEN v_details := v_details || 'CREATEDB'::text; END IF;
    IF v_row.rolreplication THEN v_details := v_details || 'REPLICATION'::text; END IF;
    IF v_row.rolbypassrls THEN v_details := v_details || 'BYPASSRLS'::text; END IF;
    IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member = v_row.oid) THEN
      v_details := v_details || 'is a member of another role'::text;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_auth_members WHERE roleid = v_row.oid) THEN
      v_details := v_details || 'has members'::text;
    END IF;
    v_problems := v_problems || format('%I already exists (%s)', v_row.rolname,
      coalesce(nullif(array_to_string(v_details, ', '), ''), 'roles are instance-wide; activation needs a fresh instance'))::text;
  END LOOP;

  IF to_regnamespace('sessions') IS NOT NULL THEN
    v_problems := v_problems || 'schema sessions already exists'::text;
  END IF;
  FOR v_row IN
    SELECT nspname FROM pg_namespace
     WHERE nspname NOT IN ('public', 'pg_catalog', 'information_schema', 'pg_toast')
       AND nspname !~ '^pg_(temp|toast_temp)_[0-9]+$'
     ORDER BY 1
  LOOP
    v_problems := v_problems || format('unexpected schema %I in hoopdb', v_row.nspname)::text;
  END LOOP;
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname <> 'plpgsql') THEN
    v_problems := v_problems || 'hoopdb has extensions besides plpgsql (expected a fresh template0 copy)'::text;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public')
     OR EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public') THEN
    v_problems := v_problems || 'schema public is not empty'::text;
  END IF;
  -- Default ACLs: the only rows allowed are the built-in owner/PUBLIC EXECUTE pieces
  -- of the provisioner's and rds_superuser's global function defaults (fragment 10 sets
  -- both). Any other row in this database is refused.
  FOR v_row IN
    SELECT pg_get_userbyid(d.defaclrole) AS role, d.defaclnamespace, d.defaclobjtype, d.defaclacl::text AS acl
      FROM pg_default_acl d
     WHERE NOT (
       d.defaclrole IN (v_me.oid, v_rds) AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'
       AND NOT EXISTS (SELECT 1 FROM aclexplode(d.defaclacl) a
                        WHERE NOT (a.privilege_type = 'EXECUTE' AND NOT a.is_grantable
                                   AND a.grantee IN (0, d.defaclrole))))
  LOOP
    v_problems := v_problems || format('unexpected default ACL for %I (%s %s): %s',
      v_row.role, v_row.defaclnamespace, v_row.defaclobjtype, v_row.acl)::text;
  END LOOP;

  -- Verifiers: exactly one well-formed, distinct SCRAM verifier per LOGIN role.
  IF jsonb_typeof(v_verifiers) <> 'object'
     OR (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(v_verifiers) k)
        IS DISTINCT FROM (SELECT array_agg(e->>'name' ORDER BY e->>'name') FROM jsonb_array_elements(v_spec) e
                           WHERE (e->>'login')::boolean)
     OR EXISTS (SELECT 1 FROM jsonb_each_text(v_verifiers) v
                 WHERE v.value !~ '^SCRAM-SHA-256\$4096:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+$')
     OR (SELECT count(DISTINCT v.value) FROM jsonb_each_text(v_verifiers) v)
        <> (SELECT count(*) FROM jsonb_each_text(v_verifiers) v) THEN
    v_problems := v_problems || 'password verifiers are missing, malformed, duplicated or do not match the LOGIN roles'::text;
  END IF;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'rds-activation refused (nothing changed): ' || array_to_string(v_problems, '; ');
  END IF;
END
$rds_activation_00$;
