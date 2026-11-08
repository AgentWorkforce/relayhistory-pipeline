-- Amazon RDS for PostgreSQL function boundary for the HoopDb migration owner
-- (HOOPDB_DB_MODE=rds; RDS for PostgreSQL only, NOT Aurora).
--
-- On RDS the master user is a member of rds_superuser, has CREATEROLE/CREATEDB and is NOT
-- a superuser; the one superuser is AWS's internal rdsadmin, whose global defaults a
-- customer cannot alter (ALTER DEFAULT PRIVILEGES FOR ROLE rdsadmin: permission denied).
-- The selfhosted hoopdb_function_default_revoke.sql therefore cannot run (it must be a
-- superuser, and it sets every superuser's default). In rds mode the catalog gate treats
-- that exact SUPERUSER as the provider trust root (RDS_PLATFORM_SUPERUSERS), while every
-- ordinary role able to CREATE in sessions remains covered. Same contract as the Cloud SQL
-- file (cloudsql_owner_function_default_revoke.sql), with the RDS trust root.
--
-- rds_superuser (NOLOGIN, not SUPERUSER, so not a trust root) has effective CREATE on
-- sessions on a real instance once the schema exists, so the gate requires it to hold the
-- exact default too: without it the first migrate passes (no schema yet) and every later
-- one fails function_boundary_unproven (#389). The master sets it through its inherited
-- rds_superuser membership.
--
-- Run this as the migration owner (the RDS master), connected to the HoopDb database,
-- before every migrate. It changes only the global function defaults of that owner and of
-- rds_superuser in the current database, together or not at all. It refuses before the
-- change unless:
--   * rds_superuser exists and is NOLOGIN and not SUPERUSER;
--   * the caller is not SUPERUSER, has CREATEROLE and inherits rds_superuser;
--   * rdsadmin exists, is SUPERUSER, and is not the migration owner;
--   * every existing sessions function belongs to the migration owner;
--   * no ordinary role other than the owner and rds_superuser can CREATE in sessions;
--   * no sessions-scoped function default grants another role; and
--   * neither global function default is poisoned.
--
-- Idempotent. The migrator independently re-derives the complete catalog evidence.
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> \
--     -f supabase/bootstrap/rds_owner_function_default_revoke.sql
DO $rds_function_boundary$
DECLARE
  v_me oid := (SELECT oid FROM pg_roles WHERE rolname = current_user);
  v_rds oid := (SELECT oid FROM pg_roles WHERE rolname = 'rds_superuser');
  v_bad text[] := '{}';
  v_row record;
  v_default text[];
  v_target oid;
  v_label text;
BEGIN
  IF current_setting('server_version_num')::int < 160000 THEN
    RAISE EXCEPTION USING ERRCODE = '0A000',
      MESSAGE = 'RDS function boundary requires PostgreSQL 16 or later';
  END IF;
  IF (SELECT rolsuper FROM pg_roles WHERE oid = v_me) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS function boundary: the migration owner must not be SUPERUSER';
  END IF;
  IF v_rds IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS function boundary: role rds_superuser does not exist (this file is for Amazon RDS for PostgreSQL only)';
  END IF;
  IF (SELECT rolsuper OR rolcanlogin FROM pg_roles WHERE oid = v_rds) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS function boundary: rds_superuser must be NOLOGIN and not SUPERUSER';
  END IF;
  IF NOT (SELECT rolcreaterole FROM pg_roles WHERE oid = v_me)
     OR NOT EXISTS (SELECT 1 FROM pg_roles r
                     WHERE r.rolname = 'rds_superuser' AND pg_has_role(v_me, r.oid, 'MEMBER')) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS function boundary: the migration owner must be a CREATEROLE member of rds_superuser';
  END IF;
  -- ALTER DEFAULT PRIVILEGES FOR ROLE needs the target's privileges, not bare membership.
  IF NOT pg_has_role(v_me, v_rds, 'USAGE') THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS function boundary: the migration owner must inherit rds_superuser to set its function default';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_roles
     WHERE rolname = 'rdsadmin' AND rolsuper AND oid <> v_me
  ) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS function boundary: the fixed rdsadmin SUPERUSER was not proven';
  END IF;

  IF to_regnamespace('sessions') IS NOT NULL THEN
    FOR v_row IN
      SELECT r.rolname
        FROM pg_roles r
       WHERE has_schema_privilege(r.oid, 'sessions', 'CREATE')
         AND r.oid <> v_me
         AND r.oid <> v_rds -- covered: its exact default is set and rechecked below
         AND NOT (r.rolname = 'rdsadmin' AND r.rolsuper)
       ORDER BY 1
    LOOP
      v_bad := array_append(v_bad, format('%I can CREATE in sessions', v_row.rolname)::text);
    END LOOP;
    IF EXISTS (
      SELECT 1 FROM pg_proc p
       WHERE p.pronamespace = to_regnamespace('sessions')::oid AND p.proowner <> v_me
    ) THEN
      v_bad := array_append(v_bad, 'a sessions function is not owned by the migration owner'::text);
    END IF;
    FOR v_row IN
      SELECT r.rolname, a.grantee, a.privilege_type
        FROM pg_default_acl d
        JOIN pg_roles r ON r.oid = d.defaclrole,
             aclexplode(d.defaclacl) a
       WHERE d.defaclnamespace = to_regnamespace('sessions')::oid
         AND d.defaclobjtype = 'f'
         AND a.grantee <> d.defaclrole
    LOOP
      v_bad := array_append(v_bad, format('%I has a sessions-scoped function default granting %s',
        v_row.rolname, v_row.privilege_type)::text);
    END LOOP;
  END IF;

  -- Before changing them, each global function default may contain only PostgreSQL's
  -- built-in owner/PUBLIC EXECUTE pieces, without grant options.
  FOREACH v_target IN ARRAY ARRAY[v_me, v_rds] LOOP
    v_label := CASE WHEN v_target = v_me THEN 'the migration owner' ELSE 'rds_superuser' END;
    IF EXISTS (
      SELECT 1
        FROM pg_default_acl d, aclexplode(d.defaclacl) a
       WHERE d.defaclrole = v_target AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'
         AND NOT (a.privilege_type = 'EXECUTE' AND NOT a.is_grantable AND a.grantee IN (0, v_target))
    ) THEN
      v_bad := array_append(v_bad, format('%s has a poisoned global function default', v_label)::text);
    END IF;
  END LOOP;
  IF cardinality(v_bad) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS function boundary refused (nothing changed): ' || array_to_string(v_bad, '; ');
  END IF;

  FOREACH v_target IN ARRAY ARRAY[v_me, v_rds] LOOP
    v_label := CASE WHEN v_target = v_me THEN 'the migration owner' ELSE 'rds_superuser' END;
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES FOR ROLE %I REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC',
      pg_get_userbyid(v_target)
    );

    SELECT coalesce(array_agg(entry ORDER BY entry), '{}') INTO v_default
      FROM (
        SELECT CASE WHEN a.grantee = v_target THEN '<self>' ELSE pg_get_userbyid(a.grantee) END
               || ':' || a.privilege_type || CASE WHEN a.is_grantable THEN '*' ELSE '' END AS entry
          FROM pg_default_acl d, aclexplode(d.defaclacl) a
         WHERE d.defaclrole = v_target AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'
      ) normalized;
    IF v_default <> ARRAY['<self>:EXECUTE']::text[] THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = format('RDS function boundary: %s default is not exactly {<self>:EXECUTE}', v_label);
    END IF;
  END LOOP;
END
$rds_function_boundary$;
