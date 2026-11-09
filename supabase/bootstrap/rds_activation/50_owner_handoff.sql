-- rds-activation/v1 fragment 50: hand hoopdb to hoop, then act as hoop.
--
-- Why hoop owns the database, not the provisioner: the RDS master must not be the
-- ongoing owner of anything HoopDb uses (infra #107: "a provisioning executor, never an
-- application login"). The migrator's CREATE SCHEMA IF NOT EXISTS and the trusted
-- extensions it creates (pg_trgm, btree_gin) need CREATE on the database and on public
-- (pg_database_owner). Both are what a dedicated-database owner already has under
-- the selfhosted and legacy rds contracts. hoop still has no CREATEROLE and no
-- rds_superuser membership.
--
-- ALTER DATABASE ... OWNER TO needs CREATEDB and the ability to SET ROLE hoop. The
-- provisioner has that ability only through the transaction-scoped chain
-- provisioner -SET-> executor -SET-> hoop.
-- As hoop: database ACL (PUBLIC loses CONNECT, TEMPORARY and CREATE; each LOGIN role
-- of the spec gets CONNECT only), CREATE SCHEMA sessions, and hoop's own global
-- function default (FOR ROLE current_user only; no IN SCHEMA grant of any kind).
DO $rds_activation_50$
DECLARE
  v_name text;
BEGIN
  RESET ROLE;
  IF current_user::text <> current_setting('hoopdb_activation.provisioner') THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'rds-activation fragment 50: RESET ROLE did not return to the provisioner';
  END IF;
  ALTER DATABASE hoopdb OWNER TO hoop;
  SET LOCAL ROLE hoop;
  IF current_user::text <> 'hoop' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'rds-activation fragment 50: SET ROLE hoop failed';
  END IF;
  REVOKE ALL ON DATABASE hoopdb FROM PUBLIC;
  FOR v_name IN SELECT e->>'name' FROM jsonb_array_elements(current_setting('hoopdb_activation.role_spec')::jsonb) e
                 WHERE (e->>'login')::boolean AND e->>'name' <> 'hoop' ORDER BY 1 LOOP
    EXECUTE format('GRANT CONNECT ON DATABASE hoopdb TO %I', v_name);
  END LOOP;
  CREATE SCHEMA sessions;
  ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
  RESET ROLE;
  IF current_user::text <> current_setting('hoopdb_activation.provisioner') THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'rds-activation fragment 50: RESET ROLE did not return to the provisioner';
  END IF;
END
$rds_activation_50$;
