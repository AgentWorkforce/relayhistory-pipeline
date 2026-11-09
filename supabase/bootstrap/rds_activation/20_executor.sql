-- rds-activation/v1 fragment 20: the named, narrowly privileged executor.
--
-- hoopdb_activation_executor is NOLOGIN NOINHERIT CREATEROLE and nothing else (no
-- CREATEDB, REPLICATION, BYPASSRLS or SUPERUSER; PostgreSQL 16+ CREATEROLE can only
-- manage roles it holds ADMIN on). The provisioner creates it, so PostgreSQL records
-- the automatic creator edge (executor <- provisioner, ADMIN, no INHERIT, no SET,
-- grantor rdsadmin). The provisioner then adds a SET-only edge (grantor provisioner)
-- so it can SET ROLE to it. INHERIT stays false, so the provisioner never gains its
-- privileges implicitly. Both edges, and every edge the executor later gains, go
-- away with DROP ROLE in fragment 60, which fragment 70 then proves.
DO $rds_activation_20$
BEGIN
  IF current_user::text <> current_setting('hoopdb_activation.provisioner') THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'rds-activation fragment 20 must run as the provisioner';
  END IF;
  CREATE ROLE hoopdb_activation_executor
    NOLOGIN NOINHERIT CREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS NOSUPERUSER;
  EXECUTE format('GRANT hoopdb_activation_executor TO %I WITH INHERIT FALSE, SET TRUE', current_user);
  -- Exact edge set of the executor: the two edges above, and nothing else.
  IF (SELECT count(*) FROM pg_catalog.pg_auth_members m
       WHERE m.roleid = 'hoopdb_activation_executor'::regrole OR m.member = 'hoopdb_activation_executor'::regrole
          OR m.grantor = 'hoopdb_activation_executor'::regrole) <> 2
     OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m
                     WHERE m.roleid = 'hoopdb_activation_executor'::regrole AND m.member = current_user::regrole
                       AND m.grantor = 'rdsadmin'::regrole AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option)
     OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m
                     WHERE m.roleid = 'hoopdb_activation_executor'::regrole AND m.member = current_user::regrole
                       AND m.grantor = current_user::regrole AND NOT m.admin_option AND NOT m.inherit_option AND m.set_option) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'rds-activation: unexpected executor edge set';
  END IF;
  SET LOCAL ROLE hoopdb_activation_executor;
END
$rds_activation_20$;
