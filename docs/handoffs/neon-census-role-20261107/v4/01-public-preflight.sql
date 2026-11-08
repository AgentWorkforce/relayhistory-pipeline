-- v4 step 1 of 3 — READ-ONLY preflight, run ONCE IN EVERY database of the branch that PUBLIC may
-- CONNECT to (the main file refuses unless the list it is given equals that set).
-- Why PUBLIC: census_catalog_ro is NOINHERIT, member of nothing and granted nothing but CONNECT on
-- neondb, so outside neondb its effective privileges are exactly PUBLIC's. Proving PUBLIC has no data
-- path in a database proves the role has none there.
-- Usage: psql -v ON_ERROR_STOP=1 -d <db> -f 01-public-preflight.sql   (as neondb_owner)
BEGIN READ ONLY;
SET LOCAL search_path = pg_catalog, pg_temp;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '2s';
DO $pre$
DECLARE v_n bigint; v_maint boolean := current_setting('server_version_num')::int >= 170000;
BEGIN
  IF current_user <> 'neondb_owner' THEN RAISE EXCEPTION 'preflight: must run as neondb_owner'; END IF;
  SELECT count(*) INTO v_n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','p','v','m','f')
      AND CASE WHEN c.relkind IN ('r','p','v','m','f') THEN
            has_table_privilege('public', c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
            OR (v_maint AND has_table_privilege('public', c.oid, 'MAINTAIN')) ELSE false END;
  IF v_n <> 0 THEN RAISE EXCEPTION 'preflight %: PUBLIC has table privileges on % app relations', current_database(), v_n; END IF;
  SELECT count(*) INTO v_n FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','p','v','m','f')
      AND a.attnum > 0 AND NOT a.attisdropped
      AND CASE WHEN c.relkind IN ('r','p','v','m','f') THEN has_column_privilege('public', c.oid, a.attnum, 'SELECT,INSERT,UPDATE,REFERENCES') ELSE false END;
  IF v_n <> 0 THEN RAISE EXCEPTION 'preflight %: PUBLIC has column privileges on % app columns', current_database(), v_n; END IF;
  SELECT count(*) INTO v_n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind = 'S'
      AND CASE WHEN c.relkind = 'S' THEN has_sequence_privilege('public', c.oid, 'USAGE,SELECT,UPDATE') ELSE false END;
  IF v_n <> 0 THEN RAISE EXCEPTION 'preflight %: PUBLIC can use % sequences', current_database(), v_n; END IF;
  SELECT count(*) INTO v_n FROM pg_namespace n
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND has_schema_privilege('public', n.oid, 'CREATE');
  IF v_n <> 0 THEN RAISE EXCEPTION 'preflight %: PUBLIC has CREATE on % schemas', current_database(), v_n; END IF;
  IF has_database_privilege('public', current_database(), 'CREATE') THEN
    RAISE EXCEPTION 'preflight %: PUBLIC has database CREATE', current_database(); END IF;
  SELECT count(*) INTO v_n FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND p.prosecdef
      AND has_schema_privilege('public', n.oid, 'USAGE') AND has_function_privilege('public', p.oid, 'EXECUTE');
  IF v_n <> 0 THEN RAISE EXCEPTION 'preflight %: % SECURITY DEFINER app functions executable by PUBLIC', current_database(), v_n; END IF;
  -- Large objects: readable via lo_get/loread even with no table access.
  IF current_setting('lo_compat_privileges') <> 'off' THEN
    RAISE EXCEPTION 'preflight %: lo_compat_privileges is on (all large objects readable)', current_database(); END IF;
  SELECT count(*) INTO v_n FROM pg_largeobject_metadata m, LATERAL aclexplode(m.lomacl) x
    WHERE x.grantee = 0 AND x.privilege_type IN ('SELECT','UPDATE');
  IF v_n <> 0 THEN RAISE EXCEPTION 'preflight %: PUBLIC can read/write % large objects', current_database(), v_n; END IF;
  RAISE NOTICE 'preflight PASS db=% residual(PUBLIC): temp=% schema_usage=% executable_app_functions=%',
    current_database(), has_database_privilege('public', current_database(), 'TEMP'),
    (SELECT count(*) FROM pg_namespace n WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND has_schema_privilege('public', n.oid, 'USAGE')),
    (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND has_function_privilege('public', p.oid, 'EXECUTE'));
END $pre$;
ROLLBACK;
