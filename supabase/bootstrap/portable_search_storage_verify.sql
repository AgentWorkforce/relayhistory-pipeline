-- Read-only admission of the portable search completeness state (migration
-- 20261105047000_portable_search_storage.sql; PR #280 SQL-owner finding M1).
-- Raises 55000, listing every problem, unless sessions.search_projection_state has
-- exactly the ledgered shape and admission:
--   owner      the migration owner (owner of sessions.__96bulls_migrations);
--   shape      exact column names/types/NOT NULL; exactly the pkey and the two named
--              CHECK constraints; exactly one index (the pkey); no triggers, rules,
--              inheritance or partitioning;
--   RLS        relrowsecurity AND relforcerowsecurity;
--   policies   exactly one: bulls96_trusted_backend, PERMISSIVE FOR ALL TO one role R,
--              USING (true) WITH CHECK (true)  (installed by src/hoopdb/migrate.ts);
--   ACL        owner, plus R with exactly SELECT/INSERT/UPDATE/DELETE without GRANT
--              OPTION; nothing for PUBLIC or any other role; no column ACL. This also
--              catches grants inherited from ALTER DEFAULT PRIVILEGES;
--   R          not SUPERUSER, not BYPASSRLS, not the owner, not a member of the owner.
-- The runtime role is taken from the policy rather than configuration, and the
-- migrate-time guard (runtimeRoleViolations) separately pins that role's complete
-- surface. No reader surface is accepted here: a bulls96_read_api grant or policy on this
-- table fails until the separately reviewed read-API slot extends this file.
-- Any role that can read the catalogs can run it; it changes nothing.
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/portable_search_storage_verify.sql
\set ON_ERROR_STOP 1
DO $verify$
DECLARE
  v_rel oid := to_regclass('sessions.search_projection_state');
  v_owner oid;
  v_migration_owner oid;
  v_runtime oid;
  v_problems text[] := ARRAY[]::text[];
  v_row record;
  v_cols text;
  v_expected_cols constant text :=
    'backend_id:text:t,canonical_session_id:text:t,complete:boolean:t,coverage_from_ms:bigint:t,'
    'org_id:text:t,revision:bigint:t,row_count:bigint:t,session_id:text:t,source:text:t,'
    'source_epoch:bigint:t,updated_at:timestamp with time zone:t,workspace_id:text:t';
BEGIN
  IF v_rel IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'portable search storage: sessions.search_projection_state is missing (migrate 20261105047000 first)';
  END IF;
  SELECT c.relowner INTO v_owner FROM pg_class c WHERE c.oid = v_rel;
  SELECT c.relowner INTO v_migration_owner FROM pg_class c WHERE c.oid = to_regclass('sessions.__96bulls_migrations');
  IF v_migration_owner IS NULL OR v_owner IS DISTINCT FROM v_migration_owner THEN
    v_problems := array_append(v_problems, 'owner is not the migration owner'::text);
  END IF;

  SELECT * INTO v_row FROM pg_class WHERE oid = v_rel;
  IF v_row.relkind <> 'r' THEN v_problems := array_append(v_problems, 'not an ordinary table'::text); END IF;
  IF NOT v_row.relrowsecurity THEN v_problems := array_append(v_problems, 'RLS is off'::text); END IF;
  IF NOT v_row.relforcerowsecurity THEN v_problems := array_append(v_problems, 'RLS is not FORCEd'::text); END IF;
  IF v_row.relhasrules THEN v_problems := array_append(v_problems, 'has rules'::text); END IF;
  IF v_row.relispartition OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = v_rel OR inhparent = v_rel) THEN
    v_problems := array_append(v_problems, 'inheritance or partitioning present'::text);
  END IF;

  SELECT string_agg(format('%s:%s:%s', a.attname, format_type(a.atttypid, a.atttypmod),
                           CASE WHEN a.attnotnull THEN 't' ELSE 'f' END), ',' ORDER BY a.attname)
    INTO v_cols
    FROM pg_attribute a WHERE a.attrelid = v_rel AND a.attnum > 0 AND NOT a.attisdropped;
  IF v_cols IS DISTINCT FROM v_expected_cols THEN
    v_problems := array_append(v_problems, format('column set differs: %s', v_cols));
  END IF;
  IF EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = v_rel AND a.attnum > 0 AND a.attacl IS NOT NULL) THEN
    v_problems := array_append(v_problems, 'a column ACL is present'::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_attrdef d WHERE d.adrelid = v_rel) THEN
    v_problems := array_append(v_problems, 'a column default is present'::text);
  END IF;

  IF (SELECT string_agg(conname || ':' || contype, ',' ORDER BY conname) FROM pg_constraint WHERE conrelid = v_rel)
     IS DISTINCT FROM 'search_projection_state_counts:c,search_projection_state_ids:c,search_projection_state_pkey:p' THEN
    v_problems := array_append(v_problems, 'constraint set differs'::text);
  END IF;
  IF (SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = v_rel AND conname = 'search_projection_state_pkey')
     IS DISTINCT FROM 'PRIMARY KEY (backend_id, org_id, workspace_id, source, session_id)' THEN
    v_problems := array_append(v_problems, 'primary key differs'::text);
  END IF;
  IF (SELECT string_agg(i.indexrelid::regclass::text, ',') FROM pg_index i WHERE i.indrelid = v_rel)
     IS DISTINCT FROM 'sessions.search_projection_state_pkey' THEN
    v_problems := array_append(v_problems, 'index set differs'::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = v_rel) THEN
    v_problems := array_append(v_problems, 'a trigger is present'::text);
  END IF;

  -- Policies: exactly the migrator's trusted-backend policy.
  FOR v_row IN
    SELECT p.polname, p.polpermissive, p.polcmd, p.polroles,
           pg_get_expr(p.polqual, p.polrelid) AS q, pg_get_expr(p.polwithcheck, p.polrelid) AS c
      FROM pg_policy p WHERE p.polrelid = v_rel
  LOOP
    IF v_row.polname = 'bulls96_trusted_backend' AND v_row.polpermissive AND v_row.polcmd = '*'
       AND cardinality(v_row.polroles) = 1 AND v_row.polroles[1] <> 0
       AND v_row.q = 'true' AND v_row.c = 'true' AND v_runtime IS NULL THEN
      v_runtime := v_row.polroles[1];
    ELSE
      v_problems := array_append(v_problems, format('unapproved policy %s', v_row.polname));
    END IF;
  END LOOP;
  IF v_runtime IS NULL THEN
    v_problems := array_append(v_problems, 'policy bulls96_trusted_backend is missing'::text);
  ELSE
    SELECT * INTO v_row FROM pg_roles WHERE oid = v_runtime;
    IF v_row.rolsuper OR v_row.rolbypassrls THEN
      v_problems := array_append(v_problems, 'runtime role is SUPERUSER or BYPASSRLS'::text);
    END IF;
    IF v_runtime = v_owner OR pg_has_role(v_runtime, v_owner, 'MEMBER') THEN
      v_problems := array_append(v_problems, 'runtime role is or is a member of the owner'::text);
    END IF;
  END IF;

  -- ACL: owner plus exactly arwd for the runtime role.
  FOR v_row IN
    SELECT a.grantee, string_agg(a.privilege_type, ',' ORDER BY a.privilege_type) AS privs, bool_or(a.is_grantable) AS grantable
      FROM pg_class c, aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
     WHERE c.oid = v_rel AND a.grantee <> c.relowner
     GROUP BY a.grantee
  LOOP
    IF v_row.grantee = v_runtime AND v_row.privs = 'DELETE,INSERT,SELECT,UPDATE' AND NOT v_row.grantable THEN
      NULL;
    ELSE
      v_problems := array_append(v_problems, format('ACL: %s granted to %s%s', v_row.privs,
        CASE WHEN v_row.grantee = 0 THEN 'PUBLIC' ELSE v_row.grantee::regrole::text END,
        CASE WHEN v_row.grantable THEN ' WITH GRANT OPTION' ELSE '' END));
    END IF;
  END LOOP;
  -- has_table_privilege with a list is true if ANY is held, so test each one.
  IF v_runtime IS NOT NULL AND NOT (has_table_privilege(v_runtime, v_rel, 'SELECT') AND has_table_privilege(v_runtime, v_rel, 'INSERT')
                                    AND has_table_privilege(v_runtime, v_rel, 'UPDATE') AND has_table_privilege(v_runtime, v_rel, 'DELETE')) THEN
    v_problems := array_append(v_problems, 'runtime role lacks SELECT/INSERT/UPDATE/DELETE'::text);
  END IF;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'portable search storage is not in the approved shape: ' || array_to_string(v_problems, '; ');
  END IF;
  RAISE NOTICE 'portable search storage: PASS';
END
$verify$;
