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
-- Identity binding: the approved runtime role is NOT inferred from the (mutable) policy.
-- The caller binds it transaction-locally as hoopdb.runtime_role: the migrator does so
-- in phase 2 from HOOPDB_RUNTIME_ROLE, the Lambda admission from its own connection
-- identity, an operator explicitly. Unset or unknown refuses. The policy and the grant
-- must both name exactly that role. Only db_mode 'local' with the owner as runtime (the
-- Docker dev owner-run profile) accepts no runtime grant or policy.
-- No reader surface is accepted here: a bulls96_read_api grant or policy on this table
-- fails until the separately reviewed reader-contract slot extends this file.
-- Read-only; any role that can read the catalogs can run it. It is a phase-2 step of
-- src/hoopdb/migrate.ts (so PASS requires it), and has no psql meta-commands. Operator:
--   psql -v ON_ERROR_STOP=1 -X -d <db> -c "BEGIN" \
--     -c "SELECT set_config('hoopdb.runtime_role', '<runtime role>', true)" \
--     -f supabase/bootstrap/portable_search_storage_verify.sql -c "ROLLBACK"
DO $verify$
DECLARE
  v_rel oid := to_regclass('sessions.search_projection_state');
  v_owner oid;
  v_migration_owner oid;
  v_runtime oid;
  v_expected oid;
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_owner_run boolean := false;
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

  SELECT r.oid INTO v_expected FROM pg_roles r WHERE r.rolname = nullif(current_setting('hoopdb.runtime_role', true), '');
  IF v_expected IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'portable search storage: the expected runtime role is not bound (hoopdb.runtime_role) or does not exist';
  END IF;
  v_owner_run := v_expected = v_owner AND v_mode = 'local';
  IF v_expected = v_owner AND NOT v_owner_run THEN
    v_problems := array_append(v_problems, 'runtime role is the owner outside local mode'::text);
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

  -- Exact constraint set and definitions, all validated and immediate. PG18 NOT NULL
  -- constraint rows are excluded (nullability is checked via attnotnull above).
  IF (SELECT string_agg(format('%s|%s|%s|%s', conname, contype::text, convalidated::text, condeferrable::text), ',' ORDER BY conname)
        FROM pg_constraint WHERE conrelid = v_rel AND contype <> 'n')
     IS DISTINCT FROM 'search_projection_state_counts|c|true|false,search_projection_state_ids|c|true|false,search_projection_state_pkey|p|true|false' THEN
    v_problems := array_append(v_problems, 'constraint set, validation or deferrability differs'::text);
  END IF;
  FOR v_row IN
    SELECT x.name, x.def, pg_get_constraintdef(c.oid) AS actual
      FROM (VALUES
        ('search_projection_state_pkey', 'PRIMARY KEY (backend_id, org_id, workspace_id, source, session_id)'),
        ('search_projection_state_counts', 'CHECK (((revision >= 0) AND (row_count >= 0) AND (coverage_from_ms >= 0)))'),
        ('search_projection_state_ids', 'CHECK (((backend_id ~ ''^[a-zA-Z0-9._-]{1,128}$''::text) AND (org_id <> ''''::text) AND (workspace_id <> ''''::text)))')
      ) AS x(name, def)
      LEFT JOIN pg_constraint c ON c.conrelid = v_rel AND c.conname = x.name
  LOOP
    IF v_row.actual IS DISTINCT FROM v_row.def THEN
      v_problems := array_append(v_problems, format('constraint %s definition differs', v_row.name));
    END IF;
  END LOOP;
  -- Index: by catalog identity, never by a search_path-dependent rendering
  -- (regclass and pg_get_indexdef qualify names only when not visible).
  IF (SELECT count(*) FROM pg_index WHERE indrelid = v_rel) <> 1 THEN
    v_problems := array_append(v_problems, 'index set differs (exactly one index expected)'::text);
  END IF;
  IF NOT EXISTS (
    SELECT 1
      FROM pg_index i
      JOIN pg_class ic ON ic.oid = i.indexrelid
      JOIN pg_am am ON am.oid = ic.relam
     WHERE i.indrelid = v_rel
       AND ic.relname = 'search_projection_state_pkey'
       AND ic.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = v_rel)
       AND am.amname = 'btree'
       AND i.indisunique AND i.indisprimary AND i.indisvalid AND i.indisready AND i.indimmediate
       AND i.indpred IS NULL AND i.indexprs IS NULL
       AND i.indnatts = 5 AND i.indnkeyatts = 5
       AND NOT coalesce((to_jsonb(i) ->> 'indnullsnotdistinct')::boolean, false)
       AND (SELECT array_agg(a.attname::text ORDER BY k.n)
              FROM unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, n)
              JOIN pg_attribute a ON a.attrelid = v_rel AND a.attnum = k.attnum)
           = ARRAY['backend_id', 'org_id', 'workspace_id', 'source', 'session_id']
       AND (SELECT bool_and(o = 0) FROM unnest(i.indoption::int2[]) o)
       AND (SELECT bool_and(oc.opcdefault)
              FROM unnest(i.indclass::oid[]) c(opc) JOIN pg_opclass oc ON oc.oid = c.opc)
       AND (SELECT bool_and(k.coll = a.attcollation)
              FROM unnest(i.indcollation::oid[], i.indkey::int2[]) k(coll, attnum)
              JOIN pg_attribute a ON a.attrelid = v_rel AND a.attnum = k.attnum)
  ) THEN
    v_problems := array_append(v_problems, 'index search_projection_state_pkey identity or properties differ'::text);
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
       AND v_row.polroles[1] = v_expected AND NOT v_owner_run
       AND v_row.q = 'true' AND v_row.c = 'true' AND v_runtime IS NULL THEN
      v_runtime := v_row.polroles[1];
    ELSE
      v_problems := array_append(v_problems, format('unapproved policy %s', v_row.polname));
    END IF;
  END LOOP;
  IF v_owner_run THEN
    NULL; -- local owner-run: no runtime grant or policy exists or is accepted
  ELSIF v_runtime IS NULL THEN
    v_problems := array_append(v_problems, 'policy bulls96_trusted_backend for the bound runtime role is missing'::text);
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
    IF NOT v_owner_run AND v_row.grantee = v_expected AND v_row.privs = 'DELETE,INSERT,SELECT,UPDATE' AND NOT v_row.grantable THEN
      NULL;
    ELSE
      v_problems := array_append(v_problems, format('ACL: %s granted to %s%s', v_row.privs,
        CASE WHEN v_row.grantee = 0 THEN 'PUBLIC' ELSE v_row.grantee::regrole::text END,
        CASE WHEN v_row.grantable THEN ' WITH GRANT OPTION' ELSE '' END));
    END IF;
  END LOOP;
  -- has_table_privilege with a list is true if ANY is held, so test each one.
  IF NOT v_owner_run AND NOT (has_table_privilege(v_expected, v_rel, 'SELECT') AND has_table_privilege(v_expected, v_rel, 'INSERT')
                                    AND has_table_privilege(v_expected, v_rel, 'UPDATE') AND has_table_privilege(v_expected, v_rel, 'DELETE')) THEN
    v_problems := array_append(v_problems, 'runtime role lacks SELECT/INSERT/UPDATE/DELETE'::text);
  END IF;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'portable search storage is not in the approved shape: ' || array_to_string(v_problems, '; ');
  END IF;
  RAISE NOTICE 'portable search storage: PASS';
END
$verify$;
