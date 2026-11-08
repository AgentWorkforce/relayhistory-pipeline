-- v3 (supersedes 319b62da / 9368e6b1) — Neon catalog-only read-only role for the Cloud public + History census.
-- pipeline-lead, 2026-11-08. Addresses Root static HOLD on v2:
--   * identity gate: refuses unless current_user=neondb_owner on database neondb, server >=15
--   * NOLOGIN staging: role is created NOLOGIN; LOGIN is granted only in a SEPARATE step after the
--     committed proof is re-run and recorded (step 5)
--   * column privileges and PG17 MAINTAIN are included in the proof (hard fail)
--   * cleanup scope narrowed to exactly this role (no DROP OWNED; owns nothing by construction,
--     proven by a pg_shdepend check before DROP ROLE)
-- Target: Neon project divine-snow-84812652, branch br-damp-mud-apbr6ifp, database neondb.
-- Executor: codex-relay-desktop-find-prior (kjg-lap), as neondb_owner, after Root's static PASS and
-- Khaliq's direct OK in the executor's session. Static reviewer: migration-handover-agent.
-- Credential: password generated client-side, set via psql \password only after step 5, stored ONLY in
-- 1Password item "neon divine-snow census-ro (catalog only)".
--
-- OPEN DECISION (Khaliq): PUBLIC-derived residuals (database TEMP, EXECUTE on non-SECURITY-DEFINER
-- functions, USAGE on schemas PUBLIC may use). Per-role REVOKE cannot subtract PUBLIC grants. This file
-- implements ONLY public_mode=accept_residual (residuals reported, not removed). A PUBLIC revoke is a
-- separate shared-DB change with its own consumer inventory and review; it is not in this file.
--
-- Usage: psql -v ON_ERROR_STOP=1 -v public_mode=accept_residual -f <this file>
\if :{?public_mode}
\else
  \echo 'public_mode not set'
  SELECT 1/0 AS refuse_missing_public_mode;
\endif
SELECT :'public_mode' = 'accept_residual' AS mode_ok \gset
\if :mode_ok
\else
  \echo 'unsupported public_mode'
  SELECT 1/0 AS refuse_unsupported_public_mode;
\endif

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15s';

-- 0. Identity gate.
DO $gate$ BEGIN
  IF current_user <> 'neondb_owner' OR session_user <> 'neondb_owner' OR current_database() <> 'neondb'
     OR current_setting('server_version_num')::int < 150000 THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'identity gate: wrong user/database/server; nothing changed';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'census_catalog_ro') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'census_catalog_ro already exists; nothing changed';
  END IF;
END $gate$;

-- 1. Role, NOLOGIN, no password.
CREATE ROLE census_catalog_ro NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS
  CONNECTION LIMIT 2 VALID UNTIL '2026-12-07';
ALTER ROLE census_catalog_ro SET default_transaction_read_only = on;
ALTER ROLE census_catalog_ro SET statement_timeout = '10s';
ALTER ROLE census_catalog_ro SET lock_timeout = '2s';
ALTER ROLE census_catalog_ro SET idle_in_transaction_session_timeout = '10s';
ALTER ROLE census_catalog_ro SET search_path = pg_catalog, pg_temp;

-- 2. CONNECT only. No schema/table/column/sequence/function grants; no pg_read_all_stats/pg_monitor.
GRANT CONNECT ON DATABASE neondb TO census_catalog_ro;

-- 3. Proof BEFORE commit (evaluated for the new role, including PUBLIC-derived privileges). Any hard
--    failure aborts the whole transaction: the role never exists.
CREATE FUNCTION pg_temp.census_ro_proof() RETURNS text LANGUAGE plpgsql AS $proof$
DECLARE r text := 'census_catalog_ro'; v_attr record; v_n bigint; v_temp boolean; v_usage bigint; v_exec bigint;
  v_has_maintain boolean := current_setting('server_version_num')::int >= 170000;
BEGIN
  SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolinherit, rolcanlogin INTO v_attr
    FROM pg_roles WHERE rolname = r;
  IF v_attr.rolsuper OR v_attr.rolcreatedb OR v_attr.rolcreaterole OR v_attr.rolreplication
     OR v_attr.rolbypassrls OR v_attr.rolinherit THEN RAISE EXCEPTION 'proof: elevated attribute'; END IF;
  SELECT count(*) INTO v_n FROM pg_auth_members m JOIN pg_roles x ON x.oid = m.member WHERE x.rolname = r;
  IF v_n <> 0 THEN RAISE EXCEPTION 'proof: role has memberships'; END IF;
  -- relations (table-level), incl. MAINTAIN on PG17+
  SELECT count(*) INTO v_n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','p','v','m','f')
      AND CASE WHEN c.relkind IN ('r','p','v','m','f') THEN
            has_table_privilege(r, c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
            OR (v_has_maintain AND has_table_privilege(r, c.oid, 'MAINTAIN'))
          ELSE false END;
  IF v_n <> 0 THEN RAISE EXCEPTION 'proof: % app relations have table-level privileges', v_n; END IF;
  -- column-level privileges (a column grant does not show in table-level checks)
  SELECT count(*) INTO v_n FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','p','v','m','f')
      AND a.attnum > 0 AND NOT a.attisdropped
      AND CASE WHEN c.relkind IN ('r','p','v','m','f') THEN
            has_column_privilege(r, c.oid, a.attnum, 'SELECT,INSERT,UPDATE,REFERENCES') ELSE false END;
  IF v_n <> 0 THEN RAISE EXCEPTION 'proof: % app columns have column-level privileges', v_n; END IF;
  SELECT count(*) INTO v_n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind = 'S'
      AND CASE WHEN c.relkind = 'S' THEN has_sequence_privilege(r, c.oid, 'USAGE,SELECT,UPDATE') ELSE false END;
  IF v_n <> 0 THEN RAISE EXCEPTION 'proof: % sequences usable', v_n; END IF;
  SELECT count(*) INTO v_n FROM pg_namespace n
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND has_schema_privilege(r, n.oid, 'CREATE');
  IF v_n <> 0 THEN RAISE EXCEPTION 'proof: CREATE on % schemas', v_n; END IF;
  IF has_database_privilege(r, current_database(), 'CREATE') THEN RAISE EXCEPTION 'proof: database CREATE'; END IF;
  -- SECURITY DEFINER functions run with owner rights: any executable + reachable one is a data path.
  SELECT count(*) INTO v_n FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND p.prosecdef
      AND has_schema_privilege(r, n.oid, 'USAGE') AND has_function_privilege(r, p.oid, 'EXECUTE');
  IF v_n <> 0 THEN RAISE EXCEPTION 'proof: % SECURITY DEFINER app functions executable', v_n; END IF;
  -- residuals (PUBLIC-derived; reported, not failed, under accept_residual). default_transaction_read_only
  -- is an overridable default, so TEMP is a real residual; no data read/write path remains.
  v_temp := has_database_privilege(r, current_database(), 'TEMP');
  SELECT count(*) INTO v_usage FROM pg_namespace n
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND has_schema_privilege(r, n.oid, 'USAGE');
  SELECT count(*) INTO v_exec FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND has_function_privilege(r, p.oid, 'EXECUTE');
  RETURN format('proof PASS canlogin=%s residual(PUBLIC-derived): temp=%s schema_usage=%s executable_app_functions=%s',
    v_attr.rolcanlogin, v_temp, v_usage, v_exec);
END $proof$;
SELECT pg_temp.census_ro_proof() AS proof;
DROP FUNCTION pg_temp.census_ro_proof();

COMMIT;

-- 4. (Recorded) the NOTICE/row above is the pre-commit proof receipt.
-- 5. LOGIN staging, SEPARATE transaction, only after recording step 4 and re-running the same proof
--    (paste the function body above into a fresh BEGIN … COMMIT and require PASS), then:
--      ALTER ROLE census_catalog_ro LOGIN;
--      \password census_catalog_ro        -- client-side SCRAM; value goes only into 1Password
-- 6. Live confirmation as the role (fresh session):
--      SELECT current_user, current_setting('transaction_read_only');   -- expect census_catalog_ro, on
--      SELECT 1 FROM <any app table> LIMIT 1;                           -- expect permission denied
-- 7. Cleanup (failure after commit, census end, or by expiry 2026-12-07), exactly this role only:
--      BEGIN;
--      ALTER ROLE census_catalog_ro NOLOGIN;
--      SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'census_catalog_ro';
--      REVOKE CONNECT ON DATABASE neondb FROM census_catalog_ro;
--      DO $c$ BEGIN IF EXISTS (SELECT 1 FROM pg_shdepend d JOIN pg_roles x ON x.oid = d.refobjid
--           WHERE x.rolname = 'census_catalog_ro') THEN RAISE EXCEPTION 'cleanup: unexpected dependencies'; END IF; END $c$;
--      DROP ROLE census_catalog_ro;
--      COMMIT;
--    then delete the 1Password item and record a cleanup receipt (role absent from pg_roles).
