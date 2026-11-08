-- v2 (supersedes 9368e6b1) — Neon catalog-only read-only role for the Cloud public + History census.
-- pipeline-lead, 2026-11-07. Addresses Root static review HOLD: checks now run BEFORE commit and
-- abort the transaction on any failure; password only after a passing commit; full cleanup.
-- Target: Neon project divine-snow-84812652, branch br-damp-mud-apbr6ifp, database neondb.
-- Executor: codex-relay-desktop-find-prior (kjg-lap) as neondb_owner, ONCE, after Root's static PASS
-- and Khaliq's direct OK in the executor's session. Static reviewer: migration-handover-agent.
-- Credential: password generated client-side, stored ONLY in 1Password item
-- "neon divine-snow census-ro (catalog only)"; never on disk, argv or in SQL text.
--
-- OPEN DECISION (Khaliq): PUBLIC-derived residuals. PostgreSQL grants PUBLIC database TEMP and
-- function EXECUTE by default; a per-role REVOKE cannot subtract them. This file runs in mode
-- :'public_mode' = 'accept_residual' (record residuals, time-boxed role) — the ONLY mode
-- implemented here. A 'revoke_public' mode (REVOKE TEMP ON DATABASE / EXECUTE FROM PUBLIC +
-- re-grant to current consumers) is a shared-DB change requiring its own consumer inventory and
-- review; it is deliberately NOT in this file.
--
-- psql -v ON_ERROR_STOP=1 -v public_mode=accept_residual -f <this file>
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

-- Refuse if the role already exists (never alter a pre-existing role).
DO $pre$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'census_catalog_ro') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'census_catalog_ro already exists; nothing changed';
  END IF;
END $pre$;

-- 1. Role WITHOUT a password (cannot log in until step 4).
CREATE ROLE census_catalog_ro LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS
  CONNECTION LIMIT 2 VALID UNTIL '2026-12-07';
ALTER ROLE census_catalog_ro SET default_transaction_read_only = on;
ALTER ROLE census_catalog_ro SET statement_timeout = '10s';
ALTER ROLE census_catalog_ro SET lock_timeout = '2s';
ALTER ROLE census_catalog_ro SET idle_in_transaction_session_timeout = '10s';
ALTER ROLE census_catalog_ro SET search_path = pg_catalog, pg_temp;

-- 2. CONNECT only. No schema/table/sequence/function grants; no pg_read_all_stats / pg_monitor.
GRANT CONNECT ON DATABASE neondb TO census_catalog_ro;

-- 3. Effective-privilege proof BEFORE commit, evaluated for the new role (not the owner).
--    Hard failures abort the whole transaction (role never exists). PUBLIC-derived residuals
--    are reported, not failed, under accept_residual.
DO $proof$
DECLARE v_rel bigint; v_seq bigint; v_schema_create bigint; v_member bigint; v_attr record;
        v_temp boolean; v_exec bigint; v_usage bigint; v_secdef bigint;
BEGIN
  SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolinherit INTO v_attr
    FROM pg_roles WHERE rolname = 'census_catalog_ro';
  IF v_attr.rolsuper OR v_attr.rolcreatedb OR v_attr.rolcreaterole OR v_attr.rolreplication
     OR v_attr.rolbypassrls OR v_attr.rolinherit THEN
    RAISE EXCEPTION 'proof: elevated attribute';
  END IF;
  SELECT count(*) INTO v_member FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member
    WHERE r.rolname = 'census_catalog_ro';
  IF v_member <> 0 THEN RAISE EXCEPTION 'proof: role has memberships'; END IF;
  SELECT count(*) INTO v_rel FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
      AND c.relkind IN ('r','p','v','m','f')
      AND CASE WHEN c.relkind IN ('r','p','v','m','f') THEN has_table_privilege('census_catalog_ro', c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') ELSE false END;
  IF v_rel <> 0 THEN RAISE EXCEPTION 'proof: % application relations readable/writable (incl. via PUBLIC)', v_rel; END IF;
  SELECT count(*) INTO v_seq FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind = 'S'
      AND CASE WHEN c.relkind = 'S' THEN has_sequence_privilege('census_catalog_ro', c.oid, 'USAGE,SELECT,UPDATE') ELSE false END;
  IF v_seq <> 0 THEN RAISE EXCEPTION 'proof: % sequences usable', v_seq; END IF;
  SELECT count(*) INTO v_schema_create FROM pg_namespace n
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
      AND has_schema_privilege('census_catalog_ro', n.oid, 'CREATE');
  IF v_schema_create <> 0 THEN RAISE EXCEPTION 'proof: CREATE on % schemas (incl. via PUBLIC)', v_schema_create; END IF;
  IF has_database_privilege('census_catalog_ro', current_database(), 'CREATE') THEN
    RAISE EXCEPTION 'proof: database CREATE';
  END IF;
  -- SECURITY DEFINER functions run with their owner's rights: one executable by this role (e.g. via
  -- PUBLIC's default EXECUTE) in a schema it can USE is a data-read path. Hard fail.
  SELECT count(*) INTO v_secdef FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND p.prosecdef
      AND has_schema_privilege('census_catalog_ro', n.oid, 'USAGE')
      AND has_function_privilege('census_catalog_ro', p.oid, 'EXECUTE');
  IF v_secdef <> 0 THEN RAISE EXCEPTION 'proof: % SECURITY DEFINER app functions executable', v_secdef; END IF;
  -- PUBLIC-derived residuals (reported only, accept_residual mode). NOTE: default_transaction_read_only
  -- is a session DEFAULT the role can override (BEGIN READ WRITE), so TEMP is a real residual, not
  -- neutralised; with no table/sequence privileges and no executable SECURITY DEFINER function it
  -- cannot read or write application data.
  v_temp := has_database_privilege('census_catalog_ro', current_database(), 'TEMP');
  SELECT count(*) INTO v_usage FROM pg_namespace n
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
      AND has_schema_privilege('census_catalog_ro', n.oid, 'USAGE');
  SELECT count(*) INTO v_exec FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
      AND has_function_privilege('census_catalog_ro', p.oid, 'EXECUTE');
  RAISE NOTICE 'proof PASS; residual (PUBLIC-derived): temp=% schema_usage=% executable_app_functions=%',
    v_temp, v_usage, v_exec;
END $proof$;

COMMIT;

-- 4. Only after COMMIT succeeded: set the password client-side (SCRAM hash; plaintext never sent
--    in SQL text or logged):   \password census_catalog_ro
--    Store the generated value in the 1Password item above. Hand cloud-migration-lead ONLY the item name.

-- 5. Live confirmation as the role (fresh session, after step 4); record output with the receipt:
--    SELECT current_user, current_setting('transaction_read_only');   -- expect census_catalog_ro, on
--    CREATE TEMP TABLE t(x int);   -- fails under the default read-only txn; BEGIN READ WRITE would allow it (PUBLIC TEMP residual)
--    SELECT 1 FROM <any app table> LIMIT 1;   -- EXPECT permission denied

-- 6. Cleanup / rollback (any failure after commit, at census end, or by 2026-12-07 expiry):
--    BEGIN;
--    REVOKE CONNECT ON DATABASE neondb FROM census_catalog_ro;
--    SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'census_catalog_ro';
--    DROP OWNED BY census_catalog_ro;   -- owns nothing by construction; defensive
--    DROP ROLE census_catalog_ro;
--    COMMIT;
--    then delete the 1Password item and record a cleanup receipt (role absent in pg_roles).
