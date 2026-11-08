-- v4 step 3 of 3 — cleanup of census_catalog_ro (failure after commit, census end, or expiry 2026-12-07).
-- Two transactions: the login fence COMMITS first so a client reconnecting via PUBLIC CONNECT cannot
-- authenticate while sessions are terminated; recoverable at every phase (re-run is safe).
-- Usage: psql -v ON_ERROR_STOP=1 -d neondb -f 03-cleanup.sql   (as neondb_owner)
SET search_path = pg_catalog, pg_temp;
-- Phase 1: fence (committed).
BEGIN;
DO $f$ BEGIN
  IF current_user <> 'neondb_owner' THEN RAISE EXCEPTION 'cleanup: must run as neondb_owner'; END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'census_catalog_ro') THEN
    EXECUTE 'ALTER ROLE census_catalog_ro NOLOGIN PASSWORD NULL';
  END IF;
END $f$;
COMMIT;
-- Phase 2: end existing sessions. pg_terminate_backend needs pg_signal_backend or role privileges the
-- owner may not have; failure is tolerated because the role's own limits end every session within 60 s
-- (idle_session_timeout 60s, statement_timeout 10s, idle_in_transaction 10s) and the committed fence
-- stops new logins. Poll up to 120 s.
DO $t$ BEGIN
  PERFORM pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'census_catalog_ro';
EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'cleanup: cannot signal backends; waiting for role timeouts';
END $t$;
DO $w$ DECLARE i int := 0; BEGIN
  WHILE EXISTS (SELECT 1 FROM pg_stat_activity WHERE usename = 'census_catalog_ro') AND i < 120 LOOP
    PERFORM pg_sleep(1); i := i + 1;
  END LOOP;
END $w$;
-- Phase 3: verify none remain, drop.
BEGIN;
DO $d$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'census_catalog_ro') THEN RAISE NOTICE 'cleanup: role already absent'; RETURN; END IF;
  IF EXISTS (SELECT 1 FROM pg_stat_activity WHERE usename = 'census_catalog_ro') THEN RAISE EXCEPTION 'cleanup: sessions still present after 120 s; fence holds, re-run'; END IF;
  EXECUTE 'REVOKE CONNECT ON DATABASE neondb FROM census_catalog_ro';
  IF EXISTS (SELECT 1 FROM pg_shdepend d JOIN pg_roles x ON x.oid = d.refobjid WHERE x.rolname = 'census_catalog_ro') THEN
    RAISE EXCEPTION 'cleanup: unexpected dependencies (another database?); nothing dropped';
  END IF;
  EXECUTE 'DROP ROLE census_catalog_ro';
END $d$;
COMMIT;
SELECT count(*) = 0 AS role_absent FROM pg_roles WHERE rolname = 'census_catalog_ro';
-- Then delete the 1Password item and record this output as the cleanup receipt.
