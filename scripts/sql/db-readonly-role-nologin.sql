-- sessions_readonly_stats rollback: no login, no password, no statistics memberships.
--
-- OWNER GATE: like the bootstrap, applying this to a real database is Khaliq's own go. Pattern:
-- supabase/bootstrap/buffered_search_feed_runtime_role_nologin.sql. Afterwards delete the
-- production-db-readonly environment secret DB_READONLY_URL. The role itself is kept (DROP ROLE is a
-- separate, optional step once nothing references it).
--   psql --single-transaction -v ON_ERROR_STOP=1 -f scripts/sql/db-readonly-role-nologin.sql

ALTER ROLE sessions_readonly_stats WITH NOLOGIN PASSWORD NULL;

-- Defensive: the role is granted nothing on sessions tables; revoke anything added by hand.
REVOKE ALL ON ALL TABLES IN SCHEMA sessions FROM sessions_readonly_stats;
REVOKE USAGE ON SCHEMA sessions FROM sessions_readonly_stats;
REVOKE pg_read_all_stats, pg_monitor FROM sessions_readonly_stats;
DO $pgss$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'extensions') THEN
    REVOKE USAGE ON SCHEMA extensions FROM sessions_readonly_stats;
  END IF;
END
$pgss$;
