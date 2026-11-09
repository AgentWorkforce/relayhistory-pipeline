-- Operator-only rollback of buffered_search_feed_runtime_role.sql: return the buffered search feeder's
-- runtime role to the bare NOLOGIN NOINHERIT profile the role bootstraps create (session_ingest_roles.sql
-- and its RDS and Cloud SQL variants). Run only under Khaliq's explicit apply OK in his own words. First set
-- INGEST_BUFFERED_SEARCH_FEED off and drop the credential from the buffered_search_feed_database_url /
-- buffered_search_feed_database_ca_pem secret fields.
-- Existing pooled connections are not terminated by this script: wait for in-flight feed invocations to finish before treating the revert as disarm.
-- Run with psql -1 -v ON_ERROR_STOP=1, and run
-- buffered_search_feed_runtime_verify.sql before and after (it accepts both profiles). Idempotent; prints
-- nothing secret. Grants (USAGE on sessions, EXECUTE on the three feed RPCs) are untouched.
BEGIN;
DO $role$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='buffered_search_feed_runtime')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed runtime: bootstrap and migrate first';END IF;
END $role$;
ALTER ROLE buffered_search_feed_runtime NOLOGIN NOINHERIT PASSWORD NULL;
ALTER ROLE buffered_search_feed_runtime RESET statement_timeout;
ALTER ROLE buffered_search_feed_runtime RESET lock_timeout;
ALTER ROLE buffered_search_feed_runtime RESET idle_in_transaction_session_timeout;
COMMIT;
