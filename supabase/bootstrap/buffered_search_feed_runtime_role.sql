-- Operator-only phase after migrations (20261105043870): make the buffered search feeder's runtime login
-- able to log in. Run only under Khaliq's explicit apply OK in his own words (a production credential/IAM
-- change); INGEST_BUFFERED_SEARCH_FEED stays off and the feeder refuses to arm without its own login
-- (src/session-pipeline/buffered-feed-login.ts), so a login alone indexes nothing. The password is set
-- separately from 1Password in memory; never place it here or in a connection file. Run
-- buffered_search_feed_runtime_verify.sql before and after. The role may EXECUTE only
-- sessions.buffered_search_feed_page / _stage / _advance and holds no table right.
BEGIN;
DO $role$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='buffered_search_feed_runtime')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed runtime: bootstrap and migrate first';END IF;
END $role$;
ALTER ROLE buffered_search_feed_runtime LOGIN NOINHERIT;
ALTER ROLE buffered_search_feed_runtime SET statement_timeout='5s';
ALTER ROLE buffered_search_feed_runtime SET lock_timeout='2s';
ALTER ROLE buffered_search_feed_runtime SET idle_in_transaction_session_timeout='10s';
COMMIT;
