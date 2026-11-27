-- Reverse of 20261105062330: RESET plan_cache_mode on sessions.buffered_search_feed_stage(jsonb) and
-- sessions.ingest_buffer_gc_ordinals(uuid), restoring their pre-062330 proconfig (the stage RPC's search_path and
-- lock_timeout, gc_ordinals' search_path). Bodies, owner, SECURITY DEFINER and ACL are untouched (none changed).
-- After this the feeder's long-lived sessions can again reach the cached generic plans (the 57014 timeouts the
-- up fixes). Refused (55000) unless 062330 is applied with this exact checksum, and while a later migration is
-- applied. Guard, reset and ledger removal are one transaction under the migrator lock; on an error the caller
-- must ROLLBACK (or disconnect). Code only: hosted execution needs Khaliq's separate explicit approval.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed custom plans: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062330_buffered_search_feed_custom_plans.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed custom plans: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062330_buffered_search_feed_custom_plans.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'99cfe771b771bd889c1006e2cb046f7ada85fd330917a253a19903bfee28b068'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed custom plans: own applied checksum required';END IF;
END $rollback_guard$;
ALTER FUNCTION sessions.buffered_search_feed_stage(jsonb) RESET plan_cache_mode;
ALTER FUNCTION sessions.ingest_buffer_gc_ordinals(uuid) RESET plan_cache_mode;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062330_buffered_search_feed_custom_plans.sql' AND sha256='99cfe771b771bd889c1006e2cb046f7ada85fd330917a253a19903bfee28b068';
COMMIT;
