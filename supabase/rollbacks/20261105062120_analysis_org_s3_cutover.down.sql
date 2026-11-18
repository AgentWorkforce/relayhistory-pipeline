-- Drops the 20261105062120 analysis org S3 cutover proof: sessions.session_analysis_cutover_free,
-- sessions.session_analysis_first_seen_v1, sessions.org_s3_cutover_correct_v1, the sessions.org_s3_cutover and sessions.org_s3_cutover_history tables
-- with their immutability triggers and their function, and the ledger row.
-- Refuses while any cutover or history row is recorded: they are the audit record, and dropping them would
-- silently erase who attested what. Export the rows and remove them in a reviewed owner transaction first.
-- Turn SESSION_ANALYSIS_CUTOVER_PROOF off on the worker before rolling back. With it on and the function gone,
-- the proof's SQL errors: the job fails with the retryable analysis_processing_failed (it is retried, never
-- completed on a missing proof). Refuses while 20261105062130 is applied (roll that back first).
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='org s3 cutover: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062120_analysis_org_s3_cutover.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='org s3 cutover: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062120_analysis_org_s3_cutover.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'c0c85a9bc425981a9aac0c4a9435e84f39b3699b98e69deb8c2fab6c334b2b6f'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='org s3 cutover: own applied checksum required';END IF;
 LOCK TABLE sessions.org_s3_cutover, sessions.org_s3_cutover_history IN ACCESS EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.org_s3_cutover) OR EXISTS(SELECT 1 FROM sessions.org_s3_cutover_history)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='org s3 cutover: recorded cutovers exist; export and remove them in a reviewed owner transaction first';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.session_analysis_cutover_free(text,text,text,text,timestamptz);
DROP FUNCTION sessions.session_analysis_first_seen_v1(text,text,text);
DROP FUNCTION sessions.org_s3_cutover_correct_v1(text,timestamptz,timestamptz,text,text,boolean);
DROP TABLE sessions.org_s3_cutover, sessions.org_s3_cutover_history;
DROP FUNCTION sessions.org_s3_cutover_immutable_v1(), sessions.org_s3_cutover_stamp_v1();
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062120_analysis_org_s3_cutover.sql';
COMMIT;
