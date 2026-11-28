-- Remove the v2 summary read surface (T130 Reader provenance). Briefs, pointers and
-- 044100's v1 read function are untouched; the API falls back to v1 (revision null).
-- Guard, function removal and ledger removal are one transaction under the
-- migrator lock. On refusal the caller must ROLLBACK or disconnect.
-- Code only: hosted execution needs Khaliq's separate explicit approval.
BEGIN;
SELECT pg_advisory_xact_lock(961996, 1);
DO $rollback_guard$
DECLARE v_sha text;
BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief summary v2: migration ledger required';
 END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations
   WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062500_session_brief_summary_v2_read.sql') THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief summary v2: a later migration is applied; roll it back first';
 END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations
  WHERE name='upstream:20261105062500_session_brief_summary_v2_read.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'2c2ea6e1f857c1861c08bff1d3895e234a74752cddd204cbf75403ebfd694739' THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief summary v2: own applied checksum required';
 END IF;
END
$rollback_guard$;
DROP FUNCTION sessions.session_brief_summary_v2_for(text,text,text);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062500_session_brief_summary_v2_read.sql';
COMMIT;
