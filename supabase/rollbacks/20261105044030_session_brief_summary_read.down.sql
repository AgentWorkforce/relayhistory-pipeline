-- Remove only the read surface; generated briefs and pointers are retained.
-- Guard, function removal and ledger removal are one transaction under the
-- migrator lock. On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996, 1);
DO $rollback_guard$
DECLARE v_sha text;
BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief summary: migration ledger required';
 END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations
   WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105044030_session_brief_summary_read.sql') THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief summary: a later migration is applied; roll it back first';
 END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations
  WHERE name='upstream:20261105044030_session_brief_summary_read.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'a6b4049e186f632cccc1b29e596cc11d8fcbcbb7b5f91c0da579918a0d31d93f' THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief summary: own applied checksum required';
 END IF;
END
$rollback_guard$;
DROP FUNCTION sessions.session_brief_summary_for(text,text,text);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105044030_session_brief_summary_read.sql';
COMMIT;
