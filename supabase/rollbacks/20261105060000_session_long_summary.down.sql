-- Drops the 060000 long summary reader, its trigger and the long summary columns, and its ledger row;
-- short summaries and the encrypted summary objects are retained.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='long summary: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105060000_session_long_summary.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='long summary: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105060000_session_long_summary.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'df3dc2fa12745a037f46a4aa16de568cf56e6e3f7bbd66c21e11bfbf1332d6eb'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='long summary: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.session_long_summary_for(text,text,text);
DROP TRIGGER session_directory_zz_long_summary_follows_short ON sessions.session_directory;
DROP FUNCTION sessions.session_directory_long_summary_follows_short();
ALTER TABLE sessions.session_directory
  DROP CONSTRAINT session_directory_long_summary_follows_short,
  DROP CONSTRAINT session_directory_long_summary_provenance,
  DROP COLUMN long_summary_updated_at,
  DROP COLUMN long_summary_generated_at,
  DROP COLUMN long_summary_source,
  DROP COLUMN long_summary_scrub_version,
  DROP COLUMN long_summary_scrub_ruleset,
  DROP COLUMN long_summary;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105060000_session_long_summary.sql';
COMMIT;
