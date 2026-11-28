-- Generated session directory binding rollback (scripts/sessions-function-boundary/lanes/session-directory-binding.ts).
-- Drops the 062380 bindings reader, the binding trigger and its function, the pair constraint and the three columns;
-- it keeps the revision sequence, so a re-apply never reissues or restarts a revision.
-- every issued binding id is forgotten, so a consumer holding one fails closed. Turn
-- READ_API_SESSION_BINDINGS_ENABLED off first.
-- Code only: hosted execution needs Khaliq's separate explicit approval. On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1);
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session directory binding: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062380_session_directory_binding.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session directory binding: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062380_session_directory_binding.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha IS DISTINCT FROM '5b62c4112ef8c7bd76c42b72efd1d2231f76b09eeac72a146e37086a75df2add'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session directory binding: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.session_directory_bindings_for(text,text[],text[]);
DROP TRIGGER zz_session_directory_binding ON sessions.session_directory;
DROP FUNCTION sessions.session_directory_binding();
ALTER TABLE sessions.session_directory DROP CONSTRAINT session_directory_binding_pair,
  DROP COLUMN session_binding_created_at, DROP COLUMN session_binding_revision, DROP COLUMN session_binding_id;
-- sessions.session_binding_revision is kept (RETAINED_BY_ROLLBACK): a re-apply continues the revision order.
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062380_session_directory_binding.sql' AND sha256='5b62c4112ef8c7bd76c42b72efd1d2231f76b09eeac72a146e37086a75df2add';
COMMIT;
