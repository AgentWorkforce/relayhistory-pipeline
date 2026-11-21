-- Rolls back 20261105062200 (session list readiness): drops sessions.list_session_directory_readiness_for and
-- sessions.query_session_directory_readiness_for and the ledger row. Both functions only read: no table, row, index
-- or other function is touched, and 034000's list_session_directory_for and 046010's query_session_directory_for
-- stay as they are. Roll the read API back to READ_API_LIST_READINESS_ENABLED off first: with the flag on, a
-- includeReadiness or readyOnly request after this rollback answers 501 directory_not_migrated, and every other
-- list request keeps working unchanged.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session list readiness: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062200_session_directory_list_readiness.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session list readiness: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062200_session_directory_list_readiness.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'df7115c8447eb37a42dad9e569d42a04e0cdab3b0d5e0e9fcdf31ddba463156f'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session list readiness: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.list_session_directory_readiness_for(text,text[],text[],text,text[],text,timestamptz,timestamptz,timestamptz,timestamptz,text[],text[],timestamptz,text,text,integer,boolean);
DROP FUNCTION sessions.query_session_directory_readiness_for(text,text[],text[],text[],text[],text[],text[],text[],text[],text[],text[],text[],text[],boolean,text[],text[],text,text[],text,timestamptz,timestamptz,timestamptz,timestamptz,timestamptz,text,text,integer,boolean);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062200_session_directory_list_readiness.sql';
COMMIT;
