-- Drops the 20261105062050 read API token reader kind: read_api_token_issue_v2, the
-- sessions.read_api_tokens.reader_kind column (with its CHECK) and its ledger row. The recorded reader
-- kinds are lost; tokens themselves stay valid. Turn READ_API_MINT_READER_KIND off first, or a mint that
-- carries reader_kind fails once the function is gone.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='read api token reader kind: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062050_read_api_token_reader_kind.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='read api token reader kind: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062050_read_api_token_reader_kind.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'6aa944809fb9e29661592d7dbbae165b69f45632fdfadd18c7e3a99f84dddf73'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='read api token reader kind: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.read_api_token_issue_v2(text,text,text,text,text,text,text,text,timestamptz,text,timestamptz,text,timestamptz,text);
ALTER TABLE sessions.read_api_tokens DROP COLUMN reader_kind;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062050_read_api_token_reader_kind.sql';
COMMIT;
