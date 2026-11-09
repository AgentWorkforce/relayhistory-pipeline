-- Removes the read API token table and its functions. Every self-issued token stops working.
-- Explicit transaction: guard, drops and ledger removal are atomic. On an error the caller must
-- ROLLBACK (or disconnect); nothing is committed.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='read api tokens: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105044000_read_api_tokens.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='read api tokens: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105044000_read_api_tokens.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'33a2ae8f07a8ebe3574ce673a7b58a17e640bc7007002a01be29ad4a66ccf3f4'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='read api tokens: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.read_api_token_delete_for(text, text, text, text);
DROP FUNCTION sessions.read_api_token_delete_by_hash(text, text);
DROP FUNCTION sessions.read_api_token_refresh(text, text, text, text, text, text, timestamptz, text, timestamptz, timestamptz);
DROP FUNCTION sessions.read_api_token_issue(text, text, text, text, text, text, text, text, timestamptz, text, timestamptz, text, timestamptz);
DROP FUNCTION sessions.read_api_token_sweep_expired(timestamptz);
DROP FUNCTION sessions.read_api_token_verify(text, timestamptz);
DROP TABLE sessions.read_api_tokens;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105044000_read_api_tokens.sql';
COMMIT;
