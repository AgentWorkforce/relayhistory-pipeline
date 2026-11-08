-- Generated: revoke the twelve search_consumer_runtime EXECUTEs the writer-grant
-- migration installed; the bare role itself stays (DROP ROLE is an operator decision).
-- Explicit transaction: guard, marker fence, revokes and ledger removal are atomic.
-- On an error the caller must ROLLBACK (or disconnect); nothing is committed.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='consumer grants: migration ledger/marker required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261108000000_session_consumer_function_grants.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='consumer grants: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261108000000_session_consumer_function_grants.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'72998e356ff9b305485dfd47931a667922f14c94b55aa236794d2340b7362b26'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='consumer grants: own applied checksum required';END IF;
 UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='consumer grants: validation marker required';END IF;
END $rollback_guard$;
REVOKE EXECUTE ON FUNCTION sessions.upsert_session_index(jsonb) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.clean_root_commit(text) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.session_publication_lock(jsonb) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.session_index_batch(jsonb) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.session_publication_is_managed(text,text,text,text,text) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.session_directory_search_text(text,text,text,text,text,text,text,text) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.applied_repo_evidence(jsonb,jsonb) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.lock_repo_catalog(text) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.resolve_repo(text,text,text,timestamp with time zone) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.legacy_remote(text) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.repo_live(text,uuid) FROM search_consumer_runtime;
REVOKE EXECUTE ON FUNCTION sessions.canonical_remote(text) FROM search_consumer_runtime;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261108000000_session_consumer_function_grants.sql' AND sha256='72998e356ff9b305485dfd47931a667922f14c94b55aa236794d2340b7362b26';
COMMIT;
