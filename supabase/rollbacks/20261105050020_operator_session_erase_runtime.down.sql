-- Generated operator erase runtime rollback. Never remove active write obligations or used erase state.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1);
DO $guard$ DECLARE v_sha text;t record;BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator runtime: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105050020_operator_session_erase_runtime.sql') THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator runtime: later migration applied';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105050020_operator_session_erase_runtime.sql' FOR UPDATE;
 IF v_sha IS DISTINCT FROM '6df6774169cac0b4179d51643de4a4c6e4ff28018fad75b8fb95f47b7eaa6bae' THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator runtime: own applied checksum required';END IF;
 IF EXISTS(SELECT 1 FROM sessions.session_erasure_writes) OR EXISTS(SELECT 1 FROM sessions.session_erasure_runs) OR EXISTS(SELECT 1 FROM sessions.session_erasure_scopes) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator runtime: used erase or write state exists';END IF;
 FOR t IN SELECT c.relname FROM pg_trigger g JOIN pg_class c ON c.oid=g.tgrelid WHERE g.tgfoid='sessions.ingest_operator_runtime_metadata_guard()'::regprocedure LOOP
  EXECUTE format('DROP TRIGGER operator_erase_metadata_guard ON sessions.%I',t.relname);END LOOP;
END $guard$;
DROP TABLE sessions.session_erasure_writes,sessions.session_erasure_scopes,sessions.session_erasure_runs;
DROP FUNCTION sessions.ingest_operator_runtime_metadata_guard(),
 sessions.ingest_operator_runtime_purge(text,text,text,text,uuid),
 sessions.ingest_operator_runtime_catalog(),
 sessions.ingest_operator_runtime_erased(text,text,text,text),
 sessions.ingest_operator_runtime_upsert(jsonb,text,text),
 sessions.ingest_operator_runtime_step(text,text,text,text,uuid,text,text,bigint,text),
 sessions.ingest_operator_runtime_release(text,text,text,text,uuid),
 sessions.ingest_operator_runtime_authorize(text,text,text,text,uuid),
 sessions.ingest_operator_runtime_next(uuid),
 sessions.ingest_operator_runtime_begin(text,text,text,text,text,text,text,text,text,text),
 sessions.ingest_operator_runtime_location_valid(text,text,text,text,text,text,text),
 sessions.ingest_operator_runtime_write_finish(text,text,uuid),
 sessions.ingest_operator_runtime_write_begin(jsonb),
 sessions.ingest_operator_runtime_ids(text,text,text,text);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105050020_operator_session_erase_runtime.sql';
COMMIT;
