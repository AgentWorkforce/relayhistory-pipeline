-- Down path for supabase/migrations/20261105045020_selfhost_output_facts.sql.
-- The facts are derived (rebuilt from the canonical read by the self-host consumer while
-- RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true), so dropping them loses no source data.
-- Both roles stay (their bootstrap files own them).
--
-- Like 20261105045000's named down, it removes this file's ledger row so the next migrate
-- reapplies it cleanly, which is only possible while it is the LAST applied upstream file:
-- refused (55000) otherwise. Run as one transaction (psql -1 -v ON_ERROR_STOP=1).
-- Code only: running it against a hosted database needs Khaliq's OK.
DO $rollback_guard$ DECLARE v_later boolean; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C" > ''upstream:20261105045020_selfhost_output_facts.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'self-host output facts: a later migration is applied; roll it back first'; END IF;
 END IF;
END $rollback_guard$;
DROP FUNCTION IF EXISTS sessions.output_facts_coverage_for(text, timestamptz, timestamptz);
DROP FUNCTION IF EXISTS sessions.output_facts_outputs_for(text, timestamptz, timestamptz, text, integer);
DROP FUNCTION IF EXISTS sessions.output_facts_events_for(text, timestamptz, timestamptz, text, integer);
DROP FUNCTION IF EXISTS sessions.output_facts_erase(text, text, text, text[]);
DROP FUNCTION IF EXISTS sessions.output_facts_record_state(text, text, text, text[], text, boolean, timestamptz, timestamptz, bigint, timestamptz);
DROP FUNCTION IF EXISTS sessions.output_facts_reconcile(text, text, text, text[], text, jsonb, jsonb, boolean, bigint);
DROP FUNCTION IF EXISTS sessions.output_facts_state(text, text, text, text[], text);
DROP FUNCTION IF EXISTS sessions.output_facts_stored_version(text, text, text, text);
DROP FUNCTION IF EXISTS sessions.output_facts_writer_scope(text, text, text, text[], text);
DROP TABLE IF EXISTS sessions.output_facts_workspace_state;
DROP TABLE IF EXISTS sessions.output_facts_session_state;
DROP TABLE IF EXISTS sessions.session_output_facts;
DROP TABLE IF EXISTS sessions.session_event_facts;
DO $ledger$ BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105045020_selfhost_output_facts.sql';
 END IF;
END $ledger$;
