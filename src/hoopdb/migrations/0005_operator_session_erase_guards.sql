-- Optional 96bulls projections are installed after the upstream chain. Install their
-- operator fence guards here too, on a fresh database as well as an existing one.
CREATE TABLE sessions.session_erasure_optional_guards (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 installed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE sessions.session_erasure_optional_guards ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_erasure_optional_guards FROM PUBLIC,anon,authenticated,service_role;
INSERT INTO sessions.session_erasure_optional_guards DEFAULT VALUES;
DO $guards$ DECLARE t text; BEGIN
 IF to_regprocedure('sessions.ingest_operator_runtime_metadata_guard()') IS NULL THEN RETURN;END IF;
 FOREACH t IN ARRAY ARRAY['session_index_ext','session_summaries','session_verdicts'] LOOP
  IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass('sessions.'||t) AND tgname='operator_erase_metadata_guard') THEN
   EXECUTE format('CREATE TRIGGER operator_erase_metadata_guard BEFORE INSERT OR UPDATE ON sessions.%I FOR EACH ROW EXECUTE FUNCTION sessions.ingest_operator_runtime_metadata_guard()',t);
  END IF;
 END LOOP;
END $guards$;
