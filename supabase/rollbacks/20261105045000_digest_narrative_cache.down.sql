-- Down path for supabase/migrations/20261105045000_digest_narrative_cache.sql.
-- The cache is derived (regenerated on demand from the deterministic digest), so dropping it
-- loses no source data; a regenerated narrative costs one model call per (day, tz, scope).
-- Both roles stay (their bootstrap files own them).
--
-- Like 20261105034000's named down, it removes this file's ledger row so the next migrate
-- reapplies it cleanly, which is only possible while it is the LAST applied upstream file:
-- refused (55000) otherwise. Run as one transaction (psql -1 -v ON_ERROR_STOP=1).
-- Code only: running it against a hosted database needs Khaliq's OK.
DO $rollback_guard$ DECLARE v_later boolean; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C" > ''upstream:20261105045000_digest_narrative_cache.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'digest narrative cache: a later migration is applied; roll it back first'; END IF;
 END IF;
END $rollback_guard$;
DROP FUNCTION IF EXISTS sessions.purge_daily_digest_narratives(text, text);
DROP FUNCTION IF EXISTS sessions.digest_narrative_upsert(text, text, date, text, text, text, text, text, timestamptz);
DROP FUNCTION IF EXISTS sessions.digest_narrative_read(text, date, text, text);
DROP FUNCTION IF EXISTS sessions.digest_narrative_scope_workspace(text);
DROP TABLE IF EXISTS sessions.daily_digest_narratives;
DO $ledger$ BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105045000_digest_narrative_cache.sql';
 END IF;
END $ledger$;
