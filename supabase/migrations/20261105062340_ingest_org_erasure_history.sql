-- Slot 20261105062340 (confirmed by layered-sessions-pipeline). Per-org erasure HISTORY for the 062320 admission hold
-- release: a durable, insert-only record of every write to the 15 per-org erasure sources, which 062320's release
-- counts, under the org key, from a bound FULL cutoff. 062320's bytes are unchanged; this slot replaces its _release
-- and _status (its generated down restores both exactly). Code only: applying it to any real database needs Khaliq's
-- explicit OK.
--
-- Why: 062320's release compared a count of the surviving rows of two sources. A surviving-row count cannot show that
-- nothing happened in a window: ingest_apply_selection DELETEs upload_deletions rows on a re-enable, so an erasure and
-- its removal both inside the window leave no row, and an update of an old row into the window keeps the count.
--
-- Objects (the three tables are retained by this slot's down as inert history, like 062320's):
--   sessions.ingest_org_erasure_history           one row per INSERT, UPDATE or DELETE of a row of any of the 15
--                                                 sources (src/hoopdb/org-erasure-history-sources.ts, pinned by test):
--                                                 the org, the source table, the operation, the time, the writer's xid.
--                                                 An UPDATE that moves a row between orgs is recorded for both.
--   sessions.ingest_org_erasure_history_coverage  'started' at this slot's apply (after the recorders exist),
--                                                 'ended' at its down, 'truncated' at a permitted source TRUNCATE.
--                                                 History covers from the latest started/truncated row only while no
--                                                 'ended' follows the latest 'started'; a down/reapply gap is never
--                                                 covered.
--   sessions.ingest_org_erasure_history_bindings  per hold epoch: the cutoff (the hold's installed_at, never a
--                                                 caller value) and the FULL run's provenance (run, start, the seal's
--                                                 evidence), bound once while sealed.
--   Org identifiers and timestamps only: no workspace, user, session, key or content (retention below).
-- Writer participation: the recorder is an AFTER ROW trigger on every source for INSERT, UPDATE and DELETE. It takes
--   the org key (062320's hold key) SHARED until the writer commits, for each org the row names. 062320's release
--   takes the key EXCLUSIVE (after (961996,1) SHARED, lifecycle order unchanged), so a write either commits before
--   the release reads the history (and is counted) or waits until the release has committed. TRUNCATE of a source
--   is refused while any hold is not released, and a permitted one restarts coverage. No runtime role holds a table
--   privilege on any source (every write is an owner-run function). An owner DISABLE/ENABLE TRIGGER, DROP/CREATE
--   TRIGGER or rename rewrites the trigger's pg_trigger row: recording() compares every row's xmin with the apply's
--   xid, so a disarm, even one already reverted, refuses every later bind and release until a down and reapply.
--   Out of model, stated: owner CREATE OR REPLACE of the recorder function, and superuser session_replication_role.
-- Cutoff: the hold's installed_at. 062320's install takes the org key EXCLUSIVE, which waits for every write still
--   holding it SHARED (the recorder holds it to commit): a write recorded before installed_at committed before the
--   install, and is visible to a FULL that starts after it; a write after it is in the history. No margin is needed
--   for transaction lifetime.
-- Retention: the history is kept with the hold history (062320 design section 6) and is immutable (guard below). It
--   names an org and a table, never a person or a session; removing it needs a separately reviewed owner procedure.

CREATE TABLE IF NOT EXISTS sessions.ingest_org_erasure_history (
  erased_org text NOT NULL,
  source text NOT NULL CHECK (source IN ('operator_user_erasures', 'session_erasure_fences', 'session_erasure_receipts', 'session_erasure_events', 'session_erasure_runs', 'session_erasure_scopes', 'session_erasure_writes', 'upload_deletions', 'upload_resets', 'upload_erasure_rearm', 'relayfacts_erasure_fences', 'session_analysis_purge_fences', 'session_analysis_purge_authority', 'ingest_frozen_source_deletions', 'ingest_frozen_source_resets')),
  op text NOT NULL CHECK (op IN ('INSERT', 'UPDATE', 'DELETE')),
  at timestamptz NOT NULL DEFAULT clock_timestamp(),
  xid xid8 NOT NULL DEFAULT pg_current_xact_id()
);
CREATE INDEX IF NOT EXISTS ingest_org_erasure_history_org ON sessions.ingest_org_erasure_history (erased_org, at);
CREATE TABLE IF NOT EXISTS sessions.ingest_org_erasure_history_coverage (
  kind text NOT NULL CHECK (kind IN ('started', 'ended', 'truncated')),
  at timestamptz NOT NULL DEFAULT clock_timestamp(),
  xid xid8 NOT NULL DEFAULT pg_current_xact_id()
);
CREATE TABLE IF NOT EXISTS sessions.ingest_org_erasure_history_bindings (
  epoch uuid PRIMARY KEY,
  bound_org text NOT NULL CHECK (bound_org ~ '^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$'),
  cutoff timestamptz NOT NULL,
  full_run text NOT NULL CHECK (full_run ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
  full_started_at timestamptz NOT NULL,
  full_evidence_sha256 text NOT NULL CHECK (full_evidence_sha256 ~ '^[0-9a-f]{64}$'),
  bound_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE sessions.ingest_org_erasure_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_org_erasure_history_coverage ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_org_erasure_history_bindings ENABLE ROW LEVEL SECURITY;

-- All three are insert-only: no UPDATE, DELETE or TRUNCATE.
CREATE OR REPLACE FUNCTION sessions.ingest_org_erasure_history_guard() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $erasure_history_guard$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure history is immutable';
END
$erasure_history_guard$;
DROP TRIGGER IF EXISTS ingest_org_erasure_history_guard ON sessions.ingest_org_erasure_history;
DROP TRIGGER IF EXISTS ingest_org_erasure_history_truncate ON sessions.ingest_org_erasure_history;
CREATE TRIGGER ingest_org_erasure_history_guard BEFORE UPDATE OR DELETE ON sessions.ingest_org_erasure_history
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_guard();
CREATE TRIGGER ingest_org_erasure_history_truncate BEFORE TRUNCATE ON sessions.ingest_org_erasure_history
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_guard();
DROP TRIGGER IF EXISTS ingest_org_erasure_history_coverage_guard ON sessions.ingest_org_erasure_history_coverage;
DROP TRIGGER IF EXISTS ingest_org_erasure_history_coverage_truncate ON sessions.ingest_org_erasure_history_coverage;
CREATE TRIGGER ingest_org_erasure_history_coverage_guard BEFORE UPDATE OR DELETE ON sessions.ingest_org_erasure_history_coverage
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_guard();
CREATE TRIGGER ingest_org_erasure_history_coverage_truncate BEFORE TRUNCATE ON sessions.ingest_org_erasure_history_coverage
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_guard();
DROP TRIGGER IF EXISTS ingest_org_erasure_history_bindings_guard ON sessions.ingest_org_erasure_history_bindings;
DROP TRIGGER IF EXISTS ingest_org_erasure_history_bindings_truncate ON sessions.ingest_org_erasure_history_bindings;
CREATE TRIGGER ingest_org_erasure_history_bindings_guard BEFORE UPDATE OR DELETE ON sessions.ingest_org_erasure_history_bindings
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_guard();
CREATE TRIGGER ingest_org_erasure_history_bindings_truncate BEFORE TRUNCATE ON sessions.ingest_org_erasure_history_bindings
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_guard();

-- Owner-only, from the catalog: every non-owner grantee (PUBLIC, any role, column grants) on the three tables and the
-- guard is revoked, so no grant survives a down and reapply.
DO $erasure_history_acl$
DECLARE r record;
BEGIN
 FOR r IN SELECT c.relname, x.grantee FROM pg_class c, aclexplode(c.relacl) x
           WHERE c.oid IN ('sessions.ingest_org_erasure_history'::regclass, 'sessions.ingest_org_erasure_history_coverage'::regclass, 'sessions.ingest_org_erasure_history_bindings'::regclass) AND x.grantee <> c.relowner GROUP BY c.relname, x.grantee LOOP
   EXECUTE format('REVOKE ALL ON sessions.%I FROM %s', r.relname, CASE WHEN r.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(r.grantee)) END);
 END LOOP;
 FOR r IN SELECT c.relname, a.attname, x.grantee FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid, aclexplode(a.attacl) x
           WHERE a.attrelid IN ('sessions.ingest_org_erasure_history'::regclass, 'sessions.ingest_org_erasure_history_coverage'::regclass, 'sessions.ingest_org_erasure_history_bindings'::regclass) AND a.attnum > 0 AND x.grantee <> c.relowner GROUP BY c.relname, a.attname, x.grantee LOOP
   EXECUTE format('REVOKE ALL (%I) ON sessions.%I FROM %s', r.attname, r.relname, CASE WHEN r.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(r.grantee)) END);
 END LOOP;
 FOR r IN SELECT x.grantee FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) x
           WHERE p.oid = 'sessions.ingest_org_erasure_history_guard()'::regprocedure AND x.grantee <> p.proowner GROUP BY x.grantee LOOP
   EXECUTE format('REVOKE ALL ON FUNCTION sessions.ingest_org_erasure_history_guard() FROM %s', CASE WHEN r.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(r.grantee)) END);
 END LOOP;
END $erasure_history_acl$;

-- A retained history (this slot's down keeps the three tables) must be exactly this one, else 55000, nothing applied.
DO $erasure_history_shape$
BEGIN
 -- IS NOT TRUE, not NOT: an empty aggregate is NULL, and NOT NULL would skip the refusal.
 IF (
-- erasure history shape begin
-- The exact retained history: owner, columns, column defaults, indexes, constraints (NOT NULL is the column check;
-- PostgreSQL 18 and PGlite also catalogue it as a constraint, so 'n' rows are excluded), the six guard triggers on the
-- guard function, enabled, and owner-only ACLs on the three tables (every grantee, column grants included) and on the
-- guard (PUBLIC included). Every aggregate sorts COLLATE "C". Shared verbatim by this slot's up and its generated down.
 (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'sessions.ingest_org_erasure_history'::regclass) = current_user
 AND (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'sessions.ingest_org_erasure_history_coverage'::regclass) = current_user
 AND (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'sessions.ingest_org_erasure_history_bindings'::regclass) = current_user
 AND (SELECT string_agg(attname || ' ' || format_type(atttypid, atttypmod) || CASE WHEN attnotnull THEN ' not null' ELSE '' END, ', ' ORDER BY attnum)
        FROM pg_attribute WHERE attrelid = 'sessions.ingest_org_erasure_history'::regclass AND attnum > 0 AND NOT attisdropped)
     = 'erased_org text not null, source text not null, op text not null, at timestamp with time zone not null, xid xid8 not null'
 AND (SELECT string_agg(attname || ' ' || format_type(atttypid, atttypmod) || CASE WHEN attnotnull THEN ' not null' ELSE '' END, ', ' ORDER BY attnum)
        FROM pg_attribute WHERE attrelid = 'sessions.ingest_org_erasure_history_coverage'::regclass AND attnum > 0 AND NOT attisdropped)
     = 'kind text not null, at timestamp with time zone not null, xid xid8 not null'
 AND (SELECT string_agg(attname || ' ' || format_type(atttypid, atttypmod) || CASE WHEN attnotnull THEN ' not null' ELSE '' END, ', ' ORDER BY attnum)
        FROM pg_attribute WHERE attrelid = 'sessions.ingest_org_erasure_history_bindings'::regclass AND attnum > 0 AND NOT attisdropped)
     = 'epoch uuid not null, bound_org text not null, cutoff timestamp with time zone not null, full_run text not null, full_started_at timestamp with time zone not null, full_evidence_sha256 text not null, bound_at timestamp with time zone not null'
 AND (SELECT array_agg(pg_get_indexdef(indexrelid)::text ORDER BY pg_get_indexdef(indexrelid)::text COLLATE "C") FROM pg_index WHERE indrelid IN ('sessions.ingest_org_erasure_history'::regclass, 'sessions.ingest_org_erasure_history_coverage'::regclass, 'sessions.ingest_org_erasure_history_bindings'::regclass))
     = ARRAY[
         'CREATE INDEX ingest_org_erasure_history_org ON sessions.ingest_org_erasure_history USING btree (erased_org, at)',
         'CREATE UNIQUE INDEX ingest_org_erasure_history_bindings_pkey ON sessions.ingest_org_erasure_history_bindings USING btree (epoch)']::text[]
 AND (SELECT array_agg(conrelid::regclass::text || ' ' || contype::text || ' ' || pg_get_constraintdef(oid) ORDER BY conrelid::regclass::text COLLATE "C", contype::text COLLATE "C", pg_get_constraintdef(oid) COLLATE "C")
        FROM pg_constraint WHERE conrelid IN ('sessions.ingest_org_erasure_history'::regclass, 'sessions.ingest_org_erasure_history_coverage'::regclass, 'sessions.ingest_org_erasure_history_bindings'::regclass) AND contype <> 'n')
     = ARRAY[
         'sessions.ingest_org_erasure_history c CHECK ((op = ANY (ARRAY[''INSERT''::text, ''UPDATE''::text, ''DELETE''::text])))',
         'sessions.ingest_org_erasure_history c CHECK ((source = ANY (ARRAY[''operator_user_erasures''::text, ''session_erasure_fences''::text, ''session_erasure_receipts''::text, ''session_erasure_events''::text, ''session_erasure_runs''::text, ''session_erasure_scopes''::text, ''session_erasure_writes''::text, ''upload_deletions''::text, ''upload_resets''::text, ''upload_erasure_rearm''::text, ''relayfacts_erasure_fences''::text, ''session_analysis_purge_fences''::text, ''session_analysis_purge_authority''::text, ''ingest_frozen_source_deletions''::text, ''ingest_frozen_source_resets''::text])))',
         'sessions.ingest_org_erasure_history_bindings c CHECK ((bound_org ~ ''^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$''::text))',
         'sessions.ingest_org_erasure_history_bindings c CHECK ((full_evidence_sha256 ~ ''^[0-9a-f]{64}$''::text))',
         'sessions.ingest_org_erasure_history_bindings c CHECK ((full_run ~ ''^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$''::text))',
         'sessions.ingest_org_erasure_history_bindings p PRIMARY KEY (epoch)',
         'sessions.ingest_org_erasure_history_coverage c CHECK ((kind = ANY (ARRAY[''started''::text, ''ended''::text, ''truncated''::text])))']::text[]
 AND (SELECT array_agg(t.tgname || ' ' || t.tgtype ORDER BY t.tgname COLLATE "C") FROM pg_trigger t
       WHERE t.tgrelid IN ('sessions.ingest_org_erasure_history'::regclass, 'sessions.ingest_org_erasure_history_coverage'::regclass, 'sessions.ingest_org_erasure_history_bindings'::regclass) AND NOT t.tgisinternal AND t.tgenabled = 'O' AND t.tgfoid = 'sessions.ingest_org_erasure_history_guard()'::regprocedure)
     = ARRAY['ingest_org_erasure_history_bindings_guard 27', 'ingest_org_erasure_history_bindings_truncate 34', 'ingest_org_erasure_history_coverage_guard 27', 'ingest_org_erasure_history_coverage_truncate 34', 'ingest_org_erasure_history_guard 27', 'ingest_org_erasure_history_truncate 34']::text[]
 AND (SELECT count(*) FROM pg_trigger t WHERE t.tgrelid IN ('sessions.ingest_org_erasure_history'::regclass, 'sessions.ingest_org_erasure_history_coverage'::regclass, 'sessions.ingest_org_erasure_history_bindings'::regclass) AND NOT t.tgisinternal) = 6
 AND (SELECT array_agg(c.relname::text || ' ' || a.attname::text || ' ' || pg_get_expr(d.adbin, d.adrelid) ORDER BY c.relname::text COLLATE "C", a.attname::text COLLATE "C")
        FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum JOIN pg_class c ON c.oid = d.adrelid
       WHERE d.adrelid IN ('sessions.ingest_org_erasure_history'::regclass, 'sessions.ingest_org_erasure_history_coverage'::regclass, 'sessions.ingest_org_erasure_history_bindings'::regclass))
     = ARRAY['ingest_org_erasure_history at clock_timestamp()', 'ingest_org_erasure_history xid pg_current_xact_id()', 'ingest_org_erasure_history_bindings bound_at clock_timestamp()', 'ingest_org_erasure_history_coverage at clock_timestamp()', 'ingest_org_erasure_history_coverage xid pg_current_xact_id()']::text[]
 AND NOT EXISTS (SELECT 1 FROM pg_class c, aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x
       WHERE c.oid IN ('sessions.ingest_org_erasure_history'::regclass, 'sessions.ingest_org_erasure_history_coverage'::regclass, 'sessions.ingest_org_erasure_history_bindings'::regclass) AND x.grantee <> c.relowner)
 AND NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid IN ('sessions.ingest_org_erasure_history'::regclass, 'sessions.ingest_org_erasure_history_coverage'::regclass, 'sessions.ingest_org_erasure_history_bindings'::regclass) AND attnum > 0 AND attacl IS NOT NULL)
 AND NOT EXISTS (SELECT 1 FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) x
       WHERE p.oid = 'sessions.ingest_org_erasure_history_guard()'::regprocedure AND x.grantee <> p.proowner)
-- erasure history shape end
 ) IS NOT TRUE THEN
   RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'org erasure history: an existing history is not this slot''s; nothing applied';
 END IF;
END $erasure_history_shape$;

-- Lifecycle serialization with this slot's down: callers have taken (961996,1) SHARED (062320's lifecycle); this
-- fresh statement then refuses if the down has committed, so a paused call never acts on a removed slot.
CREATE FUNCTION sessions.ingest_org_erasure_history_applied() RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $erasure_history_applied$
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NULL OR NOT EXISTS (
       SELECT 1 FROM sessions.__96bulls_migrations
        WHERE name = 'upstream:20261105062340_ingest_org_erasure_history.sql') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure history slot not applied';
  END IF;
END
$erasure_history_applied$;

-- The start of the current continuous coverage: the latest 'started' or 'truncated' row, valid only while no 'ended'
-- row follows the latest 'started' one (a down ends coverage; a reapply starts a new period). A permitted TRUNCATE
-- (only possible with no live hold) moves the start forward: nothing bound before it is covered.
CREATE FUNCTION sessions.ingest_org_erasure_history_coverage_start() RETURNS timestamptz
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $erasure_history_coverage_start$
  SELECT (SELECT max(c.at) FROM sessions.ingest_org_erasure_history_coverage AS c WHERE c.kind IN ('started', 'truncated'))
   WHERE EXISTS (SELECT 1 FROM sessions.ingest_org_erasure_history_coverage AS s WHERE s.kind = 'started'
                  AND NOT EXISTS (SELECT 1 FROM sessions.ingest_org_erasure_history_coverage AS e
                                   WHERE e.kind = 'ended' AND e.at >= s.at)
                  AND s.at = (SELECT max(x.at) FROM sessions.ingest_org_erasure_history_coverage AS x WHERE x.kind = 'started'))
$erasure_history_coverage_start$;

-- True only when the current continuous coverage began at or before p_cutoff. NULL cutoff: false.
CREATE FUNCTION sessions.ingest_org_erasure_history_covers(p_cutoff timestamptz) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $erasure_history_covers$
  SELECT COALESCE(sessions.ingest_org_erasure_history_coverage_start() <= p_cutoff, false)
$erasure_history_covers$;

-- The recorder: AFTER INSERT OR UPDATE OR DELETE, per row, on every source. Takes the org key SHARED for each org the
-- row names (OLD and NEW; both on a reassignment), in key order, then appends one history row per org.
CREATE FUNCTION sessions.ingest_org_erasure_history_record() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $erasure_history_record$
DECLARE v_old text; v_new text; k bigint;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN v_old := OLD.org_id; END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN v_new := NEW.org_id; END IF;
  FOR k IN SELECT DISTINCT sessions.ingest_org_admission_hold_key(o) FROM unnest(ARRAY[v_old, v_new]) AS o
            WHERE o IS NOT NULL ORDER BY 1 LOOP
    PERFORM pg_advisory_xact_lock_shared(k);
  END LOOP;
  INSERT INTO sessions.ingest_org_erasure_history (erased_org, source, op)
    SELECT DISTINCT o, TG_TABLE_NAME, TG_OP FROM unnest(ARRAY[v_old, v_new]) AS o WHERE o IS NOT NULL;
  RETURN NULL;
END
$erasure_history_record$;

-- TRUNCATE of a source records no row, so it is refused while any hold is not released. SHARE on the holds table
-- serializes it with an install (which inserts there): an install committed first is seen; one after it follows the
-- truncate, before its own FULL.
CREATE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $erasure_history_truncate$
BEGIN
  LOCK TABLE sessions.ingest_org_admission_holds IN SHARE MODE;
  IF EXISTS (SELECT 1 FROM sessions.ingest_org_admission_holds WHERE state <> 'released') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: erasure source truncate refused while an org is held';
  END IF;
  -- A truncate records no per-row history: coverage restarts after it, so no later binding can reach across it.
  INSERT INTO sessions.ingest_org_erasure_history_coverage (kind) VALUES ('truncated');
  RETURN NULL;
END
$erasure_history_truncate$;

CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.operator_user_erasures
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.operator_user_erasures
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.session_erasure_fences
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.session_erasure_fences
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.session_erasure_receipts
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.session_erasure_receipts
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.session_erasure_events
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.session_erasure_events
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.session_erasure_runs
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.session_erasure_runs
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.session_erasure_scopes
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.session_erasure_scopes
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.session_erasure_writes
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.session_erasure_writes
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.upload_deletions
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.upload_deletions
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.upload_resets
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.upload_resets
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.upload_erasure_rearm
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.upload_erasure_rearm
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.relayfacts_erasure_fences
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.relayfacts_erasure_fences
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.session_analysis_purge_fences
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.session_analysis_purge_fences
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.session_analysis_purge_authority
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.session_analysis_purge_authority
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.ingest_frozen_source_deletions
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.ingest_frozen_source_deletions
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();
CREATE TRIGGER ingest_org_erasure_history_record AFTER INSERT OR UPDATE OR DELETE ON sessions.ingest_frozen_source_resets
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_erasure_history_record();
CREATE TRIGGER ingest_org_erasure_history_refuse_truncate BEFORE TRUNCATE ON sessions.ingest_frozen_source_resets
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_erasure_history_refuse_truncate();

-- True only when all 15 recorders and all 15 TRUNCATE refusals exist, enabled (origin mode), on the right function, AND
-- none of their pg_trigger rows changed since the current coverage period began: each row's xmin must be the xid that
-- wrote the latest 'started' coverage row (this slot's apply transaction, which created them). ALTER TABLE ...
-- DISABLE/ENABLE TRIGGER, DROP/CREATE TRIGGER or a rename rewrites the row, so a disarm and re-arm, even a transient
-- one, stays visible until a down and reapply start a new period. (A no-op REVOKE also rewrites pg_proc rows, so the
-- functions are not fingerprinted this way; replacing the recorder function is owner DDL, stated in the release note.)
CREATE FUNCTION sessions.ingest_org_erasure_history_recording() RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $erasure_history_recording$
  SELECT COALESCE((
    SELECT count(*) = 30 FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'sessions' AND c.relname = ANY (ARRAY['operator_user_erasures', 'session_erasure_fences', 'session_erasure_receipts', 'session_erasure_events', 'session_erasure_runs', 'session_erasure_scopes', 'session_erasure_writes', 'upload_deletions', 'upload_resets', 'upload_erasure_rearm', 'relayfacts_erasure_fences', 'session_analysis_purge_fences', 'session_analysis_purge_authority', 'ingest_frozen_source_deletions', 'ingest_frozen_source_resets']::text[]) AND NOT t.tgisinternal AND t.tgenabled = 'O'
       AND t.xmin::text = (SELECT (s.xid::text::numeric % 4294967296)::text FROM sessions.ingest_org_erasure_history_coverage AS s
                            WHERE s.kind = 'started' ORDER BY s.at DESC LIMIT 1)
       AND ((t.tgname = 'ingest_org_erasure_history_record'
             AND t.tgfoid = 'sessions.ingest_org_erasure_history_record()'::regprocedure AND t.tgtype = 29)
         OR (t.tgname = 'ingest_org_erasure_history_refuse_truncate'
             AND t.tgfoid = 'sessions.ingest_org_erasure_history_refuse_truncate()'::regprocedure AND t.tgtype = 34))), false)
    AND (SELECT count(*) FROM pg_catalog.pg_trigger t WHERE NOT t.tgisinternal
          AND t.tgname IN ('ingest_org_erasure_history_record', 'ingest_org_erasure_history_refuse_truncate')) = 30
$erasure_history_recording$;

-- Bind the FULL run to a sealed hold epoch, once. The cutoff is NOT a caller value: it is the hold's installed_at,
-- written by 062320's install after it took the org key EXCLUSIVE. Every erasure write holds that key SHARED until it
-- commits (the recorder), so the install could not proceed while any write was uncommitted: every write recorded
-- before installed_at committed before the install, and every later write is recorded at or after it. A FULL that
-- starts after the install therefore sees every write before the cutoff, and the history holds every write since.
-- Bind refuses unless: the epoch is sealed; p_full_evidence equals the seal's evidence (the bound FULL receipt);
-- the FULL started after the install and not after the seal; coverage reaches the install; the recorders are intact.
-- The same arguments again are a no-op; anything else after a bind refuses. Lock order as every lifecycle function.
CREATE FUNCTION sessions.ingest_org_erasure_history_bind(p_org text, p_epoch uuid, p_full_run text,
  p_full_started_at timestamptz, p_full_evidence text) RETURNS timestamptz
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $erasure_history_bind$
DECLARE h sessions.ingest_org_admission_holds%ROWTYPE; b sessions.ingest_org_erasure_history_bindings%ROWTYPE; v_seal text;
BEGIN
  IF p_epoch IS NULL OR p_full_run IS NULL OR p_full_run !~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
     OR p_full_started_at IS NULL OR NOT isfinite(p_full_started_at)
     OR p_full_evidence IS NULL OR p_full_evidence !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: org erasure window bind invalid';
  END IF;
  PERFORM sessions.ingest_org_admission_hold_lifecycle(p_org);
  PERFORM sessions.ingest_org_erasure_history_applied();
  SELECT * INTO b FROM sessions.ingest_org_erasure_history_bindings WHERE epoch = p_epoch;
  IF FOUND THEN
    IF b.bound_org = p_org AND b.full_run = p_full_run AND b.full_started_at = p_full_started_at
       AND b.full_evidence_sha256 = p_full_evidence THEN RETURN b.cutoff; END IF;
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure window already bound';
  END IF;
  SELECT * INTO h FROM sessions.ingest_org_admission_holds WHERE epoch = p_epoch AND held_org = p_org;
  IF NOT FOUND OR h.state <> 'sealed' THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure window bind needs the sealed epoch';
  END IF;
  SELECT e.evidence_sha256 INTO v_seal FROM sessions.ingest_org_admission_hold_events AS e WHERE e.epoch = p_epoch AND e.kind = 'sealed';
  IF v_seal IS DISTINCT FROM p_full_evidence THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure window bind is not the sealed FULL';
  END IF;
  IF p_full_started_at <= h.installed_at OR p_full_started_at > h.sealed_at THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure window FULL not between install and seal';
  END IF;
  IF NOT sessions.ingest_org_erasure_history_covers(h.installed_at) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure history does not cover the install';
  END IF;
  IF NOT sessions.ingest_org_erasure_history_recording() THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure history recorder not armed';
  END IF;
  INSERT INTO sessions.ingest_org_erasure_history_bindings (epoch, bound_org, cutoff, full_run, full_started_at, full_evidence_sha256)
    VALUES (p_epoch, p_org, h.installed_at, p_full_run, p_full_started_at, p_full_evidence);
  RETURN h.installed_at;
END
$erasure_history_bind$;

-- 062320's release and status, replaced. Release: bridge_complete also needs this epoch's binding, coverage of its
-- cutoff, every recorder armed, and p_erasures equal to the org's history rows at or after the cutoff, all read under
-- the EXCLUSIVE key. Every other line is 062320's. Status: one snapshot (STABLE) of the hold, that count, and its
-- per-source split. The generated down restores both bodies exactly.
CREATE OR REPLACE FUNCTION sessions.ingest_org_admission_hold_release(p_org text, p_epoch uuid, p_reason text, p_evidence text,
  p_detail text, p_erasures bigint) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $hold_release$
DECLARE h sessions.ingest_org_admission_holds%ROWTYPE; b sessions.ingest_org_erasure_history_bindings%ROWTYPE;
BEGIN
  IF p_reason IS NULL OR p_reason NOT IN ('bridge_complete', 'verified_rollback') OR p_evidence IS NULL
     OR p_evidence !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: org admission hold release invalid';
  END IF;
  PERFORM sessions.ingest_org_admission_hold_lifecycle(p_org);
  SELECT * INTO h FROM sessions.ingest_org_admission_holds WHERE epoch = p_epoch AND held_org = p_org;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold epoch stale';
  END IF;
  IF h.state = 'released' THEN
    IF h.release_reason = p_reason AND h.release_evidence_sha256 = p_evidence THEN RETURN; END IF;
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold already released';
  END IF;
  -- 062340 org erasure history
  IF p_reason = 'bridge_complete' THEN
    PERFORM sessions.ingest_org_erasure_history_applied();
    SELECT * INTO b FROM sessions.ingest_org_erasure_history_bindings WHERE epoch = p_epoch AND bound_org = p_org;
    IF NOT FOUND THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure window not bound';
    END IF;
    IF NOT sessions.ingest_org_erasure_history_covers(b.cutoff) THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure history does not cover the cutoff';
    END IF;
    IF NOT sessions.ingest_org_erasure_history_recording() THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure history recorder not armed';
    END IF;
    IF p_erasures IS NULL OR p_erasures <> (SELECT count(*) FROM sessions.ingest_org_erasure_history AS e
                                             WHERE e.erased_org = p_org AND e.at >= b.cutoff) THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold erasures not replayed';
    END IF;
    -- An empty-window release says so (p_detail) and is checked: zero history since the install, not a matching count.
    IF p_detail = 'erasure_window_empty' AND p_erasures <> 0 THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org erasure window not empty';
    END IF;
  END IF;
  IF (p_reason = 'bridge_complete' AND h.state <> 'marker_attempted')
     OR (p_reason = 'verified_rollback' AND (h.state NOT IN ('held', 'sealed') OR EXISTS (
          SELECT 1 FROM sessions.ingest_org_admission_hold_events WHERE epoch = p_epoch AND kind = 'marker_attempted'))) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold transition refused';
  END IF;
  UPDATE sessions.ingest_org_admission_holds
     SET state = 'released', released_at = clock_timestamp(), release_reason = p_reason, release_evidence_sha256 = p_evidence
   WHERE epoch = p_epoch;
  INSERT INTO sessions.ingest_org_admission_hold_events (epoch, kind, detail, evidence_sha256)
    VALUES (p_epoch, 'released', p_detail, p_evidence);
END
$hold_release$;

CREATE OR REPLACE FUNCTION sessions.ingest_org_admission_hold_status(p_org text) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $hold_status$
DECLARE v_epoch uuid; v_cutoff timestamptz;
BEGIN
  -- 062340 org erasure history: STABLE, so every statement here reads the caller's one snapshot.
  SELECT h.epoch INTO v_epoch FROM sessions.ingest_org_admission_holds AS h WHERE h.held_org = p_org
   ORDER BY (h.state <> 'released') DESC, h.installed_at DESC LIMIT 1;
  SELECT b.cutoff INTO v_cutoff FROM sessions.ingest_org_erasure_history_bindings AS b
   WHERE b.epoch = v_epoch AND b.bound_org = p_org;
  RETURN jsonb_build_object(
    'hold', (SELECT to_jsonb(h) FROM sessions.ingest_org_admission_holds AS h WHERE h.held_org = p_org
              ORDER BY (h.state <> 'released') DESC, h.installed_at DESC LIMIT 1),
    'erasures', CASE WHEN v_cutoff IS NULL THEN NULL ELSE (SELECT count(*) FROM sessions.ingest_org_erasure_history AS e
                  WHERE e.erased_org = p_org AND e.at >= v_cutoff) END,
    'erasure_history', jsonb_build_object(
      'cutoff', v_cutoff,
      'full_run', (SELECT b.full_run FROM sessions.ingest_org_erasure_history_bindings AS b WHERE b.epoch = v_epoch),
      'full_started_at', (SELECT b.full_started_at FROM sessions.ingest_org_erasure_history_bindings AS b WHERE b.epoch = v_epoch),
      'covered', CASE WHEN v_cutoff IS NULL THEN NULL ELSE sessions.ingest_org_erasure_history_covers(v_cutoff) END,
      'coverage_started_at', sessions.ingest_org_erasure_history_coverage_start(),
      'recording', sessions.ingest_org_erasure_history_recording(),
      'by_source', CASE WHEN v_cutoff IS NULL THEN NULL ELSE COALESCE((SELECT jsonb_object_agg(s.source, s.n ORDER BY s.source)
                     FROM (SELECT e.source, count(*) AS n FROM sessions.ingest_org_erasure_history AS e
                            WHERE e.erased_org = p_org AND e.at >= v_cutoff GROUP BY e.source) AS s), '{}'::jsonb) END));
END
$hold_status$;

REVOKE ALL ON FUNCTION sessions.ingest_org_erasure_history_guard(), sessions.ingest_org_erasure_history_applied(),
  sessions.ingest_org_erasure_history_coverage_start(), sessions.ingest_org_erasure_history_covers(timestamptz),
  sessions.ingest_org_erasure_history_recording(), sessions.ingest_org_erasure_history_record(),
  sessions.ingest_org_erasure_history_refuse_truncate(), sessions.ingest_org_erasure_history_bind(text, uuid, text, timestamptz, text)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;
REVOKE ALL ON sessions.ingest_org_erasure_history, sessions.ingest_org_erasure_history_coverage,
  sessions.ingest_org_erasure_history_bindings
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;

-- Coverage starts last, after every recorder exists: CREATE TRIGGER waited for each source's in-flight writers, so a
-- write not recorded committed before this row.
INSERT INTO sessions.ingest_org_erasure_history_coverage (kind) VALUES ('started');
