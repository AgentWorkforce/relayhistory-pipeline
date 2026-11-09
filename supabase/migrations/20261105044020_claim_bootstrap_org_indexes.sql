-- Slot 20261105044020 assigned by the registry owner. Catch-up for the eight claim-bootstrap org_id indexes
-- that the operator builds out of band with CREATE INDEX CONCURRENTLY on production (packet:
-- docs/packets/claim-bootstrap-org-index-packet-20261108.md on branch docs/claim-bootstrap-index-packet-20261108).
-- Purpose: dev, self-host, kind and CI databases get the same indexes as production. Code only: applying this
-- to any real database needs Khaliq's explicit OK.
--
-- The first claim for a new org probes every ingest_%/upload_% table with org_id (ingest_lazy_begin, and
-- ingest_lazy_user for a new user). On these eight tables no valid, non-partial btree index leads with org_id,
-- so each probe is a sequential scan. Exactly the packet's eight definitions, in its build order: btree,
-- non-unique, non-partial, no INCLUDE, named <table>_org_id_idx.
--
-- Adopt or create, per index, read from the catalog only (pg_class, pg_index, pg_get_indexdef: no lock on the
-- table):
--  * an index of that name in sessions, on the expected table, whose pg_get_indexdef is exactly the expected
--    definition and that is valid, ready and live: adopted, nothing is executed for it (production after the
--    operator's build). CREATE INDEX IF NOT EXISTS is deliberately NOT used: it takes a SHARE lock on the table
--    before it notices the name, and it silently keeps an INVALID or drifted index;
--  * no relation of that name in sessions and the table EMPTY (fresh, dev, kind, CI): plain CREATE INDEX with
--    the expected definition;
--  * no relation of that name in sessions and the table POPULATED: REFUSE (55000), naming the index, the table
--    and the CONCURRENTLY prebuild packet, unless the run explicitly opts into a blocking build
--    (HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true on the migrator, which binds hoopdb.allow_blocking_index_build=on
--    for each migration transaction; default off). A blocking build on a populated table is never silent;
--  * anything else (an INVALID or not-ready index left by a failed CONCURRENTLY build, a different definition,
--    another table, a non-index relation of that name): RAISE, never adopted, nothing is dropped.
--
-- The migrator runs every migration inside one transaction, so CONCURRENTLY is impossible here. The create
-- path takes a SHARE lock on the table (writes wait) for the duration of the build. Emptiness is an existence
-- probe (SELECT 1 ... LIMIT 1 with row_security off, so RLS can never make a populated table look empty; not
-- reltuples), first without that lock, so a refusal never queues behind writers, then again under the SHARE
-- lock right before the build, so a row inserted in between is seen. On a populated database, build the index
-- out of band with CREATE INDEX CONCURRENTLY first (packet: docs/packets/claim-bootstrap-org-index-packet-20261108.md,
-- PR #407), then this migration adopts it.
--
-- The migrator's sequencePreflight would otherwise refuse production (an unledgered step whose objects exist):
-- these eight names are declared adoptable for this step only (ADOPTABLE_INDEXES in
-- src/hoopdb/migration-sentinels.ts) and accepted only with this exact definition, valid and ready.
-- The expected definitions below are byte-identical to that declaration (a test checks).
DO $claim_bootstrap_org_indexes$
DECLARE
 v_spec text[];
 v_tbl regclass;
 v_rel record;
 v_idx record;
 v_def text;
 v_rows boolean;
 v_allow boolean:=coalesce(current_setting('hoopdb.allow_blocking_index_build',true),'')='on';
 v_rls text:=current_setting('row_security');
BEGIN
 FOREACH v_spec SLICE 1 IN ARRAY ARRAY[
  ['ingest_history_mask_pages_org_id_idx','ingest_history_mask_pages',
   'CREATE INDEX ingest_history_mask_pages_org_id_idx ON sessions.ingest_history_mask_pages USING btree (org_id)'],
  ['ingest_history_plans_org_id_idx','ingest_history_plans',
   'CREATE INDEX ingest_history_plans_org_id_idx ON sessions.ingest_history_plans USING btree (org_id, workspace_id, user_id)'],
  ['ingest_seed_captures_org_id_idx','ingest_seed_captures',
   'CREATE INDEX ingest_seed_captures_org_id_idx ON sessions.ingest_seed_captures USING btree (org_id)'],
  ['ingest_publication_attempts_org_id_idx','ingest_publication_attempts',
   'CREATE INDEX ingest_publication_attempts_org_id_idx ON sessions.ingest_publication_attempts USING btree (org_id, workspace_id, user_id)'],
  ['ingest_publication_generations_org_id_idx','ingest_publication_generations',
   'CREATE INDEX ingest_publication_generations_org_id_idx ON sessions.ingest_publication_generations USING btree (org_id, workspace_id, user_id)'],
  ['ingest_stage_intents_org_id_idx','ingest_stage_intents',
   'CREATE INDEX ingest_stage_intents_org_id_idx ON sessions.ingest_stage_intents USING btree (org_id, workspace_id, user_id)'],
  ['ingest_custody_records_org_id_idx','ingest_custody_records',
   'CREATE INDEX ingest_custody_records_org_id_idx ON sessions.ingest_custody_records USING btree (org_id, workspace_id, user_id)'],
  ['ingest_lazy_native_slots_org_id_idx','ingest_lazy_native_slots',
   'CREATE INDEX ingest_lazy_native_slots_org_id_idx ON sessions.ingest_lazy_native_slots USING btree (org_id)']
 ] LOOP
  v_tbl:=to_regclass(format('sessions.%I',v_spec[2]));
  IF v_tbl IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='42P01',
    MESSAGE=format('claim bootstrap org indexes: table sessions.%s does not exist',v_spec[2]);
  END IF;
  SELECT c.oid,c.relkind INTO v_rel FROM pg_class c
   WHERE c.relnamespace='sessions'::regnamespace AND c.relname=v_spec[1];
  IF NOT FOUND THEN
   -- Absent. Only an empty table, or an explicit opt-in, may take the blocking build.
   PERFORM set_config('row_security','off',true);
   EXECUTE format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)',v_tbl) INTO v_rows;
   IF v_rows AND NOT v_allow THEN
    RAISE EXCEPTION USING ERRCODE='55000',
     MESSAGE=format('claim bootstrap org indexes: index sessions.%s is absent and table sessions.%s is not empty; refusing a blocking CREATE INDEX on a populated table',v_spec[1],v_spec[2]),
     HINT=format('prebuild it out of band, CONCURRENTLY, with the claim-bootstrap index packet (docs/packets/claim-bootstrap-org-index-packet-20261108.md, PR #407) and rerun, this migration then adopts it; or opt into a blocking build with HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true (expected: %s)',v_spec[3]);
   END IF;
   EXECUTE format('LOCK TABLE %s IN SHARE MODE',v_tbl);
   IF NOT v_allow THEN
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)',v_tbl) INTO v_rows;
    IF v_rows THEN
     RAISE EXCEPTION USING ERRCODE='55000',
      MESSAGE=format('claim bootstrap org indexes: index sessions.%s is absent and table sessions.%s is not empty; refusing a blocking CREATE INDEX on a populated table',v_spec[1],v_spec[2]),
      HINT=format('prebuild it out of band, CONCURRENTLY, with the claim-bootstrap index packet (docs/packets/claim-bootstrap-org-index-packet-20261108.md, PR #407) and rerun, this migration then adopts it; or opt into a blocking build with HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true (expected: %s)',v_spec[3]);
    END IF;
   END IF;
   PERFORM set_config('row_security',v_rls,true);
   EXECUTE v_spec[3];
   SELECT c.oid,c.relkind INTO STRICT v_rel FROM pg_class c
    WHERE c.relnamespace='sessions'::regnamespace AND c.relname=v_spec[1];
  END IF;
  IF v_rel.relkind<>'i' THEN
   RAISE EXCEPTION USING ERRCODE='42P07',
    MESSAGE=format('claim bootstrap org indexes: sessions.%s exists but is not a plain index (relkind %s); reconcile it by hand, nothing was changed',v_spec[1],v_rel.relkind);
  END IF;
  SELECT i.indrelid,i.indisvalid,i.indisready,i.indislive INTO STRICT v_idx FROM pg_index i WHERE i.indexrelid=v_rel.oid;
  v_def:=pg_get_indexdef(v_rel.oid);
  IF v_idx.indrelid<>v_tbl OR v_def IS DISTINCT FROM v_spec[3] THEN
   RAISE EXCEPTION USING ERRCODE='42P07',
    MESSAGE=format('claim bootstrap org indexes: index sessions.%s exists with a different definition; reconcile it (expected: %s)',v_spec[1],v_spec[3]),
    DETAIL=format('found: %s',v_def);
  END IF;
  IF NOT (v_idx.indisvalid AND v_idx.indisready AND v_idx.indislive) THEN
   RAISE EXCEPTION USING ERRCODE='55000',
    MESSAGE=format('claim bootstrap org indexes: index sessions.%s is INVALID or not ready (a failed CONCURRENTLY build); drop it with DROP INDEX CONCURRENTLY sessions.%s, rebuild it out of band or let this migration create it, and rerun',v_spec[1],v_spec[1]);
  END IF;
 END LOOP;
END
$claim_bootstrap_org_indexes$;
