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
--  * no relation of that name in sessions: plain CREATE INDEX with the expected definition;
--  * anything else (an INVALID or not-ready index left by a failed CONCURRENTLY build, a different definition,
--    another table, a non-index relation of that name): RAISE, never adopted, nothing is dropped.
--
-- The migrator runs every migration inside one transaction, so CONCURRENTLY is impossible here. The create
-- path takes a SHARE lock on the table (writes wait) for the duration of the build. That is fine on fresh and
-- small databases (dev, self-host, kind, CI). On a large database, build the index out of band with
-- CREATE INDEX CONCURRENTLY first (the packet), then this migration adopts it.
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
