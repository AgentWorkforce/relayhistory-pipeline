-- Slot 20261105058000. Foreign-key support indexes for the operator per-user erase (20261105058010). Code only:
-- applying this to any real database needs Khaliq's explicit OK.
--
-- Eleven indexes. Ten serve foreign keys whose referencing column no index leads with: deleting a referenced row
-- then makes PostgreSQL check the referencing table by sequential scan, once per deleted row. On production
-- (ingest_lazy_ack_decisions ~74M rows, ingest_lazy_native_slots ~28M) a 100k-slot user erase never finishes
-- (observed 2026-11-11). With these indexes each check is an index probe, and the erase finds a user's
-- referencing rows without a full scan:
--   ingest_lazy_ack_decisions(native_key_sha256)   -> ingest_lazy_native_slots(key_sha256)
--   ingest_lazy_ack_decisions(seat_key_sha256)     -> ingest_receipt_seats(key_sha256)
--   ingest_lazy_native_slots(current_seat_key)     -> ingest_receipt_seats(key_sha256)
--   ingest_receipt_ord_seats(key_sha256)           -> ingest_receipt_seats(key_sha256)
--   ingest_receipt_record_heads(winner_key_sha256) -> ingest_receipt_seats(key_sha256)
--   ingest_stage_intents(parent_stage_token)       -> ingest_stage_intents(stage_token)   partial: IS NOT NULL
--   ingest_stage_intents(gc_replacement_stage_token) -> ingest_stage_intents(stage_token) partial: IS NOT NULL
--   ingest_stage_intents(org,ws,origin,batch_id)   -> ingest_batches(org,ws,origin,batch_id)
--   ingest_buffer_outbox(active_stage_token)       -> ingest_stage_intents(stage_token)   partial: IS NOT NULL
--   ingest_history_plans(stage_token)              -> ingest_stage_intents(stage_token)
-- The eleventh serves the step's un-owned-slot arm (current_owner IS NULL AND origin_id = ANY(origins)), which
-- otherwise scans the org's slots on every batch:
--   ingest_lazy_native_slots(org_id,workspace_id,origin_id) WHERE current_owner IS NULL
--
-- Same adopt-or-create contract as 20261105044020 (read it there): an exact, valid, ready index of that name is
-- adopted without locking its table; an absent one is created only on an EMPTY table (or with
-- HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true); an absent one on a POPULATED table refuses (55000) naming the
-- CONCURRENTLY prebuild packet (docs/packets/operator-user-erase-index-packet.md); anything else raises.
-- Production: ingest_receipt_ord_seats and ingest_receipt_record_heads are empty (created here); the four
-- indexes on ingest_lazy_ack_decisions and ingest_lazy_native_slots are prebuilt out of band, then adopted. The
-- row counts of ingest_stage_intents, ingest_buffer_outbox and ingest_history_plans are not recorded: the packet
-- checks them first and prebuilds any index whose table is populated (this step refuses a populated table anyway).
-- The expected definitions are byte-identical to ADOPTABLE_INDEXES in src/hoopdb/migration-sentinels.ts (a test checks).
DO $operator_user_erase_indexes$
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
  ['ingest_receipt_ord_seats_key_sha256_idx','ingest_receipt_ord_seats',
   'CREATE INDEX ingest_receipt_ord_seats_key_sha256_idx ON sessions.ingest_receipt_ord_seats USING btree (key_sha256)'],
  ['ingest_receipt_record_heads_winner_key_sha256_idx','ingest_receipt_record_heads',
   'CREATE INDEX ingest_receipt_record_heads_winner_key_sha256_idx ON sessions.ingest_receipt_record_heads USING btree (winner_key_sha256)'],
  ['ingest_lazy_native_slots_current_seat_key_idx','ingest_lazy_native_slots',
   'CREATE INDEX ingest_lazy_native_slots_current_seat_key_idx ON sessions.ingest_lazy_native_slots USING btree (current_seat_key)'],
  ['ingest_lazy_ack_decisions_native_key_sha256_idx','ingest_lazy_ack_decisions',
   'CREATE INDEX ingest_lazy_ack_decisions_native_key_sha256_idx ON sessions.ingest_lazy_ack_decisions USING btree (native_key_sha256)'],
  ['ingest_lazy_ack_decisions_seat_key_sha256_idx','ingest_lazy_ack_decisions',
   'CREATE INDEX ingest_lazy_ack_decisions_seat_key_sha256_idx ON sessions.ingest_lazy_ack_decisions USING btree (seat_key_sha256)'],
  ['ingest_stage_intents_parent_stage_token_idx','ingest_stage_intents',
   'CREATE INDEX ingest_stage_intents_parent_stage_token_idx ON sessions.ingest_stage_intents USING btree (parent_stage_token) WHERE (parent_stage_token IS NOT NULL)'],
  ['ingest_stage_intents_gc_replacement_stage_token_idx','ingest_stage_intents',
   'CREATE INDEX ingest_stage_intents_gc_replacement_stage_token_idx ON sessions.ingest_stage_intents USING btree (gc_replacement_stage_token) WHERE (gc_replacement_stage_token IS NOT NULL)'],
  ['ingest_stage_intents_batch_idx','ingest_stage_intents',
   'CREATE INDEX ingest_stage_intents_batch_idx ON sessions.ingest_stage_intents USING btree (org_id, workspace_id, origin_id, batch_id)'],
  ['ingest_buffer_outbox_active_stage_token_idx','ingest_buffer_outbox',
   'CREATE INDEX ingest_buffer_outbox_active_stage_token_idx ON sessions.ingest_buffer_outbox USING btree (active_stage_token) WHERE (active_stage_token IS NOT NULL)'],
  ['ingest_history_plans_stage_token_idx','ingest_history_plans',
   'CREATE INDEX ingest_history_plans_stage_token_idx ON sessions.ingest_history_plans USING btree (stage_token)'],
  ['ingest_lazy_native_slots_unowned_origin_idx','ingest_lazy_native_slots',
   'CREATE INDEX ingest_lazy_native_slots_unowned_origin_idx ON sessions.ingest_lazy_native_slots USING btree (org_id, workspace_id, origin_id) WHERE (current_owner IS NULL)']
 ] LOOP
  v_tbl:=to_regclass(format('sessions.%I',v_spec[2]));
  IF v_tbl IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='42P01',
    MESSAGE=format('operator user erase indexes: table sessions.%s does not exist',v_spec[2]);
  END IF;
  SELECT c.oid,c.relkind INTO v_rel FROM pg_class c
   WHERE c.relnamespace='sessions'::regnamespace AND c.relname=v_spec[1];
  IF NOT FOUND THEN
   -- Absent. Only an empty table, or an explicit opt-in, may take the blocking build.
   PERFORM set_config('row_security','off',true);
   EXECUTE format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)',v_tbl) INTO v_rows;
   IF v_rows AND NOT v_allow THEN
    RAISE EXCEPTION USING ERRCODE='55000',
     MESSAGE=format('operator user erase indexes: index sessions.%s is absent and table sessions.%s is not empty; refusing a blocking CREATE INDEX on a populated table',v_spec[1],v_spec[2]),
     HINT=format('prebuild it out of band, CONCURRENTLY, with the operator user erase index packet (docs/packets/operator-user-erase-index-packet.md) and rerun, this migration then adopts it; or opt into a blocking build with HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true (expected: %s)',v_spec[3]);
   END IF;
   EXECUTE format('LOCK TABLE %s IN SHARE MODE',v_tbl);
   IF NOT v_allow THEN
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)',v_tbl) INTO v_rows;
    IF v_rows THEN
     RAISE EXCEPTION USING ERRCODE='55000',
      MESSAGE=format('operator user erase indexes: index sessions.%s is absent and table sessions.%s is not empty; refusing a blocking CREATE INDEX on a populated table',v_spec[1],v_spec[2]),
      HINT=format('prebuild it out of band, CONCURRENTLY, with the operator user erase index packet (docs/packets/operator-user-erase-index-packet.md) and rerun, this migration then adopts it; or opt into a blocking build with HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true (expected: %s)',v_spec[3]);
    END IF;
   END IF;
   PERFORM set_config('row_security',v_rls,true);
   EXECUTE v_spec[3];
   SELECT c.oid,c.relkind INTO STRICT v_rel FROM pg_class c
    WHERE c.relnamespace='sessions'::regnamespace AND c.relname=v_spec[1];
  END IF;
  IF v_rel.relkind<>'i' THEN
   RAISE EXCEPTION USING ERRCODE='42P07',
    MESSAGE=format('operator user erase indexes: sessions.%s exists but is not a plain index (relkind %s); reconcile it by hand, nothing was changed',v_spec[1],v_rel.relkind);
  END IF;
  SELECT i.indrelid,i.indisvalid,i.indisready,i.indislive INTO STRICT v_idx FROM pg_index i WHERE i.indexrelid=v_rel.oid;
  v_def:=pg_get_indexdef(v_rel.oid);
  IF v_idx.indrelid<>v_tbl OR v_def IS DISTINCT FROM v_spec[3] THEN
   RAISE EXCEPTION USING ERRCODE='42P07',
    MESSAGE=format('operator user erase indexes: index sessions.%s exists with a different definition; reconcile it (expected: %s)',v_spec[1],v_spec[3]),
    DETAIL=format('found: %s',v_def);
  END IF;
  IF NOT (v_idx.indisvalid AND v_idx.indisready AND v_idx.indislive) THEN
   RAISE EXCEPTION USING ERRCODE='55000',
    MESSAGE=format('operator user erase indexes: index sessions.%s is INVALID or not ready (a failed CONCURRENTLY build); drop it with DROP INDEX CONCURRENTLY sessions.%s, rebuild it out of band or let this migration create it, and rerun',v_spec[1],v_spec[1]);
  END IF;
 END LOOP;
END
$operator_user_erase_indexes$;
