-- Slot 20261105058000. Foreign-key support indexes for the operator per-user erase (20261105058010). Code only:
-- applying this to any real database needs Khaliq's explicit OK.
--
-- Five foreign keys point at sessions.ingest_lazy_native_slots and sessions.ingest_receipt_seats from columns no
-- index leads with. Deleting a slot or a seat then makes PostgreSQL check every referencing table by sequential
-- scan, once per deleted row: on production (ingest_lazy_ack_decisions ~74M rows, ingest_lazy_native_slots ~28M)
-- a 100k-slot user erase never finishes (observed 2026-11-11). With these indexes each check is an index probe,
-- and the erase finds a user's referencing rows without a full scan:
--   ingest_lazy_ack_decisions(native_key_sha256)   -> ingest_lazy_native_slots(key_sha256)
--   ingest_lazy_ack_decisions(seat_key_sha256)     -> ingest_receipt_seats(key_sha256)
--   ingest_lazy_native_slots(current_seat_key)     -> ingest_receipt_seats(key_sha256)
--   ingest_receipt_ord_seats(key_sha256)           -> ingest_receipt_seats(key_sha256)
--   ingest_receipt_record_heads(winner_key_sha256) -> ingest_receipt_seats(key_sha256)
--
-- Same adopt-or-create contract as 20261105044020 (read it there): an exact, valid, ready index of that name is
-- adopted without locking its table; an absent one is created only on an EMPTY table (or with
-- HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true); an absent one on a POPULATED table refuses (55000) naming the
-- CONCURRENTLY prebuild packet (docs/packets/operator-user-erase-index-packet.md); anything else raises.
-- Production: ingest_receipt_ord_seats and ingest_receipt_record_heads are empty (created here); the three
-- indexes on ingest_lazy_ack_decisions and ingest_lazy_native_slots are prebuilt out of band, then adopted.
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
   'CREATE INDEX ingest_lazy_ack_decisions_seat_key_sha256_idx ON sessions.ingest_lazy_ack_decisions USING btree (seat_key_sha256)']
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
