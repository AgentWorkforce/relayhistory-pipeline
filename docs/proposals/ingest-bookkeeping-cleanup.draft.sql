-- DRAFT, NOT A MIGRATION. No slot is taken; the registry (layered-sessions-pipeline) assigns one after
-- pipeline-lead's 043500 claim-lock fix. Design: docs/design/ingest-bookkeeping-cleanup.md.
--
-- Ingest bookkeeping cleanup: owner-only, bounded, keyset-paged, default OFF.
--  * Phase A  ingest_lazy_ack_decisions: rows of TERMINAL claims (batch committed beyond min_age, every
--             stage intent of the claim redacted, no Babysitter evidence row naming the claim).
--  * Phase B  ingest_receipt_seats: seats no decision / native slot (current) / record head / ord seat
--             references, older than min_age. ingest_custody_records: LIVE-WITNESS rows no slot / head /
--             ord seat references. Refuses (dry run included) until the FK-side indexes of
--             ingest-bookkeeping-cleanup.indexes.draft.sql exist and are valid and ready.
--  * Never    ingest_lazy_native_slots (the lazy revision fence; see design §1.3).
--  * Phase C  ingest_frozen_source_records: read-only census only; no delete exists here.
--
-- Babysitter interlock (design §3.5): retiring a claim's decisions writes an IRREVERSIBLE marker
-- (ingest_bookkeeping_retired_claims) in the same transaction, under an exclusive per-claim advisory lock.
-- ingest_babysitter_retention_register (replaced below; 042000 itself is untouched) takes the same lock SHARED
-- before any write and then refuses a retired claim. A registration in flight makes the cleanup skip the
-- claim (try-lock); a cleanup in flight makes the registration wait, then refuse. No timing assumption.
-- Index gates compare the exact canonical definition (pg_get_indexdef), not just the name.
-- SKIP LOCKED and busy-claim skips are counted: `end` = keyset exhausted, `done` = end AND nothing skipped.
--
-- Contention budget (design §5): never locks a USER fence, batch, stage intent, outbox, claim attempt,
-- the seat clock or any native slot (and never writes slots, so no 026000 trigger fires). Takes a
-- cleanup-only advisory try-lock per phase (busy => returns, never waits) and FOR UPDATE SKIP LOCKED on
-- exactly the rows it deletes. lock_timeout 100ms; the CALLER must SET LOCAL statement_timeout 1..5000ms.
-- Two independent switches: settings (enabled/dry_run/per-phase) here, and --execute in the operator tool.
-- No application role gains any privilege.

CREATE TABLE sessions.ingest_bookkeeping_cleanup_settings(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 enabled boolean NOT NULL DEFAULT false,
 dry_run boolean NOT NULL DEFAULT true,
 decisions_enabled boolean NOT NULL DEFAULT false,
 seats_enabled boolean NOT NULL DEFAULT false,
 custody_enabled boolean NOT NULL DEFAULT false,
 min_age interval NOT NULL DEFAULT interval '7 days' CHECK(min_age>=interval '1 day' AND min_age<=interval '90 days'),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO sessions.ingest_bookkeeping_cleanup_settings(singleton) VALUES(true);

-- Append-only, content-free: counts and keyset cursors (uuid / bigint seat / hex key) only.
CREATE TABLE sessions.ingest_bookkeeping_cleanup_audit(
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 at timestamptz NOT NULL DEFAULT clock_timestamp(),
 action text NOT NULL CHECK(action IN('configured','deleted')),
 phase text CHECK(phase IN('decisions','seats','custody')),
 examined integer CHECK(examined>=0),deleted integer CHECK(deleted>=0),
 cursor_after text,cursor_next text,settings jsonb
);
ALTER TABLE sessions.ingest_bookkeeping_cleanup_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_bookkeeping_cleanup_audit ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_bookkeeping_cleanup_settings,sessions.ingest_bookkeeping_cleanup_audit
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_audit_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $audit_guard$
BEGIN
 RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='bookkeeping cleanup: audit is append-only';
END $audit_guard$;
CREATE TRIGGER ingest_bookkeeping_cleanup_audit_guard BEFORE UPDATE OR DELETE OR TRUNCATE ON sessions.ingest_bookkeeping_cleanup_audit
 FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_bookkeeping_cleanup_audit_guard();

-- Irreversible per-claim decision retirement / Babysitter registration-closed state. Insert-only.
CREATE TABLE sessions.ingest_bookkeeping_retired_claims(
 claim_token uuid PRIMARY KEY,
 retired_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE sessions.ingest_bookkeeping_retired_claims ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_bookkeeping_retired_claims
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE FUNCTION sessions.ingest_bookkeeping_retired_claims_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $retired_guard$
BEGIN
 RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='bookkeeping cleanup: claim retirement is irreversible';
END $retired_guard$;
CREATE TRIGGER ingest_bookkeeping_retired_claims_guard BEFORE UPDATE OR DELETE OR TRUNCATE ON sessions.ingest_bookkeeping_retired_claims
 FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_bookkeeping_retired_claims_guard();

-- The per-claim serialization key shared by the cleanup (exclusive, try) and Babysitter registration (shared).
CREATE FUNCTION sessions.ingest_bookkeeping_claim_lock_key(p_claim uuid) RETURNS bigint
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $claim_key$
 SELECT hashtextextended('96bulls:bookkeeping-claim-v1:'||p_claim::text,0)
$claim_key$;

-- An index counts only if it is live, valid, ready AND its canonical definition is exactly the expected one
-- (columns, order, expressions, opclass/collation, predicate, uniqueness, method). A same-name drifted index
-- (e.g. left by CREATE INDEX IF NOT EXISTS) is refused.
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_index_ok(p_name text,p_def text) RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $index_ok$
 SELECT EXISTS(SELECT 1 FROM pg_index i WHERE i.indexrelid=to_regclass('sessions.'||p_name)
  AND i.indislive AND i.indisvalid AND i.indisready AND pg_get_indexdef(i.indexrelid)=p_def)
$index_ok$;

-- Phase B prerequisite: every FK-side index the RI checks (and the predicates) probe, valid and ready.
-- Without them each seat/custody DELETE would seq-scan referencing tables of up to ~27M rows.
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_phase_b_ready() RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $ready$
 SELECT bool_and(sessions.ingest_bookkeeping_cleanup_index_ok(v.n,v.d)) FROM (VALUES
  ('ingest_lazy_ack_decisions_seat_key_idx','CREATE INDEX ingest_lazy_ack_decisions_seat_key_idx ON sessions.ingest_lazy_ack_decisions USING btree (seat_key_sha256)'),
  ('ingest_lazy_native_slots_current_seat_idx','CREATE INDEX ingest_lazy_native_slots_current_seat_idx ON sessions.ingest_lazy_native_slots USING btree (current_seat_key) WHERE (current_seat_key IS NOT NULL)'),
  ('ingest_lazy_native_slots_current_legacy_idx','CREATE INDEX ingest_lazy_native_slots_current_legacy_idx ON sessions.ingest_lazy_native_slots USING btree (current_legacy_key) WHERE (current_legacy_key IS NOT NULL)'),
  ('ingest_receipt_record_heads_winner_idx','CREATE INDEX ingest_receipt_record_heads_winner_idx ON sessions.ingest_receipt_record_heads USING btree (winner_key_sha256) WHERE (winner_key_sha256 IS NOT NULL)'),
  ('ingest_receipt_record_heads_legacy_idx','CREATE INDEX ingest_receipt_record_heads_legacy_idx ON sessions.ingest_receipt_record_heads USING btree (legacy_key_sha256) WHERE (legacy_key_sha256 IS NOT NULL)'),
  ('ingest_receipt_ord_seats_seat_idx','CREATE INDEX ingest_receipt_ord_seats_seat_idx ON sessions.ingest_receipt_ord_seats USING btree (key_sha256)'),
  ('ingest_receipt_ord_seats_legacy_idx','CREATE INDEX ingest_receipt_ord_seats_legacy_idx ON sessions.ingest_receipt_ord_seats USING btree (legacy_custody_key_sha256) WHERE (legacy_custody_key_sha256 IS NOT NULL)')) v(n,d)
$ready$;

-- Shared gate. Read-only (execute=false) pages need no flag; execute needs enabled, NOT dry_run and the
-- phase flag. Refuses an unbounded or long caller statement_timeout.
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_gate(p_phase text,p_execute boolean)
RETURNS sessions.ingest_bookkeeping_cleanup_settings
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $gate$
DECLARE s sessions.ingest_bookkeeping_cleanup_settings%ROWTYPE;v_timeout bigint;
BEGIN
 IF p_phase IS NULL OR p_phase NOT IN('decisions','seats','custody','frozen') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='bookkeeping cleanup: unknown phase';END IF;
 SELECT setting::bigint INTO v_timeout FROM pg_settings WHERE name='statement_timeout';
 IF v_timeout IS NULL OR v_timeout<1 OR v_timeout>5000 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='bookkeeping cleanup: SET LOCAL statement_timeout to 1..5000 ms first';END IF;
 SELECT * INTO STRICT s FROM sessions.ingest_bookkeeping_cleanup_settings WHERE singleton;
 IF p_execute AND (p_phase='frozen' OR NOT s.enabled OR s.dry_run
   OR (p_phase='decisions' AND NOT s.decisions_enabled) OR (p_phase='seats' AND NOT s.seats_enabled)
   OR (p_phase='custody' AND NOT s.custody_enabled)) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='bookkeeping cleanup: '||p_phase||' deletion is disabled';END IF;
 -- Phase A's Babysitter probe is by claim token; without its index each probe would scan that table.
 IF p_phase='decisions' AND NOT sessions.ingest_bookkeeping_cleanup_index_ok('babysitter_evidence_index_claim_idx',
   'CREATE INDEX babysitter_evidence_index_claim_idx ON sessions.babysitter_evidence_index USING btree (claim_token)') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='bookkeeping cleanup: phase A index missing or invalid';END IF;
 IF p_phase IN('seats','custody') AND NOT sessions.ingest_bookkeeping_cleanup_phase_b_ready() THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='bookkeeping cleanup: phase B indexes missing or invalid';END IF;
 RETURN s;
END $gate$;

CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_try_lock(p_phase text) RETURNS boolean
LANGUAGE sql VOLATILE SET search_path=pg_catalog,pg_temp AS $try$
 SELECT pg_try_advisory_xact_lock(hashtextextended('96bulls:bookkeeping-cleanup-v1:'||p_phase,0))
$try$;

-- Owner-only switch (with Khaliq's OK). p: {enabled, dryRun, decisions, seats, custody, minAgeDays 1..90}
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_configure(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $configure$
DECLARE s sessions.ingest_bookkeeping_cleanup_settings%ROWTYPE;k text;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['enabled','dryRun','decisions','seats','custody','minAgeDays']);
 FOREACH k IN ARRAY ARRAY['enabled','dryRun','decisions','seats','custody'] LOOP
  IF jsonb_typeof(p->k) IS DISTINCT FROM 'boolean' THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='bookkeeping cleanup: '||k||' must be boolean';END IF;
 END LOOP;
 UPDATE sessions.ingest_bookkeeping_cleanup_settings SET enabled=(p->>'enabled')::boolean,dry_run=(p->>'dryRun')::boolean,
  decisions_enabled=(p->>'decisions')::boolean,seats_enabled=(p->>'seats')::boolean,custody_enabled=(p->>'custody')::boolean,
  min_age=make_interval(days=>sessions.ingest_bounded(p->'minAgeDays',1,90)),updated_at=clock_timestamp()
 WHERE singleton RETURNING * INTO STRICT s;
 INSERT INTO sessions.ingest_bookkeeping_cleanup_audit(action,settings) VALUES('configured',to_jsonb(s)-'singleton');
 RETURN to_jsonb(s)-'singleton';
END $configure$;

-- Phase A. p: {after: uuid|null, rowBudget 500..5000, execute: boolean}
-- One page = at most rowBudget decision rows from the PK keyset after the cursor, cut at a WHOLE claim
-- (a claim has at most 500 ordinals, so a single claim always fits), and at most 200 claims (each retired
-- claim holds one advisory lock to commit; this keeps the shared lock table bounded). The terminal predicate is final:
-- committed and redacted never revert, and no stage intent can be created for a committed claim
-- without a live acknowledged parent. min_age covers a delayed Babysitter registration.
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_decisions(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET lock_timeout='100ms' AS $decisions$
DECLARE s sessions.ingest_bookkeeping_cleanup_settings%ROWTYPE;v_after uuid;v_budget int;v_execute boolean;
 v_rows int;v_claims uuid[];v_next uuid;v_done boolean;v_deleted int:=0;v_eligible_rows int;r record;
 v_terminal uuid[];v_live int;v_young int;v_babysitter int;v_retire uuid[]:='{}';v_skipped_claims int:=0;v_skipped int:=0;c uuid;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['after','rowBudget','execute']);
 IF jsonb_typeof(p->'execute') IS DISTINCT FROM 'boolean' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='bookkeeping cleanup: execute must be boolean';END IF;
 v_execute:=(p->>'execute')::boolean;
 s:=sessions.ingest_bookkeeping_cleanup_gate('decisions',v_execute);
 v_budget:=sessions.ingest_bounded(p->'rowBudget',500,5000);
 IF p->'after'='null'::jsonb THEN v_after:='00000000-0000-0000-0000-000000000000';
 ELSIF jsonb_typeof(p->'after')='string' AND p->>'after'~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' THEN v_after:=(p->>'after')::uuid;
 ELSE RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='bookkeeping cleanup: invalid cursor';END IF;
 IF NOT sessions.ingest_bookkeeping_cleanup_try_lock('decisions') THEN RETURN jsonb_build_object('phase','decisions','busy',true);END IF;
 WITH win AS MATERIALIZED(SELECT d.claim_token FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token>v_after
   ORDER BY d.claim_token,d.ord LIMIT v_budget)
 SELECT count(*)::int,coalesce(array_agg(DISTINCT claim_token),'{}') INTO v_rows,v_claims FROM win;
 v_done:=v_rows<v_budget;
 -- A full window may end inside a claim: leave that last claim to the next page (unless it is the only one).
 IF NOT v_done AND cardinality(v_claims)>1 THEN
  v_claims:=ARRAY(SELECT x FROM unnest(v_claims) x WHERE x<(SELECT y FROM unnest(v_claims) y ORDER BY y DESC LIMIT 1));
 END IF;
 IF cardinality(v_claims)>200 THEN
  v_claims:=ARRAY(SELECT x FROM unnest(v_claims) x ORDER BY x LIMIT 200);v_done:=false;
 END IF;
 v_next:=(SELECT x FROM unnest(v_claims) x ORDER BY x DESC LIMIT 1);
 WITH judged AS MATERIALIZED(
  SELECT x AS claim_token,
   EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b
     ON b.org_id=a.org_id AND b.workspace_id=a.workspace_id AND b.origin_id=a.origin_id AND b.batch_id=a.batch_id
    WHERE a.claim_token=x AND b.user_id=a.user_id AND b.digest=a.digest AND b.state='committed'
     AND b.committed_at IS NOT NULL AND b.committed_at<=clock_timestamp()-s.min_age) AS old_committed,
   EXISTS(SELECT 1 FROM sessions.ingest_stage_intents i WHERE i.claim_token=x AND i.cleanup_state<>'redacted') AS live_stage,
   EXISTS(SELECT 1 FROM sessions.babysitter_evidence_index e WHERE e.claim_token=x) AS babysitter
  FROM unnest(v_claims) x)
 SELECT coalesce(array_agg(claim_token ORDER BY claim_token) FILTER(WHERE old_committed AND NOT live_stage AND NOT babysitter),'{}'),
  count(*) FILTER(WHERE live_stage)::int,count(*) FILTER(WHERE NOT old_committed)::int,count(*) FILTER(WHERE babysitter)::int
 INTO v_terminal,v_live,v_young,v_babysitter FROM judged;
 SELECT count(*)::int INTO v_eligible_rows FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=ANY(v_terminal);
 IF v_execute AND cardinality(v_terminal)>0 THEN
  -- Interlock with ingest_babysitter_retention_register: take each claim's lock EXCLUSIVELY without waiting
  -- (a registration in flight holds it shared: skip that claim, counted), then re-check the evidence index
  -- in a NEW statement (a registration that committed meanwhile is now visible) before retiring it.
  FOREACH c IN ARRAY v_terminal LOOP
   IF NOT pg_try_advisory_xact_lock(sessions.ingest_bookkeeping_claim_lock_key(c)) THEN
    v_skipped_claims:=v_skipped_claims+1;
    v_skipped:=v_skipped+(SELECT count(*)::int FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=c);
   ELSIF EXISTS(SELECT 1 FROM sessions.babysitter_evidence_index e WHERE e.claim_token=c) THEN
    v_babysitter:=v_babysitter+1;v_eligible_rows:=v_eligible_rows-(SELECT count(*)::int FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=c);
   ELSE v_retire:=v_retire||c;
   END IF;
  END LOOP;
  -- Retirement begins: from this commit on, registration for these claims fails (irreversible marker).
  INSERT INTO sessions.ingest_bookkeeping_retired_claims(claim_token) SELECT unnest(v_retire) ON CONFLICT DO NOTHING;
  WITH locked AS(SELECT d.claim_token,d.ord FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=ANY(v_retire)
    FOR UPDATE SKIP LOCKED),
  gone AS(DELETE FROM sessions.ingest_lazy_ack_decisions d USING locked l WHERE d.claim_token=l.claim_token AND d.ord=l.ord RETURNING 1)
  SELECT count(*)::int INTO v_deleted FROM gone;
  v_skipped:=v_skipped+((SELECT count(*)::int FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=ANY(v_retire)));
  INSERT INTO sessions.ingest_bookkeeping_cleanup_audit(action,phase,examined,deleted,cursor_after,cursor_next)
   VALUES('deleted','decisions',v_rows,v_deleted,v_after::text,v_next::text);
 END IF;
 RETURN jsonb_build_object('phase','decisions','busy',false,'mode',CASE WHEN v_execute THEN 'execute' ELSE 'dry-run' END,
  'scannedRows',v_rows,'claims',cardinality(v_claims),'terminalClaims',cardinality(v_terminal),'eligibleRows',v_eligible_rows,
  'deleted',v_deleted,'blocked',jsonb_build_object('liveStage',v_live,'uncommittedOrYoung',v_young,'babysitter',v_babysitter),
  'skipped',v_skipped,'skippedClaims',v_skipped_claims,'next',v_next,'end',v_done,'done',v_done AND v_skipped=0);
END $decisions$;

-- Phase B, seats. p: {after: decimal seat string|null, limit 1..1000, execute}
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_seats(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET lock_timeout='100ms' AS $seats$
DECLARE s sessions.ingest_bookkeeping_cleanup_settings%ROWTYPE;v_after bigint;v_limit int;v_execute boolean;
 v_rows int;v_next bigint;v_eligible text[];v_deleted int:=0;v_young int;v_decision int;v_slot int;v_other int;v_skipped int:=0;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['after','limit','execute']);
 IF jsonb_typeof(p->'execute') IS DISTINCT FROM 'boolean' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='bookkeeping cleanup: execute must be boolean';END IF;
 v_execute:=(p->>'execute')::boolean;
 s:=sessions.ingest_bookkeeping_cleanup_gate('seats',v_execute);
 v_limit:=sessions.ingest_bounded(p->'limit',1,1000);
 IF p->'after'='null'::jsonb THEN v_after:=0;
 ELSIF jsonb_typeof(p->'after')='string' AND p->>'after'~'^(0|[1-9][0-9]{0,18})$' THEN v_after:=(p->>'after')::bigint;
 ELSE RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='bookkeeping cleanup: invalid cursor';END IF;
 IF NOT sessions.ingest_bookkeeping_cleanup_try_lock('seats') THEN RETURN jsonb_build_object('phase','seats','busy',true);END IF;
 WITH win AS MATERIALIZED(SELECT x.key_sha256,x.seat,x.created_at FROM sessions.ingest_receipt_seats x WHERE x.seat>v_after
   ORDER BY x.seat LIMIT v_limit),
 judged AS MATERIALIZED(SELECT w.key_sha256,w.seat,w.created_at<=clock_timestamp()-s.min_age AS old,
   EXISTS(SELECT 1 FROM sessions.ingest_lazy_ack_decisions d WHERE d.seat_key_sha256=w.key_sha256) AS by_decision,
   EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots n WHERE n.current_seat_key=w.key_sha256) AS by_slot,
   EXISTS(SELECT 1 FROM sessions.ingest_receipt_record_heads h WHERE h.winner_key_sha256=w.key_sha256)
    OR EXISTS(SELECT 1 FROM sessions.ingest_receipt_ord_seats o WHERE o.key_sha256=w.key_sha256) AS by_other
  FROM win w)
 SELECT count(*)::int,max(seat),coalesce(array_agg(key_sha256) FILTER(WHERE old AND NOT by_decision AND NOT by_slot AND NOT by_other),'{}'),
  count(*) FILTER(WHERE NOT old)::int,count(*) FILTER(WHERE by_decision)::int,count(*) FILTER(WHERE by_slot)::int,count(*) FILTER(WHERE by_other)::int
 INTO v_rows,v_next,v_eligible,v_young,v_decision,v_slot,v_other FROM judged;
 IF v_execute AND cardinality(v_eligible)>0 THEN
  -- An ACK inserting a reference holds FOR KEY SHARE on that (current, hence never eligible) seat: skipped.
  -- The FKs re-check at DELETE time; a violation rolls the whole page back (fail closed).
  WITH locked AS(SELECT x.key_sha256 FROM sessions.ingest_receipt_seats x WHERE x.key_sha256=ANY(v_eligible) FOR UPDATE SKIP LOCKED),
  gone AS(DELETE FROM sessions.ingest_receipt_seats x USING locked l WHERE x.key_sha256=l.key_sha256 RETURNING 1)
  SELECT count(*)::int INTO v_deleted FROM gone;
  INSERT INTO sessions.ingest_bookkeeping_cleanup_audit(action,phase,examined,deleted,cursor_after,cursor_next)
   VALUES('deleted','seats',v_rows,v_deleted,v_after::text,v_next::text);
  v_skipped:=cardinality(v_eligible)-v_deleted;
 END IF;
 RETURN jsonb_build_object('phase','seats','busy',false,'mode',CASE WHEN v_execute THEN 'execute' ELSE 'dry-run' END,
  'scannedRows',v_rows,'eligibleRows',cardinality(v_eligible),'deleted',v_deleted,
  'blocked',jsonb_build_object('young',v_young,'decision',v_decision,'currentSlot',v_slot,'headOrOrdSeat',v_other),
  'skipped',v_skipped,'next',v_next::text,'end',v_rows<v_limit,'done',v_rows<v_limit AND v_skipped=0);
END $seats$;

-- Phase B, custody. Live-witness kind only (import_token IS NULL). p: {after: hex64|null, limit 1..1000, execute}
-- min_age as for seats: a custody row has no timestamp of its own, but it is inserted in the same transaction
-- as its native slot (ingest_lazy_records), so the slot's initialized_at (a PK probe) is its age. No slot: kept.
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_custody(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET lock_timeout='100ms' AS $custody$
DECLARE s sessions.ingest_bookkeeping_cleanup_settings%ROWTYPE;v_after text;v_limit int;v_execute boolean;
 v_rows int;v_next text;v_eligible text[];v_deleted int:=0;v_slot int;v_other int;v_imported int;v_skipped int:=0;v_young int;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['after','limit','execute']);
 IF jsonb_typeof(p->'execute') IS DISTINCT FROM 'boolean' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='bookkeeping cleanup: execute must be boolean';END IF;
 v_execute:=(p->>'execute')::boolean;
 s:=sessions.ingest_bookkeeping_cleanup_gate('custody',v_execute);
 v_limit:=sessions.ingest_bounded(p->'limit',1,1000);
 IF p->'after'='null'::jsonb THEN v_after:='';
 ELSIF jsonb_typeof(p->'after')='string' AND p->>'after'~'^[0-9a-f]{64}$' THEN v_after:=p->>'after';
 ELSE RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='bookkeeping cleanup: invalid cursor';END IF;
 IF NOT sessions.ingest_bookkeeping_cleanup_try_lock('custody') THEN RETURN jsonb_build_object('phase','custody','busy',true);END IF;
 -- key_sha256 is fixed-length lower-case hex (CHECK): in the database collation (ICU en-US on Supabase, digits
 -- before letters) its order equals byte order, so the keyset is stable AND the PK index serves it. A COLLATE "C"
 -- comparison would not use the PK and would sort the whole table on every page.
 WITH win AS MATERIALIZED(SELECT c.key_sha256,c.import_token,c.org_id,c.workspace_id,c.origin_id,c.record_id FROM sessions.ingest_custody_records c
   WHERE c.key_sha256>v_after ORDER BY c.key_sha256 LIMIT v_limit),
 judged AS MATERIALIZED(SELECT w.key_sha256,w.import_token IS NOT NULL AS imported,
   coalesce((SELECT n.initialized_at<=clock_timestamp()-s.min_age FROM sessions.ingest_lazy_native_slots n WHERE n.key_sha256=encode(sha256(convert_to(
    jsonb_build_array('global-native-custody-v1',w.org_id,w.workspace_id,w.origin_id,w.record_id)::text,'UTF8')),'hex')),false) AS old,
   EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots n WHERE n.current_legacy_key=w.key_sha256) AS by_slot,
   EXISTS(SELECT 1 FROM sessions.ingest_receipt_record_heads h WHERE h.legacy_key_sha256=w.key_sha256)
    OR EXISTS(SELECT 1 FROM sessions.ingest_receipt_ord_seats o WHERE o.legacy_custody_key_sha256=w.key_sha256) AS by_other
  FROM win w)
 SELECT count(*)::int,max(key_sha256),coalesce(array_agg(key_sha256) FILTER(WHERE old AND NOT imported AND NOT by_slot AND NOT by_other),'{}'),
  count(*) FILTER(WHERE by_slot)::int,count(*) FILTER(WHERE by_other)::int,count(*) FILTER(WHERE imported)::int,count(*) FILTER(WHERE NOT old)::int
 INTO v_rows,v_next,v_eligible,v_slot,v_other,v_imported,v_young FROM judged;
 IF v_execute AND cardinality(v_eligible)>0 THEN
  WITH locked AS(SELECT c.key_sha256 FROM sessions.ingest_custody_records c WHERE c.key_sha256=ANY(v_eligible) AND c.import_token IS NULL
    FOR UPDATE SKIP LOCKED),
  gone AS(DELETE FROM sessions.ingest_custody_records c USING locked l WHERE c.key_sha256=l.key_sha256 RETURNING 1)
  SELECT count(*)::int INTO v_deleted FROM gone;
  INSERT INTO sessions.ingest_bookkeeping_cleanup_audit(action,phase,examined,deleted,cursor_after,cursor_next)
   VALUES('deleted','custody',v_rows,v_deleted,nullif(v_after,''),v_next);
  v_skipped:=cardinality(v_eligible)-v_deleted;
 END IF;
 RETURN jsonb_build_object('phase','custody','busy',false,'mode',CASE WHEN v_execute THEN 'execute' ELSE 'dry-run' END,
  'scannedRows',v_rows,'eligibleRows',cardinality(v_eligible),'deleted',v_deleted,
  'blocked',jsonb_build_object('currentSlot',v_slot,'headOrOrdSeat',v_other,'imported',v_imported,'young',v_young),
  'skipped',v_skipped,'next',v_next,'end',v_rows<v_limit,'done',v_rows<v_limit AND v_skipped=0);
END $custody$;

-- Phase C census, READ-ONLY (no delete exists). PK probes only. p: {after: [org,ws,origin,record]|null, limit 1..5000}
-- slotInitialized: the native slot for the key exists (the witness's `current` is then ignored by the target).
-- ownedCovered: session_id IS NULL or the row's identity is in ingest_frozen_source_owned_sessions (a delete
-- would then leave the 041000 reset union unchanged). Both are necessary, NOT sufficient (design §2, §3.3).
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_frozen_census(p jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $frozen$
DECLARE v_limit int;a jsonb;v jsonb;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['after','limit']);
 PERFORM sessions.ingest_bookkeeping_cleanup_gate('frozen',false);
 v_limit:=sessions.ingest_bounded(p->'limit',1,5000);a:=p->'after';
 IF a<>'null'::jsonb AND (jsonb_typeof(a)<>'array' OR jsonb_array_length(a)<>4
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(a) e WHERE jsonb_typeof(e)<>'string')) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='bookkeeping cleanup: invalid cursor';END IF;
 WITH win AS MATERIALIZED(SELECT f.* FROM sessions.ingest_frozen_source_records f
   WHERE a='null'::jsonb OR (f.org_id,f.workspace_id,f.origin_id,f.record_id)>(a->>0,a->>1,a->>2,a->>3)
   ORDER BY f.org_id,f.workspace_id,f.origin_id,f.record_id LIMIT v_limit),
 judged AS(SELECT w.org_id,w.workspace_id,w.origin_id,w.record_id,
   EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots n WHERE n.key_sha256=encode(sha256(convert_to(
    jsonb_build_array('global-native-custody-v1',w.org_id,w.workspace_id,w.origin_id,w.record_id)::text,'UTF8')),'hex')) AS slot,
   w.session_id IS NULL OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_owned_sessions o WHERE o.org_id=w.org_id
    AND o.workspace_id=w.workspace_id AND o.user_id=w.user_id AND o.origin_id=w.origin_id AND o.source=w.source AND o.session_id=w.session_id) AS covered
  FROM win w)
 SELECT jsonb_build_object('phase','frozen','mode','census','scannedRows',count(*),
  'slotInitialized',count(*) FILTER(WHERE slot),'ownedCovered',count(*) FILTER(WHERE covered),
  'bothConditions',count(*) FILTER(WHERE slot AND covered),
  'next',(SELECT jsonb_build_array(j.org_id,j.workspace_id,j.origin_id,j.record_id) FROM judged j
    ORDER BY j.org_id DESC,j.workspace_id DESC,j.origin_id DESC,j.record_id DESC LIMIT 1),
  'skipped',0,'end',count(*)<v_limit,'done',count(*)<v_limit) INTO v FROM judged;
 RETURN v;
END $frozen$;

-- Babysitter registration with the retirement interlock. Body verbatim from 20261105042000 (which stays
-- untouched) except the marked block. CREATE OR REPLACE keeps its owner, SECURITY DEFINER and grants.
CREATE OR REPLACE FUNCTION sessions.ingest_babysitter_retention_register(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $register$
DECLARE v_org text;v_ws text;v_user text;v_origin text;v_machine text;v_epoch bigint;v_epochs jsonb;v_pairs jsonb;v_fenced jsonb;
 o jsonb;v_ref text;v_source text;v_session text;v_after timestamptz;v_fence bigint;v_hour text;v_refs text[]:='{}';v_object text;
 v_rec jsonb;v_b integer;v_op text;v_rev bigint;v_rid text;v_n integer;
 v_prev sessions.babysitter_retention_objects%ROWTYPE;r sessions.babysitter_retention_objects%ROWTYPE;out jsonb:='[]'::jsonb;reopened integer:=0;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId','batchDigest','claimToken','generation','resetEpoch','buckets','records']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 IF jsonb_typeof(p->'generation') IS DISTINCT FROM 'string' OR p->>'generation'!~'^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: generation malformed';END IF;
 PERFORM sessions.ingest_require_generation(p->>'generation');
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);
 v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);
 v_origin:=sessions.ingest_identifier(p->'originId',false);
 IF jsonb_typeof(p->'resetEpoch') IS DISTINCT FROM 'number' OR (p->>'resetEpoch')::numeric<>trunc((p->>'resetEpoch')::numeric)
  OR (p->>'resetEpoch')::numeric<0 OR (p->>'resetEpoch')::numeric>9007199254740991 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: reset epoch malformed';END IF;
 v_epoch:=(p->>'resetEpoch')::bigint;
 IF v_epoch<>(SELECT count(*) FROM sessions.upload_resets x WHERE x.org_id=v_org AND x.workspace_id=v_ws AND x.user_id=v_user) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='babysitter retention: reset epoch changed';END IF;
 IF jsonb_typeof(p->'buckets') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'buckets')>500 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: buckets malformed';END IF;
 -- The claim's own epoch snapshot (P0002/40001 if this is not that live claim attempt).
 v_epochs:=sessions.ingest_attempt_epochs(v_org,v_ws,v_user,v_origin,sessions.ingest_identifier(p->'batchId',false),
  sessions.ingest_digest(p->'batchDigest'),p->>'claimToken');
 -- Bookkeeping-cleanup interlock (the ONLY change from 042000): serialize with decision retirement on the
 -- claim's lock, SHARED (registrations of one claim never block each other), BEFORE any write. The check is a
 -- new statement after the lock, so a retirement that committed while this waited is visible: refuse it.
 PERFORM pg_advisory_xact_lock_shared(sessions.ingest_bookkeeping_claim_lock_key((p->>'claimToken')::uuid));
 IF EXISTS(SELECT 1 FROM sessions.ingest_bookkeeping_retired_claims rc WHERE rc.claim_token=(p->>'claimToken')::uuid) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='babysitter retention: claim decisions retired; registration closed';END IF;
 SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_array(x->>'source',x->>'sessionId')),'[]'::jsonb) INTO v_pairs
  FROM (SELECT bx.e FROM jsonb_array_elements(p->'buckets') bx(e) UNION ALL
        SELECT rx.e FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p->'records')='array' THEN p->'records' ELSE '[]'::jsonb END) rx(e)) y(x)
  WHERE jsonb_typeof(x)='object' AND x->'sessionId'<>'null'::jsonb;
 IF jsonb_array_length(v_pairs)>0 THEN
  v_fenced:=sessions.ingest_attempt_fenced(v_org,v_ws,v_user,v_origin,v_epochs,v_pairs);
  IF jsonb_array_length(v_fenced)>0 THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='babysitter retention: a claimed session is fenced now';END IF;
 END IF;
 v_machine:='srv_'||left(encode(sha256(convert_to(v_ws||chr(31)||v_origin||chr(31)||v_user,'UTF8')),'hex'),32);
 FOR o IN SELECT x FROM jsonb_array_elements(p->'buckets') x LOOP
  PERFORM sessions.ingest_exact_keys(o,ARRAY['source','sessionId','eraseAfterMs']);
  v_source:=sessions.ingest_identifier(o->'source',false);
  v_session:=CASE WHEN o->'sessionId'='null'::jsonb THEN NULL ELSE sessions.ingest_identifier(o->'sessionId',false) END;
  IF jsonb_typeof(o->'eraseAfterMs') IS DISTINCT FROM 'number' OR (o->>'eraseAfterMs')::numeric<>trunc((o->>'eraseAfterMs')::numeric)
   OR (o->>'eraseAfterMs')::numeric%3600000<>0 OR (o->>'eraseAfterMs')::numeric<=0
   OR (o->>'eraseAfterMs')::numeric>(extract(epoch FROM clock_timestamp())*1000+32::numeric*86400000) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: erase hour malformed';END IF;
  v_after:=to_timestamp((o->>'eraseAfterMs')::numeric/1000);
  v_fence:=CASE WHEN v_session IS NULL THEN 0 ELSE (SELECT (e->>'epoch')::bigint FROM jsonb_array_elements(v_epochs) e
   WHERE e->>'source'=v_source AND e->>'session_id'=v_session LIMIT 1) END;
  IF v_fence IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='babysitter retention: session outside claim attempt';END IF;
  v_hour:=to_char(v_after AT TIME ZONE 'UTC','YYYYMMDDHH24');
  v_ref:='_bx'||v_hour||'-'||left(encode(sha256(convert_to(v_source||chr(31)||coalesce(v_session,'')||chr(31)||v_epoch::text||chr(31)||v_fence::text,'UTF8')),'hex'),16);
  SELECT * INTO v_prev FROM sessions.babysitter_retention_objects WHERE org_id=v_org AND workspace_id=v_ws
   AND machine_id=v_machine AND object_ref=v_ref FOR UPDATE;
  -- Never write into a bucket an erase may be running on: retry after the lease.
  IF FOUND AND v_prev.state='leased' AND v_prev.lease_until>clock_timestamp() THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='babysitter retention: bucket is being erased';END IF;
  INSERT INTO sessions.babysitter_retention_objects AS t(org_id,workspace_id,user_id,origin_id,machine_id,object_ref,source,session_id,reset_epoch,fence_epoch,erase_after)
   VALUES(v_org,v_ws,v_user,v_origin,v_machine,v_ref,v_source,v_session,v_epoch,v_fence,v_after)
  ON CONFLICT(org_id,workspace_id,machine_id,object_ref) DO UPDATE
   SET write_seq=t.write_seq+1,last_written_at=clock_timestamp(),
    state='pending',lease_token=NULL,lease_until=NULL,leased_write_seq=NULL,lease_reason=NULL,
    erased_at=NULL,erased_versions=NULL,erase_reason=NULL,erase_checkpoint=NULL
  RETURNING * INTO r;
  IF r.user_id<>v_user OR r.origin_id<>v_origin OR r.source<>v_source OR r.session_id IS DISTINCT FROM v_session
   OR r.reset_epoch<>v_epoch OR r.fence_epoch<>v_fence THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: ref identity changed';END IF;
  IF v_prev.state='erased' THEN
   reopened:=reopened+1;
   INSERT INTO sessions.babysitter_retention_audit(action,org_id,workspace_id,machine_id,object_ref,write_seq)
    VALUES('reopened',r.org_id,r.workspace_id,r.machine_id,r.object_ref,r.write_seq);
  END IF;
  out:=out||jsonb_build_array(jsonb_build_object('ref',v_ref,'source',v_source,'sessionId',v_session,'eraseAfterMs',(o->'eraseAfterMs')));
  v_refs:=v_refs||v_ref;
 END LOOP;
 -- The content-free key index: one IMMUTABLE candidate row per routed (record, revision) of
 -- this claim, upsert or delete. Never re-pointed: an equal-revision attempt adds its own row
 -- and only the ACK-accepted candidate is ever served (babysitter_evidence_page_for).
 IF jsonb_typeof(p->'records') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'records') NOT BETWEEN 1 AND 5000 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: records malformed';END IF;
 v_object:='b='||left(encode(sha256(convert_to(p->>'batchDigest','UTF8')),'hex'),32)||'.ndjson.zst';
 v_n:=jsonb_array_length(p->'buckets');
 FOR v_rec IN SELECT e FROM jsonb_array_elements(p->'records') e LOOP
  PERFORM sessions.ingest_exact_keys(v_rec,ARRAY['bucket','originId','recordId','revision','revisionId','digest','operation','kind','source','sessionId','line','expiresAtMs']);
  -- Prepare binds every record's origin to its batch's (so to this user's machine).
  IF sessions.ingest_identifier(v_rec->'originId',false)<>v_origin THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record origin is not the batch origin';END IF;
  IF jsonb_typeof(v_rec->'revision') IS DISTINCT FROM 'string' OR v_rec->>'revision'!~'^[1-9][0-9]{0,17}$' THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record revision malformed';END IF;
  v_rev:=(v_rec->>'revision')::bigint;v_op:=v_rec->>'operation';v_rid:=sessions.ingest_identifier(v_rec->'revisionId',false);
  IF jsonb_typeof(v_rec->'operation') IS DISTINCT FROM 'string' OR v_op NOT IN('upsert','delete')
   OR jsonb_typeof(v_rec->'kind') IS DISTINCT FROM 'string' OR v_rec->>'kind' NOT IN('session_lineage','turn_receipt') THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record operation or kind malformed';END IF;
  v_source:=sessions.ingest_identifier(v_rec->'source',false);
  v_session:=CASE WHEN v_rec->'sessionId'='null'::jsonb THEN NULL ELSE sessions.ingest_identifier(v_rec->'sessionId',false) END;
  v_fence:=CASE WHEN v_session IS NULL THEN 0 ELSE (SELECT (e->>'epoch')::bigint FROM jsonb_array_elements(v_epochs) e
   WHERE e->>'source'=v_source AND e->>'session_id'=v_session LIMIT 1) END;
  IF v_fence IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='babysitter retention: session outside claim attempt';END IF;
  IF v_op='upsert' THEN
   IF jsonb_typeof(v_rec->'bucket') IS DISTINCT FROM 'number' OR (v_rec->>'bucket')::numeric<>trunc((v_rec->>'bucket')::numeric)
    OR (v_rec->>'bucket')::numeric<0 OR (v_rec->>'bucket')::numeric>=v_n
    OR jsonb_typeof(v_rec->'line') IS DISTINCT FROM 'number' OR (v_rec->>'line')::numeric<>trunc((v_rec->>'line')::numeric)
    OR (v_rec->>'line')::numeric NOT BETWEEN 0 AND 100000 OR jsonb_typeof(v_rec->'expiresAtMs') IS DISTINCT FROM 'number'
    OR (v_rec->>'expiresAtMs')::numeric<>trunc((v_rec->>'expiresAtMs')::numeric) OR (v_rec->>'expiresAtMs')::numeric NOT BETWEEN 1 AND 9007199254740991 THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record position malformed';END IF;
   v_b:=(v_rec->>'bucket')::integer+1;
   -- The record must live in its own (source, session) bucket.
   IF (p->'buckets'->(v_b-1)->>'source')<>v_source OR (p->'buckets'->(v_b-1)->'sessionId')<>(v_rec->'sessionId') THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record is not in its session bucket';END IF;
  ELSIF v_rec->'bucket'<>'null'::jsonb OR v_rec->'line'<>'null'::jsonb OR v_rec->'expiresAtMs'<>'null'::jsonb THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record position malformed';
  END IF;
  INSERT INTO sessions.babysitter_evidence_index AS t(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,operation,kind,claim_token,
    user_id,source,session_id,reset_epoch,fence_epoch,machine_id,object_ref,object_name,line,expires_at_ms)
   VALUES(v_org,v_ws,v_origin,sessions.ingest_identifier(v_rec->'recordId',false),v_rev,v_rid,sessions.ingest_digest(v_rec->'digest'),v_op,v_rec->>'kind',
    (p->>'claimToken')::uuid,v_user,v_source,v_session,v_epoch,v_fence,
    CASE WHEN v_op='upsert' THEN v_machine END,CASE WHEN v_op='upsert' THEN v_refs[v_b] END,CASE WHEN v_op='upsert' THEN v_object END,
    CASE WHEN v_op='upsert' THEN (v_rec->>'line')::integer END,CASE WHEN v_op='upsert' THEN (v_rec->>'expiresAtMs')::bigint END)
  ON CONFLICT(org_id,workspace_id,origin_id,record_id,revision,claim_token) DO NOTHING;
  -- Idempotent only for the identical candidate of the same claim; never an update.
  IF NOT EXISTS(SELECT 1 FROM sessions.babysitter_evidence_index i WHERE i.org_id=v_org AND i.workspace_id=v_ws AND i.origin_id=v_origin
    AND i.record_id=v_rec->>'recordId' AND i.revision=v_rev AND i.claim_token=(p->>'claimToken')::uuid
    AND i.revision_id=v_rid AND i.digest=v_rec->>'digest' AND i.operation=v_op AND i.kind=v_rec->>'kind' AND i.user_id=v_user
    AND i.object_name IS NOT DISTINCT FROM CASE WHEN v_op='upsert' THEN v_object END
    AND i.line IS NOT DISTINCT FROM CASE WHEN v_op='upsert' THEN (v_rec->>'line')::integer END) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: candidate identity changed';END IF;
 END LOOP;
 -- One durable obligation per bucket object and one DISPATCHED invocation (this claim), BEFORE the PUT.
 FOR v_b IN 1..coalesce(array_length(v_refs,1),0) LOOP
  INSERT INTO sessions.babysitter_write_obligations AS w(org_id,workspace_id,machine_id,object_ref,object_name)
   VALUES(v_org,v_ws,v_machine,v_refs[v_b],v_object)
  ON CONFLICT(org_id,workspace_id,machine_id,object_ref,object_name) DO UPDATE SET rev=w.rev+1;
  INSERT INTO sessions.babysitter_write_invocations(org_id,workspace_id,machine_id,object_ref,object_name,invocation_id)
   VALUES(v_org,v_ws,v_machine,v_refs[v_b],v_object,(p->>'claimToken')::uuid)
  ON CONFLICT(org_id,workspace_id,machine_id,object_ref,object_name,invocation_id) DO NOTHING;
 END LOOP;
 RETURN jsonb_build_object('objects',out,'reopened',reopened);
END $register$;

REVOKE ALL ON FUNCTION sessions.ingest_bookkeeping_cleanup_audit_guard(),sessions.ingest_bookkeeping_retired_claims_guard(),
 sessions.ingest_bookkeeping_claim_lock_key(uuid),sessions.ingest_bookkeeping_cleanup_index_ok(text,text),sessions.ingest_bookkeeping_cleanup_phase_b_ready(),
 sessions.ingest_bookkeeping_cleanup_gate(text,boolean),sessions.ingest_bookkeeping_cleanup_try_lock(text),
 sessions.ingest_bookkeeping_cleanup_configure(jsonb),sessions.ingest_bookkeeping_cleanup_decisions(jsonb),
 sessions.ingest_bookkeeping_cleanup_seats(jsonb),sessions.ingest_bookkeeping_cleanup_custody(jsonb),
 sessions.ingest_bookkeeping_cleanup_frozen_census(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
