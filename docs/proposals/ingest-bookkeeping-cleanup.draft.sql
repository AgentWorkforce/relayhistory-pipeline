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

-- Phase B prerequisite: every FK-side index the RI checks (and the predicates) probe, valid and ready.
-- Without them each seat/custody DELETE would seq-scan referencing tables of up to ~27M rows.
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_phase_b_ready() RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $ready$
 SELECT count(*)=7 FROM (VALUES
  ('sessions.ingest_lazy_ack_decisions_seat_key_idx','sessions.ingest_lazy_ack_decisions'),
  ('sessions.ingest_lazy_native_slots_current_seat_idx','sessions.ingest_lazy_native_slots'),
  ('sessions.ingest_lazy_native_slots_current_legacy_idx','sessions.ingest_lazy_native_slots'),
  ('sessions.ingest_receipt_record_heads_winner_idx','sessions.ingest_receipt_record_heads'),
  ('sessions.ingest_receipt_record_heads_legacy_idx','sessions.ingest_receipt_record_heads'),
  ('sessions.ingest_receipt_ord_seats_seat_idx','sessions.ingest_receipt_ord_seats'),
  ('sessions.ingest_receipt_ord_seats_legacy_idx','sessions.ingest_receipt_ord_seats')) v(idx,tbl)
 JOIN pg_index i ON i.indexrelid=to_regclass(v.idx) AND i.indrelid=to_regclass(v.tbl) AND i.indisvalid AND i.indisready
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
 IF p_phase='decisions' AND NOT EXISTS(SELECT 1 FROM pg_index i WHERE i.indexrelid=to_regclass('sessions.babysitter_evidence_index_claim_idx')
   AND i.indrelid='sessions.babysitter_evidence_index'::regclass AND i.indisvalid AND i.indisready) THEN
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
-- (a claim has at most 500 ordinals, so a single claim always fits). The terminal predicate is final:
-- committed and redacted never revert, and no stage intent can be created for a committed claim
-- without a live acknowledged parent. min_age covers a delayed Babysitter registration.
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_decisions(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET lock_timeout='100ms' AS $decisions$
DECLARE s sessions.ingest_bookkeeping_cleanup_settings%ROWTYPE;v_after uuid;v_budget int;v_execute boolean;
 v_rows int;v_claims uuid[];v_next uuid;v_done boolean;v_deleted int:=0;v_eligible_rows int;r record;
 v_terminal uuid[];v_live int;v_young int;v_babysitter int;
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
  v_claims:=ARRAY(SELECT c FROM unnest(v_claims) c WHERE c<(SELECT x FROM unnest(v_claims) x ORDER BY x DESC LIMIT 1));
 END IF;
 v_next:=(SELECT c FROM unnest(v_claims) c ORDER BY c DESC LIMIT 1);
 WITH judged AS MATERIALIZED(
  SELECT c AS claim_token,
   EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b
     ON b.org_id=a.org_id AND b.workspace_id=a.workspace_id AND b.origin_id=a.origin_id AND b.batch_id=a.batch_id
    WHERE a.claim_token=c AND b.user_id=a.user_id AND b.digest=a.digest AND b.state='committed'
     AND b.committed_at IS NOT NULL AND b.committed_at<=clock_timestamp()-s.min_age) AS old_committed,
   EXISTS(SELECT 1 FROM sessions.ingest_stage_intents i WHERE i.claim_token=c AND i.cleanup_state<>'redacted') AS live_stage,
   EXISTS(SELECT 1 FROM sessions.babysitter_evidence_index x WHERE x.claim_token=c) AS babysitter
  FROM unnest(v_claims) c)
 SELECT coalesce(array_agg(claim_token ORDER BY claim_token) FILTER(WHERE old_committed AND NOT live_stage AND NOT babysitter),'{}'),
  count(*) FILTER(WHERE live_stage)::int,count(*) FILTER(WHERE NOT old_committed)::int,count(*) FILTER(WHERE babysitter)::int
 INTO v_terminal,v_live,v_young,v_babysitter FROM judged;
 SELECT count(*)::int INTO v_eligible_rows FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=ANY(v_terminal);
 IF v_execute AND cardinality(v_terminal)>0 THEN
  WITH locked AS(SELECT d.claim_token,d.ord FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=ANY(v_terminal)
    FOR UPDATE SKIP LOCKED),
  gone AS(DELETE FROM sessions.ingest_lazy_ack_decisions d USING locked l WHERE d.claim_token=l.claim_token AND d.ord=l.ord RETURNING 1)
  SELECT count(*)::int INTO v_deleted FROM gone;
  INSERT INTO sessions.ingest_bookkeeping_cleanup_audit(action,phase,examined,deleted,cursor_after,cursor_next)
   VALUES('deleted','decisions',v_rows,v_deleted,v_after::text,v_next::text);
 END IF;
 RETURN jsonb_build_object('phase','decisions','busy',false,'mode',CASE WHEN v_execute THEN 'execute' ELSE 'dry-run' END,
  'scannedRows',v_rows,'claims',cardinality(v_claims),'terminalClaims',cardinality(v_terminal),'eligibleRows',v_eligible_rows,
  'deleted',v_deleted,'blocked',jsonb_build_object('liveStage',v_live,'uncommittedOrYoung',v_young,'babysitter',v_babysitter),
  'next',v_next,'done',v_done);
END $decisions$;

-- Phase B, seats. p: {after: decimal seat string|null, limit 1..1000, execute}
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_seats(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET lock_timeout='100ms' AS $seats$
DECLARE s sessions.ingest_bookkeeping_cleanup_settings%ROWTYPE;v_after bigint;v_limit int;v_execute boolean;
 v_rows int;v_next bigint;v_eligible text[];v_deleted int:=0;v_young int;v_decision int;v_slot int;v_other int;
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
 END IF;
 RETURN jsonb_build_object('phase','seats','busy',false,'mode',CASE WHEN v_execute THEN 'execute' ELSE 'dry-run' END,
  'scannedRows',v_rows,'eligibleRows',cardinality(v_eligible),'deleted',v_deleted,
  'blocked',jsonb_build_object('young',v_young,'decision',v_decision,'currentSlot',v_slot,'headOrOrdSeat',v_other),
  'next',v_next::text,'done',v_rows<v_limit);
END $seats$;

-- Phase B, custody. Live-witness kind only (import_token IS NULL). p: {after: hex64|null, limit 1..1000, execute}
CREATE FUNCTION sessions.ingest_bookkeeping_cleanup_custody(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET lock_timeout='100ms' AS $custody$
DECLARE s sessions.ingest_bookkeeping_cleanup_settings%ROWTYPE;v_after text;v_limit int;v_execute boolean;
 v_rows int;v_next text;v_eligible text[];v_deleted int:=0;v_slot int;v_other int;v_imported int;
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
 WITH win AS MATERIALIZED(SELECT c.key_sha256,c.import_token FROM sessions.ingest_custody_records c
   WHERE c.key_sha256>v_after ORDER BY c.key_sha256 LIMIT v_limit),
 judged AS MATERIALIZED(SELECT w.key_sha256,w.import_token IS NOT NULL AS imported,
   EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots n WHERE n.current_legacy_key=w.key_sha256) AS by_slot,
   EXISTS(SELECT 1 FROM sessions.ingest_receipt_record_heads h WHERE h.legacy_key_sha256=w.key_sha256)
    OR EXISTS(SELECT 1 FROM sessions.ingest_receipt_ord_seats o WHERE o.legacy_custody_key_sha256=w.key_sha256) AS by_other
  FROM win w)
 SELECT count(*)::int,max(key_sha256),coalesce(array_agg(key_sha256) FILTER(WHERE NOT imported AND NOT by_slot AND NOT by_other),'{}'),
  count(*) FILTER(WHERE by_slot)::int,count(*) FILTER(WHERE by_other)::int,count(*) FILTER(WHERE imported)::int
 INTO v_rows,v_next,v_eligible,v_slot,v_other,v_imported FROM judged;
 IF v_execute AND cardinality(v_eligible)>0 THEN
  WITH locked AS(SELECT c.key_sha256 FROM sessions.ingest_custody_records c WHERE c.key_sha256=ANY(v_eligible) AND c.import_token IS NULL
    FOR UPDATE SKIP LOCKED),
  gone AS(DELETE FROM sessions.ingest_custody_records c USING locked l WHERE c.key_sha256=l.key_sha256 RETURNING 1)
  SELECT count(*)::int INTO v_deleted FROM gone;
  INSERT INTO sessions.ingest_bookkeeping_cleanup_audit(action,phase,examined,deleted,cursor_after,cursor_next)
   VALUES('deleted','custody',v_rows,v_deleted,nullif(v_after,''),v_next);
 END IF;
 RETURN jsonb_build_object('phase','custody','busy',false,'mode',CASE WHEN v_execute THEN 'execute' ELSE 'dry-run' END,
  'scannedRows',v_rows,'eligibleRows',cardinality(v_eligible),'deleted',v_deleted,
  'blocked',jsonb_build_object('currentSlot',v_slot,'headOrOrdSeat',v_other,'imported',v_imported),
  'next',v_next,'done',v_rows<v_limit);
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
  'done',count(*)<v_limit) INTO v FROM judged;
 RETURN v;
END $frozen$;

REVOKE ALL ON FUNCTION sessions.ingest_bookkeeping_cleanup_audit_guard(),sessions.ingest_bookkeeping_cleanup_phase_b_ready(),
 sessions.ingest_bookkeeping_cleanup_gate(text,boolean),sessions.ingest_bookkeeping_cleanup_try_lock(text),
 sessions.ingest_bookkeeping_cleanup_configure(jsonb),sessions.ingest_bookkeeping_cleanup_decisions(jsonb),
 sessions.ingest_bookkeeping_cleanup_seats(jsonb),sessions.ingest_bookkeeping_cleanup_custody(jsonb),
 sessions.ingest_bookkeeping_cleanup_frozen_census(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
