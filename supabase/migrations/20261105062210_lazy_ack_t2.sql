-- Slot 20261105062210 (earmarked by the lander). T2 staged ACK: plan under the seat clock KEY SHARE, upgrade to NO KEY
-- UPDATE only for seat numbering and the writes. Gated, default OFF; held candidate. Code only: applying it to any real
-- database, setting the gate or flipping the mode anywhere need Khaliq's explicit OK. Built from MAIN's 043800 body;
-- independent of 062155 and 062156: never apply it together with either (each replaces sessions.ingest_lazy_ack_batch).
--
-- Why. Every staged ACK holds the singleton receipt seat clock FOR NO KEY UPDATE to commit, and today the whole
-- per-record loop runs inside that hold, so every ACK of every org serializes on it (55P03 under the 2 s lock_timeout).
-- T2 moves the candidate read, the native-slot locks and the decision planning out of the NO KEY UPDATE window: they run
-- under KEY SHARE, which every other ACK and claim also takes compatibly. Only the seat numbering and the writes (seat
-- INSERT, slot UPDATE, decision INSERT, last_seat) remain inside it.
--
-- Gate and mode: one owner-only singleton row, sessions.lazy_ack_t2_mode(gate, mode). No function-level SET, no custom
-- parameter, no grant: a non-superuser owner installs and changes it (PostgreSQL 15+ refuses a function-level SET of a
-- custom parameter to a non-superuser owner, 42501).
--   gate 'off' (installed): main's 043800 body (outputs, first errors, rows, sequence effects), plus one singleton read.
--   gate 'fenced': the body comes from mode ('main' installed | 't2'). CHECK (mode='main' OR gate='fenced'): the row can
--     never say off/t2.
--   on:   SELECT sessions.lazy_ack_t2_set_gate('fenced');  off: set_gate('off'). Owner-only, barriered, mode must be 'main'.
--   mode: SELECT sessions.lazy_ack_t2_set_mode('t2'|'main'); owner-only, barriered; 't2' only while the gate is 'fenced'.
--   read: SELECT gate,mode FROM sessions.lazy_ack_t2_mode;
--   Exactly one row: PK singleton CHECK(singleton) (at most one), installed with the table, and DELETE/TRUNCATE refused
--   by triggers. A missing row fails closed (55000) in the ACK and in both setters; it never means 'main'. An UPDATE is
--   refused (trigger) unless its transaction holds (961996,50000) EXCLUSIVE, as both setters do.
-- Where the ACK reads it: once, after ingest_lock_fences and after the suppressed early return, just before the clock
-- lock is chosen. ingest_lock_fences took (961996,50000) SHARED (050010's statement trigger on ingest_fence_owners),
-- held to the end of the outer transaction, and both setters TRY-lock 50000 EXCLUSIVE, so no gate or mode change can
-- commit between the read and this transaction's end.
--   READ COMMITTED: a plain read. Its statement snapshot is taken after 50000 shared was granted, so it sees every
--     change committed before, and none can commit after. T2 only if gate='fenced' AND mode='t2'.
--   REPEATABLE READ / SERIALIZABLE (treated as reachable; the shipped callers run READ COMMITTED, so this branch is
--     defensive): the transaction snapshot may predate a committed change, and a plain read then returns the stale
--     version without error. So the read is FOR SHARE, which conflicts with any UPDATE of the row: a version replaced
--     after the snapshot gives 40001 (retry the whole transaction). FOR KEY SHARE is not enough: gate and mode are
--     non-key columns, and after a barrier-held UPDATE with no prior row lock a FOR KEY SHARE read returned the stale
--     row without error (PostgreSQL 15/16/17; after the setters, whose FOR UPDATE NOWAIT precedes their UPDATE, it
--     happened to give 40001; the design does not rely on that). On a current row: gate 'off' -> main; gate
--     'fenced' -> 55000 for a staged ACK in either mode (T2 needs READ COMMITTED statement snapshots).
--   Scope: the read, the missing-row check (55000 at every isolation level, nothing written) and the fenced-isolation
--     refusal apply to the staged ACKs that need the mode decision (every non-suppressed ACK; a kind other than staged
--     is then refused as on main). Suppressed receipts
--     return before the read at every isolation level and in every gate/mode state: no read, no refusal, exactly
--     043800's suppressed path (the same text in both bodies, pinned by the PGlite suite; it selects no seat-allocation
--     body). Through the runtime entry points a suppressed receipt is ACKed inside its claim, which 043500 already
--     refuses outside READ COMMITTED (40001) in every state, as on main. r3 refused non-READ COMMITTED suppressed
--     receipts while fenced; restoring that is one hunk (move the read above the suppressed return).
--   A 40001 here means: retry the WHOLE transaction (a new snapshot). The shipped caller runs each commit as one
--     autocommit statement (src/session-ingest/ledger-sql.ts, the commit call), maps any SQLSTATE other than its lock
--     codes to IngestUnavailable (503, src/session-ingest/deliver.ts), and the client re-sends the delivery.
-- The flip barrier is (961996,50000), the existing operator-erase gate. Both setters only TRY-lock 50000 EXCLUSIVE
-- (then the row FOR UPDATE NOWAIT, in that order), so a change succeeds only at an instant when no transaction that
-- entered ingest_lock_fences (and no erase) is open, including:
--   - an invocation that selected main and is paused before the clock;
--   - a transaction paused after its ACK, before COMMIT;
--   - a claim holding the clock KEY SHARE that later enters an ACK;
--   - a multi-call transaction.
-- When busy it raises 55P03 at once with nothing changed, and it never QUEUES an exclusive request: no ingest pause
-- while it retries and no queue-order edge in any lock cycle. A successful change holds 50000 exclusive until its own
-- COMMIT (run it as one autocommit statement), so writers arriving in that window wait for that commit only. Both
-- setters require READ COMMITTED. Initial transition: off -> fenced changes nothing while the mode is 'main'. Operator:
-- src/session-ingest/lazy-ack-t2-flip.ts, ONE bounded attempt sequence per operator action in a naturally quiet window,
-- one autocommit statement per attempt. Exhausted: no switch (mode unchanged). Reply lost: indeterminate; reconcile
-- before any further attempt. No queued fallback, no admission stop, no automatic repeat. Under sustained concurrent
-- uploads the try-lock rarely finds a free instant (local bench), and that applies equally to flipping BACK to main:
-- rolling the mode back is not immediate. The down migration refuses unless gate 'off' and mode 'main'. Busy (any
-- ingest or erase transaction open): fail-fast via a try-lock on 50000, never queued, so it can starve too. A
-- successful rollback holds 50000 exclusive through its DDL and COMMIT; uploads arriving in that interval wait for it.
-- Never flip during an operator erase (same key). The gate and mode changes, the rollback and the apply are
-- real-database operations and the owner's own decisions. This does not resolve ordered-control starvation.
--
-- Mode 't2', exactly:
--   prefix as main up to the clock; the clock FOR KEY SHARE (not NO KEY UPDATE); origin lock, stage row, attempt
--   checks as main; then, still under KEY SHARE:
--     (A) the candidates once, in the loop's order (rank, then ord for ties, which the loop leaves unspecified);
--     (B) every native slot FOR UPDATE in one statement, in the loop's first-lock order;
--     (C1) the plan: the loop's running state per record (accept, lineage, running seat/digest), everything that does
--          not depend on last_seat. Each row it reads is protected by a lock this ACK holds, not by the clock:
--            - native slots: our FOR UPDATE;
--            - the seat at slot.current_seat_key: insert-only; erase is excluded by 50000;
--            - decisions keyed (claim, ord): ours, written only through our stage row and origin lock;
--            - the write-once clock identity columns: their writers take FOR UPDATE, excluded by our KEY SHARE.
--          It runs as its own statement after the slot locks (a fresh READ COMMITTED snapshot).
--   UPGRADE: SELECT last_seat ... FOR NO KEY UPDATE (last_seat is read by this locking statement itself).
--     (C2) seat numbers and keys from last_seat, the adoption check, and the loop's first error (SQLSTATE and text);
--     (D)  the writes of every record the loop completes before its first error: seat INSERT, slot UPDATE (one row per
--          statement while ingest_history_live(), else one statement per round), decision INSERT ON CONFLICT DO NOTHING;
--     (E)  the first error; then last_seat, the expiry check and the final attempt check as main.
-- Serial result: every stored row and the first error equal main's (differential PGlite suite).
-- Contention behaviour differs (stated, not hidden):
--   - an earlier validation failure can become a later slot-lock wait, because (B) locks every slot first;
--   - FOR UPDATE takers of the clock (lazy_clear, control_changed, set_live, seat_initialize) also wait behind T2's
--     KEY SHARE planning phase, as they already do behind claims.
-- Lock order (t2): fence row -> 50000 shared -> clock KEY SHARE -> origin -> stage -> slots -> clock NO KEY UPDATE.
-- No KEY SHARE -> NO KEY UPDATE deadlock between ACKs: the second upgrader waits for the first one's commit, and a queued
-- FOR UPDATE waiter does not block an upgrade (PostgreSQL 16, tested). Mixed main/t2 ACKs cannot run concurrently: the
-- row changes only under 50000 exclusive (both setters), and every staged ACK reads it under 50000 shared, current
-- (READ COMMITTED) or proven current (FOR SHARE) or refused. Owner-only; the ACK stays SECURITY INVOKER (the runtime
-- role reaches it through the SECURITY DEFINER commit entry point, as before).
-- Rollback (generated by scripts/sessions-function-boundary/lanes/lazy-ack-t2.ts):
-- supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql restores the exact 043800 body and drops the table and functions.
DO $lazy_ack_tx_creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_proc WHERE oid='sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure AND proowner<>me) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: T2 staged ACK requires original creator';END IF;
END $lazy_ack_tx_creator$;

CREATE TABLE sessions.lazy_ack_t2_mode(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 mode text NOT NULL CHECK(mode IN('main','t2')),
 gate text NOT NULL DEFAULT 'off' CHECK(gate IN('off','fenced')),
 CONSTRAINT lazy_ack_t2_mode_t2_needs_fenced CHECK(mode='main' OR gate='fenced')
);
INSERT INTO sessions.lazy_ack_t2_mode(singleton,mode,gate) VALUES(true,'main','off');
ALTER TABLE sessions.lazy_ack_t2_mode ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.lazy_ack_t2_mode FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
-- Exactly one row, changed only under the barrier. The PK allows at most one row; the triggers refuse its removal (a
-- missing row would fail every staged ACK closed) and refuse any UPDATE by a transaction that does not hold
-- (961996,50000) EXCLUSIVE, so even a direct owner UPDATE cannot change the row while an ACK holds 50000 shared.
CREATE FUNCTION sessions.lazy_ack_t2_mode_keep()RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_ack_tx_keep$
BEGIN
 IF TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
   AND classid=961996 AND objid=50000 AND objsubid=2 AND mode='ExclusiveLock' AND granted AND pid=pg_backend_pid()) THEN RETURN NEW;END IF;
 RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='T2 staged ACK: the gate row is a singleton; it is changed only by lazy_ack_t2_set_gate and lazy_ack_t2_set_mode';
END $lazy_ack_tx_keep$;
REVOKE ALL ON FUNCTION sessions.lazy_ack_t2_mode_keep() FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE TRIGGER lazy_ack_t2_mode_keep_row BEFORE UPDATE OR DELETE ON sessions.lazy_ack_t2_mode FOR EACH ROW EXECUTE FUNCTION sessions.lazy_ack_t2_mode_keep();
CREATE TRIGGER lazy_ack_t2_mode_no_truncate BEFORE TRUNCATE ON sessions.lazy_ack_t2_mode FOR EACH STATEMENT EXECUTE FUNCTION sessions.lazy_ack_t2_mode_keep();

-- The only writer of the mode row. TRY-locks the flip barrier (961996,50000) EXCLUSIVE: it succeeds only when no
-- transaction that entered ingest_lock_fences (or an erase) is open, and it never waits. Run it alone in its own
-- transaction; retry with bounded backoff outside the transaction; if the budget runs out, report no switch.
CREATE FUNCTION sessions.lazy_ack_t2_set_mode(p_mode text)RETURNS text
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_ack_tx_set_mode$
DECLARE v_old text;v_gate text;
BEGIN
 IF p_mode IS NULL OR p_mode NOT IN('main','t2') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='T2 staged ACK: mode must be main or t2';END IF;
 -- READ COMMITTED only: the reads below must see what was committed before the try-lock (a REPEATABLE READ snapshot
 -- taken before it could miss a committed gate change).
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='T2 staged ACK: the flip requires read committed';END IF;
 -- Try-lock only: never QUEUE an exclusive request (a queued one would pause every new fence call and add a
 -- queue-order edge to any cycle). Busy -> 55P03 at once, nothing changed; the caller retries in a NEW transaction.
 IF NOT pg_try_advisory_xact_lock(961996,50000) THEN
  RAISE EXCEPTION USING ERRCODE='55P03',MESSAGE='T2 staged ACK: flip barrier busy (an ingest or erase transaction is open); mode unchanged, retry';END IF;
 SELECT mode,gate INTO v_old,v_gate FROM sessions.lazy_ack_t2_mode WHERE singleton FOR UPDATE NOWAIT;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='T2 staged ACK: the gate row is missing; mode unchanged';END IF;
 -- 't2' only under the fenced gate, read from the row this call holds locked (the table CHECK enforces it too).
 IF p_mode='t2' AND v_gate<>'fenced' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='T2 staged ACK: mode t2 requires the gate fenced first (sessions.lazy_ack_t2_set_gate(''fenced'')); mode unchanged';END IF;
 UPDATE sessions.lazy_ack_t2_mode SET mode=p_mode WHERE singleton;
 RETURN v_old;
END $lazy_ack_tx_set_mode$;
REVOKE ALL ON FUNCTION sessions.lazy_ack_t2_set_mode(text) FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- The only way to change the gate, serialized with lazy_ack_t2_set_mode on the same barrier: TRY-lock (961996,50000)
-- EXCLUSIVE (busy -> 55P03, nothing changed), lock the row NOWAIT, require mode 'main' (55000 otherwise), then UPDATE
-- the gate column in this same short owner transaction. So a gate change and a mode flip can never both pass their
-- checks, and the table CHECK refuses off/t2 in any case. Returns the previous gate ('off' or 'fenced').
CREATE FUNCTION sessions.lazy_ack_t2_set_gate(p_gate text)RETURNS text
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_ack_tx_set_gate$
DECLARE v_mode text;v_old text;
BEGIN
 IF p_gate IS NULL OR p_gate NOT IN('off','fenced') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='T2 staged ACK: gate must be off or fenced';END IF;
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='T2 staged ACK: the gate change requires read committed';END IF;
 IF NOT pg_try_advisory_xact_lock(961996,50000) THEN
  RAISE EXCEPTION USING ERRCODE='55P03',MESSAGE='T2 staged ACK: flip barrier busy (an ingest or erase transaction is open); gate unchanged, retry';END IF;
 SELECT mode,gate INTO v_mode,v_old FROM sessions.lazy_ack_t2_mode WHERE singleton FOR UPDATE NOWAIT;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='T2 staged ACK: the gate row is missing; gate unchanged';END IF;
 IF v_mode<>'main' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='T2 staged ACK: change the gate only while the mode is main (sessions.lazy_ack_t2_set_mode(''main'') first); gate unchanged';END IF;
 UPDATE sessions.lazy_ack_t2_mode SET gate=p_gate WHERE singleton;
 RETURN v_old;
END $lazy_ack_tx_set_gate$;
REVOKE ALL ON FUNCTION sessions.lazy_ack_t2_set_gate(text) FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE OR REPLACE FUNCTION sessions.ingest_lazy_ack_batch(p jsonb) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_ack_batch$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;
 t sessions.ingest_stage_intents%ROWTYPE;slot sessions.ingest_lazy_native_slots%ROWTYPE;s sessions.ingest_receipt_seats%ROWTYPE;
 d sessions.ingest_lazy_ack_decisions%ROWTYPE;r record;v_last bigint;v_key text;v_material jsonb;v_accept boolean;v_first text;v_new_lineage bigint;
 v_t2 boolean;v_gate text;v_mode text;v_recs jsonb;v_plan jsonb;v_epos integer;v_ephase integer;v_lim integer;v_rounds integer;v_allocs integer;e record;
BEGIN
 PERFORM sessions.ingest_lazy_guard(p->>'org_id');
 PERFORM sessions.ingest_lock_fences(p->>'org_id',p->>'workspace_id',p->>'user_id');
 PERFORM 1 FROM sessions.ingest_lazy_user_seals u JOIN sessions.ingest_seed_fence_baselines b USING(org_id,workspace_id,user_id)
  WHERE u.org_id=p->>'org_id' AND u.workspace_id=p->>'workspace_id' AND u.user_id=p->>'user_id' AND u.privacy_fingerprint=b.fingerprint;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: real user seal required before any receipt';END IF;
 -- 043500: a suppressed receipt allocates no seat and writes nothing here. It returns BELOW the guard/fence/seal
 -- checks above and ABOVE the exclusive clock lock, holding only KEY SHARE: still excluded by set_live,
 -- control_changed and lazy_clear (FOR UPDATE), and no KEY SHARE -> FOR UPDATE upgrade inside a claim (the
 -- claim's ingest_lazy_records already holds KEY SHARE; two claims upgrading would deadlock).
 IF p->>'buffer_receipt_kind'='suppressed' THEN
  PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR KEY SHARE;RETURN;END IF;
 -- 062210 gate row, read once: after ingest_lock_fences (which took (961996,50000) SHARED, held to transaction end, so
 -- no gate or mode change can commit from here on) and after the suppressed return, before the clock lock is chosen.
 -- READ COMMITTED: a plain read; its snapshot is taken now, after 50000, so it is current. Otherwise the transaction
 -- snapshot may predate a committed change: FOR SHARE conflicts with the setters' UPDATE, so a replaced version gives
 -- 40001 (FOR KEY SHARE would not: non-key update); a current 'fenced' row is refused, a current 'off' row runs main.
 IF current_setting('transaction_isolation')='read committed' THEN
  SELECT m.gate,m.mode INTO v_gate,v_mode FROM sessions.lazy_ack_t2_mode m WHERE m.singleton;
 ELSE
  SELECT m.gate,m.mode INTO v_gate,v_mode FROM sessions.lazy_ack_t2_mode m WHERE m.singleton FOR SHARE;
 END IF;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: the T2 gate row is missing';END IF;
 IF v_gate<>'off' AND current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: the fenced staged ACK requires read committed';END IF;
 v_t2:=v_gate='fenced' AND v_mode='t2';
 -- 043800: NO KEY UPDATE, not FOR UPDATE. This ACK writes last_seat only (a non-key column), so NO KEY UPDATE
 -- is exactly what its own UPDATE below takes. It still serializes staged ACKs (NO KEY UPDATE vs NO KEY UPDATE)
 -- and is still excluded by set_live/control_changed/lazy_clear (FOR UPDATE) and the FOR SHARE holders, but it
 -- no longer blocks a claim's KEY SHARE: claims read only the write-once identity/epoch columns. Taken before
 -- the loop, as before: the seats are numbered from it, and clock-before-slot keeps ACK slot locks ordered.
 IF NOT v_t2 THEN
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR NO KEY UPDATE;v_last:=c.last_seat;
 ELSE
  -- 062210 t2: KEY SHARE now (compatible with every other ACK and claim); NO KEY UPDATE only for the writes below.
  SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR KEY SHARE;
 END IF;
 IF p->>'buffer_receipt_kind' IS DISTINCT FROM 'staged' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unexpected receipt authority';END IF;
 -- 043800: the same bounded origin advisory xact lock as ingest_lazy_records (same version string, key and seed),
 -- taken after the clock and BEFORE the stage-row lock and every native-slot lock below: fence -> clock ->
 -- origin -> slot, as in the claim. A same-origin claim of another user now queues on this lock instead of
 -- interleaving its slot locks with ours (reversed slot order would be a 40P01 cycle). Single origin per call:
 -- p is the one ingest_batches row of the ingest_receipt_seat_ack trigger, and the slot keys below use
 -- a.(org,workspace,origin), which the commit path (ingest_attempt_epochs) and the a/t checks below pin to it.
 -- So exactly one advisory lock, no multi-origin ordering needed.
 PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',p->>'org_id',p->>'workspace_id',p->>'origin_id')::text,0));
 SELECT i.* INTO t FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents i ON i.stage_token=o.active_stage_token
  WHERE o.org_id=p->>'org_id' AND o.workspace_id=p->>'workspace_id' AND o.user_id=p->>'user_id' AND o.origin_id=p->>'origin_id'
   AND o.batch_id=p->>'batch_id' AND o.digest=p->>'digest' AND o.state='buffered' FOR UPDATE OF i;
 IF NOT FOUND OR t.parent_stage_token IS NOT NULL OR t.receipt_state<>'acknowledged' OR t.cleanup_state<>'live'
  OR t.expires_at<=clock_timestamp() THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: current original stage required';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=t.claim_token;
 IF a.org_id<>t.org_id OR a.workspace_id<>t.workspace_id OR a.user_id<>t.user_id OR a.origin_id<>t.origin_id
  OR a.batch_id<>t.batch_id OR a.digest<>t.digest OR a.reset_epoch<>t.reset_epoch
  OR jsonb_array_length(a.source_processing_ranks)<>jsonb_array_length(a.records) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: original candidate order changed';END IF;
 IF NOT v_t2 THEN
 FOR r IN SELECT q.* FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) q
  ORDER BY (a.source_processing_ranks->>q.ord)::integer LOOP
  IF a.source_processing_ranks->>r.ord IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: source processing rank missing';END IF;
  v_material:=jsonb_build_array('global-native-custody-v1',a.org_id,a.workspace_id,a.origin_id,r.key_material->>7);
  v_key:=encode(sha256(convert_to(v_material::text,'UTF8')),'hex');
  SELECT * INTO slot FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;
  IF NOT FOUND OR slot.key_material IS DISTINCT FROM v_material OR slot.source_identity_sha256<>c.source_identity_sha256
   OR slot.control_epoch<>c.control_epoch THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: initialized native custody slot required';END IF;
  IF slot.current_owner IS NOT NULL AND slot.current_owner<>a.user_id THEN
   RAISE EXCEPTION USING ERRCODE='P0W01',MESSAGE='session ingest: native key currently belongs to another user';END IF;
  v_accept:=slot.current_owner IS NULL OR slot.current_revision<(r.key_material->>8)::bigint;
  v_first:=CASE WHEN v_accept THEN r.candidate_digest ELSE slot.current_digest END;
  -- Every ignored old-source candidate receives a real target order reference;
  -- it never fabricates the source row's publication seat.
  IF NOT v_accept AND slot.current_seat_key IS NOT NULL THEN
   SELECT * INTO STRICT s FROM sessions.ingest_receipt_seats WHERE key_sha256=slot.current_seat_key;
   IF s.first_digest<>v_first THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody seat drift';END IF;
  ELSE
   IF v_last=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK seats exhausted';END IF;
   v_last:=v_last+1;
   INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch)
   VALUES(encode(sha256(convert_to(jsonb_build_array('lazy-ack-seat-v1',r.key_sha256,v_last::text)::text,'UTF8')),'hex'),
    r.key_material,v_last,v_first,r.key_material->>10,r.key_material->>11,r.fence_epoch) RETURNING * INTO s;
  END IF;
  v_new_lineage:=slot.lineage;
  IF v_accept THEN
   IF slot.lineage=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: native lineage exhausted';END IF;
   v_new_lineage:=slot.lineage+1;
   UPDATE sessions.ingest_lazy_native_slots SET lineage=v_new_lineage,current_owner=a.user_id,
    current_source=r.key_material->>10,current_session=r.key_material->>11,current_revision=(r.key_material->>8)::bigint,
    current_digest=r.candidate_digest,current_reset_epoch=r.reset_epoch,current_fence_epoch=r.fence_epoch,
    current_seat_key=s.key_sha256,current_legacy_key=NULL WHERE key_sha256=v_key;
  END IF;
  INSERT INTO sessions.ingest_lazy_ack_decisions(claim_token,ord,native_key_sha256,native_lineage,candidate_key_sha256,candidate_digest,
   first_digest,body_accepted,seat_key_sha256,reset_epoch,fence_epoch,generation,source_control_version,source_witness_sha256)
  VALUES(t.claim_token,r.ord,v_key,v_new_lineage,r.key_sha256,r.candidate_digest,v_first,v_accept,s.key_sha256,r.reset_epoch,r.fence_epoch,
   a.schema_generation,a.source_control_version,a.source_witness_sha256) ON CONFLICT DO NOTHING;
  SELECT * INTO STRICT d FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=t.claim_token AND ord=r.ord;
  IF d.native_key_sha256<>v_key OR d.native_lineage<>v_new_lineage OR d.candidate_key_sha256<>r.key_sha256 OR d.candidate_digest<>r.candidate_digest
   OR d.first_digest<>v_first OR d.body_accepted<>v_accept OR d.seat_key_sha256<>s.key_sha256 OR d.reset_epoch<>r.reset_epoch OR d.fence_epoch<>r.fence_epoch
   OR d.generation<>a.schema_generation OR d.source_control_version<>a.source_control_version OR d.source_witness_sha256<>a.source_witness_sha256 THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: immutable ACK decision adoption refused';END IF;
 END LOOP;
 ELSE
  -- (A) The candidates, once, in the loop's order: rank ascending (NULLS LAST, as ORDER BY), ord breaking ties the
  -- loop leaves unspecified. Native key exactly as the loop builds it.
  SELECT coalesce(jsonb_agg(jsonb_build_object('pos',z.pos,'ord',z.ord,'prank',z.prank,'ckey',z.key_sha256,'km',z.key_material,
    'cd',z.candidate_digest,'re',z.reset_epoch,'fe',z.fence_epoch,'nm',z.nm,'nk',encode(sha256(convert_to(z.nm::text,'UTF8')),'hex')) ORDER BY z.pos),'[]')
   INTO v_recs
   FROM(SELECT q.*,(a.source_processing_ranks->>q.ord)::integer AS prank,
     row_number()OVER(ORDER BY (a.source_processing_ranks->>q.ord)::integer,q.ord)::integer AS pos,
     jsonb_build_array('global-native-custody-v1',a.org_id,a.workspace_id,a.origin_id,q.key_material->>7) AS nm
    FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) q)z;
  -- (B) Every native slot FOR UPDATE in one statement, in the order the loop first locks it (LockRows runs above the
  -- sort). Same lock order as the loop, still under fence -> clock -> origin.
  PERFORM 1 FROM sessions.ingest_lazy_native_slots n
   JOIN(SELECT x.nk,min(x.pos) AS first_pos FROM jsonb_to_recordset(v_recs) x(pos integer,nk text) GROUP BY x.nk)k ON n.key_sha256=k.nk
   ORDER BY k.first_pos FOR UPDATE OF n;
  -- (C) The loop's running state per record. Per slot, in pos order: the revision current before the record is the
  -- (C1) PRE-UPGRADE, under KEY SHARE only: the loop's running state per record, everything that does not depend on
  -- last_seat. Rows read here are protected by locks this ACK holds, not by the clock: the native slots by our FOR UPDATE
  -- (other origins never share a slot key; same-origin writers need our origin lock); the seat at slot.current_seat_key
  -- (seats are insert-only, erase is excluded by our (961996,50000) shared lock); decisions keyed (claim, ord) of our
  -- claim (written only by an ACK of this claim: our stage row and origin lock); write-once clock identity columns
  -- (their writers take the clock FOR UPDATE, excluded by our KEY SHARE). Its own statement after the slot locks, so
  -- READ COMMITTED gives it a snapshot taken after them.
  WITH x AS(SELECT * FROM jsonb_to_recordset(v_recs) x(pos integer,ord integer,prank integer,ckey text,km jsonb,cd text,re bigint,fe bigint,nm jsonb,nk text)),
  l1 AS(SELECT x.*,n.key_sha256 IS NOT NULL AS found,n.key_material AS n_km,n.source_identity_sha256 AS n_si,n.control_epoch AS n_ce,
    n.current_owner AS owner0,n.current_revision AS rev0,n.current_digest AS dig0,n.current_seat_key AS seat0,n.lineage AS lin0,
    (x.km->>8)::bigint AS rev,row_number()OVER w=1 AS first_in_slot,
    max((x.km->>8)::bigint)OVER(w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
   FROM x LEFT JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=x.nk WINDOW w AS(PARTITION BY x.nk ORDER BY x.pos)),
  l2 AS(SELECT l1.*,(l1.owner0 IS NULL AND l1.first_in_slot)
    OR CASE WHEN l1.first_in_slot THEN CASE WHEN l1.owner0 IS NOT NULL THEN l1.rev0 END
     ELSE greatest(CASE WHEN l1.owner0 IS NOT NULL THEN l1.rev0 END,l1.prev_max) END<l1.rev AS accept FROM l1),
  l3 AS(SELECT l2.*,count(*)FILTER(WHERE l2.accept)OVER(w ROWS UNBOUNDED PRECEDING) AS acc_cum,
    max(l2.pos)FILTER(WHERE l2.accept)OVER(w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS last_acc
   FROM l2 WINDOW w AS(PARTITION BY l2.nk ORDER BY l2.pos)),
  l4 AS(SELECT l3.*,l3.accept OR(l3.last_acc IS NULL AND l3.seat0 IS NULL) AS alloc FROM l3),
  l5 AS(SELECT l4.*,count(*)FILTER(WHERE l4.alloc)OVER(ORDER BY l4.pos ROWS UNBOUNDED PRECEDING) AS alloc_cum,l4.lin0::numeric+l4.acc_cum AS lineage_n FROM l4),
  l7 AS(SELECT z.*,CASE WHEN z.accept THEN z.cd WHEN z.last_acc IS NULL THEN z.dig0 ELSE y.cd END AS first_digest
   FROM l5 z LEFT JOIN l5 y ON y.pos=z.last_acc),
  l8 AS(SELECT l7.*,s0.key_sha256 IS NOT NULL AS s0_found,s0.first_digest AS s0_first,d0.claim_token IS NOT NULL AS d_found,
    d0.native_key_sha256 AS d_nk,d0.native_lineage AS d_lin,d0.candidate_key_sha256 AS d_ck,d0.candidate_digest AS d_cd,d0.first_digest AS d_fd,
    d0.body_accepted AS d_acc,d0.seat_key_sha256 AS d_sk,d0.reset_epoch AS d_re,d0.fence_epoch AS d_fe,d0.generation AS d_gen,
    d0.source_control_version AS d_scv,d0.source_witness_sha256 AS d_sw
   FROM l7 LEFT JOIN sessions.ingest_receipt_seats s0 ON s0.key_sha256=l7.seat0
    LEFT JOIN sessions.ingest_lazy_ack_decisions d0 ON d0.claim_token=t.claim_token AND d0.ord=l7.ord)
  SELECT coalesce(jsonb_agg(jsonb_build_object('pos',f.pos,'ord',f.ord,'ckey',f.ckey,'km',f.km,'cd',f.cd,'re',f.re,'fe',f.fe,'nk',f.nk,
    'accept',f.accept,'acc_cum',f.acc_cum,'alloc',f.alloc,'alloc_cum',f.alloc_cum,'last_acc',f.last_acc,'seat0',f.seat0,
    'lineage_n',f.lineage_n,'first_digest',f.first_digest,'d_found',f.d_found,'d_nk',f.d_nk,'d_lin',f.d_lin,'d_ck',f.d_ck,'d_cd',f.d_cd,
    'd_fd',f.d_fd,'d_acc',f.d_acc,'d_sk',f.d_sk,'d_re',f.d_re,'d_fe',f.d_fe,'d_gen',f.d_gen,'d_scv',f.d_scv,'d_sw',f.d_sw,
    -- The loop's checks that do not depend on last_seat, in its order within one record (1..5); 7 = lineage exhaustion.
    'phase_pre',CASE WHEN f.prank IS NULL THEN 1
     WHEN NOT f.found OR f.n_km IS DISTINCT FROM f.nm OR coalesce(f.n_si<>c.source_identity_sha256,false) OR coalesce(f.n_ce<>c.control_epoch,false) THEN 2
     WHEN f.owner0 IS NOT NULL AND f.owner0<>a.user_id THEN 3
     WHEN NOT f.accept AND f.last_acc IS NULL AND f.seat0 IS NOT NULL AND NOT f.s0_found THEN 4
     WHEN NOT f.accept AND f.last_acc IS NULL AND f.seat0 IS NOT NULL AND coalesce(f.s0_first<>f.dig0,false) THEN 5 END,
    'lin_exh',f.accept AND f.lineage_n>9223372036854775807) ORDER BY f.pos),'[]')
   INTO v_plan FROM l8 f;
  -- (UPGRADE) The writes need the clock NO KEY UPDATE, held to commit (seat order = commit order). last_seat comes from
  -- this locking statement itself: under READ COMMITTED a row-locking SELECT that waited re-reads the newest committed
  -- version (never the row variable read under KEY SHARE).
  SELECT ck.last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock ck WHERE ck.singleton FOR NO KEY UPDATE;
  -- (C2) POST-UPGRADE: seat numbers and keys from last_seat, the adoption check, and the loop's first error.
  WITH q AS(SELECT * FROM jsonb_to_recordset(v_plan) q(pos integer,ord integer,ckey text,km jsonb,cd text,re bigint,fe bigint,nk text,
    accept boolean,acc_cum integer,alloc boolean,alloc_cum integer,last_acc integer,seat0 text,lineage_n numeric,first_digest text,
    d_found boolean,d_nk text,d_lin bigint,d_ck text,d_cd text,d_fd text,d_acc boolean,d_sk text,d_re bigint,d_fe bigint,d_gen text,
    d_scv bigint,d_sw text,phase_pre integer,lin_exh boolean)),
  k AS(SELECT q.*,v_last::numeric+q.alloc_cum AS seatnum,
    CASE WHEN q.alloc THEN encode(sha256(convert_to(jsonb_build_array('lazy-ack-seat-v1',q.ckey,(v_last::numeric+q.alloc_cum)::text)::text,'UTF8')),'hex') END AS new_seat_key
   FROM q),
  k2 AS(SELECT z.*,CASE WHEN z.alloc THEN z.new_seat_key WHEN z.last_acc IS NULL THEN z.seat0 ELSE y.new_seat_key END AS seat_key
   FROM k z LEFT JOIN k y ON y.pos=z.last_acc)
  SELECT coalesce(jsonb_agg(jsonb_build_object('pos',f.pos,'ord',f.ord,'ckey',f.ckey,'km',f.km,'cd',f.cd,'re',f.re,'fe',f.fe,'nk',f.nk,
    'accept',f.accept,'acc_cum',f.acc_cum,'alloc',f.alloc,'seatnum',f.seatnum,'lineage_n',f.lineage_n,'new_seat_key',f.new_seat_key,
    'seat_key',f.seat_key,'first_digest',f.first_digest,
    'phase',coalesce(f.phase_pre,CASE WHEN f.alloc AND f.seatnum>9223372036854775807 THEN 6
     WHEN f.lin_exh THEN 7
     WHEN f.d_found AND coalesce(f.d_nk<>f.nk OR f.d_lin<>f.lineage_n OR f.d_ck<>f.ckey OR f.d_cd<>f.cd OR f.d_fd<>f.first_digest
      OR f.d_acc<>f.accept OR f.d_sk<>f.seat_key OR f.d_re<>f.re OR f.d_fe<>f.fe OR f.d_gen<>a.schema_generation
      OR f.d_scv<>a.source_control_version OR f.d_sw<>a.source_witness_sha256,false) THEN 8 END)) ORDER BY f.pos),'[]')
   INTO v_plan FROM k2 f;
  SELECT q.pos,q.phase INTO v_epos,v_ephase FROM jsonb_to_recordset(v_plan) q(pos integer,phase integer) WHERE q.phase IS NOT NULL ORDER BY q.pos LIMIT 1;
  v_lim:=coalesce(v_epos,2147483647);
  -- (D) The writes of every record the loop completes before its first error, in the loop's statement kinds.
  INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch)
  SELECT q.new_seat_key,q.km,q.seatnum::bigint,q.first_digest,q.km->>10,q.km->>11,q.fe
   FROM jsonb_to_recordset(v_plan) q(pos integer,alloc boolean,new_seat_key text,km jsonb,seatnum numeric,first_digest text,fe bigint)
   WHERE q.alloc AND q.pos<v_lim ORDER BY q.pos;
  -- ingest_history_native_changed is a STATEMENT trigger whose invalidations count calls (required_scopes.request_version,
  -- session_replay_epoch_seq). While the history switch is live, one single-row UPDATE per accept in pos order: exactly
  -- the loop's sequence of trigger calls, whatever ingest_history_active() (VOLATILE: build leases, installed scopes)
  -- answers at each of them. The switch is written only by ingest_history_set_live, which takes the clock FOR UPDATE,
  -- so it cannot change while this ACK holds the clock NO KEY UPDATE. Switch off: the trigger returns at its first line,
  -- so one UPDATE per round (round k = the k-th accept of each slot; no slot twice in one statement, per-slot order kept).
  IF sessions.ingest_history_live() THEN
   FOR e IN SELECT q.* FROM jsonb_to_recordset(v_plan) q(pos integer,accept boolean,nk text,km jsonb,cd text,re bigint,fe bigint,lineage_n numeric,seat_key text)
    WHERE q.accept AND q.pos<v_lim ORDER BY q.pos LOOP
    UPDATE sessions.ingest_lazy_native_slots SET lineage=e.lineage_n::bigint,current_owner=a.user_id,
     current_source=e.km->>10,current_session=e.km->>11,current_revision=(e.km->>8)::bigint,
     current_digest=e.cd,current_reset_epoch=e.re,current_fence_epoch=e.fe,
     current_seat_key=e.seat_key,current_legacy_key=NULL WHERE key_sha256=e.nk;
   END LOOP;
  ELSE
   SELECT coalesce(max(q.acc_cum),0) INTO v_rounds FROM jsonb_to_recordset(v_plan) q(pos integer,accept boolean,acc_cum integer) WHERE q.accept AND q.pos<v_lim;
   FOR i IN 1..v_rounds LOOP
    UPDATE sessions.ingest_lazy_native_slots n SET lineage=q.lineage_n::bigint,current_owner=a.user_id,
     current_source=q.km->>10,current_session=q.km->>11,current_revision=(q.km->>8)::bigint,
     current_digest=q.cd,current_reset_epoch=q.re,current_fence_epoch=q.fe,
     current_seat_key=q.seat_key,current_legacy_key=NULL
    FROM jsonb_to_recordset(v_plan) q(pos integer,accept boolean,acc_cum integer,nk text,km jsonb,cd text,re bigint,fe bigint,lineage_n numeric,seat_key text)
    WHERE q.accept AND q.pos<v_lim AND q.acc_cum=i AND n.key_sha256=q.nk;
   END LOOP;
  END IF;
  INSERT INTO sessions.ingest_lazy_ack_decisions(claim_token,ord,native_key_sha256,native_lineage,candidate_key_sha256,candidate_digest,
   first_digest,body_accepted,seat_key_sha256,reset_epoch,fence_epoch,generation,source_control_version,source_witness_sha256)
  SELECT t.claim_token,q.ord,q.nk,q.lineage_n::bigint,q.ckey,q.cd,q.first_digest,q.accept,q.seat_key,q.re,q.fe,
   a.schema_generation,a.source_control_version,a.source_witness_sha256
   FROM jsonb_to_recordset(v_plan) q(pos integer,ord integer,nk text,lineage_n numeric,ckey text,cd text,first_digest text,accept boolean,seat_key text,re bigint,fe bigint)
   WHERE q.pos<v_lim ORDER BY q.pos ON CONFLICT DO NOTHING;
  -- (E) The loop's first error, same SQLSTATE and text.
  IF v_ephase=1 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: source processing rank missing';
  ELSIF v_ephase=2 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: initialized native custody slot required';
  ELSIF v_ephase=3 THEN RAISE EXCEPTION USING ERRCODE='P0W01',MESSAGE='session ingest: native key currently belongs to another user';
  ELSIF v_ephase=4 THEN RAISE EXCEPTION USING ERRCODE='P0002',MESSAGE='query returned no rows';
  ELSIF v_ephase=5 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody seat drift';
  ELSIF v_ephase=6 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK seats exhausted';
  ELSIF v_ephase=7 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: native lineage exhausted';
  ELSIF v_ephase=8 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: immutable ACK decision adoption refused';
  END IF;
  SELECT count(*)::integer INTO v_allocs FROM jsonb_to_recordset(v_plan) q(alloc boolean) WHERE q.alloc;
  v_last:=v_last+v_allocs;
 END IF;
 UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;
 IF t.expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: stage expired while assigning seats';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
END $lazy_ack_batch$;
