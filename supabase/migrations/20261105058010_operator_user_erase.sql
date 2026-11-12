-- Slot 20261105058010. Operator per-user erase of BUFFERED desktop uploads (default off; code only: applying it to
-- any real database needs Khaliq's explicit OK). Design and runbook: docs/operator-user-erase.md.
--
-- A data-subject deletion request covers everything one user uploaded to one workspace. The per-session erase
-- (20261105050000) refuses lazy/buffered orgs (RH011) and needs a session key; a buffered upload that was never
-- published has no session_index row at all. This lane removes such a user's whole ingest footprint:
--
--   operator_user_erase_begin    ledger row, revoke the user's Supabase-held tokens (auth_sessions revoked,
--                                read_api_tokens deleted), fence the user's publication frontiers, capture the
--                                user's origins (the byte eraser derives the S3 machine prefixes from them),
--                                refuse published state
--   operator_user_erase_step     restartable. While the user has lazy slots, one call turns at most p_limit
--                                slots into tombstones and deletes the receipt rows hanging off them (every seat
--                                of every revision). The call after the last slot is the final purge: every other
--                                per-user row of the tables below in ONE transaction, NOT bounded by p_limit (its
--                                size is the user's batches, claims and stages; the caller's statement_timeout
--                                bounds it), then probes assert none is left
--   operator_user_erase_complete the orchestrator reports that a fresh listing found zero object versions; the
--                                probes run again before bytes_verified is recorded
--
-- All are owner-only (no EXECUTE grant): the operator job connects as the schema owner. begin, step and complete
-- take pg_advisory_xact_lock(961996,50000) exclusively, then the user's ingest_fence_owners row FOR UPDATE. Every
-- writer that takes the user's fence row through ingest_lock_fences first INSERTs it, and 20261105050010's statement
-- trigger on ingest_fence_owners takes that advisory lock SHARED for the writer's whole transaction; the buffered GC
-- claim takes it shared too. So an erase transaction and an ingest transaction for any user never overlap, and
-- publication's pointer guard still takes the fence row FOR UPDATE NOWAIT before a session_index row is written.
-- A step refuses to touch a user whose stage still holds a live GC lease (it answers retry_after_ms instead): GC
-- PUTs a redacted child under that lease, and SQL cannot stop that in-flight object write.
--
-- Ingest fence (the upload policy). Deleting a user's rows does not stop a device from uploading again: a delivery
-- from an origin begin never saw would otherwise land after the purge, and complete would still record
-- bytes_verified. This file therefore CREATE OR REPLACEs 20261104030000's sessions.ingest_lock_fences: after it
-- holds the user's fence row FOR UPDATE it refuses with SQLSTATE RH041 while an operator_user_erasures row for
-- (org, workspace, user) has state<>'bytes_verified'. Every ingest admission path (claim, buffered stage prepare,
-- buffered and direct commit, revisions, lazy records/acks/attempts, controls, resets, selections, fan-out,
-- publication and history-plan claims, GC finish, custody, seed) calls it, and begin inserts the ledger row in the
-- same transaction that holds the fence row, so the refusal starts at begin's commit. Uploads for the user are
-- refused from begin until complete records bytes_verified; then the fence lifts and the ledger row stays with
-- origins '{}' (a later upload is a new footprint, erased by a new request). The ingest edge answers 503
-- delivery_unavailable and logs db_error_code RH041; the desktop keeps the batch and retries. The one path that
-- locks the fence row without ingest_lock_fences and could reach this user, the buffered GC claim
-- (20261105050010), is replaced here so it never leases a stage of a user under erase.
--
-- The fence row is the lock anchor and is KEPT while the request is active: the final purge no longer deletes it
-- and the "rows remain" probes do not count it. complete takes the same locks and re-runs the full "rows of the
-- user remain" probe before it records bytes_verified (55000 'operator user erase: rows of the user remain at
-- completion' otherwise); only then, in that same transaction, does it delete the fence row, so a returning user
-- is adopted as a new one (20261105041000 admits an unsealed user only on the insert that creates the row).
--
-- Lazy slots are tombstoned, never deleted (20261104055000: the global native-key initialization persists so a
-- stale source row can never reseed the slot). A tombstone keeps key_sha256, org, workspace, lineage and the hex
-- source identity, control epoch and witness hashes; key_material becomes ["operator-user-erase-tombstone-v1"],
-- origin_id and record_id become '' and every current_* column NULL. A later delivery of the same record from the
-- same origin computes the same key, finds the tombstone, and is refused before it writes anything (55000 'native
-- custody identity changed' in ingest_lazy_records, 'initialized native custody slot required' at the ACK): those
-- record ids from that origin can never be uploaded again. New records, or a new device origin, are unaffected.
--
-- Receipt seats: every seat of the user goes, not only each slot's current one (a new seat is created per accepted
-- revision). Collected from the slots' ACK decisions, the user's claims' decisions and ordinal seats, and seats
-- whose key_material names (org, workspace, user) or (org, workspace, captured origin). The key_material arm and its
-- probe use 058000's expression indexes ingest_receipt_seats_key_material_{user,origin}_idx and
-- ingest_receipt_record_heads_key_material_{user,origin}_idx (keep the predicates in the key_material->>N form).
--
-- Control writers (round 3). Two SECURITY DEFINER functions granted to bulls96_session_ingest write the user's
-- ingest_source_control_heads row without ingest_lock_fences: 20261105041000's ingest_source_control_begin (legacy
-- mirror) and ingest_privacy_authority_begin (Supabase authority). This file CREATE OR REPLACEs both with their exact
-- 20261105041000 bodies plus ONE line before their authority lock: sessions.operator_user_erase_control_gate, which
-- takes (961996,50000) SHARED and refuses 55000 'operator user erase: source controls for this user are refused
-- until the erase is verified' while the user has an active erase. The gate runs before either function writes
-- anything; the rollback restores both bodies byte-exact. A BEFORE INSERT OR UPDATE row trigger on
-- ingest_source_control_heads (operator_user_erase_guard) refuses any other writer of an erased user's head with the
-- same message; it takes NO lock (see the lock order) and the erase itself only DELETEs heads, so it needs no
-- erase-writer marker.
--
-- Lock order (always 50000 -> 96 -> rows; no cycle):
--   erase begin/step/complete   (961996,50000) X, then (1919249529,96) S (step/complete), then ledger/fence rows
--   control begins (both)       (961996,50000) S in the gate, then (1919249529,96) S, then the head row
--   ingest writers              (961996,50000) S (050010's statement triggers), then 96 S if they touch a guarded table
--   authority activation        (1919249529,96) X only; it writes heads but never a 50000-triggered table
--   control completes           (1919249529,96) S, then the head row
-- The heads row trigger must not take 50000: activation holds 96 X while it writes heads, so a 50000 S there would wait
-- on a step holding 50000 X that waits for 96 S: a deadlock. Instead step and complete take 96 S right after 50000 X,
-- so a step never starts while activation runs, and activation never starts while a step holds 96 S.
--
-- Privacy authority. The final purge DELETEs the user's ingest_source_control_heads row, and 20261105041000's statement
-- guard refuses every write to that table while the authority is 'active' unless bulls96.privacy_authority_writer is
-- 'authority'. The purge sets the marker for that one DELETE and restores the previous value right after it: the erase
-- is an authority-class writer of this user's head because it holds (961996,50000) X and (1919249529,96) S (no
-- authority transition is in flight), the user's control begins are gated, and without the delete an active
-- authority would strand the request in 'purging' with the user's uploads refused (RH041) and no cancel.
--
-- Inventory rule. Every table the purge (sessions.operator_user_erase_purge) or a phase-1 step DELETEs from is probed
-- by sessions.operator_user_erase_remaining, through an index (user-erase.pglite.test.ts diffs the two bodies).
-- complete re-runs the purge (heads excepted) under the same locks and the fence row FOR UPDATE before it probes, so a
-- late metadata row (a consent row, a seal, a token) is purged, not stranded. It refuses without purging when an
-- upload root came back (a lazy slot, stage, batch, claim, outbox or publication/plan row: bytes may have landed after
-- the listing) or a control head exists (a head is never dropped silently by complete).
--
-- Seat ambiguity. A seat whose key_material names the user (or a captured origin) but which another user's slot
-- still points to is KEPT (deleting it would break that slot). It is counted in the ledger's retained jsonb, and
-- complete refuses with 55000 'operator user erase: seats naming the user remain referenced by other slots' until an
-- operator repairs it (docs/operator-user-erase.md).
--
CREATE TABLE sessions.operator_user_erasures (
 request_id text PRIMARY KEY,
 org_id text NOT NULL,
 workspace_id text NOT NULL,
 user_id text NOT NULL,
 state text NOT NULL CHECK (state IN ('purging','metadata_purged','bytes_verified')),
 -- Device origins captured at begin; cleared when the bytes are verified gone.
 origins text[] NOT NULL,
 tokens_revoked integer NOT NULL DEFAULT 0,
 deleted jsonb NOT NULL DEFAULT '{}'::jsonb,
 -- Kept material naming the user (never counted as deleted): seats another user's slot still references.
 retained jsonb NOT NULL DEFAULT '{}'::jsonb,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 metadata_purged_at timestamptz,
 bytes_verified_at timestamptz,
 CHECK ((state='purging')=(metadata_purged_at IS NULL)),
 CHECK ((state='bytes_verified')=(bytes_verified_at IS NOT NULL)),
 CHECK (state<>'bytes_verified' OR cardinality(origins)=0)
);
-- One active erase per user; a later request after completion is a new erase.
CREATE UNIQUE INDEX operator_user_erasures_active ON sessions.operator_user_erasures(org_id,workspace_id,user_id)
 WHERE state<>'bytes_verified';
ALTER TABLE sessions.operator_user_erasures ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.operator_user_erasures FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- 20261104030000's body plus the erase check (that file is unchanged; the rollback restores its exact body). The
-- check runs after the row lock, in a fresh read-committed statement snapshot: a writer that waited for begin
-- (advisory lock, then this row) sees begin's committed ledger row. Owner-only, as before; ACL restated unchanged.
CREATE OR REPLACE FUNCTION sessions.ingest_lock_fences(p_org text,p_ws text,p_user text)
RETURNS void LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lock$
BEGIN
 INSERT INTO sessions.ingest_fence_owners VALUES(p_org,p_ws,p_user) ON CONFLICT DO NOTHING;
 PERFORM 1 FROM sessions.ingest_fence_owners AS f
 WHERE f.org_id=p_org AND f.workspace_id=p_ws AND f.user_id=p_user FOR UPDATE;
 IF EXISTS(SELECT 1 FROM sessions.operator_user_erasures AS e
   WHERE e.org_id=p_org AND e.workspace_id=p_ws AND e.user_id=p_user AND e.state<>'bytes_verified') THEN
  RAISE EXCEPTION USING ERRCODE='RH041',
   MESSAGE='operator user erase: uploads for this user are refused until the erase is verified';
 END IF;
END
$lock$;
REVOKE ALL ON FUNCTION sessions.ingest_lock_fences(text,text,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- True while any row of the user remains in a table this lane erases. Not counted: the fence row (the lock anchor,
-- kept until complete deletes it), revoked auth_sessions (kept on purpose, see above), slot tombstones (below), seats
-- another user's slot still references (operator_user_erase_seat_ambiguity) and this ledger. p_origins scopes
-- un-owned lazy slots, the user's batches and origin-named seats. Every table the purge DELETEs is probed here
-- (inventory rule, header); seat-keyed and token-keyed children are probed through their parent's index.
-- plpgsql, not sql: its body is bound at first call, so the file still applies on profiles that skip a table it
-- names (043700's owner_buffered_parity).
CREATE FUNCTION sessions.operator_user_erase_remaining(p_org text,p_ws text,p_user text,p_origins text[]) RETURNS boolean
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $remaining$
DECLARE v_found boolean;
BEGIN
 IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots WHERE org_id=p_org AND workspace_id=p_ws
    AND (current_owner=p_user OR (current_owner IS NULL AND origin_id=ANY(p_origins) AND origin_id<>'')))
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_reconcile_outbox AS r JOIN sessions.ingest_lazy_native_slots AS l
    ON l.key_sha256=r.native_key_sha256 WHERE l.org_id=p_org AND l.workspace_id=p_ws
    AND (l.current_owner=p_user OR (l.current_owner IS NULL AND l.origin_id=ANY(p_origins) AND l.origin_id<>'')))
  OR EXISTS(SELECT 1 FROM sessions.ingest_stage_intents WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_deferrals AS d JOIN sessions.ingest_stage_intents AS si
    ON si.stage_token=d.stage_token WHERE si.org_id=p_org AND si.workspace_id=p_ws AND si.user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_attempts WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_images AS i JOIN sessions.ingest_publication_attempts AS pa
    ON pa.attempt_token=i.attempt_token WHERE pa.org_id=p_org AND pa.workspace_id=p_ws AND pa.user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_plans WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_plan_carriers AS c JOIN sessions.ingest_history_plans AS hp
    ON hp.plan_token=c.plan_token WHERE hp.org_id=p_org AND hp.workspace_id=p_ws AND hp.user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_plan_refs AS r JOIN sessions.ingest_history_plans AS hp
    ON hp.plan_token=r.plan_token WHERE hp.org_id=p_org AND hp.workspace_id=p_ws AND hp.user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_lazy_ack_decisions AS d JOIN sessions.ingest_claim_attempts AS ca
    ON ca.claim_token=d.claim_token WHERE ca.org_id=p_org AND ca.workspace_id=p_ws AND ca.user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_receipt_ord_seats AS o JOIN sessions.ingest_claim_attempts AS ca
    ON ca.claim_token=o.claim_token WHERE ca.org_id=p_org AND ca.workspace_id=p_ws AND ca.user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_claim_sessions WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=p_org AND workspace_id=p_ws AND origin_id=ANY(p_origins)
    AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_session_ownership WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_fence_versions WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.upload_deletions WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.upload_resets WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_frontiers WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.upload_erasure_rearm WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_control_receipts WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_source_control_heads WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_lazy_user_seals WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_seed_fence_baselines WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_tenant_slots WHERE org_id=p_org AND user_id=p_user AND workspace_id=p_ws)
  OR EXISTS(SELECT 1 FROM sessions.owner_buffered_parity WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.upload_consent_policies WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.upload_consent_sessions WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.upload_consent_origins WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user)
  OR EXISTS(SELECT 1 FROM sessions.read_api_tokens WHERE org_id=p_org AND user_id=p_user AND workspace_id=p_ws)
  OR EXISTS(SELECT 1 FROM sessions.auth_sessions WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user
    AND revoked_at IS NULL)
  -- Receipt seats and record heads carry the user's ids in key_material ([.., 3 org, 4 workspace, 5 user,
  -- 6 origin, ..], 20261104053000). Served by 058000's ingest_receipt_seats_key_material_{user,origin}_idx and
  -- ingest_receipt_record_heads_key_material_{user,origin}_idx (one per OR arm, or the shared org/workspace
  -- prefix; keep the key_material->>N form so they match). A seat a surviving slot still points to (another
  -- user's) is not counted here: operator_user_erase_seat_ambiguity counts it and complete refuses on it.
  -- ACK decisions and ordinal seats of those seats: through 058000's seat_key_sha256 / key_sha256 indexes.
  OR EXISTS(SELECT 1 FROM sessions.ingest_receipt_seats AS s WHERE s.key_material->>3=p_org AND s.key_material->>4=p_ws
    AND (s.key_material->>5=p_user OR s.key_material->>6=ANY(p_origins))
    AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots AS l WHERE l.current_seat_key=s.key_sha256))
  OR EXISTS(SELECT 1 FROM sessions.ingest_receipt_seats AS s JOIN sessions.ingest_lazy_ack_decisions AS d
    ON d.seat_key_sha256=s.key_sha256 WHERE s.key_material->>3=p_org AND s.key_material->>4=p_ws
    AND (s.key_material->>5=p_user OR s.key_material->>6=ANY(p_origins))
    AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots AS l WHERE l.current_seat_key=s.key_sha256))
  OR EXISTS(SELECT 1 FROM sessions.ingest_receipt_seats AS s JOIN sessions.ingest_receipt_ord_seats AS o
    ON o.key_sha256=s.key_sha256 WHERE s.key_material->>3=p_org AND s.key_material->>4=p_ws
    AND (s.key_material->>5=p_user OR s.key_material->>6=ANY(p_origins))
    AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots AS l WHERE l.current_seat_key=s.key_sha256))
  OR EXISTS(SELECT 1 FROM sessions.ingest_receipt_record_heads AS h WHERE h.key_material->>3=p_org AND h.key_material->>4=p_ws
    AND (h.key_material->>5=p_user OR h.key_material->>6=ANY(p_origins))) THEN
  RETURN true;
 END IF;
 -- history.probe_connections lives in the history migration track: probed only when present and deletable here
 -- (the purge deletes it under the same condition).
 IF to_regclass('history.probe_connections') IS NOT NULL AND has_table_privilege('history.probe_connections','DELETE') THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM history.probe_connections WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3)'
   INTO v_found USING p_org,p_ws,p_user;
  RETURN v_found;
 END IF;
 RETURN false;
END
$remaining$;

-- Seats whose key_material names the user (or a captured origin) in this workspace but which a surviving slot still
-- points to (another user's slot; identity and reference disagree). They are kept, never counted as deleted, and
-- complete refuses while any exists. Served by 058000's key_material_{user,origin} indexes and the slots'
-- current_seat_key index.
CREATE FUNCTION sessions.operator_user_erase_seat_ambiguity(p_org text,p_ws text,p_user text,p_origins text[]) RETURNS bigint
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $ambiguity$
 SELECT count(*) FROM sessions.ingest_receipt_seats AS s WHERE s.key_material->>3=p_org AND s.key_material->>4=p_ws
  AND (s.key_material->>5=p_user OR s.key_material->>6=ANY(p_origins))
  AND EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots AS l WHERE l.current_seat_key=s.key_sha256);
$ambiguity$;

-- Adds per-table delete counts into the ledger row.
CREATE FUNCTION sessions.operator_user_erase_count(p_request text, p_counts jsonb) RETURNS void
LANGUAGE sql VOLATILE SET search_path=pg_catalog,pg_temp AS $count$
 UPDATE sessions.operator_user_erasures AS e
 SET deleted=(SELECT coalesce(jsonb_object_agg(k, coalesce((e.deleted->>k)::bigint,0)+coalesce((p_counts->>k)::bigint,0)),'{}'::jsonb)
              FROM (SELECT jsonb_object_keys(e.deleted) UNION SELECT jsonb_object_keys(p_counts)) AS keys(k)),
     updated_at=clock_timestamp()
 WHERE e.request_id=p_request;
$count$;

CREATE FUNCTION sessions.operator_user_erase_render(p_request text) RETURNS jsonb
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $render$
 SELECT jsonb_build_object('request_id',e.request_id,'state',e.state,'origins',to_jsonb(e.origins),
   'tokens_revoked',e.tokens_revoked,'deleted',e.deleted,'retained',e.retained,'done',e.state<>'purging')
 FROM sessions.operator_user_erasures AS e WHERE e.request_id=p_request;
$render$;

CREATE FUNCTION sessions.operator_user_erase_begin(p_request text,p_org text,p_ws text,p_user text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $begin$
DECLARE v_org text;v_ws text;v_user text;v_request text;e sessions.operator_user_erasures%ROWTYPE;
 v_origins text[];v_revoked integer;v_frontiers bigint;v_read_tokens bigint;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='operator user erase: read-committed transaction required';END IF;
 v_request:=sessions.ingest_identifier(to_jsonb(p_request),false);
 v_org:=sessions.ingest_identifier(to_jsonb(p_org),false);
 v_ws:=sessions.ingest_identifier(to_jsonb(p_ws),true);
 v_user:=sessions.ingest_identifier(to_jsonb(p_user),false);
 PERFORM pg_advisory_xact_lock(961996,50000);

 SELECT * INTO e FROM sessions.operator_user_erasures WHERE request_id=v_request FOR UPDATE;
 IF FOUND THEN
  IF (e.org_id,e.workspace_id,e.user_id) IS DISTINCT FROM (v_org,v_ws,v_user) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator user erase: request id belongs to another user';END IF;
  RETURN sessions.operator_user_erase_render(v_request);
 END IF;
 IF EXISTS(SELECT 1 FROM sessions.operator_user_erasures WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
   AND state<>'bytes_verified') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: another request is active for this user';END IF;

 -- The user's fence row (the buffered GC and publication's pointer guard take it), then the publication fence: a
 -- publication or history-plan claim needs the user's frontier row FOR SHARE, so this DELETE waits out an
 -- in-flight claim, and the probe below (a fresh read-committed snapshot) sees whatever it committed.
 -- The row is created when absent (a user who never uploaded): it is the anchor every later writer waits on.
 INSERT INTO sessions.ingest_fence_owners VALUES(v_org,v_ws,v_user) ON CONFLICT DO NOTHING;
 PERFORM 1 FROM sessions.ingest_fence_owners WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user FOR UPDATE;
 DELETE FROM sessions.ingest_publication_frontiers WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 GET DIAGNOSTICS v_frontiers=ROW_COUNT;

 -- Published or canonical state is the per-session erase's (search documents, canonical bytes). Same probe in step.
 IF EXISTS(SELECT 1 FROM sessions.session_index WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_generations WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_custody_records WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_owned_sessions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_records WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_record_revisions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_scope_owners WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_privacy_outbox WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_plans WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
    AND state='published') THEN
  RAISE EXCEPTION USING ERRCODE='RH040',
   MESSAGE='operator user erase: the user has published sessions; erase them with the per-session operator erase first';
 END IF;

 SELECT coalesce(array_agg(DISTINCT o ORDER BY o),'{}') INTO v_origins FROM (
  SELECT origin_id FROM sessions.ingest_batches WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.ingest_claim_attempts WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.ingest_stage_intents WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.ingest_buffer_outbox WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.ingest_session_ownership WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.upload_deletions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.ingest_lazy_native_slots WHERE org_id=v_org AND workspace_id=v_ws AND current_owner=v_user
 ) AS u(o);
 -- Un-owned slots are erased by origin, so an origin must be this user's alone (a shared device origin would let
 -- them belong to someone else). Never observed in production; refused rather than guessed.
 IF EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=v_org AND workspace_id=v_ws AND origin_id=ANY(v_origins) AND user_id<>v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots WHERE org_id=v_org AND workspace_id=v_ws AND origin_id=ANY(v_origins)
    AND current_owner IS NOT NULL AND current_owner<>v_user) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: a device origin of this user is shared with another user';
 END IF;

 -- The ledger row is written in THIS transaction, which holds the fence row: from its commit on, every ingest
 -- writer of the user that takes the fence row sees it and is refused (RH041).
 -- Revoke the tokens THIS database holds: auth_sessions (revoked, rows kept: see the header) and the session read
 -- API's read_api_tokens (deleted: that table has no revocation column and no mirror). relayhistory-cloud's Neon
 -- auth_sessions is a separate store this function cannot reach; the runbook revokes it BEFORE begin, and until
 -- then a Neon-held token still works against relayhistory-cloud and its mirror can still add rows here.
 UPDATE sessions.auth_sessions SET revoked_at=clock_timestamp(),revoked_reason='operator_user_erase',updated_at=clock_timestamp()
 WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user AND revoked_at IS NULL;
 GET DIAGNOSTICS v_revoked=ROW_COUNT;
 DELETE FROM sessions.read_api_tokens WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 GET DIAGNOSTICS v_read_tokens=ROW_COUNT;

 INSERT INTO sessions.operator_user_erasures(request_id,org_id,workspace_id,user_id,state,origins,tokens_revoked,deleted)
 VALUES(v_request,v_org,v_ws,v_user,'purging',v_origins,v_revoked,
  jsonb_build_object('ingest_publication_frontiers',v_frontiers,'read_api_tokens',v_read_tokens));
 RETURN sessions.operator_user_erase_render(v_request);
END
$begin$;

-- The final purge (phase 2 of step; complete re-runs it with p_heads=false): every per-user row of the tables below
-- in foreign-key order. Idempotent: a second run deletes only rows that landed since. The caller holds
-- (961996,50000) X, (1919249529,96) S and the user's fence row FOR UPDATE. Returns the per-table counts, the
-- history.probe_connections outcome and whether a seat of the user's claims survives with no slot pointing to it.
CREATE FUNCTION sessions.operator_user_erase_purge(p_org text,p_ws text,p_user text,p_origins text[],p_heads boolean) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $purge$
DECLARE v_seats text[];v_claims uuid[];v_stages uuid[];v_n bigint;v_c jsonb:='{}'::jsonb;v_round integer:=0;
 v_probe text:='absent';v_prev text;
BEGIN
 SELECT coalesce(array_agg(claim_token),'{}') INTO v_claims FROM sessions.ingest_claim_attempts
  WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 SELECT coalesce(array_agg(stage_token),'{}') INTO v_stages FROM sessions.ingest_stage_intents
  WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 -- Every remaining seat of the user: those the user's claims' ACK decisions and ordinal seats reference, and those
 -- whose key_material names the user, or one of the user's origins, in this workspace (a seat whose decisions were
 -- already cleaned up). Record heads and ordinal seats go first, then every such seat nothing else references; a
 -- seat a surviving slot points to (another user's) stays. The key_material arms here and in the record-heads
 -- DELETE below are served by 058000's key_material_{user,origin} indexes on both tables.
 SELECT coalesce(array_agg(DISTINCT x),'{}') INTO v_seats FROM (
  SELECT seat_key_sha256 FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=ANY(v_claims)
  UNION SELECT key_sha256 FROM sessions.ingest_receipt_ord_seats WHERE claim_token=ANY(v_claims)
  UNION SELECT key_sha256 FROM sessions.ingest_receipt_seats WHERE key_material->>3=p_org AND key_material->>4=p_ws
   AND (key_material->>5=p_user OR key_material->>6=ANY(p_origins))) AS u(x)
  WHERE NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots AS l WHERE l.current_seat_key=x);
 DELETE FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=ANY(v_claims) OR seat_key_sha256=ANY(v_seats);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_lazy_ack_decisions',v_n);
 DELETE FROM sessions.ingest_receipt_record_heads WHERE winner_key_sha256=ANY(v_seats)
  OR (key_material->>3=p_org AND key_material->>4=p_ws
   AND (key_material->>5=p_user OR key_material->>6=ANY(p_origins)));
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_receipt_record_heads',v_n);
 DELETE FROM sessions.ingest_receipt_ord_seats WHERE claim_token=ANY(v_claims) OR key_sha256=ANY(v_seats);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_receipt_ord_seats',v_n);
 DELETE FROM sessions.ingest_receipt_seats AS s WHERE s.key_sha256=ANY(v_seats)
  AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots AS l WHERE l.current_seat_key=s.key_sha256)
  AND NOT EXISTS(SELECT 1 FROM sessions.ingest_receipt_record_heads AS h WHERE h.winner_key_sha256=s.key_sha256)
  AND NOT EXISTS(SELECT 1 FROM sessions.ingest_receipt_ord_seats AS o WHERE o.key_sha256=s.key_sha256)
  AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_ack_decisions AS d WHERE d.seat_key_sha256=s.key_sha256);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_receipt_seats',v_n);
 DELETE FROM sessions.ingest_publication_deferrals WHERE stage_token=ANY(v_stages);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_publication_deferrals',v_n);
 DELETE FROM sessions.ingest_publication_images WHERE attempt_token IN
  (SELECT attempt_token FROM sessions.ingest_publication_attempts WHERE stage_token=ANY(v_stages));
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_publication_images',v_n);
 DELETE FROM sessions.ingest_publication_attempts WHERE stage_token=ANY(v_stages);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_publication_attempts',v_n);
 DELETE FROM sessions.ingest_history_plan_refs WHERE plan_token IN
  (SELECT plan_token FROM sessions.ingest_history_plans WHERE stage_token=ANY(v_stages));
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_history_plan_refs',v_n);
 DELETE FROM sessions.ingest_history_plan_carriers WHERE plan_token IN
  (SELECT plan_token FROM sessions.ingest_history_plans WHERE stage_token=ANY(v_stages));
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_history_plan_carriers',v_n);
 DELETE FROM sessions.ingest_history_plans WHERE stage_token=ANY(v_stages);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_history_plans',v_n);
 DELETE FROM sessions.ingest_buffer_outbox WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_buffer_outbox',v_n);
 -- Stage intents reference each other (parent, GC replacement; RESTRICT): leaves first.
 LOOP
  DELETE FROM sessions.ingest_stage_intents AS si WHERE si.stage_token=ANY(v_stages)
   AND NOT EXISTS(SELECT 1 FROM sessions.ingest_stage_intents AS c WHERE c.stage_token<>si.stage_token
     AND (c.parent_stage_token=si.stage_token OR c.gc_replacement_stage_token=si.stage_token));
  GET DIAGNOSTICS v_n=ROW_COUNT;
  v_c:=v_c||jsonb_build_object('ingest_stage_intents',coalesce((v_c->>'ingest_stage_intents')::bigint,0)+v_n);
  v_round:=v_round+1;
  EXIT WHEN v_n=0 OR v_round>=10000;
 END LOOP;
 -- Frontiers the external writer registered since begin; after the stage rows, the claims' own lock order.
 DELETE FROM sessions.ingest_publication_frontiers WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_publication_frontiers',v_n);
 DELETE FROM sessions.ingest_claim_sessions WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_claim_sessions',v_n);
 DELETE FROM sessions.ingest_claim_attempts WHERE claim_token=ANY(v_claims);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_claim_attempts',v_n);
 DELETE FROM sessions.ingest_batches WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_batches',v_n);
 DELETE FROM sessions.ingest_session_ownership WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_session_ownership',v_n);
 DELETE FROM sessions.ingest_fence_versions WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_fence_versions',v_n);
 DELETE FROM sessions.upload_deletions WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('upload_deletions',v_n);
 DELETE FROM sessions.upload_resets WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('upload_resets',v_n);
 DELETE FROM sessions.upload_erasure_rearm WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('upload_erasure_rearm',v_n);
 DELETE FROM sessions.ingest_control_receipts WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_control_receipts',v_n);
 -- The user's control head: only from step (complete refuses on a head instead, header). The erase is an
 -- authority-class writer of this row (header): 20261105041000's statement guard refuses this DELETE while the
 -- authority is 'active' unless the writer marker is set, so it is set for this one statement and restored right
 -- after (on an error the transaction aborts and the transaction-local value goes with it).
 IF p_heads THEN
  v_prev:=current_setting('bulls96.privacy_authority_writer',true);
  PERFORM set_config('bulls96.privacy_authority_writer','authority',true);
  DELETE FROM sessions.ingest_source_control_heads WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
  GET DIAGNOSTICS v_n=ROW_COUNT;
  PERFORM set_config('bulls96.privacy_authority_writer',coalesce(v_prev,''),true);
  v_c:=v_c||jsonb_build_object('ingest_source_control_heads',v_n);
 END IF;
 DELETE FROM sessions.ingest_lazy_user_seals WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_lazy_user_seals',v_n);
 DELETE FROM sessions.ingest_seed_fence_baselines WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_seed_fence_baselines',v_n);
 DELETE FROM sessions.ingest_tenant_slots WHERE org_id=p_org AND user_id=p_user AND workspace_id=p_ws;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_tenant_slots',v_n);
 DELETE FROM sessions.owner_buffered_parity WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('owner_buffered_parity',v_n);
 -- 20261105057010 upload consent: with no policy and no confirmed origin the receiver holds (fails closed).
 -- Containment holds (upload_consent_holds) are operator records and stay.
 DELETE FROM sessions.upload_consent_sessions WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('upload_consent_sessions',v_n);
 DELETE FROM sessions.upload_consent_origins WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('upload_consent_origins',v_n);
 DELETE FROM sessions.upload_consent_policies WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('upload_consent_policies',v_n);
 -- ingest_fence_owners is NOT deleted: it is the lock anchor the ingest fence and complete take (header).
 -- Tokens: revoke (never delete) any auth session that arrived since begin; read API tokens again.
 UPDATE sessions.auth_sessions SET revoked_at=clock_timestamp(),revoked_reason='operator_user_erase',updated_at=clock_timestamp()
 WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user AND revoked_at IS NULL;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('auth_sessions_revoked_late',v_n);
 DELETE FROM sessions.read_api_tokens WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('read_api_tokens',v_n);
 -- history.probe_connections lives in the history migration track: only when present and deletable here.
 IF to_regclass('history.probe_connections') IS NOT NULL THEN
  IF has_table_privilege('history.probe_connections','DELETE') THEN
   EXECUTE 'DELETE FROM history.probe_connections WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3'
    USING p_org,p_ws,p_user;
   GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('history.probe_connections',v_n);v_probe:='erased';
  ELSE v_probe:='not_permitted';END IF;
 END IF;
 RETURN jsonb_build_object('counts',v_c,'probe',v_probe,'seats_left',
  EXISTS(SELECT 1 FROM sessions.ingest_receipt_seats AS s WHERE s.key_sha256=ANY(v_seats)
   AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots AS l WHERE l.current_seat_key=s.key_sha256)));
END
$purge$;

CREATE FUNCTION sessions.operator_user_erase_step(p_request text,p_limit integer) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $step$
DECLARE e sessions.operator_user_erasures%ROWTYPE;v_lease timestamptz;v_keys text[];v_seats text[];
 v_n bigint;v_c jsonb:='{}'::jsonb;v_r jsonb;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='operator user erase: read-committed transaction required';END IF;
 IF p_limit IS NULL OR p_limit<1 OR p_limit>20000 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator user erase: limit must be 1..20000';END IF;
 PERFORM pg_advisory_xact_lock(961996,50000);
 -- Then the privacy-authority key shared (lock order 50000 -> 96, header): no authority transition runs meanwhile.
 PERFORM pg_advisory_xact_lock_shared(1919249529,96);
 SELECT * INTO e FROM sessions.operator_user_erasures
  WHERE request_id=sessions.ingest_identifier(to_jsonb(p_request),false) FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator user erase: unknown request';END IF;
 IF e.state<>'purging' THEN RETURN sessions.operator_user_erase_render(e.request_id);END IF;

 -- The user's fence row first, as the buffered GC does. A live GC lease may still PUT a redacted child object.
 PERFORM 1 FROM sessions.ingest_fence_owners WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id FOR UPDATE;
 -- A publication that committed after begin: refuse (RH040 rolls this step back; the request stays 'purging').
 IF EXISTS(SELECT 1 FROM sessions.session_index WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_generations WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_custody_records WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_owned_sessions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_records WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_record_revisions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_scope_owners WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_privacy_outbox WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_plans WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id
    AND state='published') THEN
  RAISE EXCEPTION USING ERRCODE='RH040',
   MESSAGE='operator user erase: the user has published sessions; erase them with the per-session operator erase first';
 END IF;
 -- begin's shared-origin refusal, again: another user may have started uploading from a captured origin since
 -- (that user is not fenced). Their rows are never touched; the request stays 'purging'.
 IF EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND origin_id=ANY(e.origins)
    AND user_id<>e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots WHERE org_id=e.org_id AND workspace_id=e.workspace_id
    AND origin_id=ANY(e.origins) AND current_owner IS NOT NULL AND current_owner<>e.user_id) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: a device origin of this user is shared with another user';
 END IF;
 SELECT max(gc_lease_until) INTO v_lease FROM sessions.ingest_stage_intents
  WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id AND gc_lease_until>clock_timestamp();
 IF v_lease IS NOT NULL THEN
  RETURN sessions.operator_user_erase_render(e.request_id)||jsonb_build_object('phase','gc_lease_active',
   'retry_after_ms',ceil(extract(epoch FROM v_lease-clock_timestamp())*1000)::bigint+1000);
 END IF;

 -- Phase 1: one batch of the user's lazy slots and the receipt rows hanging off them. A slot is never deleted
 -- (20261104055000: the global native-key initialization persists so a stale source row can never reseed it): it
 -- becomes a TOMBSTONE that keeps key_sha256, org, workspace, lineage and the hex identity/witness hashes, and loses
 -- everything naming the user, the device or the record (key_material, origin_id, record_id, current_*). A replay of
 -- the same record from the same origin finds the slot, sees a different key_material and is refused (55000 'native
 -- custody identity changed', or 'initialized native custody slot required' at the ACK) before it writes anything.
 SELECT array_agg(key_sha256),array_agg(DISTINCT current_seat_key) FILTER (WHERE current_seat_key IS NOT NULL)
 INTO v_keys,v_seats FROM (
  SELECT key_sha256,current_seat_key FROM sessions.ingest_lazy_native_slots
  WHERE org_id=e.org_id AND workspace_id=e.workspace_id
    AND (current_owner=e.user_id OR (current_owner IS NULL AND origin_id=ANY(e.origins) AND origin_id<>''))
  LIMIT p_limit) AS s;
 IF v_keys IS NOT NULL THEN
  -- EVERY seat of these slots, not only the current one: a new seat is created per accepted revision, and the
  -- ACK decisions of earlier revisions still reference theirs. A seat a slot outside this batch references (another
  -- user's; never expected) stays with its rows. A seat shared with a later batch of this user goes with it.
  SELECT coalesce(array_agg(x),'{}') INTO v_seats FROM (
   SELECT unnest(coalesce(v_seats,'{}'))
   UNION SELECT seat_key_sha256 FROM sessions.ingest_lazy_ack_decisions WHERE native_key_sha256=ANY(v_keys)) AS u(x)
   WHERE NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots AS l WHERE l.current_seat_key=x AND l.key_sha256<>ALL(v_keys));
  DELETE FROM sessions.ingest_lazy_ack_decisions WHERE native_key_sha256=ANY(v_keys) OR seat_key_sha256=ANY(v_seats);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_lazy_ack_decisions',v_n);
  UPDATE sessions.ingest_lazy_native_slots SET key_material='["operator-user-erase-tombstone-v1"]'::jsonb,origin_id='',record_id='',
   current_owner=NULL,current_source=NULL,current_session=NULL,current_revision=NULL,current_digest=NULL,
   current_reset_epoch=NULL,current_fence_epoch=NULL,current_seat_key=NULL,current_legacy_key=NULL
  WHERE key_sha256=ANY(v_keys);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_lazy_native_slots_tombstoned',v_n);
  -- After the UPDATE: 20261105026000's slot trigger may enqueue a reconcile request (it carries the owner).
  DELETE FROM sessions.ingest_history_reconcile_outbox WHERE native_key_sha256=ANY(v_keys);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_history_reconcile_outbox',v_n);
  DELETE FROM sessions.ingest_receipt_record_heads WHERE key_sha256=ANY(v_keys) OR winner_key_sha256=ANY(v_seats);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_receipt_record_heads',v_n);
  DELETE FROM sessions.ingest_receipt_ord_seats WHERE key_sha256=ANY(v_seats) OR candidate_key_sha256=ANY(v_keys);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_receipt_ord_seats',v_n);
  DELETE FROM sessions.ingest_receipt_seats AS s WHERE s.key_sha256=ANY(v_seats)
   AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots AS l WHERE l.current_seat_key=s.key_sha256)
   AND NOT EXISTS(SELECT 1 FROM sessions.ingest_receipt_record_heads AS h WHERE h.winner_key_sha256=s.key_sha256)
   AND NOT EXISTS(SELECT 1 FROM sessions.ingest_receipt_ord_seats AS o WHERE o.key_sha256=s.key_sha256)
   AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_ack_decisions AS d WHERE d.seat_key_sha256=s.key_sha256);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_receipt_seats',v_n);
  PERFORM sessions.operator_user_erase_count(e.request_id,v_c);
  RETURN sessions.operator_user_erase_render(e.request_id)||jsonb_build_object('phase','slots');
 END IF;

 -- Phase 2 (the final purge, one transaction, not bounded by p_limit): everything else of the user, in
 -- foreign-key order (sessions.operator_user_erase_purge), then prove nothing is left.
 v_r:=sessions.operator_user_erase_purge(e.org_id,e.workspace_id,e.user_id,e.origins,true);
 v_c:=v_r->'counts';

 -- A publication these deletes waited for may have committed: the published-state probe once more, fresh snapshot.
 IF EXISTS(SELECT 1 FROM sessions.session_index WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_generations WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_custody_records WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_owned_sessions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_records WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_record_revisions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_scope_owners WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_privacy_outbox WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id) THEN
  RAISE EXCEPTION USING ERRCODE='RH040',
   MESSAGE='operator user erase: the user has published sessions; erase them with the per-session operator erase first';
 END IF;
 -- Nothing of the user may remain in any table this lane owns (the fence row and slot tombstones excepted), and no
 -- seat the user's claims referenced may survive unless a surviving slot still points to it.
 IF sessions.operator_user_erase_remaining(e.org_id,e.workspace_id,e.user_id,e.origins) OR (v_r->>'seats_left')::boolean THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: rows of the user remain after the purge';
 END IF;

 PERFORM sessions.operator_user_erase_count(e.request_id,v_c);
 -- Seats kept because another user's slot points to them: recorded (not as deleted); complete refuses on them.
 UPDATE sessions.operator_user_erasures SET state='metadata_purged',metadata_purged_at=clock_timestamp(),updated_at=clock_timestamp(),
  retained=jsonb_build_object('user_seats_referenced_by_other_slots',
   sessions.operator_user_erase_seat_ambiguity(e.org_id,e.workspace_id,e.user_id,e.origins))
 WHERE request_id=e.request_id;
 RETURN sessions.operator_user_erase_render(e.request_id)||jsonb_build_object('phase','purged','probe_connections',v_r->>'probe');
END
$step$;

-- The orchestrator calls this only after a fresh, strict listing found zero versions and zero delete markers under
-- every machine prefix of the captured origins. Same locks as step. An upload root of the user that came back (a lazy
-- slot, stage, batch, claim, outbox, publication attempt or history plan: bytes may have landed after that listing)
-- refuses at once. Otherwise the final purge runs again (control heads excepted: a head refuses below), which also
-- revokes late auth_sessions and deletes late read_api_tokens and frontiers; then the full "rows of the user remain"
-- probe: anything left refuses the completion (55000) and the request stays metadata_purged. A seat naming the user
-- that another user's slot references refuses with its own 55000 message. Otherwise the fence row is deleted, the
-- origins are cleared (the ledger keeps counts only) and the ingest fence lifts.
CREATE FUNCTION sessions.operator_user_erase_complete(p_request text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $complete$
DECLARE e sessions.operator_user_erasures%ROWTYPE;v_n bigint;v_c jsonb;v_r jsonb;v_ambiguous bigint;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='operator user erase: read-committed transaction required';END IF;
 PERFORM pg_advisory_xact_lock(961996,50000);
 PERFORM pg_advisory_xact_lock_shared(1919249529,96);
 SELECT * INTO e FROM sessions.operator_user_erasures
  WHERE request_id=sessions.ingest_identifier(to_jsonb(p_request),false) FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator user erase: unknown request';END IF;
 IF e.state='purging' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: metadata purge is not finished';END IF;
 IF e.state='metadata_purged' THEN
  PERFORM 1 FROM sessions.ingest_fence_owners WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id FOR UPDATE;
  IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots WHERE org_id=e.org_id AND workspace_id=e.workspace_id
     AND (current_owner=e.user_id OR (current_owner IS NULL AND origin_id=ANY(e.origins) AND origin_id<>'')))
   OR EXISTS(SELECT 1 FROM sessions.ingest_stage_intents WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
   OR EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
   OR EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
   OR EXISTS(SELECT 1 FROM sessions.ingest_claim_sessions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
   OR EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
   OR EXISTS(SELECT 1 FROM sessions.ingest_publication_attempts WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
   OR EXISTS(SELECT 1 FROM sessions.ingest_history_plans WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: rows of the user remain at completion';END IF;
  v_r:=sessions.operator_user_erase_purge(e.org_id,e.workspace_id,e.user_id,e.origins,false);
  v_c:=v_r->'counts';
  IF sessions.operator_user_erase_remaining(e.org_id,e.workspace_id,e.user_id,e.origins) OR (v_r->>'seats_left')::boolean THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: rows of the user remain at completion';END IF;
  v_ambiguous:=sessions.operator_user_erase_seat_ambiguity(e.org_id,e.workspace_id,e.user_id,e.origins);
  IF v_ambiguous>0 THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: seats naming the user remain referenced by other slots',
    DETAIL=format('%s seat(s); the ledger''s retained.user_seats_referenced_by_other_slots records the count',v_ambiguous);
  END IF;
  -- The fence row goes with the transition, inside the transaction that holds it: a returning user is adopted
  -- like a new one (20261105041000's ingest_lazy_user admits an unsealed user only on the INSERT that creates
  -- the fence row). A writer queued on the row sees it gone, is not refused, and fails that adoption check
  -- (55000, nothing written); its retry creates a fresh row.
  DELETE FROM sessions.ingest_fence_owners WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_fence_owners',v_n);
  PERFORM sessions.operator_user_erase_count(e.request_id,v_c);
  UPDATE sessions.operator_user_erasures SET state='bytes_verified',bytes_verified_at=clock_timestamp(),origins='{}',
   retained=jsonb_build_object('user_seats_referenced_by_other_slots',0),updated_at=clock_timestamp() WHERE request_id=e.request_id;
 END IF;
 RETURN sessions.operator_user_erase_render(e.request_id);
END
$complete$;

-- 20261105050010's buffered GC claim plus one candidate filter: never lease a stage of a user with an active erase
-- (the claim takes the user's fence row directly, not through ingest_lock_fences). Otherwise unchanged; the
-- rollback restores 20261105050010's body.
CREATE OR REPLACE FUNCTION sessions.ingest_buffer_gc_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE v_limit int;v_generation text;candidate record;s sessions.ingest_stage_intents%ROWTYPE;
 child sessions.ingest_stage_intents%ROWTYPE;parent sessions.ingest_stage_intents%ROWTYPE;
 v_keep jsonb;v_ordinals jsonb;v_phase text;v_ticket jsonb;v_tickets jsonb:='[]';v_has_outbox boolean;v_abort boolean;v_token uuid;v_outbox_version bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM pg_advisory_xact_lock_shared(961996,50000);
 PERFORM sessions.ingest_exact_keys(p,ARRAY['limit','generation']);
 v_generation:=sessions.ingest_digest(p->'generation');
 -- Marker first: metadata operations never wait for it while holding user/table
 -- locks. The migrator's PENDING update waits for this short operation to finish.
 PERFORM sessions.ingest_require_generation(v_generation);
 v_limit:=sessions.ingest_bounded(p->'limit',1,100);
 FOR candidate IN SELECT stage_token,org_id,workspace_id,user_id,origin_id,batch_id
  FROM sessions.ingest_stage_intents
  WHERE cleanup_state<>'redacted' AND (gc_lease_until IS NULL OR gc_lease_until<=clock_timestamp())
   AND NOT EXISTS(SELECT 1 FROM sessions.operator_user_erasures AS e WHERE e.org_id=ingest_stage_intents.org_id
    AND e.workspace_id=ingest_stage_intents.workspace_id AND e.user_id=ingest_stage_intents.user_id AND e.state<>'bytes_verified')
  ORDER BY coalesce(gc_checked_at,created_at),stage_token LIMIT 4
 LOOP
  -- GC-only admission hint; legacy writers do not take this advisory lock.
  -- The existing USER row lock below remains the authority shared with them.
  -- An acquisition-only subtransaction releases EVERY lock on a busy skip.
  -- No retention error, timeout, generation failure or mutation is swallowed.
  BEGIN
   IF NOT pg_try_advisory_xact_lock(hashtextextended(jsonb_build_array(
      '96bulls:buffer-gc-user-v1',candidate.org_id,candidate.workspace_id,candidate.user_id)::text,0)) THEN
    RAISE lock_not_available USING MESSAGE='buffer gc: user busy';
   END IF;
   PERFORM 1 FROM sessions.ingest_fence_owners WHERE org_id=candidate.org_id
    AND workspace_id=candidate.workspace_id AND user_id=candidate.user_id FOR UPDATE NOWAIT;
   IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: user fence missing';
   END IF;
   PERFORM 1 FROM sessions.ingest_batches WHERE org_id=candidate.org_id AND workspace_id=candidate.workspace_id
    AND origin_id=candidate.origin_id AND batch_id=candidate.batch_id FOR UPDATE NOWAIT;
   IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: batch missing';
   END IF;
   SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token=candidate.stage_token FOR UPDATE NOWAIT;
   IF NOT FOUND OR s.cleanup_state='redacted' OR s.gc_lease_until>clock_timestamp() THEN
    RAISE lock_not_available USING MESSAGE='buffer gc: candidate changed';
   END IF;
   PERFORM 1 FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id
    AND origin_id=s.origin_id AND batch_id=s.batch_id FOR UPDATE NOWAIT;
  EXCEPTION WHEN lock_not_available THEN
   CONTINUE;
  END;
  IF ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id) IS DISTINCT FROM
    ROW(candidate.org_id,candidate.workspace_id,candidate.user_id,candidate.origin_id,candidate.batch_id)
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: identity changed';END IF;
  -- A prospective child cannot be independently aborted during its parent's copy.
  IF s.parent_stage_token IS NOT NULL THEN
   SELECT * INTO parent FROM sessions.ingest_stage_intents WHERE stage_token=s.parent_stage_token;
   -- Prospective bytes have a deterministic key. They remain owned through
   -- repeated parent crashes; retiring them merely because a lease expired would
   -- make that same replacement key permanently unusable after its barrier.
   IF parent.gc_replacement_stage_token=s.stage_token AND parent.cleanup_state='live' AND parent.receipt_state='acknowledged'
    AND ROW(parent.org_id,parent.workspace_id,parent.user_id,parent.origin_id,parent.batch_id,parent.claim_token,parent.digest)
      IS NOT DISTINCT FROM ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.claim_token,s.digest)
    AND EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=parent.org_id AND o.workspace_id=parent.workspace_id
      AND o.user_id=parent.user_id AND o.origin_id=parent.origin_id AND o.batch_id=parent.batch_id AND o.digest=parent.digest
      AND o.state='buffered' AND o.active_stage_token=parent.stage_token)
   THEN
    UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
    EXIT;
   END IF;
  END IF;
  PERFORM 1 FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id
   AND origin_id=s.origin_id AND batch_id=s.batch_id FOR UPDATE;
  SELECT row_version INTO v_outbox_version FROM sessions.ingest_buffer_outbox WHERE active_stage_token=s.stage_token;
  v_has_outbox:=FOUND;
  v_abort:=false;
  IF s.cleanup_state='retired' THEN
   IF v_has_outbox THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: current object cannot be cleaned';END IF;
   v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
  ELSE
   v_ordinals:=sessions.ingest_buffer_gc_ordinals(s.stage_token);
   SELECT coalesce(jsonb_agg(idx-1 ORDER BY idx),'[]') INTO v_keep
    FROM jsonb_array_elements(s.group_ordinals) WITH ORDINALITY g(value,idx)
    WHERE EXISTS(SELECT 1 FROM jsonb_array_elements(value)o WHERE v_ordinals @> jsonb_build_array(o));
   IF s.receipt_state='preparing' THEN
    IF v_has_outbox THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: preparing current object';END IF;
    v_abort:=s.expires_at<=clock_timestamp() OR jsonb_array_length(v_ordinals)<>(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o);
    IF NOT v_abort THEN
     UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
     EXIT;
    END IF;
    v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
   ELSE
    IF NOT v_has_outbox THEN
     -- Acknowledged but unreferenced intent is a known retired object.
     v_abort:=true;v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
    ELSIF jsonb_array_length(v_ordinals)=(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o) THEN
     UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
     EXIT;
    ELSE v_phase:='plan';END IF;
   END IF;
  END IF;
  -- A stale prospective child is never adopted into another keep set.
  IF s.gc_replacement_stage_token IS NOT NULL THEN
   SELECT * INTO child FROM sessions.ingest_stage_intents WHERE stage_token=s.gc_replacement_stage_token FOR UPDATE;
   IF NOT FOUND OR child.cleanup_state<>'live' OR child.group_ordinals IS DISTINCT FROM
     sessions.ingest_buffer_gc_groups(s.group_ordinals,v_ordinals)
   THEN
    IF FOUND AND NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox WHERE active_stage_token=child.stage_token) THEN
     UPDATE sessions.ingest_stage_intents SET cleanup_state='retired',row_version=row_version+1,
      gc_lease_token=NULL,gc_lease_until=NULL,gc_lease_version=NULL WHERE stage_token=child.stage_token;
    END IF;
    s.gc_replacement_stage_token:=NULL;
   END IF;
  END IF;
  v_token:=gen_random_uuid();
  UPDATE sessions.ingest_stage_intents SET gc_generation=v_generation,gc_lease_token=v_token,gc_lease_until=clock_timestamp()+interval '5 minutes',
   row_version=row_version+1,gc_lease_version=row_version+1,gc_checked_at=clock_timestamp(),gc_keep_groups=v_keep,gc_keep_ordinals=v_ordinals,
   gc_replacement_stage_token=s.gc_replacement_stage_token,gc_outbox_version=v_outbox_version,
   cleanup_state=CASE WHEN v_abort THEN 'retired' ELSE cleanup_state END
   WHERE stage_token=s.stage_token RETURNING * INTO s;
  v_ticket:=jsonb_build_object('stageToken',s.stage_token,'claimToken',s.claim_token,'leaseToken',v_token,
   'rowVersion',s.row_version::text,'key',s.stage_key,'sha256',s.stage_sha256,'bytes',s.stage_bytes,
   'groupOrdinals',s.group_ordinals,'keepGroupIndexes',v_keep,'keepOrdinals',v_ordinals,'receiptState',s.receipt_state,'phase',v_phase,
   'resetEpoch',s.reset_epoch::text,'cleanupState',s.cleanup_state);
  IF s.gc_replacement_stage_token IS NOT NULL THEN
   SELECT * INTO child FROM sessions.ingest_stage_intents WHERE stage_token=s.gc_replacement_stage_token;
   v_ticket:=v_ticket||jsonb_build_object('replacement',jsonb_build_object('stageToken',child.stage_token,
    'key',child.stage_key,'sha256',child.stage_sha256,'bytes',child.stage_bytes,'groupOrdinals',child.group_ordinals));
  END IF;
  v_tickets:=v_tickets||jsonb_build_array(v_ticket);
  EXIT; -- At most ONE fully evaluated candidate per transaction, including healthy rotations.
 END LOOP;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_tickets)t JOIN sessions.ingest_stage_intents i
   ON i.stage_token=(t->>'stageToken')::uuid WHERE i.gc_lease_token IS DISTINCT FROM (t->>'leaseToken')::uuid
    OR i.gc_lease_until<=clock_timestamp())
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: claim lease expired';END IF;
 IF octet_length(v_tickets::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='buffer gc: ticket byte bound';END IF;
 RETURN jsonb_build_object('tickets',v_tickets);
END $claim$;
-- The source-control gate (header): the user's control begins call it before their authority lock. Read committed (the
-- ledger check below must see a committed begin), then the erase lock SHARED (it waits out an erase transaction; lock
-- order 50000 -> 96), then the refusal while the user has an active erase. Not SECURITY DEFINER: it runs inside the
-- SECURITY DEFINER begins, as their owner. Owner-only.
CREATE FUNCTION sessions.operator_user_erase_control_gate(p_org text,p_ws text,p_user text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $gate$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: privacy authority requires read committed';END IF;
 PERFORM pg_advisory_xact_lock_shared(961996,50000);
 IF EXISTS(SELECT 1 FROM sessions.operator_user_erasures AS e
   WHERE e.org_id=p_org AND e.workspace_id=p_ws AND e.user_id=p_user AND e.state<>'bytes_verified') THEN
  RAISE EXCEPTION USING ERRCODE='55000',
   MESSAGE='operator user erase: source controls for this user are refused until the erase is verified';
 END IF;
END
$gate$;

-- 20261105041000's Supabase-first begin, exact body plus the gate line before its authority lock (the rollback restores
-- the exact body). CREATE OR REPLACE keeps its owner and ACL.
CREATE OR REPLACE FUNCTION sessions.ingest_privacy_authority_begin(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $authority_begin$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;h sessions.ingest_source_control_heads%ROWTYPE;
 v_org text;v_ws text;v_user text;v_kind text;v_epoch text;v_payload jsonb;d text;fresh boolean;v_deleted bigint;s jsonb;
BEGIN
 -- The writer marker the statement guard checks: transaction-local, set here (a function-level SET of a custom
 -- parameter needs superuser on PG15+) and cleared before every normal return. It is not a trust boundary.
 PERFORM set_config('bulls96.privacy_authority_writer','authority',true);
 PERFORM sessions.ingest_exact_keys(p,ARRAY['epoch','tenant','kind','payload']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);v_kind:=p->>'kind';v_epoch:=p->>'epoch';v_payload:=p->'payload';
 IF v_kind IS NULL OR v_kind NOT IN('selection','reset') OR jsonb_typeof(v_payload) IS DISTINCT FROM 'object' OR octet_length(v_payload::text)>2113536 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid control';END IF;
 IF v_kind='selection' THEN
  PERFORM sessions.ingest_exact_keys(v_payload,ARRAY['origin_id','sessions','enabled']);
  PERFORM sessions.ingest_identifier(v_payload->'origin_id',false);
  IF jsonb_typeof(v_payload->'enabled') IS DISTINCT FROM 'boolean' OR jsonb_typeof(v_payload->'sessions') IS DISTINCT FROM 'array'
   OR jsonb_array_length(v_payload->'sessions') NOT BETWEEN 1 AND 2048 THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid selection';END IF;
  FOR s IN SELECT value FROM jsonb_array_elements(v_payload->'sessions') LOOP
   PERFORM sessions.ingest_exact_keys(s,ARRAY['source','session_id']);
   PERFORM sessions.ingest_identifier(s->'source',false);PERFORM sessions.ingest_identifier(s->'session_id',false);
  END LOOP;
 ELSE
  PERFORM sessions.ingest_exact_keys(v_payload,ARRAY['request_id']);PERFORM sessions.ingest_identifier(v_payload->'request_id',false);
 END IF;
 PERFORM sessions.operator_user_erase_control_gate(v_org,v_ws,v_user);
 PERFORM sessions.ingest_privacy_authority_lock(false);
 pol:=sessions.ingest_frozen_source_policy();
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_privacy_authority WHERE state='active' AND control_epoch=pol.control_epoch) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: supabase privacy authority not active';END IF;
 IF v_epoch IS DISTINCT FROM pol.control_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control epoch';END IF;
 d:=encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',v_epoch,v_org,v_ws,v_user,v_kind,v_payload)::text,'UTF8')),'hex');
 INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
  VALUES(v_org,v_ws,v_user,v_epoch,1,d,v_kind,v_payload,true) ON CONFLICT DO NOTHING RETURNING true INTO fresh;
 SELECT * INTO STRICT h FROM sessions.ingest_source_control_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user FOR UPDATE;
 IF coalesce(fresh,false) THEN
  -- PRE before the apply below can change it (Neon persists PRE before selection/reset create fences).
  UPDATE sessions.ingest_source_control_heads SET before_privacy=sessions.ingest_frozen_source_privacy(v_org,v_ws,v_user),privacy_version=0
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user RETURNING * INTO h;
 END IF;
 IF h.epoch IS DISTINCT FROM v_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control epoch';END IF;
 IF NOT coalesce(fresh,false) THEN
  IF h.pending THEN
   IF h.digest IS DISTINCT FROM d OR h.kind IS DISTINCT FROM v_kind THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control pending';END IF;
   PERFORM set_config('bulls96.privacy_authority_writer','',true);
   RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,
    'deleted',h.deleted::text,'apply',false);
  END IF;
  IF h.version=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control version exhausted';END IF;
  UPDATE sessions.ingest_source_control_heads SET version=h.version+1,digest=d,kind=v_kind,payload=v_payload,pending=true,
   before_privacy=NULL,privacy_version=h.version,deleted=NULL,updated_at=clock_timestamp()
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user RETURNING * INTO h;
 END IF;
 v_deleted:=sessions.ingest_privacy_authority_apply(v_org,v_ws,v_user,v_kind,v_payload);
 IF v_kind='reset' THEN
  UPDATE sessions.ingest_source_control_heads SET deleted=v_deleted WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 END IF;
 PERFORM set_config('bulls96.privacy_authority_writer','',true);
 RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,
  'deleted',v_deleted::text,'apply',true);
END $authority_begin$;

-- 20261105041000's legacy mirror begin, exact body plus the gate line before its authority lock (the rollback restores
-- the exact body). CREATE OR REPLACE keeps its owner and ACL.
CREATE OR REPLACE FUNCTION sessions.ingest_source_control_begin(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $control_begin$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;h sessions.ingest_source_control_heads%ROWTYPE;
 v_org text;v_ws text;v_user text;v_kind text;v_epoch text;d text;fresh boolean;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['epoch','tenant','kind','payload']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);v_kind:=p->>'kind';v_epoch:=p->>'epoch';
 IF v_kind IS NULL OR v_kind NOT IN('selection','reset') OR p->'payload' IS NULL OR jsonb_typeof(p->'payload')='null'
  OR octet_length((p->'payload')::text)>2113536 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid control';END IF;
 PERFORM sessions.operator_user_erase_control_gate(v_org,v_ws,v_user);
 PERFORM sessions.ingest_privacy_authority_lock(false);
 IF EXISTS(SELECT 1 FROM sessions.ingest_privacy_authority) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: privacy authority moved; legacy control refused';END IF;
 pol:=sessions.ingest_frozen_source_policy();
 IF v_epoch IS DISTINCT FROM pol.control_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control epoch';END IF;
 d:=encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',v_epoch,v_org,v_ws,v_user,v_kind,p->'payload')::text,'UTF8')),'hex');
 INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
  VALUES(v_org,v_ws,v_user,v_epoch,1,d,v_kind,p->'payload',true) ON CONFLICT DO NOTHING RETURNING true INTO fresh;
 SELECT * INTO STRICT h FROM sessions.ingest_source_control_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user FOR UPDATE;
 IF coalesce(fresh,false) THEN
  UPDATE sessions.ingest_source_control_heads SET before_privacy=sessions.ingest_frozen_source_privacy(v_org,v_ws,v_user),privacy_version=0
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user RETURNING * INTO h;
 END IF;
 IF h.epoch IS DISTINCT FROM v_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control epoch';END IF;
 IF NOT coalesce(fresh,false) THEN
  IF h.pending THEN
   IF h.digest IS DISTINCT FROM d OR h.kind IS DISTINCT FROM v_kind THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control pending';END IF;
   RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,'apply',false);
  END IF;
  IF h.version=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control version exhausted';END IF;
  UPDATE sessions.ingest_source_control_heads SET version=h.version+1,digest=d,kind=v_kind,payload=p->'payload',pending=true,
   before_privacy=NULL,privacy_version=h.version,updated_at=clock_timestamp()
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user RETURNING * INTO h;
 END IF;
 RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,'apply',true);
END $control_begin$;

-- Any other writer of an erased user's control head (header). Row level and lock-free on purpose: activation writes
-- heads under (1919249529,96) X, and a 50000 lock here would invert the lock order. The begins are serialized by the
-- gate; this trigger is the backstop (a replica-mode write bypasses it, and complete refuses on a head).
CREATE FUNCTION sessions.operator_user_erase_heads_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $heads_guard$
BEGIN
 IF EXISTS(SELECT 1 FROM sessions.operator_user_erasures AS e
   WHERE e.org_id=NEW.org_id AND e.workspace_id=NEW.workspace_id AND e.user_id=NEW.user_id AND e.state<>'bytes_verified') THEN
  RAISE EXCEPTION USING ERRCODE='55000',
   MESSAGE='operator user erase: source controls for this user are refused until the erase is verified';
 END IF;
 RETURN NEW;
END
$heads_guard$;
CREATE TRIGGER operator_user_erase_guard BEFORE INSERT OR UPDATE ON sessions.ingest_source_control_heads
 FOR EACH ROW EXECUTE FUNCTION sessions.operator_user_erase_heads_guard();

REVOKE ALL ON FUNCTION sessions.operator_user_erase_count(text,jsonb),sessions.operator_user_erase_render(text),
 sessions.operator_user_erase_remaining(text,text,text,text[]),
 sessions.operator_user_erase_seat_ambiguity(text,text,text,text[]),
 sessions.operator_user_erase_purge(text,text,text,text[],boolean),
 sessions.operator_user_erase_control_gate(text,text,text),sessions.operator_user_erase_heads_guard(),
 sessions.operator_user_erase_begin(text,text,text,text),sessions.operator_user_erase_step(text,integer),
 sessions.operator_user_erase_complete(text) FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
