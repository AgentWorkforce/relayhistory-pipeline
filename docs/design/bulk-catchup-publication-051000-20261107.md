# Bulk catch-up publication path (slot 051000) - design v1

Status: DESIGN ONLY. Slot 051000 is RESERVED for this lane (registry v3, 2026-11-07 entry). No migration
file, code, grant, flag flip, capture, ceremony, apply or deploy is authorized by this document.
Base: origin/main fc10bb03 (030000-043000 contiguous), v3 attester design FINAL sha256 51d358b2...
(owner verdict PASS WITH CONDITIONS SF1-SF8 / S1-S7), 049000 design packet on 47425aca.

## 1. Summary

The v3 attester publishes one candidate per supervised 300 s window (~288/day). The backlog is
~234,237 buffered stages growing ~3.5k/h. Reading the claim RPC on main
(`sessions.ingest_publication_claim`, 20261104060000) shows that a valid frontier row for
(org, workspace, user, origin) already authorizes claiming ANY eligible buffered stage of that
user/origin until `valid_until`; the "1 candidate" limit is a v3 canary choice, not a protocol limit.
The catch-up path therefore does NOT add a new frontier writer or relax any proof. It adds:

1. a scheduler (051000 tables + SECURITY DEFINER RPCs) that leases whole-user "lanes" to a bounded
   pool of publisher workers, in fair order, and opens one "wave" per freshly attested frontier;
2. an attended ceremony driver that runs the UNCHANGED v3 sequence (capture -> independent verifier ->
   exact-receipt reviewer approval -> one 050000 attest RPC) for many lanes in parallel;
3. a `--catchup` mode of the existing default-off publication runtime that drains every eligible stage
   of a leased lane through the UNCHANGED claim/prepare/register/publish RPCs while the frontier is valid.

Key finding (section 8): with the unchanged 300 s TTL and per-write reviewer approval, per-window yield
is ~23 stages per lane, so draining 234k in ~22 h needs ~610 attested windows/hour. That is only
feasible if the "assigned independent reviewer" approval step can run at machine speed (Q1) or the
049000 TTL is amended (Q2). Neither is assumed; both are open questions for the lead.

## 2. Goals / non-goals
Goals: G1 drain every eligible stage of a lane inside its frontier window (k>=1 in flight); G2 bounded
parallel leases (lane lease table, TTL, server-enforced max concurrency); G3 fair order by user/scope
(least-served, then oldest pending); G4 whole-user B1 gate enforced by the 050000 attest RPC; G5 resumable,
idempotent, crash-safe, no double publication (inherited claim/publish CAS); G6 > 3.5k/h with margin and
explicit arithmetic; G7 own default-off flag + server settings gate + per-user enrollment; G8 DB-enforced
handoff with the v3 manual attester; G9 counters, lag metric, park-never-publish dead-letter.
Non-goals: N1 no new frontier writer, codec, verifier, approval semantics, TTL, or LOGIN role/secret; N2 no
change to `ingest_publication_frontiers` (B4 eleven-column pin) or any 060000 body; N3 no unattended frontier
renewal (every window is a fresh full ceremony); N4 no recovery-import implementation; N5 no 049000 plans for
waves in v1 (Q5); N6 no production census, capture, or ETA claim (planning arithmetic only).

## 3. Data model for 051000 (proposal; not frozen DDL)

Stem (registry convention: date prefix 20261105 + slot):
`supabase/migrations/20261105051000_publication_catchup.sql` and
`supabase/rollbacks/20261105051000_publication_catchup.down.sql`. Lands only after 050000 (itself gated
on 049000 code and the contiguous 044000-049000 prefix). Transactional only; no CONCURRENTLY index.
### 3.1 `sessions.ingest_catchup_settings` (singleton)
- `id boolean PRIMARY KEY DEFAULT true CHECK(id)`
- `enabled boolean NOT NULL DEFAULT false` (server-side second gate)
- `max_active_lanes integer NOT NULL DEFAULT 1 CHECK(1..64)`
- `per_lane_concurrency integer NOT NULL DEFAULT 1 CHECK(1..4)` (values >1 gated on Q4 proof)
- `lane_lease_ms integer NOT NULL DEFAULT 90000 CHECK(30000..120000)`
- `min_admission_remaining_ms integer NOT NULL DEFAULT 150000 CHECK(>=150000)` (v3 SF5 constant)
- `park_after_deferrals integer NOT NULL DEFAULT 8 CHECK(2..50)`
- `row_version bigint NOT NULL DEFAULT 1`
Changes only through owner RPC `ingest_catchup_settings_v1`, each appending an event.
### 3.2 `sessions.ingest_catchup_lanes` (one row per user; mutable, owner-enrolled)
- PK `(org_id, workspace_id, user_id)` - the v3 B1 key (whole user, all origins).
- `state text CHECK IN ('enrolled','attesting','publishing','idle','held','released')`
- `held_reason text NULL CHECK IN ('verifier_fail','unmeasured','approval_refused','window_unusable',
   'park_threshold','privacy_invalidated','generation_stranded','owner_hold')` (bounded codes only)
- `wave_id uuid NULL` (current wave), `lease_token uuid NULL`, `lease_owner text NULL CHECK(len 1..64)`
  (worker label, observational only - not authority), `lease_until timestamptz NULL`,
  `CHECK((lease_token IS NULL)=(lease_until IS NULL))`
- fairness: `served_waves bigint NOT NULL DEFAULT 0`, `last_served_at timestamptz NULL`
- checkpoint (observational): `published_stages bigint`, `deferred_stages bigint`, `parked_stages bigint`,
  `last_published_at timestamptz` - never used as authority (truth is outbox/attempt state).
- custody cutoff (16.1): `cohort_cutoff_at timestamptz NOT NULL` (DB clock at enrollment, immutable),
  `cohort_state text CHECK IN ('open','proof_pending','proven','held')` - set 'proven' only by owner RPC 16.1.
- `enrolled_at`, `row_version bigint NOT NULL DEFAULT 1`. No consumer-specific columns (16.4).
- FK-free by design to `ingest_fence_owners` (enroll reads it noncreating, A1); no FK to frontiers (B4).
### 3.3 `sessions.ingest_catchup_waves` (one row per attested window; insert + guarded close)
- PK `wave_id uuid`; lane key columns + FK to lanes ON DELETE RESTRICT
- `approval_receipt_sha256 text NOT NULL UNIQUE CHECK(hex64)` - the exact 050000 approval consumed
- `frontier_image_sha256 text NULL CHECK(hex64)` - set by the 051000 audit trigger when attest commits
- `schema_generation text NOT NULL CHECK(hex64)`, `valid_until timestamptz NULL` (copied for display only)
- `planner_image_sha256 text NOT NULL CHECK(hex64)` (exact planner/frontier image bound by the approval, 16.3),
  `prefix_root_sha256 text NULL CHECK(hex64)` (resumable prefix proof at close, 16.3)
- `state text CHECK IN ('bound','open','closed','expired','aborted')`
- counters: `admitted`, `published`, `deferred`, `parked` (bigint, observational)
- `bound_at`, `opened_at`, `closed_at`
- Partial UNIQUE `(org_id,workspace_id,user_id) WHERE state IN ('bound','open')` - at most one live wave/user.
### 3.4 `sessions.ingest_catchup_stage_outcomes` (append-only, idempotency ledger)
- PK `(wave_id, stage_token)`; `stage_token uuid REFERENCES sessions.ingest_stage_intents ON DELETE RESTRICT`;
  `origin_seq bigint NOT NULL` (position in the (origin, outbox created_at, batch_id COLLATE "C") order;
  UNIQUE `(wave_id, origin, origin_seq)`), `in_cohort boolean NOT NULL` (outbox created_at <= cutoff)
- `outcome text CHECK IN ('published','replayed','deferred','parked')`, `code text CHECK (bounded enum:
  '40001','55P03','55000','57014','40P01','publication_rpc_invalid','publication_job_bounds',
  'publication_aborted','image_hash_mismatch','cross_scope_move')`, `at timestamptz`
- No payloads, no record IDs, no keys. Doubles as the "exactly once per wave" record guard.
### 3.5 `sessions.ingest_catchup_parked` (dead letter; insert + owner-only resolution)
- PK `stage_token` FK stage_intents ON DELETE RESTRICT; lane key; `reason` (same enum);
  `deferrals integer`, `first_at`, `last_at`, `resolution text CHECK IN ('open','owner_released')`
  DEFAULT 'open', `resolved_at`. Release requires owner RPC + appended event; parked rows are never deleted.
### 3.6 `sessions.ingest_catchup_events` (append-only audit, B4 pattern)
- `id bigint GENERATED ALWAYS AS IDENTITY PK`, `at timestamptz DEFAULT clock_timestamp()`,
  lane key (nullable for settings events), `wave_id uuid NULL`,
  `kind text CHECK IN ('settings','enrolled','lease_acquired','lease_renewed','lease_released',
   'lease_expired','wave_bound','wave_opened','wave_closed','wave_expired','lane_held','lane_resumed',
   'lane_idle','lane_released','park_released')`, `code text NULL` (bounded), `counts jsonb NULL`
  (CHECK object of small integers only). Per-stage outcomes live in 3.4, not here (storage budget).
### 3.7 Indexes
- lanes: `(state, served_waves, last_served_at, org_id, workspace_id, user_id)` partial
  `WHERE state IN ('enrolled','idle')` (ceremony fair pick);
  `(state, lease_until)` partial `WHERE state='publishing'` (worker lease pick).
- waves: partial unique above; `(state, valid_until)`.
- outcomes: PK suffices; `(stage_token)` for parity checks.
- parked: `(org_id, workspace_id, user_id) WHERE resolution='open'`.
- events: `(org_id, workspace_id, user_id, id)`.
All created inside the migration transaction (tables are new and empty; no CONCURRENTLY needed).
### 3.8 RLS, grants, append-only guards
- ENABLE + FORCE ROW LEVEL SECURITY on all six tables; NO policies.
- `REVOKE ALL ... FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest,
  bulls96_session_ingest_seeder, bulls96_read_api, bulls96_publication_frontier_attester`;
  no column/sequence/TRUNCATE/MAINTAIN rights. Access ONLY through SECURITY DEFINER RPCs (049000 req 5/6).
- events, stage_outcomes: BEFORE UPDATE/DELETE row trigger RAISE 55000 + BEFORE TRUNCATE statement guard
  (v3 B4). waves/parked: UPDATE trigger allows only the state/counter/resolution transitions listed in
  section 4 (enforced by a guard comparing OLD/NEW immutable columns), DELETE/TRUNCATE refused.
- Functions: fixed `search_path=pg_catalog,pg_temp`, fully qualified objects, VOLATILE, closed input
  shape via `sessions.ingest_exact_keys`, `sessions.ingest_require_role`, `sessions.ingest_require_generation`.
- Runtime RPC EXECUTE -> `bulls96_session_ingest` only. Owner RPC EXECUTE -> the v3 NOLOGIN attester role
  only (no new role; Q6). Internal helpers ungranted. All registered via a new LaneManifest
  (`scripts/sessions-function-boundary/lanes/`, prefix `sessions.ingest_catchup_`), generated ACL entries,
  migration-sentinels.ts, migrate-guards.ts before/after checks (v3 C2 pattern).

## 4. Lease and scheduling algorithm
### 4.1 Lane state machine
```
enrolled --(owner wave_plan picks lane)--> attesting --(attest commits; audit trigger opens wave)--> publishing
publishing --(frontier remaining < min_admission and no stage leases)--> idle   [wave closed/expired]
idle --(wave_plan, pending stages > 0)--> attesting
attesting --(verifier FAIL/UNMEASURED/approval refused/L too high)--> held
any --(owner hold | privacy invalidation observed | generation stranded)--> held
held --(owner resume, fresh evidence)--> enrolled
idle --(owner release, zero pending)--> released   [v3 manual attester owns the user again]
```
### 4.2 Runtime RPC sketches (role `bulls96_session_ingest`, SECURITY DEFINER)
```sql
-- 1. Lease up to n lanes with an OPEN wave and >= min_admission remaining. Never touches fences.
CREATE FUNCTION sessions.ingest_catchup_lease_v1(p jsonb) RETURNS jsonb LANGUAGE plpgsql VOLATILE
SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$ BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');        -- + exact_keys, require_generation
 SELECT * INTO s FROM sessions.ingest_catchup_settings FOR UPDATE;      -- serializes lease admission
 IF NOT s.enabled THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='catchup: disabled'; END IF;
 v_n:=least(bounded(p->'limit',1,8), s.max_active_lanes - <count of publishing lanes with live lease>);
 FOR r IN SELECT l.* FROM sessions.ingest_catchup_lanes l
   JOIN sessions.ingest_catchup_waves w ON w.wave_id=l.wave_id AND w.state='open'
   JOIN sessions.ingest_publication_frontiers f USING(org_id,workspace_id,user_id)   -- plain read, no lock
  WHERE l.state='publishing' AND (l.lease_until IS NULL OR l.lease_until<=clock_timestamp())
   AND f.schema_generation=p->>'generation'       -- + frontier image = wave image (16.3)
   AND f.valid_until-clock_timestamp()>=make_interval(secs=>s.min_admission_remaining_ms/1000.0)
  ORDER BY l.served_waves, w.opened_at, l.org_id COLLATE "C", l.workspace_id COLLATE "C", l.user_id COLLATE "C"
  LIMIT greatest(v_n,0) FOR UPDATE OF l SKIP LOCKED
 LOOP  -- new lease_token, lease_until=now+lane_lease_ms, row_version+1, 'lease_acquired' event
 END LOOP; RETURN jsonb_build_object('lanes',out);
END $$;
```
```sql
-- 2. Next stages for a leased lane: same predicates as ingest_publication_candidates (060000),
--    restricted to the lane user and origins whose frontier has >= min_admission remaining,
--    excluding parked stages and stages already recorded for this wave. Read-only.
sessions.ingest_catchup_next_v1({generation, leaseToken, limit<=per_lane_concurrency, keyRoot, bucket, region})
  -> {stageTokens:[...], remainingMs}
   refuses 40001 if lease_token mismatch or lease expired; 55000 if wave not open / settings disabled.
-- 3. Record an outcome (idempotent): INSERT ... ON CONFLICT (wave_id,stage_token) DO NOTHING;
--    'parked' also inserts ingest_catchup_parked; counters bumped only when the insert happened.
sessions.ingest_catchup_record_v1({generation, leaseToken, stageToken, outcome, code})
-- 4. Renew (heartbeat every lane_lease_ms/3): extends lease_until only if lease_token matches and
--    the wave is still open; never past the frontier valid_until.
sessions.ingest_catchup_renew_v1({generation, leaseToken})
-- 5. Release: clears the lease; if remaining < min_admission and no stage of the lane has a live
--    gc_lease_until, closes the wave (closed/expired) and moves lane to idle; bumps served_waves.
sessions.ingest_catchup_release_v1({generation, leaseToken})
```
### 4.3 Owner RPCs (role `bulls96_publication_frontier_attester`, attended driver only)
- `ingest_catchup_enroll_v1({org,ws,user})`: SELECT existing `ingest_fence_owners` row (no lock needed,
  noncreating per A1; missing -> 55000, zero mutation); refuses if a 049000 scoped plan admission is active
  for the user (Q5 dependency); inserts lane `enrolled` + event. Enrollment is not B1 proof.
- `ingest_catchup_wave_plan_v1({limit})`: picks `enrolled|idle` lanes with pending eligible stages in fair
  order (`served_waves, oldest pending outbox created_at, key COLLATE "C"`), FOR UPDATE SKIP LOCKED, moves
  them to `attesting`; returns lane keys only.
- `ingest_catchup_bind_wave_v1({lane, approvalReceiptSha256, generation})`: called AFTER the reviewer
  has written the exact approval into the 050000 approval ledger and BEFORE the attest RPC; verifies the
  approval row exists, is unconsumed and names this user; inserts wave `bound`. Does not consume it.
- `ingest_catchup_hold_v1`, `ingest_catchup_resume_v1`, `ingest_catchup_release_lane_v1`,
  `ingest_catchup_release_parked_v1`, `ingest_catchup_settings_v1`: guarded transitions + events.
- `ingest_catchup_census_v1`: READ ONLY aggregate (section 12), no user IDs.
### 4.4 Attest-to-wave binding (new 051000-owned trigger; no 050000 body change, SF6)
`AFTER INSERT ON <050000 attestation audit table> FOR EACH ROW` (exact table/column names pinned when
050000 lands; Q7). Runs inside the attest RPC's single transaction after v3 has locked the existing user
row and the frontier (A1 order), then:
- if action is revoke/emergency revoke or privacy invalidation: do nothing except, for a laned user,
  mark the open wave `aborted` and lane `held('privacy_invalidated')` - NEVER refuse (A8: must not block
  a legitimate revocation or privacy operation);
- if action is attest/renew and the user has a lane in `attesting|publishing`: require a `bound` wave whose
  `approval_receipt_sha256` equals the audit row's consumed approval; else RAISE 55000 (manual ceremony
  refused while catch-up owns the user). On match: wave -> `open`, store frontier image hash, lane ->
  `publishing`;
- if the user has no lane or the lane is `released|held|enrolled|idle`: no effect (manual v3 path untouched,
  except `idle`/`enrolled` laned users: refuse manual attest too, see section 10.1).
Lock order: user row -> frontier -> lane -> wave. Catch-up runtime/owner RPCs lock settings -> lane -> wave
only and never lock fences or frontiers, so no cycle exists. The claim RPC (user fence -> batch -> intent ->
frontier FOR SHARE) never touches catch-up tables.
### 4.5 Worker loop (`publication-main.ts --catchup`, existing runtime, role bulls96_session_ingest)
```
assert INGEST_CATCHUP_PUBLICATION_ENABLED == 'true' (exact) and generation/verifier assets as today
loop until SIGTERM or budget:
  lanes = rpc.catchup.lease(limit = slots_free)
  for each lane (bounded pool, <= per_lane_concurrency in flight per lane):
    while (tokens = rpc.catchup.next(lane)).length:
      for token in tokens (parallel <= k):
        try   r = publishBufferedStage(deps, token, {enabled:true, maxJobMs:120000})   # UNCHANGED path
              record(lane, token, r.replay ? 'replayed' : 'published')
        catch code in transient(40001,55P03,55000,57014,40P01): rpc.defer(token, code)   # existing table
              record(lane, token, deferrals >= park_after ? 'parked' : 'deferred', code)
        catch fail-closed code (publication_rpc_invalid, image_hash_mismatch, ...): record 'parked'
      renew(lane) on schedule; stop admitting when next() returns empty or remainingMs < 150 s
    release(lane)
```
`maxJobMs=120000` and `remaining >= 150 s` are the v3 SF5 canary constants applied to every catch-up stage.

## 5. Per-user recovery import gate (B1) and ordering
- Gate owner: the 050000 attest RPC. It already refuses unless the independent verifier PASS receipt binds a
  whole-user (all origins) recovery-completion component with VERIFIED_COMPLETE/VERIFIED_EMPTY + terminal
  proof, including the mandatory drained_pre_switch_ACK component (SF7) and queue/DLQ components. Catch-up
  cannot open a wave without that RPC succeeding, so it cannot be weaker than v3.
- Ordering per user: (1) recovery import for the whole user (separate, Khaliq-authorized operation) ->
  (2) import-completion receipts -> (3) enroll lane -> (4) first wave ceremony -> (5) first catch-up claim,
  which irreversibly sets publication_state=canonical and closes seed baselines (060000 claim). Enrollment
  is allowed before (1)-(2) only as a queue entry; `wave_plan` skips lanes whose last verifier run returned
  UNMEASURED until the owner resumes them with new evidence.
- Users with UNMEASURED origins never publish any origin (B1 "unmeasured"); other users proceed.
- Late first admission risk (SF5): the first claim of a user closes baselines even if PUT fails; the `next`
  RPC refuses when remaining < 150 s, and the worker budget is 120 s, so the first claim always has time.

## 6. Idempotency and resume
- No double publication is inherited: only the 060000 publish CAS (attempt/lease tokens, outbox version,
  frontier image) publishes; a published outbox leaves `buffered` so `next`/`claim` cannot return it; an
  ambiguous COMMIT is retried with the same immutable request and returns `replay:true`.
- Outcome PK `(wave_id, stage_token)`: re-recording is a no-op; counters change only on first insert.
- Worker crash: lane lease (<=120 s) expires and is re-leased under a new token; the stage `gc_lease_until`
  (5 min) expires before `next` returns it (16.3); orphan objects stay under the separately armed `--gc`.
- Driver crash: `bound` waves past capture validity are marked `aborted`; the 050000 approval is single-use.
- Flag-off or generation change: open waves expire with their stranded frontiers (B3); resume = new waves,
  continuing the 16.3 prefix. Checkpoints are observational; pending work is recomputed from outbox state.

## 7. Attestation/verification reuse (v3 sections reused unchanged)
| v3 section | How catch-up reuses it |
|---|---|
| irreversible_user_wide_gate_B1 | Enforced inside the unchanged attest RPC; lane key = B1 key; UNMEASURED -> lane held. |
| A1_noncreating_owner_lock | Attest unchanged. Enroll only SELECTs the existing owner row; no catch-up code calls `ingest_lock_fences` or inserts owner/baseline/seal rows. |
| A2_C1_role_and_membership | Owner RPCs granted to the same NOLOGIN attester role via the same SET-only edge and member-exact verifier (SF2); no new LOGIN/secret. Runtime RPCs use existing bulls96_session_ingest. |
| writer_RPC_and_snapshot_rules_A3_A4 | One attest RPC per window, READ COMMITTED, no client-held fence; catch-up RPCs are single bounded calls, VOLATILE, fixed search_path, fresh `clock_timestamp()` lease checks. |
| codec_and_independent_evidence_B2_A5_A6_A7_C4 (+SF3/SF4) | Each wave = fresh capture + independent verifier PASS + exact-receipt reviewer approval. Catch-up stores only the approval receipt hash; no new codec. |
| unchanged_frontier_and_append_only_audit_B4 | No frontier column change; catch-up audit tables follow the same RLS/REVOKE/UPDATE-DELETE-TRUNCATE guards; anti-ABA stays in 050000 CAS. |
| flag_and_invalidation_A8 (+SF1) | Attester flag + settings still gate attest; catch-up adds its own flag; v3 invalidation hooks unchanged; catch-up only observes them (wave aborted, lane held). |
| generation_drain_B3_A9_C5 | 300 s TTL, 120 s job, 150 s admission, no unattended renewal, drain runbook before any apply incl. 051000. |
| source_plan_reuse_C3 / S2 | Same planner evaluator/adapter, landed by the 049000 code PR; no vendoring. |

## 8. Throughput model
Symbols: B0 = 234,237 backlog stages; lambda = 3,500 stages/h ingest; c = per-stage worker wall time
(claim -> S3 read -> images/PUTs -> publish), assumed 4 s (range 2-10 s, UNMEASURED; first-takeover heads
copy carrier chunks and cost more); TTL = 300 s; L = capture->attest-commit latency (capture scan +
verifier + approval + RPC), assumed 60 s; admission allowed while remaining >= 150 s.

Per-lane admission phase: A = TTL - L - 150 = 300 - 60 - 150 = 90 s.
Per-window yield (k = in-flight per lane): Y = k * (floor(A / c) + 1) = 1 * (22 + 1) = 23 stages.
Worker capacity: 3600 / c = 900 stages/h at full duty; with lane hopping/ramp losses u = 0.75 -> 675/h.

Target R = 4 * lambda = 14,000/h (4x margin); net drain R - lambda = 10,500/h.
Catch-up time T = B0 / (R - lambda) = 234,237 / 10,500 = 22.3 h.
Workers W = R / 675 = 20.7 -> 24 (6 runtime processes x 4 pool slots; pool max is 4 today).
Concurrent publishing lanes ~= W / k = 24. DB load ~ 14,000/h * ~8 RPCs = ~31 RPC/s.
Windows needed per hour = R / Y = 14,000 / 23 = 609/h (~10/min), i.e. 609 captures, verifier runs,
exact-receipt approvals and attest RPCs per hour; total over T: (B0 + lambda*T) / Y = 312,287 / 23
= ~13,600 windows (lower bound: users with < 23 pending stages use a whole window for fewer stages).
Minimum to merely keep pace: lambda / Y = 3,500 / 23 = 152 approved windows/h.

Scenario table (c = 4 s, L = 60 s unless stated):
| scenario | Y | windows/h for R=14k | feasible with human approval (~30/h)? | R at 30 approvals/h |
|---|---|---|---|---|
| v3 unchanged, k=1 | 23 | 609 | no | 690/h (< lambda: never drains) |
| v3 unchanged, k=2 (Q4) | 46 | 305 | no | 1,380/h (< lambda) |
| L = 150 s (slow capture) | 0 | infinite | no | 0 (HOLD; window unusable) |
| TTL 1800 s amendment (Q2), k=1 | 398 | 35 | marginal | 11,940/h (net 8,440/h, T = 27.8 h) |
| automated reviewer lane (Q1), k=1 | 23 | 609 | n/a (machine) | 14,000/h, T = 22.3 h |
(Y for TTL 1800 s: A = 1800-60-150 = 1590 s; floor(1590/4)+1 = 398.)

Conclusion: the scheduler, leases and workers easily exceed lambda; the binding constraint is the
per-window ceremony rate. Catch-up is feasible under unchanged v3 rules ONLY if Q1 is answered yes
(an automated, independently owned reviewer/approval lane is an acceptable "assigned independent reviewer")
or Q2 is approved through a reviewed 049000 amendment. Additionally, B0 is drainable only for users whose
B1 evidence exists; today S4 (~12.9k pre-switch ACKs) and DLQ cohorts leave affected users UNMEASURED.
Canary gate before any volume: measure c (p50/p95), L, and the per-user backlog distribution.

## 9. Flag and rollout
- Runtime flag `INGEST_CATCHUP_PUBLICATION_ENABLED` (default false; exact 'true'/'false', else
  `publication_enabled_invalid`), read only in `--catchup` mode; distinct from `INGEST_PUBLISH_ENABLED` and
  the v3 `INGEST_FRONTIER_ATTESTER_ENABLED`. Server gate `ingest_catchup_settings.enabled=false` by default;
  both are armed in separate operations. The driver also needs the unchanged v3 attester flag/settings;
  arming one never enables the other. Per-user enable = lane enrollment; `max_active_lanes` starts at 1.
- Rollout ladder (each step needs Khaliq's own-words OK per C7/S4): apply 050000+051000 in ONE window (one
  generation outage) -> canary 1 lane, k=1, measure c/L -> 4 -> 24 lanes -> Q4 proof then k=2.
- Flag-off stops new leases/admissions; in-flight jobs finish or expire; frontiers are not revoked by flag-off
  (A8); emergency revoke stays the v3 RPC and aborts waves via 4.4.

## 10. Interaction with the v3 attester and 049000
### 10.1 Frontier ownership
- A user with a lane in `enrolled|attesting|publishing|idle|held` is owned by catch-up: the 4.4 trigger
  refuses manual attest/renew (55000) without a bound wave; revoke/emergency revoke/privacy invalidation are
  never refused. A user with no lane or a `released` lane is owned by the v3 manual ceremony.
- Handoff catch-up -> v3: owner `release_lane` only when the lane is `idle`, its wave closed/expired, no live
  publication or history leases for the user, and zero pending eligible stages at that instant; event logged.
  Handoff v3 -> catch-up: enroll refuses while any v3 frontier for the user is still valid (wait for expiry
  or revoke), so the first catch-up wave always starts from an expired frontier (anti-ABA unaffected).
- Even if both paths raced, the 050000 CAS on prior image + strictly increasing expiry lets only one write
  win; ownership is an efficiency and clarity rule layered on that safety.
- Note: after drain, ongoing ingest for that user still needs windows; v3 manual ceremonies (~288/day) cannot
  carry 3.5k/h, so lanes are expected to stay enrolled (idle <-> publishing) as the steady-state path (Q3).
### 10.2 049000 scoped claims
- Catch-up uses only the ordinary `ingest_publication_claim`; 049000 places its reservation check in the
  common core, so a scoped-reserved stage refuses catch-up (40001) -> recorded `deferred`, never parked,
  never stolen. Catch-up never calls `ingest_scoped_publication_*` and never installs plans.
- Enroll refuses users with an active 049000 plan admission (needs a read helper from 049000; Q5).

## 11. Failure handling and dead-letter
- Transient (40001, 55P03, 55000, 57014, 40P01): existing `ingest_publication_defer` (retry_after);
  after `park_after_deferrals` consecutive deferrals for the same stage across waves -> parked.
- Fail-closed park immediately: ticket validation failure (`publication_rpc_invalid`), image/hash mismatch,
  55000 "cross-scope record move requires reconciliation", any unknown error code. Parked stages are never
  retried by catch-up until an owner release with an event; they still remain eligible to the ordinary
  protocol's own guards (which refuse them again).
- Lane-level: verifier FAIL/UNMEASURED, approval refused, L >= 150 s (window unusable), > N parked in a wave,
  privacy invalidation, generation stranded -> lane `held` with a bounded reason; no frontier, no publication.
- A parked stage blocks later stages of the same origin (prefix rule 16.3) until owner release.

## 12. Observability
- Per-tick JSON log (no IDs): leasedLanes, admitted, published, replayed, deferred, parked,
  wavesExpiredUnused, stageMs p50/p95, remainingMsMin. Driver log per wave: captureMs, verifierMs,
  approvalWaitMs, L, outcome code. `ingest_catchup_census_v1` (READ ONLY, 3 s / 250 ms guards): lanes by
  state/held_reason, cohort states, waves by state, open parks, pending eligible stages, oldest pending age.
- Lag = pending eligible stages + oldest pending age; net drain/h. Alerts: net drain <= 0 over 1 h,
  wavesExpiredUnused > 10%, park growth, L p95 > 120 s, lease expiries. Counts never mean coverage (16.5).

## 13. Test gates
- Unit/PGlite: lease cap under concurrent calls, SKIP LOCKED fairness, disabled -> 55000 with zero mutation,
  re-lease under new token, renew bounded by valid_until; `next` parity with `ingest_publication_candidates`
  (property test) and exclusion of parked/recorded/scoped-reserved/generation- or image-mismatched/pending
  privacy/live-lease stages and remaining < 150 s; record idempotency; prefix gap refusal and root
  re-verification; cohort proof refuses with buffered pre-cutoff rows and ignores queue counts.
- Trigger 4.4: manual attest refused for laned user, bound-wave attest opens wave, revoke/emergency/privacy
  never refused, unlaned user untouched. Append-only: UPDATE/DELETE/TRUNCATE and illegal transitions refused.
- Negatives: missing owner row -> 55000 with NO owner/baseline/lane insert; UNMEASURED -> lane held, no attest;
  approval reuse refused; late post-cutoff recovery import -> lane held.
- Real PG (pg-proof two-session, PG16+PG17, PG15 "ceremony unavailable" leg per S5): attest+trigger vs lease vs
  claim in both orders without deadlock; worker crash -> single publication; frontier expiry mid-wave.
- Migration manifest: LaneManifest + generated ACL `--check`, migration-sentinels, migrate-guards, rollback
  refusal with rows/later migration, frontier 11-column sentinel; fd3f vectors and bigint fixtures unchanged.
- Proof script: extend `scripts/proof-canonical-publication.ts` (disposable PG17 + MinIO): 3 synthetic users,
  ~60 stages, 2 workers, one verifier failure, one parked stage; every non-parked stage published once.

## 14. Open questions for the lead
1. Q1 Can an automated, independently owned reviewer/approval lane (distinct from writer and verifier)
   approve exact verifier receipts at machine rate? Without it, catch-up cannot outpace ingest (section 8).
2. Q2 Is a reviewed 049000 amendment to FRONTIER_MAX_AGE_SECONDS (e.g. 1800 s) and to the SF5 "remaining
   <= 330 s" refusal on the table? It raises per-window yield ~17x.
3. Q3 Steady state: after catch-up, do lanes remain the publication path for new ingest (3.5k/h), with v3
   manual ceremonies only for exceptions?
4. Q4 Is concurrent publication of different stages of the same user/origin safe for head ordering
   (k > 1)? (v1 default: a parked stage blocks later seqs of its origin, 16.3.)
5. Q5 Should catch-up waves instead be expressed as 049000 installed plans (stronger stage binding, one
   owner install per wave)? If not, which 049000 read helper exposes "active plan admission for user"?
6. Q6 Reuse the v3 NOLOGIN attester role for catch-up owner RPCs, or a separate NOLOGIN role (new bootstrap,
   verifier, provider inventories)?
7. Q7 Exact 050000 audit/approval table and column names for the 4.4 trigger (approval hash, action).
8. Q8 Per-user backlog distribution (counts only) and measured capture/verifier latency L: needed before
   sizing W and before any canary.
9. Q9 Storage headroom for ~13.6k waves and ~310k outcome rows (plus publication attempts/images/objects)
   given the earlier storage-pressure note.
10. Q10 Who runs the attended driver for multi-hour waves, and is an attended operator session with the
    owner credential from 1Password acceptable for 22+ h of shifts?

## 15. Risks
- R1 Approval throughput (Q1/Q2) makes the design inert; mitigated only by a lead decision.
- R2 Capture/verifier latency L >= 150 s for large users -> every window unusable (HOLD, not relaxed).
- R3 Irreversible baseline closure on first claim; mitigated by B1 in attest and the 150 s/120 s gates.
- R4 Generation outages: every later migration strands all frontiers; bundle 050000+051000 in one window.
- R5 Lock contention with ingress on hot users' fences; mitigated by k=1 default and NOWAIT-free design.
- R6 Audit growth on a storage-constrained DB (Q9).
- R7 Trigger on a 050000 table couples migrations; rollback of 051000 must drop it before 050000 rollback.
- R8 Operator fatigue in attended multi-hour ceremonies leads to rushed approvals; approvals stay exact.
- R9 Cohort/custody misclassification (16.1) could declare a user drained early; completion is proof-based only.
## 16. Co-owner constraints (find-prior)
Binding constraints from the v3 contracts/vectors co-owner; each is reflected in sections 3-13.
### 16.1 Cohort is a point-in-time count, not an inventory; explicit custody cutoff
- ~234k is a point-in-time count of staged/committed batches: never a complete inventory, target, or
  completion signal. Cohort per lane = the user's stages with outbox `created_at <= lanes.cohort_cutoff_at`
  (DB clock at enrollment, immutable, taken after B1 recovery closed, 16.2). Later stages are "live"
  (`in_cohort=false`): same lane, same rules, reported separately.
- ACKs arriving during the drain create post-cutoff outbox rows, so they are live. Redelivered ACKs of cohort
  batches stay idempotent at ingest. A late recovery import after the cutoff is a B1 violation: the lane is
  held (`unmeasured`) and nothing more is admitted.
- Completion is set only by owner RPC `ingest_catchup_cohort_proof_v1`, from one snapshot of authoritative
  tables: no buffered outbox row of the user with `created_at <= cutoff` (parked rows are listed and keep the
  cohort `proof_pending`), and every cohort stage has a published/replayed outcome or an open park. It is
  never inferred from queue == 0, DLQ == 0, counters, or the 234k figure (consistent with v3 and SF8).
### 16.2 Whole-user, all-origin recovery closes irreversibly before any first admission
- Binding order (section 5): close recovery for ALL origins of (org, workspace, user) -> enroll (fixes the
  cutoff) -> first wave -> first claim. The 050000 attest RPC refuses without a verifier PASS that covers the
  preserved pre-switch, main and DLQ evidence as mandatory components (SF7; S6 new-path import receipts first).
- `wave_plan` never selects a lane with any UNMEASURED scope. Such scopes stay held, with no admission of that
  user on any origin; there is no partial-origin or "measured origins first" mode.
### 16.3 Exact image/generation, privacy invalidation, resumable prefix proof, renewal rule
- Each wave binds the approved planner image, frontier image and generation. `lease`/`next` refuse on any
  mismatch with the frontier row; a changed frontier aborts the wave and is never adopted.
- v3 privacy invalidation is unchanged. A hook, or a pending `ingest_source_control_heads` row (re-read on
  every `next`), aborts the open wave and holds the lane.
- Prefix proof: outcomes carry `origin_seq` in outbox order per (wave, origin). The admitted set must be a
  gap-free prefix (with k>1: seq <= last terminal + k), and a parked stage stops later seqs of that origin.
  At close, `prefix_root_sha256` hashes the ordered (origin, seq, stage, outcome) list. The next wave resumes
  at last terminal + 1 after re-verifying the prior root; a mismatch holds the lane.
- Renewal rule:
  - catch-up never renews a frontier, on expiry or otherwise;
  - a lane lease renews only while unexpired, while its wave is open, and never past frontier valid_until;
  - an expired lane lease is never revived; it is re-leased under a NEW token, and that holder's `next`
    excludes every stage with a live publication gc/build lease or history plan lease;
  - wave close, lane release and handoff wait until all such leases of the user have ended or expired.
### 16.4 Consumer-neutral contracts
- Tables and RPC payloads carry only canonical keys (org, workspace, user, origin, stage token), hashes,
  bounded codes and counts. They carry no consumer, product, client or route fields. `lease_owner` is an
  opaque worker label. Consumers read through the existing canonical publication interfaces.
### 16.5 Frozen vectors; synthetic counts are not coverage
- The existing fd3f test vectors and bigint fixtures stay byte-frozen. 051000 adds new fixture files only.
- Synthetic counts (proof script, PGlite, the S7 synthetic DLQ cohort) prove mechanics only and are never
  production coverage. Production coverage is measured per user: verifier VERIFIED_COMPLETE receipts per
  origin/component, bound to new-path import receipts with reviewer approval, plus the 16.1 cohort proof.
  It is reported as users proven / held / UNMEASURED, never as aggregate stage counts.
identifiers-check: 0 (counted with the three-pattern grep from the brief over the Supabase, AWS and Neon host suffixes; the 12-digit pattern is excluded because 14-digit migration timestamps such as 20261105051000 are false positives)
