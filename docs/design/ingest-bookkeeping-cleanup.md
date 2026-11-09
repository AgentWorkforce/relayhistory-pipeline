# Ingest bookkeeping cleanup (design, DRAFT)

Status: design plus default-off scaffolding. **Nothing here is applied, scheduled or enabled.**
No migration slot is taken. The draft SQL is at
`docs/proposals/ingest-bookkeeping-cleanup.draft.sql`, and the supporting-index operator file is at
`docs/proposals/ingest-bookkeeping-cleanup.indexes.draft.sql`. The registry (layered-sessions-pipeline)
assigns a slot later. It must land after pipeline-lead's 043500 claim-lock fix.
Enabling any phase needs Khaliq's separate, explicit OK (AGENTS.md: applying migrations, operator
index builds and turning on deletes are not covered by the standing code-integration order).

Scope: the five largest ingest bookkeeping tables in `sessions` on Supabase project
`pyamkrhoyszgzirqqffq`. Planner estimates come from the 2026-11-07 21:14Z census, taken before the
apply.

| table | est. rows | verdict |
|---|---|---|
| `ingest_lazy_ack_decisions` | ~27.4M | **Phase A**: delete per terminal claim |
| `ingest_receipt_seats` | ~25.7M | **Phase B**: delete unreferenced, non-current seats (needs new indexes) |
| `ingest_lazy_native_slots` | ~15.6M | **never delete** |
| `ingest_frozen_source_records` | ~8.0M (see UNKNOWN U1) | **Phase C**: blocked; census only |
| `ingest_custody_records` | ~3.3M | **Phase B**: delete detached live-witness rows (needs new indexes) |

Every statement below comes from the migrations on `main` (through 20261105043000), the
session-ingest worker (`src/session-ingest/*`) and the buffered publication, GC and custody SQL.
Line references are to `supabase/migrations/`. Anything the code cannot settle is marked
**UNKNOWN** in the last section.

---

## 1. What each table is, and who reads it

### 1.1 `ingest_lazy_ack_decisions` (055000 l.59)

- **One row per `(claim_token, ord)`**, written once by `ingest_lazy_ack_batch` (055000 l.616).
  That function is the lazy-mode body of the ACK trigger `ingest_receipt_seat_ack` on
  `ingest_batches` (055000 l.1325). The row fires inside the same transaction that commits the
  buffered receipt, and it holds the global seat-clock row `FOR UPDATE`.
- **What it proves:** the immutable first-ACK decision for that original candidate. The row records:
  - which native slot it judged (`native_key_sha256`, `native_lineage`);
  - whether the body was accepted (`body_accepted`);
  - which first digest won (`first_digest`);
  - which seat orders it (`seat_key_sha256`);
  - the candidate's reset and fence epochs;
  - the generation and source-witness identity it was decided under.

  `ON CONFLICT DO NOTHING` plus a strict re-read refuses any later change ("immutable ACK
  decision adoption refused").
- **Readers:**
  1. `ingest_lazy_stage_seats` (055000 l.695), through `ingest_receipt_seats_for_stage`. It requires
     the stage to be `receipt_state='acknowledged'`, `cleanup_state='live'`, with an outbox row
     `state='buffered'` pointing at it.
     - Callers: `ingest_publication_claim` (060000 l.221) and `ingest_history_plan_claim`
       (026000 l.445).
  2. `ingest_buffer_gc_ordinals` (055000 l.1479). It returns early for `preparing` stages. It
     reads decisions only for an acknowledged stage, and it raises "immutable ACK decisions
     required" if any is missing.
     - Callers: `ingest_buffer_gc_claim` (039000) only for stages with `cleanup_state<>'retired'`,
       plus `gc_prepare`/`gc_swap` and the history plan claim. All require a live acknowledged
       stage with a buffered outbox.
  3. `babysitter_evidence_page_for` (042000 l.744). The Babysitter reader serves a candidate only if
     `EXISTS(decision WHERE claim_token=r.claim_token AND candidate_digest=r.digest AND body_accepted)`.
     It runs for every `babysitter_evidence_index` row. Delete tombstones in that index are
     **permanent**. Upsert rows live until their bucket is erased.
- **Not read by:**
  - ACK replay (`ingest_commit_buffered_batch` with an acknowledged stage returns `committed` +
    `ingest_lazy_attempt_ack`, which reads `ingest_batches`/`ingest_claim_attempts` only);
  - claim replay of a committed batch;
  - `ingest_publication_publish` replay (reads `ingest_claim_attempts.records`);
  - 038000 retirement;
  - any TS module (only tests mention the table).
- **When it stops being needed:** once its claim is **terminal**:
  - (a) its batch is committed;
  - (b) every stage intent of the claim (parents and GC children share the claim token) is
    `cleanup_state='redacted'`, so no live-stage reader can reach it;
  - (c) no `babysitter_evidence_index` row names the claim token;
  - (d) a grace period since `committed_at` has passed.

### 1.2 `ingest_receipt_seats` (053000 l.40)

- **One row per assigned ordering seat.** `seat` is a strictly increasing bigint taken from the
  singleton clock `ingest_receipt_seat_clock.last_seat` inside the ACK transaction.
  - In lazy mode, `ingest_lazy_ack_batch` inserts a seat (`lazy-ack-seat-v1`) for every accepted
    candidate.
  - It also inserts one for every ignored candidate whose slot has no current seat.
  - An ignored candidate whose slot has a current seat **reuses** `slot.current_seat_key`
    (055000 l.659-668).
  - The non-lazy paths (053000/054000 `ingest_receipt_seat_ack`) also reach it through
    `ingest_receipt_record_heads` and `ingest_receipt_ord_seats`.
- **What it proves:** the canonical order (`accepted_seq`) and the first digest for a record
  lifecycle. `publication-seats.ts` `injectReceiptSeat` writes `seat` into each canonical image
  line as `accepted_seq`. Once an image is published, the order lives in S3. The SQL row matters
  only to SQL readers.
- **Incoming FKs (all `NO ACTION`/`RESTRICT`, **none indexed today**):**
  - `ingest_lazy_ack_decisions.seat_key_sha256`;
  - `ingest_lazy_native_slots.current_seat_key`;
  - `ingest_receipt_record_heads.winner_key_sha256`;
  - `ingest_receipt_ord_seats.key_sha256`.
- **Readers:**
  - `ingest_lazy_stage_seats` / `ingest_receipt_seats_for_stage`, through the decision or ord map
    for a live stage;
  - `ingest_lazy_ack_batch`, which reads the slot's **current** seat to check `first_digest` drift
    for an ignored candidate;
  - the non-lazy `ingest_receipt_seat_ack`, which reads the record head's winner seat.
- **Contiguity:** seats are dense by construction, because the clock and its inserts commit
  together. **No reader relies on that density.** `parseReceiptSeatSnapshot` checks only
  `allocatedHighWater < seat <= lastAssignedSeat` and alias consistency within one stage. No SQL
  or TS code counts, max-scans or gap-checks the seat table (repository grep). The clock row is
  never touched here, so allocation stays monotonic and a deleted seat number is never reissued.
- **When it stops being needed:** when nothing references it:
  - no decision;
  - no native slot `current_seat_key`;
  - no record head winner;
  - no ord seat;
  - plus a grace period on `created_at`.

  This is **monotonic**: a slot's `current_seat_key` only ever moves to a *freshly inserted*
  seat or to NULL (`ingest_lazy_ack_batch`, `ingest_lazy_clear`). A new decision references
  either a fresh seat or the slot's current one. A non-current, unreferenced seat can therefore
  never gain a reference again.

### 1.3 `ingest_lazy_native_slots` (055000 l.43)

- **One row per global native key** `sha256(['global-native-custody-v1',org,ws,origin,record_id])`.
  The row holds the record's current owner, revision, digest, seat and legacy custody, plus a
  `lineage` counter.
- **What it proves:** in lazy mode the slot *is* the target's revision fence and current
  custody.
  - "GLOBAL native-key initialization persists across deletion/reset. Only current custody/owner
    is cleared; a stale original Neon row can never reseed the slot" (055000 l.41-42).
  - "An initialized slot is authoritative, even when empty" (055000 l.366).
- **Readers:**
  - `ingest_claim_batch_mode` stale judgement (055000 ~l.1198);
  - `ingest_lazy_ack_batch`;
  - `ingest_lazy_records` (existence = authority);
  - `ingest_lazy_stage_seats`, `ingest_buffer_gc_ordinals`;
  - `ingest_history_native_snapshot`, `ingest_history_reconcile_backfill`, which pages **every**
    slot, cleared ones included (026000 l.688);
  - `ingest_history_reconcile_outbox` (FK);
  - the Babysitter reader's "newer revision wins" check;
  - statement triggers `ingest_history_native_insert/update`.
- **When it stops being needed:** never, under the current design. Deleting a slot would:
  1. let a stale frozen or Neon witness `current` reseed it;
  2. reset `lineage` to 0, so history consumers could not tell old lineages from new;
  3. break the FKs from decisions and the history outbox.

  **Retention rule: keep forever.** The only slimming available is outside this design (for
  example dropping unused columns), so it is not proposed.

### 1.4 `ingest_custody_records` (054000 l.38, widened in 055000 l.76-85)

There are two kinds, set by the CHECK `ingest_custody_authority_kind`:

- **imported baseline** (`import_token`, `page` NOT NULL). Written by the eager owner ceremony
  `ingest_custody_page` (054000). It is referenced by `ingest_receipt_record_heads.legacy_key_sha256`.
  `ingest_custody_complete` checks `count(*)` against `expected_records`.
- **live witness** (`live_*` NOT NULL). Written by `ingest_lazy_records` (055000 l.400) when a slot
  is first initialized from a witness whose `current` (the Neon/frozen current row) is non-null.
  It is referenced by `ingest_lazy_native_slots.current_legacy_key`.

Both kinds:

- **What it proves:** the pre-cutover current row (revision, digest, source, session, acceptance
  sequences in `metadata`) that the target adopted as current custody, bound to the witness digest.
- **Incoming FKs (none indexed today):**
  - `ingest_lazy_native_slots.current_legacy_key`;
  - `ingest_receipt_record_heads.legacy_key_sha256`;
  - `ingest_receipt_ord_seats.legacy_custody_key_sha256`.
- **Readers:**
  - the non-lazy `ingest_receipt_seat_ack` / `ingest_receipt_seats_for_stage`, through the head
    or ord map;
  - `ingest_custody_complete` (count, imported kind, while importing).

  Lazy-mode paths copy what they need into the slot's `current_*` columns at initialization and
  never read the custody row back. No TS reader exists.
- **When it stops being needed:** for the **live-witness kind**, once no slot, head or ord seat
  references it. A slot detaches (`current_legacy_key=NULL`) when:
  - a higher revision is accepted (055000 l.673-676);
  - a disable or reset clears it (`ingest_lazy_clear`).

  This is monotonic: `current_legacy_key` is only ever set when the slot row is **inserted**,
  together with a freshly inserted custody row. The imported kind is excluded from cleanup,
  because its count is ceremony evidence (UNKNOWN U4 covers whether any exist).

### 1.5 `ingest_frozen_source_records` (010000)

- **One row per Neon native key** `(org, ws, origin, record_id)`: the frozen Neon `delivery_records`
  metadata. There is no payload.
- **What it proves:** what Neon's current-custody witness would answer for that key.
  `ingest_frozen_source_witness` builds the byte-identical witness packet from it.
- **Readers:**
  1. `ingest_frozen_source_witness` (`current`, and the user check behind P0W01, for every
     candidate). The target ignores `current` once the key's native slot exists (055000 l.361-368).
     The witness can still be compared with Neon's in **shadow** mode
     (`INGEST_CUSTODY_SOURCE=shadow`, docs/p1-frozen-source-custody.md).
  2. The privacy authority (041000):
     - disable deletes the session's rows;
     - **reset fences `owned_sessions ∪ frozen records with a session_id`** (041000 l.415-432);
       the frozen rows can only *add* fences;
     - the E3 eraser enumerates `ingest_frozen_source_deletions ∪ upload_deletions`.
  3. `scripts/frozen-source-copy.ts --seal` and the per-org seals: a count and digest compared
     with Neon.
- **Write guard:** once `ingest_privacy_authority.state='active'`, a statement trigger refuses
  every non-authority writer (041000 l.95-125). A cleanup DELETE would be refused unless it posed
  as the authority writer.
- **When it stops being needed:** a frozen row is redundant for upload custody once its native slot
  is initialized. It is **still** privacy input: removing a row whose session is not in
  `ingest_frozen_source_owned_sessions` would narrow a future reset's fence set. The verdict and
  rule are in §2.

---

## 2. Retention rules

Each rule is applied in one bounded statement per page, under the settings and gates of §5.

| table | delete row iff ALL of | never if |
|---|---|---|
| decisions | claim batch `state='committed'` AND `committed_at <= now()-min_age`; NOT EXISTS stage intent of the claim with `cleanup_state<>'redacted'`; NOT EXISTS `babysitter_evidence_index.claim_token = claim`, re-checked under the claim's exclusive interlock lock, which also writes the irreversible registration-closed marker (§3.5) | phase flag off; dry run; a Babysitter registration for the claim in flight (skipped, counted) |
| seats | `created_at <= now()-min_age`; NOT EXISTS decision / slot `current_seat_key` / head winner / ord seat referencing it | required FK-side indexes missing (§5.3) |
| native slots | (none) | always |
| custody | `import_token IS NULL` (live-witness kind); its native slot's `initialized_at <= now()-min_age` (the custody row is inserted in the same transaction as its slot; no slot means kept); NOT EXISTS slot `current_legacy_key` / head `legacy_key_sha256` / ord seat `legacy_custody_key_sha256` referencing it | required indexes missing; imported kind |
| frozen | **Phase C, not implemented.** Proposed: native slot for the key exists AND (`session_id IS NULL` OR the row's `(org,ws,user,origin,source,session)` ∈ `ingest_frozen_source_owned_sessions`) AND shadow mode is off AND it runs through the privacy authority | always, until Phase C is approved |

Why each condition is necessary (the matching causal negative test is in §8):

- **Decisions, committed:** decisions exist only after the ACK trigger. `committed` is final
  (no code moves a batch back to pending, and no code deletes `ingest_batches`).
- **Decisions, all stages redacted:** `redacted` is final.
  - No UPDATE sets `cleanup_state` back from `redacted`.
  - A new stage intent for the claim needs either a pending batch (`ingest_prepare_buffer_stage`)
    or a live acknowledged parent (`ingest_buffer_gc_prepare`), and both are excluded.
  - So once true it stays true. No lock is needed to keep it true.
  - Every decision reader requires a live stage, so dropping this condition breaks the
    publication claim, GC and the history plan. The test shows `ingest_buffer_gc_ordinals` and
    `ingest_receipt_seats_for_stage` raising once a live stage's decisions are removed.
- **Decisions, no Babysitter row, and registration closed:** the reader looks decisions up by
  claim token, with no stage needed. Delete tombstones are permanent.
  - `ingest_babysitter_retention_register` (042000 l.263-290, 375-381) checks only
    `ingest_attempt_epochs`. It has no pending-batch requirement and no deadline, so a valid
    registration for an old committed claim can arrive at any time.
  - The absence of an evidence row is therefore not a proof by itself, and age is not one either.
    The rule is backed by a database interlock (§3.5): retirement is irreversible and serialized
    with registration on a per-claim lock, so registration fails once retirement begins.
    `min_age` stays only as an operational margin.
- **Seats, unreferenced:** the FKs would refuse the delete anyway. The predicate keeps a page from
  failing, and it keeps the slot's **current** seat (needed by the next ignored candidate's
  `first_digest` drift check). Monotonicity is shown in §1.2.
- **Custody, live kind, aged and detached:** monotonic (§1.4). It ages like seats; the age is its
  slot's `initialized_at`. The imported kind is excluded because
  `ingest_custody_complete`'s coverage count and the import evidence describe the full imported
  set.
- **Frozen, owned-session coverage:** this keeps the reset union of 041000 unchanged (§3.3).

**Order:** decisions first (Phase A), then seats and custody (Phase B). A seat or custody row
becomes deletable only after the decisions or slot state referencing it are gone or have moved on.

---

## 3. Interactions

### 3.1 Custody and receipt-seat contiguity

- The clock (`ingest_receipt_seat_clock`) is never read for update, written or locked here.
  Allocation stays strictly monotonic, and seat numbers are never reused.
- The seat table becomes sparse after cleanup. No reader requires density (§1.2). The one bound
  that matters, `allocatedHighWater < seat <= lastAssignedSeat`, is per stage and comes from the
  clock.
- Current custody (a slot's `current_*` and the custody row it points to) is never deleted.
- Only seats and custody rows that are **no longer current** for any slot and unreferenced are
  eligible. That set is monotonic, so a concurrent ACK cannot resurrect one.
- If the predicate were ever wrong, the FK raises and the whole page rolls back (fail closed).

### 3.2 Frozen-source custody

- Phases A and B never read or write frozen tables.
- Frozen rows matter to upload custody only while the key's slot is absent. They are never
  touched before Phase C.
- In shadow mode a deleted frozen row would show up as `custody.shadow` mismatches. Phase C
  therefore requires shadow mode off (U2) and must also re-plan the `--seal` re-verification.

### 3.3 Erasure, privacy (041000 privacy authority, E3)

- **Deletion never re-enables anything.** Fences live in:
  - `upload_deletions`, `ingest_fence_versions`, `upload_resets`;
  - the `ingest_frozen_source_*` privacy tables;
  - `ingest_lazy_user_seals` / `ingest_seed_fence_baselines`.

  None of these is touched.
- **Cleared slots survive**, so a reset cannot be undone by a stale witness reseeding a slot.
- The lazy first-contact "prior authority" scan (`ingest_lazy_user` / `ingest_lazy_begin`) cannot
  be fooled into treating a user as fresh. `ingest_fence_owners` (the `v_first` check) and the
  seals are untouched, and the scan only runs when no seal exists.
- **Identifiers leave SQL.** Seat `key_material` (org/ws/user/origin/record/source/session) and
  custody `metadata` hold identity metadata (no transcript content). Deleting them *reduces*
  retained identity data. No archive copy is written in the proposed mode, so no new E3 erasure
  obligation arises. If an archive mode is added later (§6), it must use org-prefixed keys under
  the same erasure reach, as the 038000 claim-records keys do.
- **Frozen (Phase C):**
  - Deleting a frozen row whose session is outside `ingest_frozen_source_owned_sessions` would
    shrink the set of sessions a later reset fences and the E3 eraser erases. The rule requires
    owned-set coverage.
  - Once the authority is `active`, only authority writers may change the table, so Phase C must
    run as an authority-path maintenance writer under the shared authority lock (041000 l.82-95).
    That is a privacy-authority decision, not a code-integration one.

### 3.4 Buffered publication backlog

- Every batch still in the backlog has a live stage: acknowledged, `cleanup_state='live'`,
  outbox `buffered`. Batches mid-GC also count: `retired` but not yet `redacted`, with the
  barrier pending.
- Their decisions (and through them their seats) are kept by the stage condition.
- The publication candidate and claim path, GC and the history plan claim all keep working
  unchanged.
- The reclaim is therefore bounded by how much of the backlog has been drained (published and
  GC-redacted). The size of the backlog is UNKNOWN U3.

### 3.5 Babysitter registration interlock (decision retirement is irreversible)

`babysitter_evidence_page_for` (042000 l.782-787) serves a candidate only if the claim's decision
exists. Registration has no deadline. A decision deleted before a later or concurrent
registration would make that registered evidence invisible. Timing does not close this. The draft
closes it with state plus serialization:

1. **Marker:** `sessions.ingest_bookkeeping_retired_claims(claim_token)`. It is insert-only: a
   statement trigger refuses UPDATE, DELETE and TRUNCATE. The cleanup inserts it **in the same
   transaction** that deletes the claim's decisions. If that transaction rolls back, both roll back.
2. **Lock:** a per-claim advisory key, `ingest_bookkeeping_claim_lock_key(claim)`.
   - The cleanup takes it **exclusively with `pg_try_advisory_xact_lock`** and never waits. A
     claim whose lock is held is skipped and counted (`skippedClaims`), so the run is not `done`.
   - After acquiring the lock, the cleanup re-checks `babysitter_evidence_index` in a new statement,
     so a registration that committed in between is seen.
3. **Registration:** `ingest_babysitter_retention_register` is replaced by
   `CREATE OR REPLACE` in the new SQL. The 042000 file is untouched. The body is verbatim except one
   block placed right after `ingest_attempt_epochs`, before any write:
   - it takes the claim's lock **shared**, so registrations never block each other;
   - then, in a new statement, it refuses with `55000 ... registration closed` if the marker exists.

Both orders are safe:

- **Registration first:** it holds the shared lock until commit. The cleanup's try-lock fails, so
  the claim is skipped. After the registration commits, the evidence row blocks retirement.
- **Cleanup first:** it holds the exclusive lock until commit. The registration waits on that lock,
  then sees the committed marker and fails. Nothing invisible is ever registered.

Deadlocks are impossible. The cleanup never waits: it uses try-locks, SKIP LOCKED and
`lock_timeout`. Registration waits only on a cleanup transaction, which holds no lock registration
needs first. Each retired claim holds one advisory lock until commit, so a page is capped at 200
claims to keep the shared lock table bounded.

A refused late registration is the correct outcome. That claim's ACK decision is already gone, so
the reader could never have served its evidence.

---

## 4. ACK and replay safety

These are every path that can run for an existing or new batch, and why it survives each phase:

| path | rows it needs | after A | after B |
|---|---|---|---|
| lost-reply commit replay `ingest_commit_buffered_batch` (acknowledged stage) | stage, outbox, batch, claim attempt | unaffected | unaffected |
| claim replay of a committed batch `ingest_claim_buffered_batch` | batch, claim attempt; `ingest_lazy_begin` (policy, seals) | unaffected (`ingest_lazy_records` skipped once committed) | unaffected |
| new ACK `ingest_lazy_ack_batch` | slot (kept), slot's current seat (kept: referenced), clock | unaffected | current seat never eligible |
| new claim stale judgement `ingest_claim_batch_mode` | slots | unaffected | unaffected |
| publication claim/publish (live stage) | decisions + seats of that stage's claim | claim non-terminal → kept | seats referenced → kept |
| publication publish replay | `ingest_claim_attempts.records` | unaffected | unaffected |
| GC claim/prepare/swap (live stage) | decisions + slots | kept | kept |
| history plan / native snapshot / reconcile backfill | live-stage decisions, all slots | kept | kept |
| Babysitter reader | decisions by claim token | kept while any index row names the claim | n/a |
| frozen witness | frozen records | untouched | untouched |

The tests prove this by deleting, then re-running the original batch (claim + commit replay), a
higher-revision batch and an equal-revision ignored batch for the same records, and checking the
outcomes match a control fixture without cleanup (§8).

---

## 5. Batch and lock budget (no contention with ingest claims)

The cleanup lands after pipeline-lead's 043500 claim-lock fix and is sized to stay out of its way.

### 5.1 Locks it never takes

- `ingest_fence_owners` (the USER fence, `ingest_lock_fences`);
- `ingest_batches`, `ingest_stage_intents`, `ingest_buffer_outbox`, `ingest_claim_attempts`.
  These are read only through MVCC.
- the seat clock row;
- the privacy authority lock;
- any `ingest_lazy_native_slots` row. It never writes slots, so the 026000 statement triggers
  never fire.

The terminal conditions are final states (§2), so no lock is needed to hold them stable.

### 5.2 Locks it does take

- `pg_try_advisory_xact_lock` on a cleanup-only key per phase (one cleaner per phase). If busy, it
  returns `busy` and does not wait.
- `FOR UPDATE SKIP LOCKED` on exactly the candidate rows of the five tables it deletes. A row
  locked by anyone is skipped and never waited on.
  - For seats, an ACK inserting a decision takes `FOR KEY SHARE` on the seat it references. That
    seat is current, so not a candidate. Any such seat is skipped anyway.
- RI checks on seat and custody deletes. With the §5.3 indexes these are index probes. They find
  no referencing row (by the predicate), so they lock nothing.
- per claim (Phase A only), the Babysitter interlock lock, by try-lock (§3.5). Ingest claims and
  ACKs never take it. Only Babysitter registration does, shared.
- `lock_timeout = 100ms` (function `SET`) as a backstop.
- **Skips are never lost.** Every page returns `skipped` (rows of claims whose lock was busy, plus
  rows `SKIP LOCKED` passed over), `end` (keyset exhausted) and `done = end AND skipped = 0`.
  - The operator reports `done=true` only when a run reached the end with zero skips across all
    its pages.
  - Otherwise it reports `incomplete` (`locked`, `busy` or `max_pages`) and `resumeFrom`: the
    cursor of the earliest page that skipped anything, where null means from the start.
  - A page with `next=null` that is not `end` is refused rather than restarting the keyset.
- **Index gates check definitions.** Each required index must be live, valid, ready, **and** its
  `pg_get_indexdef` must equal the exact expected definition: columns, order, opclass and
  collation, predicate, uniqueness, method. A same-name drifted index is refused.
  - The operator index file has no `IF NOT EXISTS`, so building over a drifted name fails loudly.

### 5.3 Bounds

- **One page per transaction.** The caller must `SET LOCAL statement_timeout` to a value of
  5 s or less. The functions refuse with 0 (unbounded) or more than 5 s (`statement_timeout`-friendly).
- **Decision pages:** at most `rowBudget` (500 to 5000, default 2000) decision rows scanned from
  the PK `(claim_token, ord)` keyset after the cursor. The page is cut at a whole claim: a claim has
  at most 500 ordinals, so one claim always fits. The terminal predicate uses indexed probes:
  - `ingest_batches` PK;
  - `ingest_stage_intents` `UNIQUE(claim_token, stage_key)`;
  - the new `babysitter_evidence_index(claim_token)` index.
  - A page writes about 1 MB of heap changes.
- **Seat and custody pages:** at most 1000 rows from the `seat` UNIQUE index and the PK keyset.
- **Pacing:** the operator tool sleeps between pages (default 500 ms). Start at no more than about
  1M deleted rows per hour. Stop or pause on any of:
  - claim-lock waits (`pg_stat_activity` `wait_event_type='Lock'` on ingest functions);
  - ingest ACK p99 regression;
  - `n_dead_tup` growing faster than autovacuum clears it;
  - replication or WAL pressure.
- **Supporting indexes**, built **CONCURRENTLY** by a non-transactional operator file (the same
  pattern as 041000's `ingest_privacy_authority_index.sql`). They need an explicit apply OK.
  - **Phase A** needs only `babysitter_evidence_index(claim_token)`. That table is small and off
    the ACK path.
  - **Phase B** needs FK-side indexes, all of which are absent today, so without them each seat
    or custody delete would seq-scan up to 27M rows:
    - `ingest_lazy_ack_decisions(seat_key_sha256)`;
    - `ingest_lazy_native_slots(current_seat_key) WHERE NOT NULL`;
    - `ingest_lazy_native_slots(current_legacy_key) WHERE NOT NULL`;
    - `ingest_receipt_record_heads(winner_key_sha256)` / `(legacy_key_sha256)` partial;
    - `ingest_receipt_ord_seats(key_sha256)`, and `(legacy_custody_key_sha256)` partial.
  - **This is the real cost of Phase B.**
    - The decision index adds one btree insert per ACKed record **inside the ACK transaction,
      while the global seat-clock row is held `FOR UPDATE`**.
    - The slot `current_seat_key` index stops HOT updates for the slot update in every accepted
      ACK.
    - Both lengthen the globally serialized ACK critical section. pipeline-lead must measure
      them after 043500 and agree before Phase B. The functions refuse to run Phase B until
      every index exists and is `indisvalid AND indisready`.

---

## 6. Partitioning vs paged delete vs move-to-S3; the reclaim path

| option | fit | verdict |
|---|---|---|
| **Paged delete in place** (this design) | Retention is *state*-based (terminal claim, unreferenced seat), not time-based; a paged anti-join matches it directly; no schema change to hot tables in Phase A | **chosen** |
| Partitioning (by time) | No time column on decisions; terminal state is not aligned to insertion time (old backlog rows outlive newer published ones); converting means rewriting 27M+ rows, changing PKs to include the partition key and re-pointing four FKs; dropping a partition needs *every* row in it terminal | rejected now; reconsider only if growth outpaces paged delete |
| Move-to-S3 then clear (038000 pattern) | 038000 archives `claim_attempts.records` because **publication replay still reads it**. These rows have **no reader** once eligible, so an archive adds S3 IO, a second identity-metadata copy (a new E3 erasure obligation) and per-org paging, for forensics only | not default; an optional archive-first mode (org-prefixed keys, create-only, read-back verified, then delete) can be added before first enablement if Khaliq wants recoverability. Not built |

**The reclaim path.** Postgres does not return space on DELETE.

- Autovacuum marks dead tuples reusable. Reclaimed space then serves new ingest rows, so the
  tables **stop growing**.
- Btree pages that empty out are recycled.
- The files do not shrink.

What is realistic on Supabase:

1. **Stop growth (Phase A alone).** This is effective as soon as autovacuum keeps up, and needs no
   lock beyond normal vacuum.
2. **`REINDEX INDEX CONCURRENTLY`** on the bloated PKs, for example the random-uuid decision PK.
   It runs online and as the owner.
3. **`pg_repack`** for the heap. Supabase documents it as an available extension, run with the
   client's `-k`/no-superuser mode (availability on this project is UNKNOWN U7).
   - It needs about 1x the table size free temporarily and a brief `ACCESS EXCLUSIVE` at the swap.
   - Run it per table in a quiet window, with Khaliq's OK.
4. **`VACUUM FULL`** takes `ACCESS EXCLUSIVE` for the whole rewrite. On decisions or seats that
   blocks every ACK for minutes (the trigger writes both). Only inside an explicit write hold.
   **Not recommended.**
5. **Shrinking the disk itself:** whether this project's plan allows downsizing the
   provisioned disk is UNKNOWN U7. Without that, the win is headroom and slower growth, not a
   smaller bill.

**Expected reclaim.** These are order-of-magnitude estimates from row widths, not measurements:

- **decisions:** 7 hex64 text columns, a uuid and 6 scalars, about 550 B/row including the tuple
  header, plus a PK of about 40 B/row. About 16 GB at 27.4M rows. Eligible = rows of terminal
  claims older than `min_age` (UNKNOWN U3; if most of the backlog is drained, most of the table).
- **seats:** `key_material` jsonb (~400 B), hex keys and scalars, about 700 B/row plus 2 indexes.
  About 18-20 GB. Eligible after Phase A ≥ seats − slots with a current seat. With 25.7M seats and
  15.6M slots, that is **at least ~10M rows (~7 GB)**, more as slots are cleared.
- **custody:** about 1 KB/row, about 3.3 GB. Eligible = live-witness rows no longer current
  (UNKNOWN U5).
- **native slots:** 0 (kept).
- **frozen:** 0 in this design.
- **Upper bound:** about 30 GB of reusable space across decisions, seats and custody. A realistic
  first result is the decision table's terminal share plus about 7 GB of seats.

The operator should measure `pg_total_relation_size` and the read-only census (§7) before
enablement.

---

## 7. Rollback and dormant default

Two independent flags must both say go:

1. **SQL settings singleton** `sessions.ingest_bookkeeping_cleanup_settings`:
   - `enabled=false`, `dry_run=true`;
   - per phase `decisions_enabled=false`, `seats_enabled=false`, `custody_enabled=false`;
   - `min_age='7 days'`.

   It is changed only by the owner-only `ingest_bookkeeping_cleanup_configure`. A disabled or
   dry-run setting deletes nothing. A dry run still returns exact per-page eligibility counts.
2. **The operator tool** (`scripts/ops/ingest-bookkeeping-cleanup.ts`; the owner connection uses
   discrete pinned pg fields from `src/shared/owner-pg-config.ts`:
   - no `connectionString`;
   - TLS verified against the pinned Supabase CA;
   - a fixed `application_name`;
   - no PG* environment or `.pgpass` fallback.

   It parses arguments strictly and prints only a closed reason on failure.) It runs a dry run
   unless given
   `--execute`.

Other properties:

- **Read-only census:** a dry-run page of each phase (`execute:false`) is the census. It gives
  bounded keyset-window counts of eligible rows and of blocked rows per reason. It never deletes
  and needs no flag, but it does need its phase's indexes: Phase A requires the Babysitter
  claim index, Phase B the FK-side indexes. `ingest_bookkeeping_cleanup_frozen_census` is the
  Phase C equivalent. It is read-only and uses PK probes only.
- **Rollback:**
  - set `enabled=false` (one statement). Any page in flight finishes or rolls back within its
    statement timeout.
  - Dropping the draft objects is a guarded down file when the slot is assigned.
  - Deleted rows are not restorable without the optional archive mode (§6). That is why the
    rules are proven unreferenced and the first runs are dry runs.
- **Generation:** registering the draft as a migration changes the derived `generation()`. It
  therefore needs the normal coordinated generation window (rebind policy and admissions,
  redeploy Worker and GC with the new expected generation), like every schema release.
- The 038000-style function-boundary lane and the `sessions-function-acl.json` entries are added
  at slot-assignment time.
- **Enablement sequence** (each step needs Khaliq's OK in his own words):
  1. register and apply the slot after 043500;
  2. build the Phase A index;
  3. run the census;
  4. `configure({enabled:true, dryRun:true, decisions:true})` and dry-run pages;
  5. execute pages with slow pacing, then normal pacing;
  6. later, Phase B indexes after pipeline-lead's measurement;
  7. then Phase B.

  Phase C needs a separate privacy-authority design.

---

## 8. Test plan

The tests run on real PG17 in an owned loopback container. They apply every migration on `main`
through the real migrator, then load the draft SQL. Fixtures come from the real producer path
(`deliverBatch` in lazy mode with real witnesses, the real ACK trigger, real `ingest_lazy_clear`).
The publication and GC lifecycle (outbox `erased`, stage `retired` → `redacted`) is modeled the
same way the 038000 test models it.

**Positive tests:**

- a fully terminal claim's decisions are deleted;
- after a higher-revision re-upload, the superseded seats and the detached live custody row are
  deleted;
- slot rows, the slot's current seat and current custody remain.

**Causal negatives.** Each uses the same fixture with exactly one condition flipped, and the
outcome flips:

- decisions kept when:
  - the stage is still live (backlog);
  - the stage is `retired` but not `redacted`;
  - a Babysitter index row names the claim;
  - `committed_at` is inside `min_age`;
  - the settings are disabled;
  - the run is a dry run (counts reported, nothing deleted);
  - the phase flag is off;
- seats kept when:
  - a decision still references the seat;
  - the seat is a slot's current seat;
  - the seat is inside `min_age`;
- custody kept when:
  - it is still a slot's current legacy key;
  - it is the imported kind;
- Phase B refused while the supporting indexes are missing;
- refusal when `statement_timeout` is 0 or more than 5 s;
- `busy` (not a wait) when another cleaner holds the phase lock.

**Why the predicate matters (proof by violation):**

- removing a *live* stage's decisions directly makes `ingest_buffer_gc_ordinals` and
  `ingest_receipt_seats_for_stage` raise;
- deleting a slot's current seat directly is refused by the FK.

These show the conditions are load-bearing, not decorative.

**Live paths after cleanup:**

- the original batch replays through the real producer with the same outcome as before cleanup;
- a higher revision is accepted and becomes current;
- an equal revision with a different body is judged stale (the slot is unchanged);
- `ingest_buffer_gc_ordinals` and `ingest_receipt_seats_for_stage` on a live (backlog) stage still
  return full coverage.

**Non-contention:**

- with another session holding the user's fence lock, the batch `FOR UPDATE`, the stage and
  outbox locks (an in-flight claim or ACK), a cleanup page completes without appearing in
  `pg_blocking_pids` and well inside its timeout;
- with another session holding `FOR KEY SHARE` on a candidate seat, that seat is skipped, not
  waited on.

**Index-servability:** with `enable_seqscan=off` and `enable_sort=off`, every keyset window
(decisions, seats, custody, frozen) and every reference probe has a plan with no seq scan and no
sort once its indexes exist. That is what keeps a page inside its statement timeout at production
size.

**Operator tool:** dry run by default; it pages with the cursor, refuses malformed results, and
`--execute` is still a no-op while SQL `enabled=false`.

**Babysitter interlock (§3.5), causal both ways:**

- **Sequential:** a control claim registered before cleanup keeps its decisions and is served by
  the real reader. A cleaned claim's later registration is refused (55000).
- **Proof by violation:** with 042000's original register restored inside a rolled-back
  transaction, the same registration succeeds, but the reader serves nothing for it.
- **Two-connection race:**
  - registration uncommitted during the cleanup page: the claim is skipped (no wait), then kept
    by its evidence row;
  - cleanup uncommitted during registration: registration blocks on the claim lock, then fails.

**Index drift:**

- A same-name wrong index (wrong column, or wrong predicate) is refused by both gates, and the
  probe it should serve does not use it.
- Rebuilding over a drifted name fails with `42P07`.

**Skips:** a seat held `FOR KEY SHARE`, or a decision row held `FOR UPDATE`, across one multi-page
operator run yields `done=false`, `incomplete=locked`. The run's `resumeFrom` revisits the row,
and a second run deletes it and reports `done`.

**Mutation proof:** each of 15 safety elements was removed in turn from a copy of the draft:

- decisions: live stage, Babysitter, `min_age`;
- seats: current slot, age;
- custody: imported kind (both guards), current slot, age;
- the register refusal and the register lock;
- the cleanup claim lock and the marker;
- the index-definition check;
- seat/custody and decision skip counting.

Every mutant fails at least one test. The test file accepts
`BOOKKEEPING_CLEANUP_DRAFT_SQL` only for this purpose.

Still to do before slot assignment: a function-boundary lane, ACL JSON, and a guarded down file
with a reapply round trip, mirroring 039000's test. A proof at the seat-index write cost
(ACK p99 with and without the Phase B indexes) is pipeline-lead's measurement.

---

## 9. UNKNOWNs (cannot be established from source)

- **U1, frozen row count.** The census says ~8.0M. 041000's comment calls the table "66M-row" and
  055000 says "61M-row prewarm". The planner estimate may be stale (no ANALYZE after the bulk
  copy). An exact count or a fresh `ANALYZE` by the operator is needed.
- **U2, custody source mode.** Is the ingest Worker in `relayhistory`, `shadow` or
  `frozen-supabase` mode? Is the privacy authority `fenced` or `active`? Both gate Phase C only.
- **U3, backlog size.** How many committed claims still have a non-redacted stage? This decides
  Phase A's reclaim. The census (§7) answers it read-only.
- **U4, imported custody.** Do any imported-kind `ingest_custody_records` exist? Inferred **no**,
  because `ingest_lazy_policy_set` refuses when `ingest_custody_imports` has rows and lazy mode is
  evidently live (15.6M slots). Not verified.
- **U5, current-legacy share.** How many slots still point at their live-witness custody row? Not
  derivable from source.
- **U6, Babysitter routing.** Is Babysitter routing on in production (`babysitterRetention`),
  and how large is `babysitter_evidence_index`? This affects only the reclaim (claims with evidence
  are kept). Correctness no longer depends on it, because of the interlock in §3.5.
- **U7, Supabase capability.** Is `pg_repack` available on this project and plan, and can the
  provisioned disk be reduced? Without downsizing, reclaim means headroom, not a smaller disk.
- **U8, ACK cost of Phase B indexes.** What extra latency do they add under the global seat-clock
  lock? This must be measured after 043500 by pipeline-lead.
- **U9, `ingest_receipt_record_heads` / `ingest_receipt_ord_seats` populated?** Lazy mode never
  writes them (the trigger branches to `ingest_lazy_ack_batch`). Whether production ever ran the
  non-lazy path is unknown. The Phase B indexes cover them either way.
- **U10, later migrations.** pipeline-lead's 043500 and any slot between 043000 and the assigned
  one may add readers of these tables. The reader inventory in §1 must be re-checked against
  `main` at slot-assignment time.
