# History 620 operations: kill switch, rollback and reapply

Covers `supabase/migrations/20261105026000_canonical_history_lineage.sql` (620) and its generated
rollback `supabase/rollbacks/20261105026000_canonical_history_lineage.down.sql`. Every step here is an
owner (migration role) operation. None of it is run by the migration itself, and none of it is approval
to run anything against a real database: applying, rolling back or switching a production database
still needs Khaliq's explicit OK.

## What "managed" means

A session is managed by 620 only when it has an **installed** current authority
(`sessions.ingest_history_scope_current`) and the kill switch is live. That single gate
(`sessions.ingest_history_scope_managed` / `ingest_history_scope_required`) controls every effect 620
has on legacy state: invalidation, `ingest_history_required_scopes`, directory summary/provenance and
work-state clears, replay-epoch advances, `session_publication_is_managed`, `session_brief`,
`claim_session_large_read`, `session_directory_history_summary` and the replay snapshot's `history`
member. A build in flight, an abandoned or expired build, or a stray `required_scopes` row never gates
anything. A live ACK or control that touches a build in flight only fences it: the build row moves to
`abandoned`, its install refuses with `history: scope build lease lost`, and the builder starts again.

Lock order on every live path: user fences, then the receipt seat clock
(`sessions.ingest_receipt_seat_clock`), then the switch row, then build rows (sorted), then per-scope
advisory locks (sorted). The install only try-locks the scope, and begin and install take the switch row
before any build row.

Since 043500 the lazy batch claim reads the seat clock `FOR KEY SHARE` (claims no longer exclude each
other there); writers that must exclude claims (`set_live`, control changes, `lazy_clear`, seat
initialization) still take it `FOR UPDATE`. After the fence lock and the clock read, and
before any write, each claim takes exactly one transaction advisory lock keyed
`'96bulls:lazy-native-origin-v1'` per (org, workspace, origin).

Since 043800 the staged ACK (`ingest_lazy_ack_batch`) takes the clock `FOR NO KEY UPDATE`: it writes
`last_seat` only, so staged ACKs still serialize with each other and with every `FOR SHARE` /
`FOR UPDATE` holder, but a claim's `KEY SHARE` no longer queues behind an in-flight ACK. Right after
the clock, and before any stage-row or native-slot lock, the staged ACK takes the same
`'96bulls:lazy-native-origin-v1'` advisory lock as the claim for its one (org, workspace, origin).
The ACK lock order is therefore fence -> clock -> origin -> slot, the same as the claim's, so an
ACK and a same-origin claim of another user cannot deadlock on native slots. A same-origin claim
that runs while a staged ACK is in flight waits for that ACK's transaction on the origin lock.

## Kill switch (usable at any time, including after installs)

```sql
SET lock_timeout='5s';                           -- see the note below: the function's own 5s wins
SELECT sessions.ingest_history_set_live(false);  -- off
SELECT sessions.ingest_history_set_live(true);   -- on again
```

Both calls fail with 55P03 (lock_not_available) when contended; nothing has changed, so retry.
`set_live(true)` takes the seat clock first and then `session_index` SHARE with **NOWAIT**. Main
takes those two locks in both orders (the lazy batch claim: seat clock `FOR KEY SHARE`, which still conflicts
with `set_live`'s `FOR UPDATE`, then a `session_index` write;
a lazy control: `session_index`, then the seat clock), so resume never waits for `session_index`. It
fails at once with `history: session_index has an in-flight writer; nothing changed, retry
ingest_history_set_live(true)` while any indexer write is uncommitted. Retry in a loop with a short
pause until it succeeds. A call that asks for the state already in place returns `changed:false`
without taking any lock.

`lock_timeout` is a `SET` clause on `set_live` and `set_pointer_tracking`, so inside them it is always
5s. It overrides a shorter (or longer) timeout set by the caller. The caller's own setting applies only
to statements outside the function.

If `set_live(true)` keeps returning 55P03, something is holding a lock on `session_index` that
conflicts with SHARE. Look in `pg_stat_activity` and `pg_locks` for:
- autovacuum or `ANALYZE` on `sessions.session_index`, or a `CREATE INDEX CONCURRENTLY` on it (each
  holds SHARE UPDATE EXCLUSIVE);
- a long-running indexer transaction.

NOWAIT never cancels autovacuum: wait for it to finish, or pause the indexer briefly, then retry.

```sql
SELECT a.pid, a.state, a.backend_type, l.mode, l.granted, left(a.query, 120) AS query
  FROM pg_locks l JOIN pg_stat_activity a USING (pid)
 WHERE l.relation = 'sessions.session_index'::regclass;
```

There is also a known, pre-existing three-way 40P01 between `set_live(false)`, a running
`ingest_history_pointer_ref_backfill` page and a lazy selection/reset control. PostgreSQL's deadlock
detector resolves it by aborting one of them. It only involves operator calls, and nothing is left
half-done, so retry the aborted call (the switch or the backfill page).

- **Off:** every in-flight build is abandoned; every 620 trigger (ACK, control, directory,
  session_index) returns immediately; every read gate behaves as main; `ingest_history_scope_begin`
  and `ingest_history_scope_install` refuse with `history: 620 is switched off`. Pointer tracking stops
  too, so `refs_complete` is cleared and the inventory is no longer attested. All ledgers stay:
  scope versions, current rows, derived objects, the feature journal, outboxes and builds.
- **On:** takes the seat clock, then SHARE on `session_index` with NOWAIT. If a writer from the off
  period is still uncommitted, it refuses with 55P03 and you retry. It never restores
  `refs_complete`, so rerun the full pointer backfill before any inventory work. Every in-flight build is abandoned again (one that raced the switch saw untracked changes),
  and every installed scope is invalidated in the same transaction (ACKs and controls while off
  were not tracked), so its directory row is cleared and its next build re-derives it. This is five
  set-based statements over all installed scopes (`ingest_history_invalidate_installed`), not a
  per-scope loop; indexer writes wait for it behind the SHARE lock.

## Lock-wait alert (55P03 / 57014)

The ingest Worker writes one extra log line when a buffered lazy claim or a staged ACK fails with
SQLSTATE 55P03 (`lock_not_available`, e.g. the 2s `lock_timeout`) or 57014 (query cancelled;
usually the statement timeout, but not provably so): `route` = `ingest.lock_alert`, `stage` =
`lazy_claim` (`ingest_claim_buffered_batch`) or `staged_ack` (`ingest_commit_buffered_batch`),
and `sqlstate`. It carries no tenant data and no ids. The request's own 503 line is unchanged
(`route` `delivery.batches` / `turns.batches` / `delivery.recovery`, `outcome`
`delivery_unavailable`, `stage` `claim` or `buffer_receipt`, `error_code`, `db_error_code`).
Since 043800 it also fires with `stage` = `staged_intent` for the buffered stage-intent call
(`ingest_prepare_buffer_stage`, deliver stage `buffer_intent`), which takes no clock lock but waits
on the fence and batch rows a staged ACK holds. This line covers only those three calls. Control changes and `lazy_clear` run nested inside the
ordered selection/reset SQL (`ingest_apply_selection` / reset and the history-control triggers);
their 55P03/57014 currently reach the Worker's selection/reset routes as a generic unavailable
error without the SQLSTATE, so this alert line does NOT cover them. A follow-up PR will preserve
the closed SQLSTATE through that conversion and emit `ingest.lock_alert` with stage tags
`control_changed`, `lazy_clear`, `set_live`, plus an exact attempt/success counter at the commit
call site. Until then those paths are observable only in the database/operator log.

Alert when any of these is true:

- commit lock-alert request rate above 1% over 5 minutes (numerator: `ingest.lock_alert` lines
  with `stage=staged_ack`; denominator: buffered requests that reached `buffer_receipt`, i.e.
  successful batch/recovery lines plus 503 lines with `stage=buffer_receipt`). This is a request
  rate, not an exact RPC failure rate: the denominator includes replay/suppressed successes that
  never called `ingest_commit_buffered_batch`;
- any 55P03 on `set_live`, `control_changed` or `lazy_clear`, read from the database/operator log
  (not from the Worker line) until the follow-up lands;
- more than 1% of claim-path requests return 503 over 5 minutes (`outcome=delivery_unavailable`
  with `stage=claim`, divided by all batch, turns and recovery requests).

Per-stage latency (default off): with `INGEST_STAGE_LATENCY_LOG=on` the Worker writes one
`route` = `ingest.stage_latency` line per deliver stage with `stage`, `outcome` (`ok`, `refused`,
`unavailable`), `duration_ms` and, for a database failure, `sqlstate`; no tenant data, no ids.

Claim lock-wait busy (default off): with `INGEST_CLAIM_LOCK_BUSY=on` a claim (any acceptance
mode) whose statement loses its lock wait (55P03 or 40P01, `error_code` `ledger_lock_timeout`)
answers 429 `delivery_busy` with `Retry-After` 5-10 s instead of the 503. The lower bound is the
role `statement_timeout` (5 s), so the retry lands after the holding claim has committed or been
cancelled. The statement rolled back, so nothing was written; the retry is the same idempotent claim.
The flag changes only the HTTP answer: the async admission consumer still retries a claim lock wait
with the normal admission backoff (15, 30, 60 ... 300 s by attempt), with the same `error_code`
`ledger_lock_timeout`, `stage` `claim` and `db_*` fields on its `ingest.admission` retry line. The
route line keeps `stage` `claim`, `error_code` and `db_error_code`, with `outcome` `delivery_busy`.
The `ingest.lock_alert` `lazy_claim` line still fires for 55P03 (buffered mode) only: the lock
alert set is 55P03 and 57014, so a 40P01 (deadlock) turned into 429 emits no lock alert and shows
only as `outcome=delivery_busy` with `stage=claim` and `db_error_code` `40P01`. The
`ingest.stage_latency` line (if enabled) still records the converted claim as `outcome`
`unavailable`, because it is observed before the conversion. 57014 (`ledger_timeout`) and every
other cause stay 503. With the flag on, the 503 claim-path alert above no longer counts 55P03 or
40P01. Watch `outcome=delivery_busy` with `stage=claim` (all claim lock waits), or the `lazy_claim`
lock alert (55P03 only). Unset or `off`: unchanged. The Cloudflare stack renders the var from the
stack input `sessionIngest.claimLockBusy: "on"` (canonical or buffered acceptance; omit for `off`,
which renders no var). Operator note: if the deploy actor previously set `INGEST_CLAIM_LOCK_BUSY`
out of band as a Worker version secret of the same name, it must remove that secret before the
stack-rendered var applies, or the apply conflicts.

Claim statement-timeout busy (default off, independent of `INGEST_CLAIM_LOCK_BUSY`): with
`INGEST_CLAIM_TIMEOUT_BUSY=on` (parsed like `INGEST_CLAIM_LOCK_BUSY`: only `on`, any case, trimmed)
a claim whose statement hit the role `statement_timeout` or was cancelled (`error_code`
`ledger_timeout` with SQLSTATE 57014 by code only, i.e. `db_error_code` `57014` and `db_error_basis`
`code`; name-only client timeouts such as a driver/pool `TimeoutError` also log `ledger_timeout` but
stay 503) answers the same 429 `delivery_busy` with `Retry-After` 5-10 s instead of the 503.
Proven: production claim-stage 503s are dominated by 57014 at about 5 s; the role runs
`statement_timeout` 5 s and `lock_timeout` 2 s (`supabase/bootstrap/session_ingest_role.sql`); the
claim is one autocommit statement (`src/session-ingest/ledger-sql.ts`, `call`). Hypothesis, pending
operator `pg_stat_activity` / lock / plan evidence: because `lock_timeout` applies to each lock
acquisition separately, a claim queued behind several same-user claims on the per-user fence row
waits several times (each under 2 s) and reaches the 5 s `statement_timeout` first. This is
unproven: a reviewer's controlled PG15 probe (4-8 concurrent callers on one `FOR UPDATE` row,
`statement_timeout` 5 s, `lock_timeout` 2 s) produced shallow successes plus deep 55P03s and no
57014. The flag does not depend on the cause: a 57014 (timeout or `pg_cancel_backend`) rolls back
the single autocommit claim statement just like 55P03, so nothing was written and the retry is the
same idempotent claim;
57P01/57P02 (terminate, shutdown) and 08xxx are other codes and stay 503. Only `stage=claim`
converts: 57014 at `buffer_intent`, `buffer_receipt`, `slot`, `commit` and every other stage stays
503. The route line keeps `error_code` `ledger_timeout` and `db_error_code` `57014` with `outcome`
`delivery_busy`, so it stays separable from the 55P03 lock wait (`ledger_lock_timeout`); the
`ingest.lock_alert` `lazy_claim` line still fires for 57014 (buffered mode); the admission consumer
is unchanged (normal admission backoff). Caveat: a claim that is slow without contention also
yields 57014 and would retry as 429 instead of 503; watch `outcome=delivery_busy` with
`error_code=ledger_timeout` per tenant. Unset or `off`: unchanged. The Cloudflare stack renders
the var only from the stack input `sessionIngest.claimTimeoutBusy: "on"` (canonical or buffered
acceptance; omit for `off`, which renders no var; any other value is refused). The hosted Worker
sees the flag only when the stack sets that input; without it the flag is inert in hosted prod. The
same out-of-band Worker version secret note as for `INGEST_CLAIM_LOCK_BUSY` applies. Not read by
the self-host server (follow-up).

Buffer-stage lock wait (two flags, both default off, buffered acceptance only; stack inputs
`sessionIngest.bufferLockBusy: "on"` / `sessionIngest.bufferLockRetry: "on"`, omit for off): they act
only on a `buffer_intent` / `buffer_receipt` ledger call that failed with `error_code`
`ledger_lock_timeout` (55P03 / 40P01). `INGEST_BUFFER_LOCK_RETRY=on` re-issues that call once after
50-250 ms and writes one warn line per retry, `route` = `ingest.buffer_lock_retry`, with `stage`,
`db_error_code` (the SQLSTATE that caused the retry) and `outcome` `ok` or `failed` (then also the second
failure's `error_code` and `sqlstate`); no tenant data, no ids. A retry that succeeds leaves no other trace
(no lock alert, a normal route line), so watch this line when the flag is on. `INGEST_BUFFER_LOCK_BUSY=on`
answers a lock wait still standing with 429 `delivery_busy` (`Retry-After` 5-10 s) instead of the 503. The
DB-error rate alert counts only 503s, so for a converted wait the `ingest.lock_alert` `staged_intent` /
`staged_ack` line fires for 40P01 as well as 55P03 (a 40P01 that stays 503 is unchanged). Time bound: one
attempt that ends in 55P03 / 40P01 can take up to the 5 s `statement_timeout` (the 2 s `lock_timeout` is per
lock acquisition), so a retry adds at most about 5.25 s per stage and about 10.5 s when both stages retry.
On the private-ingest release both are cutover controls exactly like `claimTimeoutBusy` (never tracked
in the public profile): the `private-ingest-production` environment variables
`PRIVATE_INGEST_BUFFER_LOCK_BUSY` / `PRIVATE_INGEST_BUFFER_LOCK_RETRY` accept only `on`; unset or empty is
off and renders no var; any other value refuses (`private_ingest_profile_buffer_lock_busy` /
`private_ingest_profile_buffer_lock_retry`). Setting one and then planning and deploying the private-ingest
Worker is a production flag change that needs its own approval. A deploy may add or remove them at the live
phase; rollback is unset (or empty), then plan and deploy, no code change.

In-flight lease before the claim (default off; stack input `sessionIngest.inflightLease: "observe"` or
`"on"`, omit for off): `INGEST_INFLIGHT_LEASE` keeps a per-isolate, in-memory lease per signed (org,
workspace, user) on the HTTP sync delivery path (`inflight-lease.ts`; an empty workspace id is its own key).
Unset, empty or any other value is off: the request path never enters it. The lease is taken in
`acceptBatch` after the per-org HTTP stop and the body parse, before the first call that can take a ledger
or fence lock, and released when the delivery ends (success or error). `observe` refuses nothing; a delivery
that `on` would refuse proceeds and writes one line, `route` = `ingest.inflight_lease`, `outcome` =
`would_refuse`, `state` = `observe`, `kind` = the request route, `reason` = `tenant_inflight`, `count` = the
live holders the key already had, plus the request line's own `org_id`, `workspace_id`, `correlation_id` and
client fields. `on` answers that delivery 429 `delivery_busy` (`Retry-After` 2-10 s, jittered) before any
ledger call and writes the same line with `outcome` = `refused`; its request line shows `delivery_busy`. It
is not a database-lock 429: there is no `ingest.lock_alert` line for it, and the DB-error rate alert counts
it in the denominator only, like the slot limit's 429. A holder whose release never ran expires after 90 s
(the slot TTL); if one isolate's map is full of live holders the lease fails open and writes `outcome` =
`fail_open`. Limits: per isolate, so it only sees overlaps that land in the same isolate (observe first to
measure that share); HTTP sync path only: the queued ACK, the admission consumer and the self-host inbox
path are never leased. Clients that ignore `Retry-After` keep their own backoff; each refused retry is cheap
because it costs no ledger call. On the private-ingest release it is a cutover control like the buffer-lock
flags: `PRIVATE_INGEST_INFLIGHT_LEASE` accepts `observe` or `on`; unset or empty is off and renders no var;
any other value refuses (`private_ingest_profile_inflight_lease`). Setting it, planning and deploying is a
production flag change that needs its own approval; rollback is unset (or empty), then plan and deploy, no
code change.

Advertised records per batch (default off; stack input `sessionIngest.advertisedMaxRecords`, an integer
1..500, omit for off): `INGEST_ADVERTISED_MAX_RECORDS` changes only the `maxRecords` the Worker publishes on
`GET /v1/delivery/limits` and in every receipt (`advertised-limits.ts`). Acceptance stays
`MAX_DELIVERY_RECORDS` (500) at every gate, so lowering it refuses nothing and strands no batch an uploader
already persisted: such a batch is retried with the same body and accepted. Uploaders that follow the
published limit prepare their NEXT batches at the lower size after their next receipt or limits request; any
other uploader keeps sending up to 500, still accepted. Which clients follow it is a source reading, not a
verified rollout: desktop-core's delivery jobs apply it through `DeliveryLimits::with_published`, and
relayhistory sdk-ts through `exportLimitsFromPublished`; no released client version is asserted here and client
behaviour was not tested end to end, so confirm from client telemetry (batch sizes) after setting it.
Why: each staged commit holds the global receipt seat clock for a time roughly linear in its records, and a
waiting commit fails 55P03 once the queue ahead of it outlasts the 2 s `lock_timeout` in one lock stage;
smaller batches shorten each hold (more commits, about the same clock time per record). A value that is not a
strict integer in 1..500 is ignored (500 published) with one warn line per Worker isolate (not per request),
`error_code` `advertised_max_records_invalid`; it never refuses startup. On the private-ingest release it is a cutover
control like the buffer-lock flags: `PRIVATE_INGEST_ADVERTISED_MAX_RECORDS` accepts decimal 1..500, unset or
empty is off and renders no var, anything else refuses (`private_ingest_profile_advertised_max_records`).
Setting it, planning and deploying is a production change that needs its own approval; rollback is unset
(or empty), then plan and deploy, no code change.

Claim lock wait (off unless set; production sets it): with `INGEST_CLAIM_LOCK_WAIT_MS` (25..2000; stack input
`sessionIngest.claimLockWaitMs`) each claim attempt runs in its own short transaction, `BEGIN;
SELECT set_config('lock_timeout', '<ms>ms', true), set_config('idle_in_transaction_session_timeout',
'1000ms', true); <the same claim statement>; COMMIT` (ROLLBACK on any error;
`src/session-ingest/ledger-sql.ts` `inClaimLockWait`). The claim function, its input and its
lock order are unchanged; only the wait is shorter. A claim queued behind the same user's in-flight
claim on the per-user fence row fails with 55P03 after `<ms>` (`ledger_lock_timeout`, rolled back,
nothing written), which with `INGEST_CLAIM_LOCK_BUSY=on` is the 429 `delivery_busy` with Retry-After
5-10 s, instead of waiting up to 2 s per lock and stacking into the 5 s `statement_timeout`. A free
lock: the same claim, committed. `SET LOCAL` ends with the transaction, so it never reaches another
use of a pooled connection. It needs a `run` bound to one connection used sequentially (the
Worker's per-request and per-queue-batch pg Client); a pooled `run` must not set it. Production
sets 250 ms with `claimTimeoutBusy` and `claimLockBusy` on (2026-11-16, customer upload retries).
`claimTimeoutBusy` and `claimLockWaitMs` are cutover controls of the private-ingest release contract
(like the fence mirror and the stop list), never tracked in the public profile: set the
`private-ingest-production` environment variables `PRIVATE_INGEST_CLAIM_TIMEOUT_BUSY=on` and
`PRIVATE_INGEST_CLAIM_LOCK_WAIT_MS=250`, then plan and deploy the private-ingest Worker. A deploy may
add, change or remove them at the live phase.
Watch `outcome=delivery_busy` with `error_code=ledger_lock_timeout` per tenant: a rise is expected
and means fast back-off, not failure. Residual risk: the claim is now a multi-statement transaction,
so a Worker that dies between the claim and its COMMIT/ROLLBACK leaves the transaction open, holding
the user's fence row until the server ends it; the local `idle_in_transaction_session_timeout` of
1000 ms (instead of the role's 10 s) bounds that window, and a dropped connection ends it at once. During
it that user's claims answer 429 and retry. Rollback: unset (or empty) `PRIVATE_INGEST_CLAIM_LOCK_WAIT_MS`
and/or `PRIVATE_INGEST_CLAIM_TIMEOUT_BUSY` in the `private-ingest-production` environment, then plan and
deploy the private-ingest Worker, the same steps as a stop-list change (no var rendered: the claim
returns to the single autocommit statement and 57014 to the 503). No code change is needed. Only `stage=claim`
is affected: `buffer_intent` and `buffer_receipt` run after the claim has committed its attempt, so
a short lock wait there would throw away a successful claim and force a resume; they keep the role
`lock_timeout` and stay 503 on a lock or statement timeout.

The rules above are still on paper: none of them has a sink. The one alert wired in code is the
coarser "Ingest DB-error alert" below (DB-error 503s above 20% over 2 minutes). The lock-wait rules can
be added later as more custom alerts of the same kind, using the fields `route`, `stage`, `sqlstate`,
`outcome` and `db_error_code`. A sustained `lazy_claim` 55P03 with a single origin
lock holder is expected same-origin serialization (see the lock order above): check the
uploader for overlapping same-origin uploads before treating it as an incident.

### Route-line refusal class and client (diagnostic only)

A ledger refusal with SQLSTATE 55000 (`error_code` `ledger_sqlstate_55000`, `db_error_code` `55000`;
`db_error_class` stays `unknown`) also carries `db_refusal`, a closed class taken from the start of the
database message (`src/session-ingest/db-refusal-class.ts`; the message itself is never logged):
`generation_not_ready`, `policy_unavailable` (the custody policy is at the other generation),
`policy_expired`, `org_admission_closed`, `org_prior_authority_unbound`, `user_privacy_authority`,
`ack_seal`, `native_custody_identity_changed`, `source_row_privacy_fenced`,
`custody_privacy_inconsistent`, `user_seal_missing`, `receipt_proof_missing`, `publication_transition`,
or `other` (any other 55000 text). In buffered mode a call refused at both accepted generations logs the
first refusal that is not a generation refusal (`generation_not_ready`, `policy_unavailable`), so a real
refusal at one generation is not hidden behind the other's `generation_not_ready`. A sustained
`generation_not_ready` or `policy_unavailable` points at the schema generation overlap; any other class
is a per-org or per-user state to investigate.

### `org_prior_authority_unbound`: org admission repair

A sustained `org_prior_authority_unbound` for an org means `ingest_lazy_begin` found prior rows for that org and
will refuse every claim until the org is admitted. If those rows are only seed-epoch counters, admit the org through
`.github/workflows/ingest-org-admission-repair.yml` (design: `docs/design/zero-503-schema-windows.md` 3.4). Each
dispatch is from `main` at the exact head SHA and needs Khaliq's approval on `session-schema-production`. Pass the
org ids only as the `orgs` input, comma-separated, and use the same list for every step. Never commit org ids.

1. **readback** (allowed while dormant: with the flag off, plan and admit cannot write; readback still takes the
   migrator advisory lock for its scan). For each org index, it reports whether the org is admitted and which
   tables the live scan sees rows in, never row counts. Continue only if every org shows only
   `ingest_seed_source_epochs` and `drift` is `null`. Also check that every id passed the format check; an id
   outside `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$` is refused and needs a reviewed widening first.
2. **Enable.** Set `ORG_ADMISSION_REPAIR_ENABLED` to `true` in a reviewed one-line change, and set it back to
   `false` after the repair.
3. **plan.** This refuses if any org is admitted, has rows in another table, or has no rows, or if the policy is
   expired or not at the PASS generation. From the 1-day plan artifact, copy the
   `ADMIT-ORGS <project> <sha> GENERATION <g> VALID_UNTIL <v> ORGS <d> COUNT <n> PLAN <run>` confirmation.
   Plan and admit must run at the same `main` head: ask `layered-sessions-pipeline` to hold landings from the
   enable commit until the admit has run, and wait for push CI on the enable commit before planning.
4. **admit** with the same `sha`, the same `orgs`, `plan_run` and that confirmation, within 24 h. Any drift since
   the plan refuses and writes nothing. That includes a renewal, a new admission, a new table, or new rows outside
   the allowlist. If it refuses, run readback and plan again. Expect a brief claim stall during the admit: it holds
   `ingest_buffer_admission` in `SHARE ROW EXCLUSIVE` for the scan, the inserts and the guard calls, so every
   `ingest_lazy_begin` (its `INSERT ... ON CONFLICT`) waits behind it, as with renewal.
5. **Verify.** Check that the receipt says `ORG_ADMISSION_REPAIRED` with `committed: true` and that
   `org_prior_authority_unbound` stops for those orgs. The clients' pending claims then apply on retry.

This repairs only the orgs you list, at most 50 per dispatch: a stop-gap. 040000 backfilled
`ingest_seed_source_epochs` from every `session_index` row, so any org with session history and no admission row
hits the same loop. The class fix, a forward migration that removes `ingest_seed_source_epochs` from the scan, is
separate. After it lands, this operator refuses with `org_admission_repair_scan_drift`.

Every line built from the batch route's fields (`route` `delivery.batches` and `turns.batches`: the
outcome line, and the capture-policy and upload-consent lines emitted from that route) carries
`client_kind` from the request User-Agent (`src/session-ingest/client-agent.ts`). The Desktop sends
`agent-relay-desktop/<version> <os>`: that is `client_kind` `desktop` with `client_os` `macos`,
`linux`, `windows` or `other`, and `client_version` only when it is a release version such as
`2026.11.12` or `2026.11.12-beta.1`. Any other User-Agent is `client_kind` `other` and nothing more;
User-Agent text is never logged. Use it to split refusals and busy answers by Desktop release.

## Ingest DB-error alert (DB-error deliveries above 20% for 2 minutes)

Defined in code: `src/session-ingest/ingest-error-alert.ts` (query, policy body, plan/apply), applied by
`.github/workflows/ingest-error-alert.yml` (`scripts/ingest-error-alert.ts`, gate
`scripts/ingest-error-alert-github.ts`), tests `src/session-ingest/__tests__/ingest-error-alert.test.ts`.
It only reads the Worker's existing log lines. The Worker, its tracked profile, the private-ingest release
contract and its readback digests are untouched, and the private-ingest deploy never applies it.

- **Signal.** Workers Logs of the private Worker `96bulls-session-ingest`, one terminal line per request on
  `route` `delivery.batches`, `turns.batches` or `delivery.recovery`. Numerator: `outcome`
  `delivery_unavailable` (the 503) with a `db_error_class` from the closed set (`terminated`, `reset`,
  `timeout`, `pool`, `tls`, `unknown`; `src/session-ingest/db-error-class.ts`), unless `db_refusal` is one of
  the known per-org or per-user 55000 classes (`user_seal_missing`, `org_admission_closed`, ...: every
  class except `generation_not_ready`, `policy_unavailable` and `other`), so one stuck user cannot page. An
  unrecognised 55000 (`other`, for example a reworded or new fence message) counts, so a fleet-wide fence
  still pages. A test pins every known prefix to a 55000 `MESSAGE` in the latest function definitions in
  `supabase/migrations`, so a reworded message fails CI instead of silently changing class.
  Denominator: every line on those routes except the capture-policy report lines that reuse the route
  (`outcome` `missing`, `stale`, `session_predates`). An absent attribute reads as `''` on both sides
  (`ifNull`), and the TS model in the module (`countsAsDbErrorDelivery`) is tested against real Worker lines.
- **Blind spots (not counted).** A 503 without a DB diagnosis (object store, custody, admission). A claim
  turned into 429 `delivery_busy` by `INGEST_CLAIM_LOCK_BUSY` or `INGEST_CLAIM_TIMEOUT_BUSY` (designed
  back-pressure: denominator only); with production's flags on, that is most claim-stage 57014/55P03. With
  `asyncAdmission` on, a queued delivery answers `accepted_queued` and its DB errors land on the consumer's
  `ingest.admission` lines, which this alert does not read. Control routes (selection, reset, custody).
- **Workers Logs.** The query needs Workers Logs on for the Worker. `stacks/cloudflare/session-ingest.ts`
  sets no `observability`, so Alchemy's default applies (`DEFAULT_OBSERVABILITY` in
  `@alchemy.run/cloudflare` `Workers/WorkerAsyncBindings.ts`: `{ enabled: true, logs: { enabled: true,
  invocationLogs: true } }`, no head sampling). The test pins that the stack sets no override and that the
  installed Alchemy's default block is still that; it does not read the live Worker's settings. An
  observability override in the Worker profile must keep logs on and unsampled or this alert under-counts.
  Setting it explicitly on the Worker props changes the Worker profile and its release digests: a separate
  follow-up through the private-ingest release, not part of this alert.
- **Threshold and window.** Fires when `toFloat64(countIf(DB-error 503)) / greatest(count(), 20) > 0.2`
  over the last 2 minutes (`timestamp >= NOW() - INTERVAL '2' MINUTE`, threshold short window `2m`). It is evaluated every
  minute and re-notifies every 30 minutes while firing. The floor of 20 means a quiet window needs at least
  5 DB-error 503s to fire.
- **Mechanism.** A Cloudflare Notifications custom alert (`alert_type` `custom_analytics`, `source` `sql_api`,
  threshold trigger) named `96bulls-session-ingest-db-error-rate`. It is a SQL API "Monitor" (beta).
  Cloudflare's built-in Workers analytics count only exceptions and exceeded resources as errors. A
  deliberate 503 is a success there, so no built-in Worker error-rate alert can express this.
- **Destination.** One generic webhook destination, `96bulls-ingest-alerts-agent-relay`, that posts into Agent
  Relay. Cloudflare sends the destination secret only as the `cf-webhook-auth` header. An Agent Relay inbound
  hook (`POST /v1/hooks/<id>`) requires `Authorization: Bearer <token>` and answers 401
  `webhook_token_required` otherwise. Cloudflare also test-POSTs the URL when the destination is created and
  refuses one that does not return 2xx. So a Relay hook URL cannot be the destination as-is. One of these
  must come first, and both need Khaliq's OK:
  - Agent Relay accepts `cf-webhook-auth` as the hook token. This is a small change in
    relaycast `packages/engine/src/routes/inboundWebhook.ts`, in a separate repo, and is the recommended
    path.
  - A minimal adapter receives the Cloudflare POST, checks `cf-webhook-auth`, and forwards
    `{text, source: "cloudflare", payload}` with the Bearer token. It is not built here.

  The Cloudflare payload's `text` field becomes the Relay message text.

**Deploy (guarded; Khaliq's OK for every step that creates or changes something).**

1. Operator, once (Khaliq OK): create the Agent Relay inbound hook for the channel or agent that should
   receive the alert. Use `POST /webhooks` on the desktop socket or relaycast `POST /v1/webhooks`. Store its
   token only where the bridge needs it. Never put it in a Relay message, the repo or argv.
2. Operator, once (Khaliq OK): resolve the auth mismatch above.
3. Operator, once (Khaliq OK): create the Cloudflare destination `96bulls-ingest-alerts-agent-relay` with
   `POST /accounts/<account>/alerting/v3/destinations/webhooks` (`name`, `url`, `secret`), or in the
   dashboard under Notifications > Destinations. Cloudflare must report its type as `generic`.
4. Operator, once (Khaliq OK): create the protected GitHub environment `ingest-alerting-production`. It allows
   deployments only from `main`. Add Khaliq as required reviewer if the repository plan allows it; it did not
   on 2026-11-04 (GitHub 422, `runbooks/guarded-session-schema-release.md` "Operator read-only plan while
   reviewer billing is blocked"). Without a reviewer rule the gate falls back, like the schema release, to
   the first-attempt dispatch by Khaliq's own pinned User account; with one, a recorded approval is
   required and a rejection refuses. Add the secret
   `CLOUDFLARE_ALERTING_API_TOKEN`: an account API token with Notifications edit and read on this account
   only. Add the variable `INGEST_ALERT_WEBHOOK_ID`: the destination id from step 3.
5. Precondition, recorded before any apply (kept in the apply handoff). In the dashboard, open Alerts >
   Monitors > create > custom SQL on `logs.workersLogs` and preview the query (read-only), widening only the
   `timestamp` bound:
   - over a busy hour, `count()` > 0 with the same `WHERE` (the attribute lookups resolve for this Worker's
     lines; the SQL API introspection endpoint with `include_attributes=true` lists the indexed keys);
   - over a known failure window, for example 2026-11-16 15:43-16:19Z, `value` well above 0 (the numerator
     matches real DB-error 503s, and `toFloat64`, `countIf`, `ifNull` and `greatest` are accepted).
   Record both results (window, count, value). If either fails, fix the query in a reviewed change; do not
   apply. Cloudflare also validates the query on save (`InvalidAlertQuery`), so a malformed query fails the
   apply, but a query that parses and never matches would be a silent alert: that is what this step rules out.
6. Dispatch `ingest error alert` with `mode=plan` and `sha=<main head>`. This is read-only, but its job still
   runs in the protected environment, so it waits for the same environment approval. It checks that the
   destination exists, is generic and has that name. It diffs the live policy by name and prints `action`
   (`create`, `update` or `noop`), the drifted field names, the body `digest` and the exact confirmation. A
   policy list Cloudflare reports as more than one page refuses (`ingest_error_alert_paginated`).
7. Dispatch `mode=apply` at the same sha with confirmation `APPLY-INGEST-DB-ERROR-ALERT <sha> BODY <digest>`.
   Khaliq must approve the environment. The apply re-plans, refuses a different body, writes the one policy,
   reads it back and refuses any drift. A refusal after the write (lost response, readback drift) can leave
   the new or updated policy live: re-plan to learn the true state before anything else. A later change to
   the threshold or the query is a reviewed PR plus the same plan and apply.
8. Verify delivery once: in the dashboard, use the policy's test notification, or temporarily lower the
   threshold in a reviewed PR. Confirm that the message arrives in Agent Relay.

Rollback: disable or delete the policy in the dashboard. The next plan will report it as drifted or missing.

**When it fires.** The Worker is answering 503 to more than one delivery in five because the database is
failing. Clients keep their batches and retry, so nothing is lost while it lasts, but uploads stall.

1. Split the window by `db_error_code`, `db_error_class` and `stage` in Workers Logs:
   - `57014` or `55P03` on `stage=buffer_intent` or `buffer_receipt`: lock waits or statement timeouts on the
     staged path (claim-stage ones are mostly 429 `delivery_busy` under the claim-busy flags and are not
     counted). See "Lock-wait alert" above.
   - `pool`, `terminated` or `reset`: connection capacity or a pooler/Hyperdrive drop.
   - `55000` on every request: generation fence (`db_refusal` `generation_not_ready` or
     `policy_unavailable`, see "Route-line refusal class and client" above). Uploads fail closed because the
     database generation and the Worker's accepted generations disagree. `other` on every request is
     probably a reworded or new fence message: treat it as the fence. Any other `db_refusal` class is a
     per-org or per-user state, not the fence (not counted).
   - The separate `session ingest db error` line comes from the pg client's `error` event, not the route,
     and is not counted. It carries `db_error_detail` (closed message shape), `error_frame` (the first
     stack frame's function name only) and `db_client_phase`. `socket_closed` with
     `CloudflareSocket._listen` and `ending` is expected to be the client's own `end()` closing the
     socket after the response (inferred from the code path, not yet confirmed on a deployed Worker); in
     that case the response is unchanged. `active` means the socket failed during the request. The queue
     consumers' db error lines carry no `db_client_phase`.
2. Generation fence after a schema apply: readiness already moved to `(H, pass)` while the Worker offers
   only G. Follow `docs/design/zero-503-schema-windows.md` section 3.2, "Post-apply recovery": deploy the
   Worker at G+H with `OVERLAP <H>`, then dispatch the overlap workflow `rebind-dry-run` and `rebind`.
   Uploads resume at the rebind.
3. Generation fence before or during a release: check the release overlap gate
   (`runbooks/guarded-session-schema-release.md`, "Buffered ingest: required order (overlap gate)").
   A window that expired before the rebind fails closed the same way. Recover as in step 2.
4. Otherwise treat it as a database incident: check Supabase health, connections and `pg_stat_activity`.

## Pointer-reference tracking

The `session_index` pointer-reference trigger is dormant until armed, so the legacy indexer pays
nothing before History needs the inventory.

1. `SET lock_timeout='5s'; SELECT sessions.ingest_history_set_pointer_tracking(true);` Arming takes a
   brief SHARE lock on `session_index`, which waits out in-flight indexer writes, so no write that began
   before the arm can commit an untracked row after the backfill has passed it.
2. Page the backfill until it returns NULL. Each page locks its `session_index` rows (`FOR UPDATE`),
   so keep pages small (for example 500) in production:
   `SELECT sessions.ingest_history_pointer_ref_backfill(<cursor or NULL>, 500);`
3. Each page also prunes references in its key range whose `session_index` row is gone or lost its
   pointer (an untracked delete). The final page sets `refs_complete`. `ingest_history_inventory_claim`
   refuses until then, and it also refuses a carrier that still has a dangling reference.

Normal indexer writes resume immediately after arming or resume, before any backfill: a tracked
insert, update or delete replaces whatever stale reference is left at its key from an untracked period
(no unique violation) and queues every carrier involved for a rescan. The backfill is still what sets
`refs_complete`.

Disarming (`set_pointer_tracking(false)`) clears `refs_complete`. Re-arming requires a fresh full
backfill pass, which rewrites any reference left stale while disarmed.

## Constraint validation left for an operator (022000)

`20261105022000_session_desktop_catalog.sql` adds its four CHECKs `NOT VALID`, so the migration holds
no validation scan under ACCESS EXCLUSIVE. Validate later; this takes SHARE UPDATE EXCLUSIVE, and
readers and writers keep running:

```sql
SET lock_timeout='5s';
ALTER TABLE sessions.session_directory VALIDATE CONSTRAINT session_directory_catalog_read_version_check;
ALTER TABLE sessions.session_directory_relationships VALIDATE CONSTRAINT session_directory_relationships_child_model_check;
ALTER TABLE sessions.session_directory_relationships VALIDATE CONSTRAINT session_directory_relationships_child_agent_name_check;
ALTER TABLE sessions.session_directory_relationships VALIDATE CONSTRAINT session_directory_relationships_child_agent_type_check;
```

The 022000 partial index is still built inside the migration transaction, after the ALTER, so it is
built under that ALTER's lock. Building it `CONCURRENTLY` needs a non-transactional step kind in
`src/hoopdb/migrate.ts`, which this change does not add. Every `2026110502xxxx` file starts with
`SET LOCAL lock_timeout='5s'`, so a blocked DDL lock fails the file (it rolls back whole) instead of
queueing live traffic behind it. A lock_timeout failure during apply rolls that file back and the
migrator stops: the files before it stay applied, and the role-validation marker stays `pending` (the
Worker gate stays closed) until a rerun of the migrator gets through and passes validation. Rerun it at
a quieter moment; nothing needs to be undone.

## Schema rollback (down) and reapply

Use the down only when the kill switch is not enough, that is, before any authority was installed.

Once any later upstream migration has landed—including 027000 project events,
028000 semantic state, 029000 P3b reads or 030000 consumer progress—the 026000
down refuses until every later file has been rolled back in reverse order and
its ledger row removed. The 030000 named down removes its own row after safely
dropping the restartable producer-progress state.

The down **refuses** (nothing is dropped) when:
- any plan, scope version, derived object or model-work function exists
  (`history: retain authority/object ledgers; use sessions.ingest_history_set_live(false)`); or
- any upstream migration that sorts after 026000 is in `sessions.__96bulls_migrations`
  (`history: a later migration is applied; ...`). Deleting 026000's ledger row would leave a gap, and
  `sequencePreflight` would refuse every later migrate with `ledger_gap`.

Procedure:

1. Stop the 620/630 workers (scope builders, inventory, reconcile and privacy consumers).
2. Run the down file in one transaction as the owner. It drops 620's triggers, functions and tables,
   restores the eight replaced functions from their pre-620 definitions, and deletes the row
   `upstream:20261105026000_canonical_history_lineage.sql` from `sessions.__96bulls_migrations`.
   Re-derivable queues (reconcile and privacy outboxes, required scopes, in-flight builds) go with
   the schema.
3. To reapply, run the normal migrator. Because the ledger row is gone, it applies 026000 again, writes
   the ledger row, and the post-validation function-ACL check matches the committed manifest
   (`function_acl_drift` does not fire). Then repeat the pointer-tracking steps above.

`src/session-ingest/__tests__/history-ack-safety.pglite.test.ts` covers: down, then reapply, then a
live ACK, both dormant and after queued in-flight work; the later-migration refusal; the
installed-authority refusal; and the kill switch after install.
