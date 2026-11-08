# S3-first async admission: Supabase off the upload hot path

Status: implemented behind `INGEST_ASYNC_ADMISSION` (default `off`). Merging the code changes
nothing in production. Turning it on also needs the stack (PR #367), relayhistory and queue
retention steps listed under "Rollout".

Code:
- `src/session-ingest/async-admission.ts`: request path, inbox, privacy journal, outcomes.
- `src/session-ingest/admission-consumer.ts`: the queue consumer.
- `deliver.ts`: the `admissionGate` hook.
- `app.ts`, `worker.ts`: wiring.

Tests:
- `__tests__/async-admission.test.ts`: unit.
- `__tests__/async-admission.pglite.test.ts`: the real buffered SQL, every migration, including
  the causal privacy and loss scenarios.
- `stacks/aws/__tests__/admission-inbox-iam.test.ts`: the keys under the unchanged IAM grant.

## Problem

In buffered mode, `POST /v1/delivery/batches` (and `/v1/turns/batches`) makes four Supabase round
trips before it answers:
- the lazy claim `ingest_claim_buffered_batch` (per-user fence `FOR UPDATE`, seat clock `KEY SHARE`)
- the slot lease
- the stage intent `ingest_prepare_buffer_stage`
- the receipt ACK `ingest_commit_buffered_batch` (fence, seat clock `NO KEY UPDATE`, origin
  advisory lock)

Under `lock_timeout 2s` / `statement_timeout 5s` the waits fail with SQLSTATE 55P03 / 57014, and
the Worker answers 503. That is 6-16% of uploads. In frozen-supabase mode relayhistory adds two
more Supabase reads to the same request: `/v1/custody/legacy-receipt` before forwarding and
`/v1/custody/pending` after a 2xx.

## Design in one paragraph

The request path does the following, with no Supabase call:
1. Witnesses the user's **privacy journal** sequence (one S3 GET), before reading the body.
2. Authenticates (HMAC, unchanged).
3. Parses and prepares the batch (CPU, unchanged, including every 400/403/413/422 answer).
4. Writes ONE create-only, scrubbed **inbox object** to S3.
5. Sends ONE message to a new **INGEST_ADMISSION** queue.
6. Answers 200 with the same receipt body.

The queue consumer (the same Worker) reads the inbox and runs the **unchanged** buffered
admission through `deliverPrepared`: frozen custody, then claim -> stage intent -> stage PUT ->
receipt ACK. It uses the same SQL, generation fence, slots and retry semantics. One new check
runs between the claim and any write: the **privacy gate**, which compares the journal with the
upload's witness. Lock waits still happen, but in the consumer, where they are retried with
backoff and nobody waits on them.

```
Desktop -> relayhistory (signed identity carries admission: "queued-v1")
        -> ingest fetch(): witness journal seq (S3 GET) -> read body + verify HMAC -> prepare
                           -> PUT inbox (S3) -> send INGEST_ADMISSION -> 200 + x-relay-admission
ingest queue(): GET inbox -> [frozen receipt + witness] -> claim -> privacy gate -> intent -> PUT stage -> ACK
             -> PUT outcome-admitted|refused -> overwrite inbox with a tombstone -> ack message
reset / re-enable: journal pending -> SQL -> journal complete (seq + 1)
```

## (a) What a 2xx means now

| Path | 2xx means | Response |
| --- | --- | --- |
| synchronous (flag off, or no signed opt-in) | admitted: receipt ACKed in SQL | unchanged; custody ACK header as today |
| async (`on` + signed `admission: "queued-v1"`) | durably stored in S3, not yet admitted | the same JSON body; `x-relay-admission: {"version":1,"state":"queued"}`; no custody ACK header |

The receipt body is byte-identical. It is content-addressed (`receiptId` = digest of tenant,
origin, batch id and batch digest) and its revision ids are the request's own, so nothing about
it depends on the database. `acceptanceLevel: "durable"` stays true: the bytes are in S3
(SSE-KMS, versioned) before the answer. Desktop needs no change. A unit test pins the body
against the synchronous `prepareDelivery` receipt.

Every validation answer keeps its code and status, all before anything is written:
- 400 `invalid_delivery`
- 403 `delivery_account_mismatch`
- 413, 415
- 422 `delivery_budget_exceeded` (the Babysitter budget is a pure CPU check)

The answers that need the ledger move later: the 409s (`batch_id`, `record_revision`), 403 P0W01
(record owned by another user) and the deletion filter.

**Opt-in (signed).** The async path is taken only when ALL of these hold:
- mode is `on`
- acceptance is `buffered`
- the request's **signed identity** (`x-relay-ingest-identity`, covered by the HMAC) carries
  `admission: "queued-v1"`
- the journal witness succeeded
- the body is not a private custody envelope (relayhistory's Neon witness needs the custody ACK
  the claim produces; recovery stays synchronous too)

A loose header does nothing (tested). An identity with any other `admission` value is a 401.
Older ingests ignore the field, so relayhistory can send it before this ingest is on. A witness
failure falls back to the synchronous path and never refuses.

**relayhistory's paired contract** (`delivery-forward.ts`, per org behind its own flag):
1. Put `admission: "queued-v1"` in the signed identity only for an org in the async canary.
2. On a 2xx, accept EITHER the custody ACK (unchanged) OR exactly
   `x-relay-admission: {"version":1,"state":"queued"}`. The latter is accepted ONLY when step 1
   put the opt-in in that request's signed identity. A queued header on a request that did not
   opt in is a failure (503), never a success.
3. For a queued 2xx, skip `requireFrozenCustodyAck`, the post-forward `pendingGate` and the
   pre-forward `/v1/custody/legacy-receipt` call. The consumer runs both frozen reads (section
   c). Keep the S3 legacy-marker read, and its `pendingGate`, only on the branch where
   relayhistory itself answers from a committed legacy marker.

## (b) Idempotency, dedupe, replay and conflicts

- One inbox object per request attempt:
  `96bulls/.admission-inbox/<org>/v1/ws=<ws>/m=srv_<machine>/r=<receipt hex>/a=<receivedAtMs>-<nonce>.ndjson.zst`.
  The `r=` directory is content-addressed by the receipt id. The attempt suffix exists because
  each attempt carries its own journal witness.
- The leading `.` keeps the root disjoint from every tenant prefix `96bulls/<org>/`. No org id
  may start with `.` (`assertTenantSegment`; the stack's `ORG_RE`).
- Dedupe is the claim's, unchanged: the same `(org, workspace, origin, batch_id)` with the same
  digest is `resume` (pending) or `replay` (committed); a different digest is `batch_conflict`.
  So a Desktop retry, a duplicate queue delivery and two concurrent attempts all converge on
  one admission, exactly as concurrent synchronous retries do today.
- A duplicate message after the terminal outcome reads the tombstone and acks
  (`already_terminal`).
- Conflicts become terminal refusals (section e) with the same codes, such as
  `delivery_conflict`.
- The decoder recomputes the receipt id from the header and requires the key path to name the
  same tenant, origin, receipt and upload time. A message cannot point one tenant's object at
  another's admission. Tenancy is never read from the message.

## (c) Privacy, deletion and reset fences

Everything the synchronous claim enforces, the consumer enforces through the same SQL:
- the deletion filter
- the reset count
- frozen custody (a pending control refuses)
- the owner (P0W01)
- the ACK-time recheck of original epochs and reset count
- the generation

One thing moves in time: the claim now runs after the upload, possibly after a privacy control
the user issued after uploading. Two cases matter:

1. **Reset after upload.** Reset is not a claim filter: it tombstones what exists. A pre-reset
   upload claimed after the reset would be admitted as post-reset content.
2. **Disable, then re-enable, after upload.** The claim sees the session enabled again and
   admits a pre-disable write. `ingest_fence_versions` exists to prevent exactly this ("the
   disappearance of a tombstone must not make a pre-disable write trusted again").

A disable alone is safe without anything new: the consumer's claim filters the deleted session.

**The privacy journal.** One S3 object per (org, workspace, user):
`…/m=srv_<sha(ws,user)>/privacy-journal.json`. It holds `{v:2, seq, pending[], done[], ordered?}`,
and every edit is a compare-and-swap (If-Match / If-None-Match).

- **Witness (upload).** The request path reads `seq` BEFORE the body is read, for the identity
  the request claims. It keeps the witness only if the verified identity names the same tenant.
  The inbox header stores the witness.
- **Controls.** `/v1/delivery/sessions/reset` (always) and `/v1/delivery/sessions/selection`
  with `enabled: true` are journaled in modes `on` and `drain`:
  - **begin, BEFORE the SQL:** record the control `pending`. If the journal cannot be written,
    answer 503 and never call SQL (fail closed).
  - **complete, AFTER the SQL applied:** remove the pending mark, `seq + 1`, remember the id. A
    completion failure answers 503 so the caller retries; SQL replays and completion runs again.
  - **abort, when it definitely did not apply:** a typed 4xx refusal; a database error carrying a
    real SQLSTATE (a single autocommit statement that errored rolled back); or an ordered control
    the SQL answered `stale`. Drop the pending mark; seq is unchanged.
  - Only an ambiguous failure (connection class 08xxx, server shutdown 57P0x, a code-less
    transport drop) leaves it pending. Without the abort rule, a control the buffered SQL always
    refuses (a non-ordered one) would leave pending marks that the consumer later completes
    conservatively, refusing uploads for nothing.
- **Control identity (no advance on replay).**
  - An ordered control is `(epoch, version)`. A version at or below the completed maximum is a
    replay or stale, and changes nothing.
  - A legacy reset is its `request_id`, which SQL keeps idempotent forever. Its completion writes
    a durable create-only marker `…/reset-<sha256(request_id)>.json` (one per id, never evicted)
    before the journal edit, so a replay after any number of other controls never advances `seq`.
    Causal test: R0, 256 other resets, U, R0 replayed, U admitted. With the marker disabled the
    test fails.
  - A legacy re-enable has no idempotency key, so every one is new; the cost is over-refusal on
    a literal double submit.
- **Gate (consumer, AFTER its claim, before any write).**
  - A fresh pending control means retry in 15 s; never admit across it.
  - A pending control older than 120 s (its Worker died between SQL and completion) is completed
    by the consumer itself (`seq + 1`), because it may have applied.
  - Then refuse (`delivery_privacy_superseded`) iff `seq > witness`.

Why it is sufficient, for an upload U (witness taken at its request entry, before its body) and
a control C:

| Case | Outcome |
| --- | --- |
| C completed before U's witness | `seq` includes C: admitted. U entered after C applied, so it is post-control content, as on the synchronous path. |
| C begun but not completed at U's witness | U's witness predates C's completion. The consumer waits while C is pending, then sees `seq` grow and refuses. This includes the stalled-stream case: U witnessed, C runs entirely while U's body is still streaming, U finishes later (P0 causal test). |
| C begins after U's witness, applied before U's claim | The post-claim read sees it pending (wait) or completed (refuse). |
| C applied after the claim, before the ACK | The existing ACK recheck refuses the stale intent (reset count or session epoch changed). The message retries, and the gate then refuses. |
| C applied after the ACK | The batch was admitted before the control and is erased by it, as today. |
| A replay of an already-completed C | Changes nothing (P1-b causal test). |

There is no wall-clock comparison anywhere. Only the journal's own monotonic `seq` and its
compare-and-swap order decide.

Over-refusal is bounded to uploads witnessed before a control completed. The Desktop re-upload
after a re-enable or a reset is witnessed after the control's completion (its request starts
after the control's response), so it is admitted (tested). A refused batch's content is never
admitted. A later NEW submission of the same batch carries its own witness, as a synchronous
retry after a control would.

**Mutation evidence** (PGlite, real SQL):
- Remove the journal: the reset and re-enable tests fail.
- Take the witness after the body: the P0 stalled-stream test fails.
- Advance on a replay: the P1-b test fails.

**Read-time gate (R2 contract v5.3).** It is untouched and still re-verifies at display.
Nothing under `.admission-inbox/` is readable through any read path:
- no SQL row names it;
- the read API and the owner-buffered reader resolve objects only through SQL outbox pointers
  and canonical manifests;
- the session pipeline handles only `…/manifest.json` keys.

The gate's rule 3 (admission epoch/version equals the current head) would NOT catch a
post-control admission, because such an admission carries the new head. That is why enforcement
belongs in the consumer, before the ACK.

**At rest.**
- The inbox holds the service-form payloads (`servicePayload`) run once more through the
  pipeline scrubber (`scrubRecords`, the same call `stageGroups` makes), so nothing unscrubbed is
  at rest (tested with a planted secret).
- Record identities (record id, revision id, revision, digest, kind, source, session, operation)
  sit in the header unscrubbed, as SQL holds them. A scrubber rule matching a client id must
  never re-key an acknowledged record (tested).
- **Retention.** After a terminal outcome the consumer overwrites the inbox with a tombstone. The
  content becomes a noncurrent version, which the bucket's existing rule purges within 48 h
  (K-45). Until then the bytes are an acknowledged upload, and nothing expires or deletes them
  (see g).

## (d) Quota and rate limits without the database on the hot path

`INGEST_MAX_PER_USER` / `INGEST_MAX_PER_ORG` stay where they are. The slot lease is taken by the
consumer inside `deliverPrepared`, unchanged. A busy slot retries the message after its
`Retry-After` (2-10 s); no user is refused.

Database pressure is bounded by the admission consumer's `max_concurrency` (4), so a burst of
uploads becomes queue depth, not lock contention. The request path's cost per request is
bounded: `MAX_DELIVERY_BYTES`, two S3 calls (witness GET, inbox PUT) and one queue send.
relayhistory's own authentication and limits sit in front of it.

A Durable Object or the Workers rate-limit binding can add a per-user request ceiling later if
abuse appears. It is not needed for correctness and is not part of this change.

## (e) How a later refusal is surfaced, and what Desktop does

- **Terminal refusals:** `delivery_conflict` (batch id or revision reuse), `forbidden` (P0W01),
  `delivery_budget_exceeded`, `delivery_privacy_superseded`.
  - A refusal writes `…/r=<receipt>/outcome-refused.json` `{state, code, atMs}`.
  - An admission writes `outcome-admitted.json`. Admitted wins if both exist.
- **Status route:** `GET /v1/delivery/admission?originId=<origin>&receiptId=<rhr_…>` (`rth:sync`,
  tenant from the signed identity, S3 only) answers `{receiptId, state: admitted | refused |
  pending, code?}`.
- **Logs:** every outcome is one `ingest.admission` log line (`outcome`, `error_code`, `offset` =
  lag ms, `attempt`).
- **Desktop today:** no change. It treats the 200 as delivered, and a refused batch is not
  retried automatically. In practice that is only the conflict class (a client bug that 409s
  today) and the privacy refusals, where not admitting is the point.
- **Follow-up (Desktop, not blocking):** poll the status route for recent receipts and treat
  `refused` like today's 409 (isolate, or re-send as a new batch).

## (f) Generation fence

The request path does not read the generation. The settings check of `INGEST_EXPECT_GENERATION`
(a hex shape check) is unchanged. The consumer's ledger passes its own `expectedGeneration` on
every buffered call. A schema upgrade in flight therefore makes the claim refuse (55000/40001),
which is classified transient and retried until a Worker on the matching generation consumes the
message (tested). No pending generation is ever admitted. An ACKed-but-unadmitted batch simply
waits through the upgrade.

## (g) Failure modes: uploads keep succeeding, acknowledged bytes are never lost

| Failure | Request path | Consumer |
| --- | --- | --- |
| Supabase locks / timeouts / outage | unaffected | retry, 15 s .. 5 min backoff by attempt; then DLQ |
| baseline_pending, generation mismatch, pending custody control | unaffected | retry (transient) |
| reset / re-enable in flight (journal pending) | unaffected | retry in 15 s; a pending mark older than 120 s is completed conservatively |
| slot busy | unaffected | retry after Retry-After |
| journal witness GET fails | synchronous path (never a refusal) | n/a |
| S3 inbox PUT fails | 503 (nothing ACKed) | n/a |
| queue send fails | 503; inbox tombstoned | n/a |
| queue backlog | unaffected (the ACK is S3 + send) | lag grows; `offset` on every outcome line |
| consumer outage (deploy, crash) | unaffected | messages wait (14-day retention), then DLQ |
| poison / malformed / missing inbox | n/a | retried with 300 s delay -> DLQ for an operator; never silently acked |
| outcome or tombstone write fails | n/a | retry (re-running is idempotent: claim replay or the same refusal) |
| journal unreadable | uploads: synchronous; controls: 503 | retry (fail closed, never read as seq 0) |

**No age limit and no lifecycle on pending bytes.** An acknowledged inbox object is never expired
or deleted by any rule or code path. Only a terminal outcome (admitted or refused) retires its
content, via the tombstone. The admission queue and DLQ keep messages for 14 days (the
Cloudflare maximum; a post-create setting, PR #367). A DLQ redrive at any age re-runs every
check, so a redrive after a reset refuses the pre-reset uploads. The P1-a causal test runs:
200, twenty transient failures (max retries), eight days later a redrive, admitted.

**Durable recovery: the queue is not the authority.** If a message is ever lost or expires past
the DLQ, its bytes remain under `96bulls/.admission-inbox/<org>/…`. `scripts/admission-inbox-recover.ts`
is read-only: credentials come from the environment only, and it needs `s3:ListBucket`. It
rebuilds the exact admission message for every inbox object that still holds content and is
older than a threshold (`pendingAdmissions`), and prints one JSON line each for the operator to
publish to the admission queue. Two properties make this safe:
- Tombstones and markers are told apart by size, an invariant enforced at encode: an inbox is
  always at least 256 bytes and a tombstone never is.
- Over-sending is harmless: a terminal object acks, and a re-run admission replays the claim.

Tested: pending objects are listed; tombstoned, fresh and journal/marker objects are not.

Hygiene: tombstones, outcome markers and journals are small objects that accumulate. That is
acceptable at current volumes. A future rule may expire only tombstones and outcomes (for
example `ObjectSizeLessThan`), never journals or pending inbox objects. It is not part of this
change.

## (h) Rollout, canary, rollback

Flag `INGEST_ASYNC_ADMISSION`: `off` (default) | `on` | `drain`. `on` and `drain` require
buffered acceptance and the `INGEST_ADMISSION` binding. Without the binding the Worker reports
`not_configured`.

| Mode | Async ACK | Controls journaled | Consumer admits |
| --- | --- | --- | --- |
| off | no | no | no (messages wait, 300 s) |
| on | yes, for signed opt-ins | yes | yes |
| drain | no (synchronous path) | yes | yes |

Steps:
1. **IAM: none.** The srv-writer resources are unchanged. `96bulls/*/v1/ws=*/m=srv_*` matches
   every inbox, outcome and journal key, because an IAM resource `*` spans `/`. The production
   buffered stages `96bulls/<org>/buffered-batches/v1/ws=…/m=srv_…` already depend on exactly
   that. The KMS encryption context `96bulls/*` covers them too. Regression test:
   `stacks/aws/__tests__/admission-inbox-iam.test.ts`. The dormant inbox-authority expansion is
   NOT used: its root grammar rejects a dot root and it also grants the unrelated `upload-inbox`
   candidate root.
2. **Cloudflare stack (PR #367):**
   - queue `96bulls-session-ingest-admission` and DLQ `96bulls-session-ingest-admission-dlq`,
     **not paused** (separate from the paused buffered fanout queue, whose pause guards
     canonical drain and is untouched)
   - consumer = the ingest Worker, batch 1, max retries 20, max concurrency 4
   - binding `INGEST_ADMISSION`, var `INGEST_ASYNC_ADMISSION=drain`
   - required post-create step: message retention 1209600 s on both queues (PR #367 body)
3. **Deploy with `drain`.** The only behaviour change is two S3 compare-and-swaps around each
   reset / re-enable. Let `drain` reach 100% of Worker versions (no gradual split with an `off`
   version still serving) BEFORE `on`. An `off` version would apply a control without journaling
   it while an `on` version's consumer admits.
4. **relayhistory-cloud:** the paired contract in section (a).
5. **Flip the ingest to `on`**, then canary one org in relayhistory. Watch:
   - request-path p99 and 5xx rate (the 55P03/57014 503s should vanish from `delivery.batches`)
   - `ingest.admission` outcomes
   - lag (`offset`)
   - DLQ depth
   - `ingest.admission_lock_alert`
6. **Widen** org by org.

Rollback:
- Turn relayhistory's per-org opt-in off. This is instant: back to synchronous answers.
- And/or set the ingest to `drain`.
- Do NOT go straight from `on` to `off` while messages are in flight. `off` stops journaling, so
  the consumer refuses to admit and messages wait. Go `on -> drain`, wait for queue + DLQ to be
  empty, then `off`.

## (i) Observability

- **`delivery.batches` / `turns.batches`:** `outcome: accepted_queued`, `duration_ms`,
  `bytes_raw`.
- **`ingest.admission`** (one per message): `outcome` (admitted, refused, already_terminal or
  retry), `error_code` (deliver outcome, refusal code or transient cause, including
  `privacy_control_pending`), `stage`, `offset` (lag ms from upload start), `attempt`,
  `duration_ms`, and `db_error_*` for a database cause.
- **`ingest.admission_lock_alert`:** the existing lock-wait alert (`stage`, `sqlstate`), labelled
  as the background consumer's. It is no longer user-facing, so page on a sustained rate or on
  the DLQ, not on single lines.
- **Queue metrics:** backlog, oldest message age, DLQ depth (Cloudflare Queues analytics).

## Deploy inputs (for the Worker deploy actor)

- **Source:** the merge SHA of this change on `main`, with PR #367 merged.
- **Var:** `INGEST_ASYNC_ADMISSION=drain` first; then `on`, as a var-only update.
- **Queue:** `96bulls-session-ingest-admission` (producer `INGEST_ADMISSION` + consumer, this
  Worker) and DLQ `96bulls-session-ingest-admission-dlq`. Batch 1, max retries 20, max
  concurrency 4, not paused, retention 1209600 s (post-create).
- **Generation impact:** none. No migration, no SQL function, no grant; `generation()` is
  unchanged.
- **AWS:** none (no IAM, KMS or lifecycle change).
- **relayhistory-cloud:** section (a), step 4.

## Decisions taken (and why)

- **A dedicated queue, not the INGEST_FANOUT binding.** In buffered mode the fanout queue and its
  DLQ are declared paused by a reviewed provider whose reconcile only permits `true` ("UNPAUSE
  is not authorized"), because resuming it is the canonical-drain decision. Admission must run
  while canonical drain stays closed. The Worker's `queue()` handler dispatches by message kind,
  and fanout messages are still refused in buffered mode.
- **An S3 journal sequence, not a new SQL function or a wall clock.** The fence needs a value
  witnessed before the body and re-read after the claim, without touching the claim's lock
  order, the function ACL boundary or the generation. A monotonic compare-and-swap counter with
  pending/complete marks does that with the srv-writer credential the Worker already holds, and
  it has no cross-isolate clock skew.
- **Per-attempt inbox objects.** A single content-addressed key would make the first attempt's
  witness govern every retry. A legitimate re-send after a re-enable would then be refused.
- **No lifecycle on the inbox.** An expiry long enough to be safe is useless for privacy, and one
  short enough to matter can delete acknowledged bytes still in the DLQ. Terminal outcomes
  tombstone, and the bucket's noncurrent rule purges.
