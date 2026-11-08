# S3-first async admission: Supabase off the upload hot path

Status: implemented behind `INGEST_ASYNC_ADMISSION` (default `off`). Merging the code changes
nothing in production. Turning it on needs the stack, relayhistory and lifecycle steps under
"Rollout".

Code: `src/session-ingest/async-admission.ts` (request path, inbox, privacy journal, outcomes),
`src/session-ingest/admission-consumer.ts` (the queue consumer), the `admissionGate` hook in
`deliver.ts`, wiring in `app.ts` / `worker.ts`. Tests: `__tests__/async-admission.test.ts`
(unit) and `__tests__/async-admission.pglite.test.ts` (the real buffered SQL, every migration).

## Problem

`POST /v1/delivery/batches` (and `/v1/turns/batches`) in buffered mode runs four Supabase
round trips before it answers: the lazy claim (`ingest_claim_buffered_batch`: per-user fence
`FOR UPDATE`, seat-clock `KEY SHARE`), the slot lease, the stage intent
(`ingest_prepare_buffer_stage`) and the receipt ACK (`ingest_commit_buffered_batch`: fence,
seat clock `NO KEY UPDATE`, the origin advisory lock). Under `lock_timeout 2s` /
`statement_timeout 5s` the waits fail with SQLSTATE 55P03 / 57014, and the Worker answers 503.
That is 6-16% of uploads. In frozen-supabase mode relayhistory adds two more Supabase reads on
the same request: `/v1/custody/legacy-receipt` before forwarding and `/v1/custody/pending`
after a 2xx.

## Design in one paragraph

The request path authenticates (HMAC, unchanged), parses and prepares the batch (CPU,
unchanged, including every 400/403/413/422 answer), writes ONE create-only, scrubbed **inbox
object** to S3, sends ONE message to a new **INGEST_ADMISSION** queue and answers 200 with the
same receipt body. It makes no Supabase call. The queue consumer (the same Worker) reads the
inbox and runs the **unchanged** buffered admission through `deliverPrepared`: frozen custody,
then claim -> stage intent -> stage PUT -> receipt ACK, with the same SQL, the same generation
fence, the same slots and the same retry semantics. One new check runs between the claim and
any write: the **upload-time privacy gate**. Lock waits still happen, but in the consumer,
where they are retried with backoff and nobody waits on them.

```
Desktop -> relayhistory (x-relay-ingest-admission: queued-v1)
        -> ingest fetch(): auth -> prepare -> PUT inbox (S3) -> send INGEST_ADMISSION -> 200 + x-relay-admission
ingest queue(): GET inbox -> [frozen receipt + witness] -> claim -> privacy gate -> intent -> PUT stage -> ACK
             -> PUT outcome-admitted|refused -> overwrite inbox with a tombstone -> ack message
```

## (a) What a 2xx means now

| Path | 2xx means | Response |
| --- | --- | --- |
| synchronous (flag off, or the caller did not opt in) | admitted: receipt ACKed in SQL | unchanged; custody ACK header as today |
| async (`on` + `x-relay-ingest-admission: queued-v1`) | durably stored in S3, not yet admitted | the same JSON body; `x-relay-admission: {"version":1,"state":"queued"}`; no custody ACK header |

The receipt body is byte-identical: it is content-addressed (`receiptId` = digest of tenant,
origin, batch id and batch digest) and its revision ids are the request's own, so nothing about
it depends on the database. `acceptanceLevel: "durable"` stays true: the bytes are in S3
(SSE-KMS, versioned) before the answer. Desktop needs no change; it already treats the 200 as
final and does not read any field that could change. A unit test pins the body against the
synchronous `prepareDelivery` receipt.

Every validation answer keeps its code and status: 400 `invalid_delivery`, 403
`delivery_account_mismatch`, 413, 415, 422 `delivery_budget_exceeded` (the Babysitter budget
is a pure CPU check), all before anything is written. What moves later are the answers that
need the ledger: the 409s (`batch_id`, `record_revision`), 403 P0W01 (record owned by another
user) and the deletion filter.

The async path is taken only when ALL hold: mode `on`, acceptance `buffered`, the caller sent
`x-relay-ingest-admission: queued-v1`, and the body is not a private custody envelope
(relayhistory's Neon witness needs the custody ACK the claim produces; recovery stays
synchronous too). The opt-in header makes the rollout order-independent: an ingest with the flag
on still answers relayhistory's current code synchronously, and relayhistory can canary per org.

## (b) Idempotency, dedupe, replay and conflicts

- One inbox object per request attempt:
  `96bulls/admission-inbox/<org>/v1/ws=<ws>/m=srv_<machine>/r=<receipt hex>/a=<receivedAtMs>-<nonce>.ndjson.zst`.
  The `r=` directory is content-addressed by the receipt id. The attempt suffix exists because
  the privacy gate judges each attempt by its own start time.
- Dedupe is the claim's, unchanged: the same `(org, workspace, origin, batch_id)` with the same
  digest is `resume` (pending) or `replay` (committed); a different digest is `batch_conflict`.
  So a Desktop retry, a duplicate queue delivery and two concurrent attempts all converge on
  one admission, exactly as concurrent synchronous retries do today.
- A duplicate message after the terminal outcome reads the tombstone and acks
  (`already_terminal`).
- Conflicts become terminal refusals (section e) with the same codes: `delivery_conflict`, and
  so on.
- The decoder recomputes the receipt id from the header and requires the key path to name the
  same tenant, origin, receipt and upload time. A message cannot point one tenant's object at
  another's admission. Tenancy is never read from the message.

## (c) Privacy, deletion and reset fences

Everything the synchronous claim enforces, the consumer enforces through the same SQL: deletion
filter, reset count, frozen custody (pending control refuses), owner (P0W01), the ACK-time
recheck of original epochs and reset count, and the generation. One thing moves in time: the
claim now runs after the upload, possibly after a privacy control the user issued after
uploading. Two cases matter:

1. **Reset after upload.** Reset is not a claim filter: it tombstones what exists. A pre-reset
   upload claimed after the reset would be admitted as post-reset content.
2. **Disable, then re-enable, after upload.** The claim sees the session enabled again and
   admits a pre-disable write. `ingest_fence_versions` exists to prevent exactly this ("the
   disappearance of a tombstone must not make a pre-disable write trusted again").

A disable alone is safe without anything new: the consumer's claim filters the deleted session
(PGlite test "a disable alone needs no journal").

**The privacy journal** closes both cases. It is one small S3 object per (org, workspace,
user), `…/m=srv_<sha(ws,user)>/privacy-journal.json` = `{v:1, watermarkMs}`:

- Writers: `/v1/delivery/sessions/reset` (always) and `/v1/delivery/sessions/selection` with
  `enabled: true`, in modes `on` and `drain`. They raise the watermark to their own start time
  with a compare-and-swap max (If-Match / If-None-Match) BEFORE the SQL control runs. If the
  journal cannot be raised, the control answers 503 and SQL is never called (fail closed).
- Reader: the consumer, AFTER its claim (`deliver.ts admissionGate`, before any slot or write).
  If `watermarkMs >= upload start` the batch is refused as `delivery_privacy_superseded`.

Why it is sufficient, for a control C and an upload U (U = request start, the same Worker
clock as C):

- C applied before U's claim: C's journal write finished before C's SQL ran, so the
  post-claim read sees it (S3 is strongly consistent). If C started after U, U is refused. If C
  started before U, U is a post-control upload and is admitted, which is what the synchronous
  path does for a request that arrives during or after a control.
- C applied after the claim and before the ACK: the existing ACK recheck refuses the stale
  intent (the reset count or the session epoch changed). The message retries, the new claim sees
  C, and the gate refuses.
- C applied after the ACK: the batch was admitted before the control and is erased by it, as
  today.

The PGlite suite proves both cases on the real SQL. A mutation run that disables the journal
raise makes exactly those two tests fail: the pre-reset and pre-enable uploads are then
admitted.

Over-refusal is bounded to uploads that started before the control's own start (plus clock
skew between Worker isolates, milliseconds). The Desktop re-upload after a re-enable or a reset
starts after the control completes, so it is admitted (tested). A refused batch's content is
never admitted. A later NEW submission of the same batch (a new attempt object after the
control) is judged on its own start, as a synchronous retry after a control would be.

**Read-time gate (R2 contract v5.3).** It is untouched and still re-verifies at display.
The inbox and journal are not readable through any read path. No SQL row names them; the read
API and the owner-buffered reader resolve objects only through SQL outbox pointers and
canonical manifests; the session pipeline handles only `…/manifest.json` keys. The gate's
rule 3 (admission epoch/version equals the current head) would NOT catch a post-control
admission, because such an admission carries the new head. That is why enforcement belongs in
the consumer, before the ACK, and not only at read time.

**At rest.** The inbox holds the service-form payloads (`servicePayload`) run once more through
the pipeline scrubber (`scrubRecords`, the same call `stageGroups` makes), so nothing
unscrubbed is at rest. A test plants an AWS secret and checks it is absent from the stored
bytes. Record identities (record id, revision id, revision, digest, kind, source, session,
operation) sit in the header unscrubbed, as SQL holds them. A scrubber rule that matched a
client id must never re-key an acknowledged record: the claim, receipt and witness would then
disagree and the batch would dead-letter. A test pins a secret-shaped id through the round trip. After a terminal outcome the consumer overwrites the inbox with a tombstone, which makes
the content a noncurrent version. The bucket's existing noncurrent rule purges it within 48 h
(K-45). An unacknowledged inbox (queue send failed) is tombstoned on the spot. The only content
that can outlive this is an orphan whose message was lost past the DLQ; the prefix lifecycle
rule (Rollout step 1) bounds it to 7 days + the noncurrent purge.

## (d) Quota and rate limits without the database on the hot path

`INGEST_MAX_PER_USER` / `INGEST_MAX_PER_ORG` stay where they are: the slot lease is taken by the
consumer inside `deliverPrepared`, unchanged. A busy slot retries the message after its
`Retry-After` (2-10 s); no user is refused. Database pressure is bounded by the admission
consumer's `max_concurrency`, so a burst of uploads becomes queue depth, not lock contention.
The request path's cost is bounded per request: `MAX_DELIVERY_BYTES`, one S3 PUT and one queue
send. relayhistory's own authentication and limits sit in front of it. A Durable Object or the
Workers rate-limit binding can add a per-user request ceiling later if abuse appears. It is not
needed for correctness and is not part of this change.

## (e) How a later refusal is surfaced, and what Desktop does

- Terminal refusals: `delivery_conflict` (batch id / revision reuse), `forbidden` (P0W01),
  `delivery_budget_exceeded`, `delivery_privacy_superseded`, `admission_expired`. Each writes
  `…/r=<receipt>/outcome-refused.json` `{state, code, atMs}`. An admission writes
  `outcome-admitted.json`, and admitted wins if both exist.
- `GET /v1/delivery/admission?originId=<origin>&receiptId=<rhr_…>` (`rth:sync`, tenant from the
  signed identity, S3 only) answers `{receiptId, state: admitted | refused | pending, code?}`.
- Every outcome is one `ingest.admission` log line (`outcome`, `error_code`, `offset` = lag ms,
  `attempt`).
- Desktop today: no change. It treats the 200 as delivered. A refused batch is not retried
  automatically. In practice this is only the conflict class (a client bug that 409s today) and
  the privacy refusals, where not admitting is the point. Follow-up (Desktop, not blocking): poll
  the status route for recent receipts and treat `refused` like today's 409 (isolate, or re-send
  as a new batch).

## (f) Generation fence

The request path does not read the generation. The settings check of
`INGEST_EXPECT_GENERATION` (a hex shape check) is unchanged. The consumer's ledger passes its own
`expectedGeneration` on every buffered call, so a schema upgrade in flight makes the claim refuse
(55000/40001). That is classified transient and retried until a Worker on the matching
generation consumes the message (PGlite test "a stale generation is transient"). No pending
generation is ever admitted. An ACKed-but-unadmitted batch simply waits through the upgrade.

## (g) Failure modes: uploads keep succeeding

| Failure | Request path | Consumer |
| --- | --- | --- |
| Supabase locks / timeouts / outage | unaffected | retry, 15 s .. 5 min backoff by attempt; then DLQ |
| baseline_pending, generation mismatch, pending control | unaffected | retry (transient) |
| slot busy | unaffected | retry after Retry-After |
| S3 PUT fails | 503 (nothing ACKed) | n/a |
| queue send fails | 503; inbox tombstoned | n/a |
| queue backlog | unaffected (ACK is S3 + send) | lag grows; `offset` on every outcome line |
| consumer outage (deploy, crash) | unaffected | messages wait (queue retention), then DLQ |
| poison message / malformed inbox / missing inbox | n/a | retried with 300 s delay -> DLQ for an operator; never silently acked |
| outcome or tombstone write fails | n/a | retry (re-running is idempotent: claim replay or the same refusal) |
| inbox older than 6 days | n/a | refused `admission_expired` (visible), never admitted after its journal could expire |

DLQ: `96bulls-session-ingest-admission-dlq`. The redrive is a plain re-send of the DLQ'd
messages to `INGEST_ADMISSION`. Every check is re-run, so a redrive after a reset refuses the
pre-reset uploads.

## (h) Rollout, canary, rollback

Flag `INGEST_ASYNC_ADMISSION`: `off` (default) | `on` | `drain`. `on`/`drain` require buffered
acceptance and the `INGEST_ADMISSION` binding. Without the binding the Worker reports
`not_configured`.

| Mode | Async ACK | Journal raised by controls | Consumer admits |
| --- | --- | --- | --- |
| off | no | no | no (messages wait, 300 s) |
| on | yes, for opted-in callers | yes | yes |
| drain | no (synchronous path) | yes | yes |

Steps:

1. AWS: add the lifecycle rule `expire-admission-inbox` (Prefix `96bulls/admission-inbox/`,
   Expiration 7 days) to the transcripts bucket. The existing noncurrent rule then purges.
   No IAM change: the srv-writer grant `96bulls/*/v1/ws=*/m=srv_*` and the KMS context
   `96bulls/*` already cover the inbox keys. Org id `admission-inbox` is refused.
2. Cloudflare stack: queue `96bulls-session-ingest-admission` + DLQ
   `96bulls-session-ingest-admission-dlq`, **not paused** (separate from the paused buffered
   fanout queue, whose pause guards canonical drain and is untouched). Consumer = the ingest
   Worker, `max_batch_size 1`, `max_retries 20`, `max_concurrency` 4-8 (the database budget),
   `retry_delay` set per message by the code. Producer binding `INGEST_ADMISSION`, var
   `INGEST_ASYNC_ADMISSION=drain`.
3. Deploy with `drain`: no behaviour change except one S3 compare-and-swap before each reset /
   re-enable. Let `drain` reach 100% of Worker versions (no gradual split with an `off` version
   still serving) BEFORE `on`. An `off` version would apply a control without raising the
   journal while an `on` version's consumer admits.
4. relayhistory-cloud (`delivery-forward.ts`), per org behind its own flag: send
   `x-relay-ingest-admission: queued-v1`. On a 2xx accept EITHER the custody ACK (unchanged) OR
   exactly `x-relay-admission: {"version":1,"state":"queued"}`, never neither. For a queued 2xx,
   skip `requireFrozenCustodyAck` and the post-forward `pendingGate`, and skip the pre-forward
   `/v1/custody/legacy-receipt` call: the consumer runs both frozen reads. Keep the S3 legacy
   marker read and its `pendingGate` only on the branch where relayhistory itself answers from a
   committed legacy marker.
5. Flip the ingest to `on`, then canary one org in relayhistory. Watch: request-path p99 and the
   5xx rate (the 55P03/57014 503s should vanish from `delivery.batches`); `ingest.admission`
   outcomes; lag (`offset`); DLQ depth; `ingest.admission_lock_alert`.
6. Widen org by org.

Rollback: turn relayhistory's per-org flag off (instant: back to synchronous answers), and/or set
the ingest to `drain`. Do NOT go straight from `on` to `off` while messages are in flight. `off`
stops raising the journal, so the consumer refuses to admit and messages wait. Go `on -> drain`,
wait for queue + DLQ to empty, then `off`.

## (i) Observability

- `delivery.batches` / `turns.batches`: `outcome: accepted_queued`, `duration_ms`,
  `bytes_raw`.
- `ingest.admission` (one per message): `outcome` admitted | refused | already_terminal |
  retry, `error_code` (deliver outcome, refusal code or transient cause), `stage`, `offset` (lag
  ms from upload start), `attempt`, `duration_ms`, and `db_error_*` for a database cause.
- `ingest.admission_lock_alert`: the existing lock-wait alert (`stage`, `sqlstate`), labelled as
  the background consumer's. It is no longer user-facing, so page on sustained rate or DLQ, not
  on single lines.
- Queue metrics: backlog, oldest message age, DLQ depth (Cloudflare Queues analytics).

## Deploy inputs (for the Worker deploy actor)

- Source: the merge SHA of this change on `main`.
- Var: `INGEST_ASYNC_ADMISSION=drain` first, then `on`.
- Queue: `96bulls-session-ingest-admission` (producer `INGEST_ADMISSION` + consumer, this
  Worker), DLQ `96bulls-session-ingest-admission-dlq`, batch size 1, max retries 20, max
  concurrency 4-8, not paused.
- Generation impact: none. No migration, no SQL function, no grant. `generation()` is
  unchanged.
- AWS: the lifecycle rule (step 1). No IAM or KMS change.
- relayhistory-cloud: step 4.

## Decisions taken (and why)

- **A dedicated queue, not the INGEST_FANOUT binding.** In buffered mode the fanout queue and
  its DLQ are declared paused by a reviewed provider whose reconcile only permits `true`
  ("UNPAUSE is not authorized"), because resuming it is the canonical-drain decision. Admission
  must run while canonical drain stays closed, so it gets its own queue. The Worker's `queue()`
  handler dispatches by message kind. Fanout messages are still refused in buffered mode.
- **An S3 journal, not a new SQL function.** The upload-time check needs a watermark that is
  written before every control and read after every claim, without touching the claim's lock
  order, the function ACL boundary or the generation. A compare-and-swap object with the srv-writer
  credential the Worker already holds does that.
- **Per-attempt inbox objects.** A single content-addressed key would make the first attempt's
  start time govern every retry. A legitimate re-send after a re-enable would then be refused
  until the object expired.
