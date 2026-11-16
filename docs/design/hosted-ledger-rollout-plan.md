# Hosted ledger: rollout plan after the benchmark

**Status:** proposed. Khaliq approved drafting it on 2026-11-16, after the benchmark that decision 0008
asked for. It plans; it changes nothing. Every deploy, cloud resource, IAM or KMS change, bucket move and
stream switch below needs his explicit OK in his own words (AGENTS.md), and lands as code.

**Numbers:** the median of three benchmark repeats on 2026-11-16 (morning, afternoon and evening UTC), each
cell 60 to 120 seconds of steady state. One figure rests on a single repeat and says so. The benchmark's
harness and stack are `benchmarks/ledger-bench` and `stacks/aws/ledger-bench`; the full results are in the
private ops documentation repository.

**Builds on:** decision 0008 (hosted ledger backend, benchmark first), the log-commit design
(`docs/design/s3-ledger-log-commit.md`, implemented behind `INGEST_LEDGER_LOG_COMMIT`, default off), the
group-commit design (`docs/design/s3-ledger-group-commit.md`), and 0007 (hosted sessions on the agentrelay
EKS clusters).

## What the benchmark found

Client: a pod on the production cluster in us-west-2, the ingest-edge image, the same object-store client.
Targets: throwaway buckets, a table and keys in a development account. SSE-KMS throughout.

| 0008 option | Measured | Against the plan's threshold |
| --- | --- | --- |
| A1: ledger in the compute's Region | The per-batch chain takes 717 ms in-region against 2,088 ms cross-region (p50; p99 1,108 against 2,701 ms): 2.9 times faster | Fails (needed 8 times; under 4 times was the rejection line) |
| B: Bucket Keys | A 1 KiB PUT 37 ms against 43 ms; GET and HEAD unchanged; chain throughput 1.12 times | Fails (needed half the latency and 1.3 times) |
| C: log-entry commit | One stream commits 175 batches/s at a group of 16 (84 ms per group), against 11.0 batches/s for the chain with 8 in flight: 15.9 times | Passes (needed 10 times, and 23 batches/s for one stream) |
| G: S3 Express One Zone | Create-if-absent 7.7 ms p50 and 9.8 ms p99 against 45 and 103 ms; one stream 949 batches/s at a group of 16; `If-Match` works on directory buckets; no failed request across session refreshes; a dual-zone write adds 1 to 5 ms | Passes every check |
| E: DynamoDB commit | Commit p99 2% to 15% below the S3 log entry's on one stream (the payload PUT dominates both); faster across many streams at small groups; one partition takes 1,000 writes/s and no more, and 100-item transactions on one partition were throttled throughout | Passes its checks, narrowly |

Correctness, at 200 rounds per check and 20 killed clients per target in each repeat, on standard buckets
(Bucket Keys off and on) and on directory buckets in two zones: exactly one winner in every create-if-absent and every
compare-and-swap race; no gap and no duplicate batch in any racing-writer log; every write caught in flight
by a killed client was afterwards whole or absent, never torn; nothing acknowledged was lost.

Three results were not what 0008 assumed:

- **An in-region S3 request is 15 to 45 ms, not 5 ms.** HEAD is 17 ms, GET 25 ms, an SSE-KMS PUT 43 ms
  (cross-region: 78, 88 and 105 ms). The bare network round trip to an in-region endpoint is about 1 ms; the rest is S3. So
  co-location removes about 65 ms from each request, which is two thirds of the chain's time, not 12 parts
  in 13.
- **KMS costs a PUT about 15 ms and a GET about 1 ms**, in either Region (SSE-KMS against SSE-S3 objects in
  the same bucket). Bucket Keys recover
  that on PUTs only. They are still worth having for KMS request cost and quota at fleet scale.
- **More than about 128 requests in flight in one process buys nothing.** Throughput peaks near 2,000
  requests/s there; beyond it the p50 climbs from 58 ms to 138 ms at 256 and 288 ms at 512, S3 answers 503 on
  some PUTs at every level, and connection resets appear from 256. This matches the 2026-11-15 transients.

Limits of these numbers:

- The chain and the log commit were replayed with synthetic objects of production's sizes, in production's
  request order. A real batch touches more revision shards than the two the replay used, so the chain's
  absolute batches/s is an upper bound for production's; the ratios are what carry over.
- The client pod had a one-core limit so that it could not crowd production pods. It was throttled in 1% to
  14% of periods; cells at 64 or more requests in flight are partly bound by the client.
- The DynamoDB transactions put every item of a stream under one partition key and retried without backoff.
  That is the simplest design, not the best one.

## What this means for the log commit that is already built

`INGEST_LEDGER_LOG_COMMIT` makes the *admission commit* one create-if-absent entry after the payload PUTs.
That is the path the benchmark measured at about 15 times the chain. But a batch is visible to readers only
after the materializer has applied its entry, and the materializer replays each logged batch through the
unchanged legacy core: the same chain, off the critical path, one batch at a time per stream, with streams
applied in parallel (confirmed by the log-commit design's owner).

So with the flag on:

- the **acknowledged-and-ordered rate** per stream rises by an order of magnitude, and the upload inbox stops
  being where the backlog sits;
- the **visible rate** of one stream is still the chain's rate, now paid by the materializer; the aggregate
  scales with the number of active streams. Until the materializer
  coalesces work (one manifest update per session per several entries, shared shard reads: the group-commit
  design's idea, applied at apply time), a hot user's backlog moves from the inbox to unapplied log entries.
  It does not disappear.

This is why co-location still matters after option C, even though it failed its own threshold: it makes the
materializer's chain about 3 times faster, and that chain is what sets the visible rate.

## Proposed decisions for 0008

1. **Adopt C.** It is built; finish what its design lists as needed before production, then roll it out
   stream by stream (phases 1 to 3).
2. **Adopt A1, as part of the same rollout rather than before it** (phase 4). It needs a data move; C on the
   existing bucket does not.
3. **Adopt B only as a property of the new bucket.** It is born with Bucket Keys on and a key policy written
   for the bucket ARN, so the existing bucket's policy never changes in place.
4. **Defer G.** It is the strongest accelerator measured, and option C needs only create-if-absent, which it
   does correctly. It has three prerequisites (phase 6) and is its own decision.
5. **Do not adopt E.** It adds a storage dependency and amends 0005 for a commit that is not faster per stream.
   Revisit it only if a need for multi-key transactions appears that the log cannot meet.
6. **Cap in-flight object-store requests per process at about 128**, in code, whatever else is decided.

## Phases

Each phase lands in dev first. Each lists what must be true before it starts, what ships, how it is judged,
how it is undone, and what needs Khaliq's OK. No phase starts on the strength of the one before it having
been approved.

### Phase 0: close the benchmark

Done. Three repeats ran on 2026-11-16; the harness and stack are on main; both sides were torn down and
verified gone (the cluster side on 2026-11-16, the data side on 2026-11-17, with only the three KMS keys left
in their pending-deletion window); the results and the write-up are filed in the private ops documentation
repository; this document carries the three-repeat medians.

### Phase 1: make the log commit ready for production

The log-commit design's own "before production" items are closed on main (slice 6: consent re-checked at
apply time, readers start from a verified checkpoint, the registry keyed by hash only and taken before the
stream, the roles' IAM in the stack). What is still open, from that design's owner and from the benchmark:

- A bucket-policy exemption so that the consumer can delete applied payloads, admitted outcomes and registry
  documents. Today the bucket's "only the eraser deletes" denial refuses them.
- The operator role's grant, which lives outside the stack, brought into code.
- The hosted refusal of the flag and the stack-input refusal, lifted together and not before the rest.
- Erase paths call `eraseLogPayloads` before any erase is admitted under the S3 ledger (the design's Remaining
  3). It is safe today only because erase is refused there.
- A process-wide cap on in-flight object-store requests (proposal 6), with the cause of every status-less
  transient logged. Not built. The group-commit design already names it as a prerequisite.
- The operator tool refuses a mutating step on any production-like bucket. Phase 3 needs a deliberate,
  reviewed way to switch a production stream; that is a code change with its own review, not a flag flip.
- A decision on where `ledger/log/` lives. The ledger can now be given its own bucket (off by default), which
  is also the natural seam for phase 4.

**Judged by:** the existing log-commit suites, plus the benchmark's racing-writer and killed-client checks
pointed at the real writer on MinIO in CI. **Undone by:** nothing to undo; the flag is off.

### Phase 2: dev, and a benchmark of the real path

- `INGEST_LEDGER_LOG_COMMIT=on` in the dev stack; synthetic streams switched with the operator tool; the
  pipeline end-to-end flow green with log-mode streams.
- The benchmark harness gains a lane that drives real admissions through ingest-edge (not replayed request
  sequences) against one stream and against 64.

**Judged by**, on the dev stack in-region, for one stream:

| Measure | Must reach | From |
| --- | --- | --- |
| Admission commit rate, one stream | at least 23 batches/s, and at least 10 times the legacy path on the same bucket | the plan's headroom rule (10 times the observed peak of 2.3/s) |
| Commit latency p99 | under 300 ms | the replay measured 261 ms at a group of 16 |
| Materializer lag on one stream, at twice the hottest real stream's arrival rate | bounded: it does not grow over 30 minutes | the visible rate must keep up with the hottest machine, with headroom |
| Gaps, duplicate batches, torn or lost acknowledged writes | 0 | every correctness check in the benchmark |
| A stale writer after an ownership change | stops with `LogFenced`; no entry of its own after the epoch entry | the fencing rule |

The third row's rate has to be measured first. The observed peak of 2.3 batches/s is roughly the whole
fleet's arrival, not one stream's. The materializer applies one batch at a time per stream, so at the
in-region chain's 0.7 s it holds about 1.4 batches/s per stream (and about 0.5 cross-region); streams
apply in parallel. Phase 2 therefore starts by reading the hottest real stream's arrival rate from
production's own logs. If twice that rate is above what one stream can apply, coalescing at apply time comes
before phase 3, not after.

**Undone by:** the close marker hands a stream back to legacy once every entry is applied; the flag off for
new streams. **Needs OK:** the dev deploy.

### Phase 3: production pilot on the existing bucket

No data moves. The hot user's streams (one per machine) are switched with the reviewed operator path from
phase 1, a few first, then the rest.

- **Why first:** it is the only step that gives production relief without a migration. The commit is two
  dependent requests per group instead of 17 per batch, in either Region.
- **Measured, in one repeat:** on the cross-region bucket one stream commits a group of 16 in 136 ms (p99
  662 ms), 105 batches/s, against 3.9 batches/s for the chain there with 8 in flight: 27 times. Two and eight
  writers racing on one stream there left no gap and no duplicate.
- **What it does not fix:** the materializer still pays the cross-region chain per batch (about 2.1 s), one
  batch at a time per stream, so one stream becomes visible at about 0.5 batches/s there. Visibility for the
  hot user improves because that user's machines are separate streams applied in parallel, not because any
  one stream got faster. This has to be watched from the first switched stream.

**Checked before the first switch**, both from the log-commit design's owner:

- The production key policy carries statements added by hand that a stack deploy would revert. They are
  reconciled into code first, so that the pilot's deploy cannot silently drop them.
- The log commit's KMS grants match by encryption-context pattern. They cover every operator PUT in code, but
  nobody has checked that the live key policy covers `ledger/log/` and the registry prefix. A plan and a
  readback diff against the live policy settle it.

**Judged by:** for each switched stream, over 24 hours: no `LogCorrupt`, no fenced writer outside an ownership
change, admission p99 under 1 s, the inbox backlog for those machines draining, materializer lag bounded.
**Undone by:** per stream: stop appends, let the materializer apply every entry, write the close marker.
**Needs OK:** the image rollout, the IAM and key-policy change if phase 1 found one, and each batch of stream
switches.

### Phase 4: the ledger working set moves to the compute's Region

- A new bucket and key in us-west-2, in the cluster's account: the sessions bucket 0007 plans, brought
  forward for the ledger working set (inbox, log, batch docs, fences, journals, revision shards, owners,
  outcomes). Transcripts and manifests follow 0007's own phase.
- Born with Bucket Keys on. Its key policy is written for the bucket ARN as the encryption context. Per-prefix
  isolation, which the existing bucket gets from the per-object encryption context, moves to IAM conditions on
  the S3 actions. Whether that is enough, or per-user envelope keys (0008 option F) are needed first, is an
  open question for Khaliq below.
- New organisations and new streams are born there. An existing organisation moves with the marker flow used
  on 2026-11-15: stop its admission, copy and verify its working set, write the marker, switch, keep the old
  objects until the verification window closes.

**Judged by:** the phase 2 table again on the new bucket, and the materializer's chain at or under 750 ms p50
(the benchmark's in-region median was 717 ms). **Undone by:** per organisation: the marker points back; the old objects
are still there. **Needs OK:** the bucket, key, policy and grants (as code), and each organisation's move.

### Phase 5: retire the legacy admission path for switched streams

Compaction and retention for entries, materializer markers and the stream registry (the log-commit design's
Remaining 5 and 8), then removal of the per-batch admission path for log-mode streams. Not before every
organisation has run in log mode for a full retention window.

### Phase 6, a separate decision: S3 Express as an accelerator

Worth deciding only after phase 4, and only for hosted. Prerequisites the benchmark found:

- **The object-store client cannot use directory buckets today.** It signs for `s3` with
  `x-amz-security-token`; directory buckets need `s3express` and a session token. The benchmark wrapped it.
- **Its credential cache would create a session per request.** It refreshes 300 s before expiry and a session
  lasts 300 s. Measured: 56 ms per PUT instead of 8. The cache needs a refresh window per credential kind.
- **One zone.** A directory bucket lives in one Availability Zone and keeps one KMS key for life (a put naming
  another key is refused). Durability across zones needs a second bucket and a background copy to standard
  storage; the dual-zone write measured 1 to 6 ms on top of a single write.

It would hold log entries and payloads only; standard storage stays the system of record.

## Risks

- **The backlog moves instead of shrinking** (see "What this means"). Phase 2's third row and phase 3's lag
  watch exist for this. The fix, if needed, is coalescing in the materializer.
- **Mixed writers on one stream.** Entry 0 is the per-stream marker and every writer checks it; phase 1's
  reviewed switch path must keep the drain check the operator tool has today.
- **Two ledgers during phase 4.** One organisation must never be admitted through both buckets; the marker flow
  and its verification are the guard, as on 2026-11-15.
- **Key-policy isolation changes with Bucket Keys.** A revocation takes effect only when the bucket-level key
  ages out, and prefix isolation no longer comes from KMS.
- **503s under load.** S3 answered 503 on PUTs from 16 in flight on fresh prefixes. The writer's retry of the
  same bytes covers the entry; the payload PUTs need the same backoff.
- **The benchmark's replay is lighter than a real batch.** Phase 2 measures the real path before anything is
  switched in production.

## Open questions for Khaliq

1. Is IAM-condition prefix isolation acceptable for the new bucket, or do per-user envelope keys (0008 option
   F) have to come first? This decides whether phase 4 can start when phase 3 is stable.
2. Phase 3 before phase 4 (relief first, on the existing bucket), as proposed, or the reverse (one cutover per
   organisation instead of two changes)?
3. Who owns the materializer's coalescing if phase 2 shows it is needed: the log-commit work, or a separate
   change?
4. Does self-host get the log commit at the same time, or after hosted has run it? It is portable (it needs
   only create-if-absent), but the GCS protocol run on a real backend is still open in the log-commit design.

## Not in this plan

- Relisting the whole admission inbox on every scan. It is what makes ingest-edge's scans take minutes at a
  backlog of 17,000 objects, and it is a separate fix (a cursor or an index), already noted in the group-commit
  design. The log commit reduces how much sits in the inbox; it does not remove the relist.
- GCS and Azure benchmarks. Conformance stays in CI.
- Durable actors per stream (0008 option D beyond the per-machine stream, which the log commit already is).
