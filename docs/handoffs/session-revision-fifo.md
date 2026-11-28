# Per-session revision FIFO: coalescing and serialization (T126)

Status: app-side code on `review/latency-session-coalesce-fifo`, everything default off. The
queue, event source mapping, IAM and alarms are infra work (AgentWorkforce/infra, via the
hosted composition). Nothing here deploys or applies anything.

## Problem

Every manifest PUT notifies the standard `session-events` queue (DelaySeconds 30, batch 10,
window 20 s). Revisions of one session land in different concurrent batches, so:

- passes on one session contend for its search lock, which requeues the loser with a 60 s delay
  (`scope_busy`: thousands per hour at peak);
- a long session is re-indexed once per batch that touched it: quadratic work on the internal fleet;
- the fixed 30 s delay plus the 20 s window come before the first pass of every upload.

## Design

```
writer (manifest CAS ack) ──► FIFO revision queue ──► Lambda (ordered mode)
                                  ▲   MessageGroupId = session scope
S3 notification ─► standard queue ─┤   MessageDeduplicationId = one physical write
     (SESSION_EVENTS_MODE=forward: re-key only)
scheduled reconcile ──────────────┘
```

- **Group** = sha256 of (key prefix with its org, workspace, harness, session): every machine
  and start date of one logical session shares one group. SQS delivers a group to one batch at a
  time, in order, so a session never runs two passes at once and its lock is never contended
  from this path.
- **Coalescing.** Arrivals during an in-flight pass wait in the group and come together in the
  next batch, which collapses them into one pointer pass per key and one search pass per scope.
  The first eligible revision starts at once: DelaySeconds 0 and no batching window (FIFO
  sources do not support one).
- **Identity** = sha256(key, put|delete, S3 versionId, ETag). The bucket is versioned, so every
  PUT, including a re-create after erase that reuses a revision number, gets a new ID. The
  writer copy and the forwarded copy of one write share an ID and SQS keeps one. A delete is
  named by its S3 sequencer. The revision number is logged but never part of the identity.
- **Commit check.** The event is a hint. The pointer stage re-reads the live manifest under
  the session lock, and the consumer compares the event's versionId (else its ETag) with the
  live one. A mismatch is logged as `revision superseded` and nothing from that event is
  published. A delayed retry cannot expose an older revision, because the consumer never reads
  the event's content, and the pointer upsert stays revision-fenced as before.
- **Busy, deferred, liveness change** in ordered mode: FIFO has no per-message delay and a
  requeue would reorder the group. The group's messages fail with a shortened visibility
  instead: 5 s, then doubling per receive up to 300 s. Any failure in a group also fails every
  later message of that group in the batch (FIFO partial-batch rule). Followers get the short
  visibility; a message that failed on its own keeps the normal visibility and DLQ path.
- **Superseded-only batches** (for example the trailing ceil(n/10) batches of a backlog over 10): the
  search pass is skipped (`search index skipped`, reason `superseded_in_group`) only when coverage is
  PROVEN. Every message behind the scope must be a stamped revision whose write is no longer live, none
  may be a delete, and every key's pointer outcome must be `unchanged`, AND there must be DURABLE proof that a COMPLETE search
  pass already covers the scope's live pointer set (`coverageProven` in search-index.ts: the same directory
  readiness and sources evidence skip-current uses). An `unchanged` pointer alone proves nothing: it can be this
  message's own earlier attempt, which advanced the pointer and then crashed its search pass (Pipeline review of
  16feaf75). No proof, or a failed proof read, runs the pass. Because that proof compares keys and REVISION numbers
  only, the consumer first checks PHYSICAL currency against the pointer rows: for every key of the batch, the
  stored `manifest_etag` must be the live object the pointer pass just read. Otherwise (for example A indexed, then
  B replacing it at the same key and revision with its notification lost) there is no proof and the pass runs
  (Pipeline review of b41e49bf). If THIS pass advanced the pointer, it runs the search pass itself:
  the live write's own message may be lost, and reconcile would then see the pointer matching S3 and send
  nothing (Pipeline review of 51a8721b). No search state is read, so this does not depend on
  `SEARCH_INDEX_SKIP_CURRENT_ENABLED`.
- **Enqueue-failure recovery** (commit succeeded, enqueue lost):
  1. the writer hook never fails a write (`WriteTarget.onManifestCommitted`);
  2. the write's own S3 notification is the durable outbox. With `SESSION_EVENTS_MODE=forward`
     the standard queue only re-keys manifest notifications onto the FIFO queue, with no
     database or manifest read. A failed forward fails exactly that notification, which the
     standard queue retries and dead-letters as today;
  3. the scheduled reconcile, in forward mode, sends to the FIFO queue too: the net under a lost
     S3 notification. A reconcile message is a recovery HINT, not the write's identity. The S3
     listing carries an ETag but no versionId, so it cannot reproduce the writer's ID. It is deduped
     in its own namespace, one hint per key per reconcile chain link, so it never collides with a
     write or an earlier hint (an identical-byte re-create has the same ETag).
- **Erase fences** are unchanged. Deletes arrive through the forwarder in the session's group,
  in order. Operator-erase fences live in the pointer upsert and are not bypassed.

## Measurement

| line | fields |
|---|---|
| `revision searchable` (one per live upload) | `upload_kind` initial/append, `revision`, legs on the consumer clock: `picked_up_at_ms`, `commit_to_pickup_ms` and `pickup_wait_ms` (to consumer PICKUP: the batch arrived, before any pointer work; the 7s budget), `pointer_started_at_ms` and `pointer_ms` (this key's pointer work; the 2s budget), `pass_started_at_ms`, `commit_to_start_ms` and `start_wait_ms` (to the covering search pass's start), `commit_to_searchable_ms` and `queue_wait_ms` (to its completion), `enqueued_at_ms`, `receive_count`, `event_source`, `already_current`, `incarnation_sha256` |
| `revision superseded` (one per coalesced or stale event) | `revision`, `superseded_by_revision` or `superseded_by_delete`, `group_lag_ms`, `event_source` |
| `search index reconciled` / `skipped` | adds `enqueued_at_ms`, `queue_wait_ms`, `receive_count`, `coalesced` |
| `revision retry scheduled` | `reason`, `delay_seconds`, `count` |
| `revision events forwarded` | `count`, `failed`, `skipped` |

Readout labels (use these exactly):

| field | label | start clock |
|---|---|---|
| `commit_to_pickup_ms` | manifest commit to consumer pickup (worker start, before pointer work) | `event_source=writer`: writer clock at the CAS ack; `forward`: S3 eventTime; `reconcile`: none (field absent) |
| `pickup_wait_ms` | SQS enqueue to consumer pickup | SQS SentTimestamp |
| `pointer_ms` | this key's own pointer work | consumer clock |
| `pointer_started_at_ms + pointer_ms - picked_up_at_ms` | the 2s pointer leg: pickup to this key's pointer completion, INCLUDING time waiting behind earlier keys of the batch | consumer clock |
| `commit_to_start_ms` / `start_wait_ms` | manifest commit / SQS enqueue to covering search pass start | as above |
| `commit_to_searchable_ms` | manifest commit to covering-pass completion | as above |
| `queue_wait_ms` | SQS enqueue to covering-pass completion (includes processing) | SQS SentTimestamp |

The 7s pickup budget is `commit_to_pickup_ms` (worker start, `picked_up_at_ms`). The 2s pointer budget is pickup to
this key's pointer completion (row above), not `pointer_ms` alone. `pass_started_at_ms` is the later search-pass start,
never the pickup endpoint.
`commit_*` uses the writer's commit time, or the S3 eventTime for forwarded events. These are
pipeline-leg diagnostics, not the end-to-end score: they start at the manifest commit, not at
Desktop's first HTTP attempt, and end at the pass, not at an authorized read. Count superseded
events, `superseded_in_group` batches and executed passes separately; they are not additive. A
reduction percentage needs an uncoalesced baseline on the same arrivals. A pass that crashes logs
no line, so its revisions show up only as a later `revision searchable` (with `receive_count` > 1)
or not at all; count unobserved starts from the arrivals side.

## Switches (all default off)

| env | values |
|---|---|
| `SESSION_REVISION_QUEUE_URL` | an SQS FIFO URL. Unset: exactly the previous behaviour. Set: FIFO batches run in ordered mode. |
| `SESSION_EVENTS_MODE` | `direct` (default): the standard queue is consumed as today. `forward`: its manifest events are re-keyed onto the FIFO queue, and reconcile sends there. |
| `SESSION_REVISION_BUSY_RETRY_SECONDS` | 1-60, default 5 |

The writer hook is code only: wiring it into the ingest Worker needs a FIFO send grant for the
writer identity, which is its own reviewed slice. Until then the forwarder is the only producer,
and it adds one sub-second hop. When wired, the hook runs on the ingest request path after each
group's CAS, so its sender must use a short timeout (about 1 s): a slow SQS only delays the fast
path, and the forwarder still delivers the event.

## Rollout

1. Infra: create the FIFO queue, its DLQ and the ESM (disabled), and grant the function
   `sqs:SendMessage` on the FIFO queue plus the ESM read actions.
2. Set `SESSION_REVISION_QUEUE_URL` and enable the FIFO ESM. No producer yet, so nothing
   changes.
3. Set `SESSION_EVENTS_MODE=forward` and drop the standard queue's DelaySeconds to 0 and its
   batching window to 0 (the forwarder is cheap). Watch `scope_busy`, which should go to about
   0, `revision superseded` versus `revision searchable`, and `commit_to_searchable_ms` by
   `upload_kind`.
4. Rollback: unset `SESSION_EVENTS_MODE` (back to direct consumption), drain the FIFO queue,
   and disable its ESM.

## Known limit (pre-existing, not changed here)

`sessions.upsert_session_index` applies only when `excluded.revision > s.revision`. After a plain
delete tombstones a row at revision N, a re-create that reuses revision ≤ N is `unchanged`: the
new content is not searchable until revision N+1. This happens on the standard path too, and it
is pinned in `revision-fifo.pglite.test.ts` (T126) so a fence fix flips it deliberately.

## Tests

- `src/session-pipeline/__tests__/revision-events.test.ts`: group and dedup derivation; T126
  IDs (both incarnations admitted, identical retries deduplicated); the writer hook never
  throws; forwarder recovery of a failed writer enqueue; partial and thrown SQS failures;
  deletes; batching; the reconcile sender; parsing; the env grammar.
- `src/session-pipeline/__tests__/revision-fifo.pglite.test.ts`, on the real pointer stage
  (PGlite with every migration): a burst of five revisions gives one pass and four
  superseded; initial and append measured separately; two machines coalesce; busy gives a
  visibility retry with no requeue, then coalesces with a later arrival; backoff per receive;
  the FIFO partial-batch rule; a delayed older revision is never exposed; T126 erase and
  re-create with old work paused inside the lock; dispatch fallbacks.

Mocked boundaries: SQS (records built as the ESM delivers them; visibility recorded), the search
stage (a model index over live pointer rows), and the fs store's ETag standing in for the S3
versionId.
