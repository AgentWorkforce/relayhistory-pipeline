# 0005: Upload accounting lives in S3, not in SQL

**Status:** accepted (Khaliq, 2026-11-12, in the relayhistory-pipeline session: "don't have another
sqlite to manage the accounting around uploading to s3", then "go ahead with this basis").
Each deploy, flag change and the backlog drain still needs his explicit OK, as AGENTS.md says.

**Amends:** `docs/decisions/0004-direct-upload-inbox.md`. 0004's front half stays: the desktop PUT,
the witness, `promoteUpload`'s checks, the status and outcome objects, and the infra. Only the hand-off
changes. 0004's consumer passes each upload to `deliverPrepared` with the SQL ledger
(`admission-consumer.ts:120-139`, `ledger-sql.ts`). That is the accounting this decision removes.

**Covers:** the hosted upload-admission Lambda (#561, reworked) and the self-host consumer
(ingest-edge, `selfhost-admission.ts`). Neither may admit through `ledger-sql.ts` once this lands.

## Context

The buffered cutover moved upload bytes to S3, but not the bookkeeping. Every batch still makes four
Supabase round trips before its receipt: `ingest_claim_buffered_batch`, the slot lease,
`ingest_prepare_buffer_stage` and `ingest_commit_buffered_batch`. Those calls leave one
`ingest_batches` row and one `ingest_buffer_outbox` row. The publisher that would drain the outbox into
the canonical layout has not run, so the outbox only grows, and sessions uploaded since the cutover
never reach the dashboard.

The async designs move the SQL off the request path (`docs/design/s3-first-async-ingest.md`,
`docs/selfhost-async-admission.md`, 0004), but they keep it as the accounting. Every admission is
still a claim, a stage intent and a receipt ACK in Postgres. The fix is to stop needing it.

## What the SQL ledger decides, and where each decision goes

| Today (`ledger.ts`, SQL) | 0005 (S3) |
| --- | --- |
| Batch idempotency (`ingest_batches`): a retry replays; a reused batch id with other content is a 409 `batch_id` | A **batch record** object, written create-only. Same digest and committed: replay. Same digest and pending: resume. Other digest: 409. |
| Stored revisions (`ingest_record_revisions`, keyed per origin and record): an equal or lower revision is `stale` and skipped | A **revision index** keyed per (origin, record id), sharded by a hash of the record id. Each entry carries its generation: the session's fence epoch and the user's reset count when it was written. A claim reads the shards its records fall in, and an equal or lower revision of the current generation is `stale`, exactly as relayhistory-cloud #250. An entry of an older generation counts as absent, so a disable followed by a re-enable, or a reset, lets the re-upload through. The commit merges written revisions with `If-Match` (a first write is create-only): max wins, and an equal revision keeps the stored digest. This is required, not an optimisation: the reader fold is per origin and refuses equal revisions with different content (`chooseRelayhistoryRevision`, `packages/pipeline/src/lib/pipeline/relayhistory-transform.ts:377-392`). |
| In-batch revision reuse: 409 `record_revision` | Computed from the batch alone. It never needed stored state. |
| Deletion fences and resets (`upload_deletions`, `upload_resets`) | One **fence object** per user, edited with CAS. The claim reads it to get `deleted` and `resets`. |
| Fence recheck at commit, erasure re-arm (`upload_erasure_rearm`) | The commit re-reads the fence. If a reset, or a deletion of a written session, landed in between, it writes a **re-arm marker** for the eraser. That is today's `rearmed` semantics. |
| Tenant slots (`acquireSlot`) | None. The Lambda's reserved concurrency (4) and SQS batch size 1 bound it. Self-host is bounded by its scheduler. |
| Outbox and publisher (`ingest_buffer_outbox`, `ingest_publication_*`) | **None.** The consumer writes the canonical layout directly (`writeBatchObjects`, inline mode). The existing `manifest.json` bucket notification drives the indexer, as it did before Nov 5. |
| Privacy journal, admission status, outcomes | Already S3. Unchanged. |

### Objects

All of these sit under the per-user admission directory that already holds `privacy-journal.json`
(`async-admission.ts:125`). That prefix is never desktop-writable.

- `…/ledger/batches/o=<sha256(origin)[0:32]>/b=<sha256(batchId)[0:32]>.json`
  `{v:1, originId, batchId, digest, receiptId, state:"pending"|"committed", claimToken, fenceVersion}`.
  It is written `If-None-Match: *` at claim and moved from `pending` to `committed` with `If-Match` at
  commit.
- `…/ledger/revisions/o=<sha256(origin)[0:32]>/r=<shard>.json`
  `{v:1, records:{<recordId>:[revision, revisionId, digest, epoch, resets]}}`. It is read at claim and
  merged at commit (create-only for the first write, `If-Match` after), retrying on
  `precondition_failed`. The shard is a fixed-width prefix of `sha256(recordId)`.
- `…/ledger/owners/o=<sha256(origin)[0:32]>/s=<sha256(JSON [source, sessionId])[0:32]>.json`
  The session's ownership stub, written at **claim**, before any canonical write, as
  `ingest_session_ownership` is. A reset lists these, so a session first uploaded while a reset is in
  flight is still caught by it.
- `…/upload-fences.json`
  `{v:1, instance, version, resets, lastResetAtMs, resetRequests, epochs, deleted:{<origin>:[[source, sessionId], …]}}`.
  `instance` is random per fence, so a deleted and recreated fence never matches an old snapshot.
  `resetRequests` makes a retried reset idempotent by request id. A reset tombstones every owned
  session whose generation is below the new reset count, so overlapping or interrupted resets still
  finish. Every selection, reset or erase
  edits it with `If-Match`, retrying on `precondition_failed` as `editJournal` does. It fails closed:
  an unreadable fence is a retry, never an empty fence.
- `…/ledger/rearm/<sha256(source \x1f sessionId)[0:32]>.json`
  `{v:1, originId, source, sessionId, machineId, atMs}`. It is written create-only, and the eraser
  sweeps it.

### What the S3 ledger refuses

It refuses these rather than falling back: buffered acceptance mode, the staged fan-out, babysitter
retention routing, frozen-supabase custody and the Neon replica. Each needs SQL today, and none of
them is on the direct-upload path. The consumer runs inline with no `inlineMaxSessions`, because a
Lambda has no Worker subrequest cap.

## Request cost

- **Claim:** one GET (fence), one GET per session in the batch (revision index) and one create-only
  PUT (batch record).
- **Commit:** one GET (fence), one CAS PUT per written session (revision index) and one CAS PUT
  (batch record).
- **Write:** the same canonical writes as before Nov 5.

A typical batch touches a handful of sessions, so that is roughly 4 + 2 × sessions small S3 requests,
against four Supabase round trips today. The difference is that S3 requests have no lock waits, and
the consumer is off the request path anyway.

## Controls (selection, reset, erase)

The fence object becomes the authority for what the ingest stores. Until reads move:

1. The control routes write the S3 fence first and SQL second (`applyOrderedControl`,
   `setSessionUploads`, `resetUploadedSessions`). A failed S3 write fails the control.
2. The privacy journal still brackets every control, so an upload witnessed before a reset is refused
   after it.
3. The operator user erase (058010) also deletes the user's fence, batch records and re-arm markers.

The read side (session-read-api deletion filters) and the eraser move to the fence object in a
follow-up. They are named here, so nothing new may start reading `upload_deletions`.

## The buffered backlog

There is no outbox replay. A one-off **drain job** works through the S3 objects directly, after a
one-time **fence backfill**:

0. **Fence backfill.** The backlog's deletions and resets were recorded in SQL (`upload_deletions`,
   `upload_resets`). Before the drain, a one-time job copies each affected user's current fence state into
   `upload-fences.json`, and from then on controls dual-write it (see Controls). This is a one-time
   migration of existing state, not ongoing accounting. The drain refuses a user whose fence has not been
   backfilled, so it can never republish a deleted session.
0b. **Accepted-stage export.** S3 alone cannot tell an accepted stage from an abandoned one: a stage
   object can exist after `putBufferedStage` while its receipt was never committed, for example when
   the consent gate refused it between the PUT and the ACK (`deliver.ts`). A one-time read-only export
   lists the stage keys whose receipt was acknowledged (`ingest_buffer_outbox.active_stage_token` and the
   committed `ingest_batches` rows) into an S3 manifest. The drain writes only stages on that list and
   reports the rest. Like the fence backfill, this is a one-time migration read, not ongoing accounting.
1. List `buffered-batches/v1/` staged objects. Each carries its tenant, origin and grouped scrubbed
   lines (`staging.ts:222`).
2. Read the user's fence and privacy journal. Drop deleted sessions and batches the user's last reset
   superseded.
3. Read the sessions' revision indexes and drop stale records. Write the rest to the canonical layout
   through a drain-specific entry, idempotent under manifest CAS. The staged fan-out (`staging.ts:154`)
   refuses buffered objects on purpose (`buffered_publication_refused`, `staging.ts:160`), so the drain
   does not relax that check; it calls the writer itself. Then merge the written revisions into the
   indexes.
4. Write a create-only `drained` marker next to each staged object, so a rerun skips it.

The drain replaces the publisher path for the existing backlog. Until it has run and been verified,
the `ingest_publication_*` tables, their claim and ack functions, and the frontier row contract
(`20261104060000`) stay intact. Rate is bounded by job concurrency. The job is dry-run first against dev, then against prod with
read-only listing, and then for real with his OK. Afterwards the outbox, batches and publication
tables are dead weight to drop, with his OK. Dropping them needs its own migration slot and updates to the
schema drift lists.

## Rollout

- **Flag:** `INGEST_LEDGER` = `sql` | `s3`. It defaults to `sql`, which leaves the Worker and every
  other path byte for byte as today. The upload-admission Lambda and the self-host consumer refuse to
  start with `sql` once this lands. Their only ledger is `s3`.
- **Dev:** the agent-relay-dev end-to-end test with direct uploads for one workspace. A desktop upload
  must become searchable with zero new `ingest_*` rows.
- **Prod:**
  1. Deploy the infra from 0004.
  2. Set `HISTORY_UPLOAD_DIRECT=allowlist` for Khaliq's workspace.
  3. Compare against the HTTP path.
  4. Widen, then set it to `on`.
- **The Worker's HTTP path:** once direct uploads carry all traffic, the Worker's buffered SQL path is
  retired with it (0004, "Worker: retired last").

## Risks

- **Revision index contention.** Two batches for records in the same shard race on its CAS. The loser
  re-reads and re-merges. Merging is commutative (max wins, an equal revision keeps the first, a newer
  generation is never overwritten by an older one), so the order doesn't matter.
- **Index and manifest disagree after a crash.** A crash between the canonical write and the index
  merge leaves the batch `pending`. The retry resumes, rewrites idempotently under manifest CAS, and
  then merges. The index can lag the manifest, but it never leads it.
- **Fence growth.** The fence only grows (deleted sessions, epochs, reset request ids). Compaction of
  entries older than the user's last reset is a follow-up.
- **Fence contention.** One user's controls and admissions all touch one fence object. Controls are
  rare, admissions only read it, and CAS retries are bounded (`privacy_journal_contended` is the
  model).
- **Read side lags the authority.** Until session-read-api reads the fence, a selection is enforced at
  ingest and by the SQL dual-write, not by S3 alone. The dual-write keeps reads correct meanwhile.
