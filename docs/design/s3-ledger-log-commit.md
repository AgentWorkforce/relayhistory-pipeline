# S3 ledger: numbered-log-entry commit

Status: proposed. Slices 1 (writer and reader), 2 (admission wiring), 3 (materializer, operator tool), 4 and 5 (review
fixes) and 6 (the before-production items of Remaining) are implemented behind `INGEST_LEDGER_LOG_COMMIT` (default
off). With `on`, only a stream whose entry 0 exists is admitted through its log; entry 0 is written only by the
operator tool (`scripts/ledger-log-stream.ts switch`). Production still needs: the review of slice 6, the
`OnlyTheEraserDeletes` carve-out and the operator role's grants (Remaining 10), lifting the hosted and stack refusals
together, and an explicit OK to deploy; the hosted runtime and the stack input both still refuse `on`. A deliberate,
dormant operator path to switch ONE production stream (and back) is in "Production pilot" below; it changes none of
those refusals.

Builds on the group-commit design (`docs/design/s3-ledger-group-commit.md`, rev 3) and the hosted ledger
decision record (0008, benchmark first). It replaces that design's per-session manifest CAS on the critical path
with an append-only log per stream, which is the pattern behind the Delta log, WarpStream, Turbopuffer and
Datadog Husky.

## Why

Today one batch admission is a chain of about 13 dependent S3 requests (claim, fence and batch docs, revision
shards, manifest CAS, commit, outcome, tombstone), and they serialize per user. The benchmark smoke run
(2026-11-16, in-region us-west-2 S3 Standard) measured:

| Commit path | Throughput | p50 |
|---|---|---|
| Today's 13-op chain, 8 in flight | about 13 batches/s | about 600 ms |
| Log-entry commit, 1 stream | about 22/s | about 40 ms |
| Log-entry commit, 64 streams, 8 writers | about 200/s | about 40 ms |

Ambiguous-commit kill tests lost nothing and tore nothing, and readback showed no gaps and no duplicates.

## Streams

A stream is one `(org, workspace, user, origin)`. Order across one user's machines is not required (decision
2026-11-16), so each machine's stream commits independently. Per-session order still holds, because one session
lives on one origin. Key root:

```
<keyRoot>/<admissionUserDir>/ledger/log/o=<h(origin)>/
  e/<first 17 digits>/<seq, 20 digits>.json
                              immutable entries, written once, 1000 per directory
  c/<seq>.json                checkpoints (optional, every 64 entries)
  head.json                   hint: newest checkpoint seq (no CAS; may lag; always verified)
  p/<sha256>.json.zst         content-addressed payloads
```

Only create-if-absent on NEW numbered keys is used on the commit path: S3 `If-None-Match: *`, GCS
`ifGenerationMatch=0`, Azure `If-None-Match: *`. The object-store conformance cases
`create_is_idempotent_for_same_content` and `create_conflicts_on_different_content` already pin these per
backend. No object is compare-and-swapped on the commit path, which avoids GCS's limit of about one write per
second per object name.

## Entries

```
{ v: 1, stream: <the stream's key root>, seq, prev: sha256(previous entry bytes) | null, kind, epoch, writer, atMs, ... }
kind "open"    seq 0 only, epoch 1: the MODE MARKER; carries legacyCutover
kind "epoch"   takes ownership: epoch = previous + 1 exactly
kind "batches" batches: [{ batchId, digest, receiptId, sessions: [[source, sessionId]], payloads: [{key, sha256, size}] }],
               inboxCursor
```

The `prev` hash chain makes a reorder or a torn write detectable, and the reader fails closed (`LogCorrupt`). A
deleted or lost MIDDLE entry is caught too: when head + 1 reads missing, the reader lists the two entry
directories that could hold head + 1 and its successors (at most two LIST pages, whatever the stream's length).
If an entry is stored past head, it reads head + 1 AGAIN: a stream being appended to keeps growing, so a concurrent
append is replayed, not mistaken for a gap. Only a direct re-read still missing, with a later entry present, is
`log_entry_gap`. With no entry 0, the first page of `e/` is listed; if any entry exists, entry 0 is read again
(a concurrent `enableLogMode` may have just created it) and only a re-read still missing is `log_open_missing`, so
the stream never reads as legacy. A transient store fault (network, 408, 429, 5xx) on any listing, entry read during open/replay, or
read-back is `LogUnavailable` (retryable). Any other error (for example access denied, or a programming error) stays
raw and is never reported as `LogCorrupt`.

What is detectable: a lost entry 0, and a lost middle entry while one of its next 1000 successors still exists. What
is NOT: a deleted TAIL (the newest entries, with nothing stored after them) reads as a shorter log; the next append
then writes that seq again. Bounding that harm (for example by having the materializer's durable cursor refuse a head
below it) is for the materializer slice. The entry directories (rather than a
start-after LIST) keep the proof bounded on every backend with no change to the ObjectStore interface. A batches entry must carry the current epoch AND be written by
the writer whose open or epoch entry set it. Its payload keys must be this stream's content addresses, and an entry
whose `stream` names another stream is refused.

## Commit path

1. PUT the payloads, content addressed, in parallel. They are idempotent, so a retry rewrites the same key.
2. Dedupe against the remembered-batches window. Same `batchId` and digest is a replay; same `batchId` with a
   different digest is a conflict (as the legacy `batch_conflict`). A batch repeated inside one call is a replay
   of itself.
3. Create entry `head + 1` with the admitted batches and the inbox cursor. That one request is the commit.

The window is `LOG_REMEMBERED_BATCHES` = 4096 batchIds PER STREAM, carried in full in every checkpoint and replayed
from entries on open. A batchId older than the window is not remembered: an evicted id is admitted again (tested). This
is NOT the legacy bound (the legacy manifest remembers 64 batches per session), so the legacy batch docs and outcome
markers stay the long-term replay answer during the transition. An append dedupes against the window plus its own
batches without copying the window.

## Single writer and fencing

One writer per stream. A writer takes the stream by appending an `epoch` entry. The state records which writer set
the current epoch (`epochWriter`), and `appendBatches` refuses any other writer (`log_not_owner`) before writing.
Two openers racing for entry 0 therefore get the same state, but only the one whose entry 0 landed may append. A
stale writer's next append targets a seq that is already taken: the create conflicts, the writer reads the entry
back, finds different bytes, and stops (`LogFenced`). There is no fence object to CAS, and the fence changes only
when ownership changes, never per batch.

The writer id IS the ownership identity: the same id is the same owner. A restart reusing an id regains the stream
without a take, and two processes sharing one would interleave appends (the chain stays linear, but the inbox
cursor could move backwards). The wiring therefore uses a per-process-lifetime id minted at process start, never a
configured or host-derived one.

`takeStream` first resolves any pending entry. It keeps its own epoch entry pending until that resolves, so an
ambiguous take resends exactly that entry. A take that loses its race does NOT take again: it catches the state up
and throws `log_take_lost`, so two takers back off (the caller's retry decides) instead of fencing each other in a
loop. Linearity always held (the head + 1 create); this makes ownership explicit too. A take by the writer that
already holds the epoch (after its pending entry is settled) appends nothing (slice 6), so retries never add epoch
entries.

## Ambiguous commits

A 5xx or timeout on the entry PUT may still have landed. The writer:

- retries the same bytes up to `LOG_PUT_ATTEMPTS`. An identical re-put is an idempotent success.
- then reads the entry back. Our bytes mean it landed; different bytes mean fenced; absent means `LogUnavailable`,
  with the entry kept as `pending`.
- on a pending entry, must resend exactly those bytes at that seq before writing anything else. It never writes
  different content at a seq whose outcome is unknown, because a late landing would then conflict.

## Readers and consumers

`readEntries` yields the validated chain from any seq. A batch stays invisible to readers (manifests, the revision
index, search, the read API) until it is materialized from the log. Planned materializer:

- a per-stream consumer reads entries after its own durable cursor (`c/` checkpoint plus a consumer cursor
  object);
- it applies batches to the existing per-session manifests and the revision index in log order;
- it advances its cursor.

This takes manifest and index work off the admission critical path and batches it, one manifest update per
session per N entries. The reads stay as they are today; only the writer of those objects changes.

Compaction: payloads stay content-addressed. Entry 0 is NEVER deleted: it is the mode marker, and its loss with
other entries present reads as `log_open_missing`. Since slice 6, `readEntries` and `openStream` start from a verified
checkpoint, so entries strictly between entry 0 and a materialized checkpoint are no longer read by either. Deleting
them (after a retention window) is what compaction would do; it is NOT implemented (Remaining 5), and a reader whose
checkpoint fails verification still falls back to entry 0 and needs every entry, so compaction must also keep the
checkpoint it relies on and stop deleting while a checkpoint fails to verify. Payloads live under `ledger/log/o=<origin>/p/` in the
user's admission directory. The erase paths (operator erase, session-selection erase, reset) must find and delete
them there, and that is part of the materializer slice.

## Privacy gates (unchanged in meaning, rev 3)

These checks run per member batch, BEFORE the entry is written. A refused batch is left out of the entry and
answered individually:

- the privacy journal's reset seq against the batch's own journal seq;
- consent, using the inbox `consentCap`, at admit and again before the entry write;
- `upload_deletions` and the fence's deleted sessions (read once per group, after the claim).

A deletion or reset that lands after the entry is applied by the materializer, which re-checks the fence for each
session before writing a manifest. It is the same "entry forgotten by generation" rule the revision index uses
today.

## Mixed versions and rollback

- A stream is either legacy or log mode. Entry 0 is the durable per-stream marker.
- Before a stream switches, every writer that can touch it must run code that checks for entry 0 and refuses
  (retries) a legacy commit on a log-mode stream. That check is part of the wiring slice, and the flag stays
  refused until it ships everywhere.
- Switching a stream: drain its legacy in-flight work, write entry 0 with `legacyCutover` (the last legacy batch
  committed), then admit through the log.
- Rollback: turn the flag off for new streams. A log-mode stream rolls back only after the materializer has
  applied every entry. Then a "close" marker (planned) hands the stream back to legacy.

## Slice 1: writer and reader

- `src/session-ingest/ledger-log.ts`: the keys, entry and checkpoint schema, validation, `enableLogMode`,
  `takeStream`, `appendBatches` (dedupe, create-if-absent, read-back, pending resend), `openStream` (from a
  verified checkpoint or entry 0), `readEntries`, `putPayload`, and the `INGEST_LEDGER_LOG_COMMIT` parse.
- `selfhost-config.ts`: in slice 1, `INGEST_LEDGER_LOG_COMMIT=on` was refused at startup (slice 2 accepts it with
  `INGEST_LEDGER=s3`); `off` changes nothing. The hosted upload-admission runtime (`upload-admission-hosted.ts`)
  refuses `on` (`ledger_log_commit_unsupported`); the hosted Worker never runs the S3 ledger at all (it refuses
  `INGEST_LEDGER=s3`).
- A checkpoint is trusted only as far as its hash plus its fields cross-checked against entry `seq`: the entry's
  epoch and writer must equal the checkpoint's epoch and epoch writer, and for a batches entry its inbox cursor must
  equal the checkpoint's and every batch in it must be in the checkpoint's remembered window. One that fails any
  check, or has another `legacyCutover`, an epoch below 1 or a window larger than `LOG_REMEMBERED_BATCHES`, is
  ignored, and the open replays from entry 0.
- Tests (`ledger-log.test.ts`, fs store):
  - flag parsing and the refusal;
  - the mode marker: idempotent; only the opener whose entry 0 landed owns it (two openers racing); a different
    `legacyCutover` is refused;
  - append, replay and conflict, including a different digest inside one call;
  - the 4096 window: an evicted id is admitted again, a remembered one conflicts;
  - a reopen matches, with or without a valid checkpoint (a missing checkpoint and a hash-mismatched one are
    ignored);
  - a foreign payload key is refused before any write;
  - an epoch entry fences the old writer, and a reopened old writer is not the owner;
  - two appends genuinely racing (a store that releases both creates together): exactly one lands;
  - two takers racing: one owns, the loser gets `log_take_lost` and no second epoch entry is written;
  - an ambiguous take keeps its epoch entry pending and resends it;
  - landed-but-503 counts as landed;
  - never-landed gives `LogUnavailable`, and the retry resends the same bytes first;
  - crash at every PUT prefix, failing before the write OR the process dying after it landed (each asserted to
    fail), with the process state dropped and the work finished by a new writer: a valid chain, and every batch
    exactly once;
  - readers refuse a missing middle entry (also when its successor sits in the next entry directory), a lost
    entry 0, a bad prev hash, a stale-epoch batches entry, malformed JSON and an entry from another stream;
  - concurrency is not corruption: an entry appended between a reader's last read and its listing is replayed
    (open and readEntries), and entry 0 created between an opener's read and its listing is read again;
  - two openers genuinely racing for entry 0, and an owner's append racing another writer's take: exactly one
    lands, the other backs off;
  - a store fault on the gap listing is `LogUnavailable`;
  - a checkpoint with another `legacyCutover`, or an oversized window, is ignored;
  - payloads are idempotent.

## Slice 2: admission wiring

- `ledger-log-admit.ts` `LogStreams`: per batch, in order:
  1. the fence: a pending reset or an unfinished one is a retry, and records of deleted sessions (tombstones plus
     pending ordered disables) are dropped, as the legacy judge drops them;
  2. the privacy journal: a reset completed after the upload began refuses it (409), and one in flight is a retry;
  3. consent at admit;
  4. the payload PUT (the kept records and the receipt);
  5. consent at commit;
  6. the entry append. Replay and conflict come from the window, and a conflict is the legacy `batchIdConflict`
     409.

  One owner per stream per process: the first write takes the stream with an epoch entry and caches the state. A
  `LogFenced` (`log_take_lost`, `log_not_owner`, `log_entry_taken`) drops the cached state, so the queue retry reopens
  and takes again. A `LogUnavailable` keeps the state, so the retry resends the pending entry.
- `admission-consumer.ts`: with `logStreams` set, a batch whose stream has entry 0 goes through `LogStreams.admit`;
  every other batch takes the unchanged legacy path. The outcome markers and tombstones are the same, so
  `/v1/delivery/admission` answers the same.
- `ledger-s3.ts` `logModeGuard`: the legacy claim and commit HEAD entry 0 in parallel with their first reads, adding
  no serial round trip, and refuse a log-mode stream with `ledger_stream_log_mode` (a retry). So a legacy writer
  never commits behind the log, and that includes the synchronous HTTP path, which therefore answers 503 for a
  log-mode stream. Log mode needs async admission for that stream.
- `selfhost-config.ts` / `selfhost-server.ts`: `INGEST_LEDGER_LOG_COMMIT=on` is accepted with `INGEST_LEDGER=s3`
  (refused with sql). It turns on `logModeGuard` and gives the consumer its `LogStreams`. With `off` (the default)
  every byte is unchanged.
- Tests (`ledger-log-admit.test.ts`, the self-host edge end to end, fs store):
  - `on` alone changes nothing (legacy admits, the log stays empty);
  - a log-mode stream is admitted through its log (open, epoch, batches), never the legacy chain, and a conflicting
    batch id is refused;
  - the synchronous legacy claim refuses a log-mode stream (mutation-checked);
  - a fence-deleted session's records are dropped, a pending reset is a retry, and a later completed reset refuses;
  - fault injection on the entry PUT, before and after landing: exactly one entry and one admission;
  - equivalence: the same uploads reach the same per-upload outcome on a legacy edge and a log-mode edge.

## Slice 3: materializer and operator tool

- `ledger-log-materialize.ts` `LogMaterializer`: applies a stream's entries, in log order, to the read-side state by
  replaying each logged batch through the UNCHANGED legacy core (`deliverPrepared`: claim, canonical writes, commit)
  on a ledger without `logModeGuard`. It is the one legacy writer allowed on a log-mode stream. Because the legacy core
  is the only writer of manifests, chunks, the revision index, owners and batch docs, log mode yields the same state
  legacy admission yields, and the legacy properties carry over: a re-applied batch is a replay, a crash resumes the
  pending claim, two materializers race on the same claim tokens and the loser retries into a replay.
  - Progress: `m/<seq>.json`, written once after entry `seq` is applied; the highest is the cursor. A crash before
    the marker re-applies the entry (idempotent).
  - `received_at` is the admission time: the payload carries `receivedAtMs` (the append), and the replay writes with
    that clock, so a record's `received_at` is when it was accepted, not when it was applied.
  - Privacy at apply time (never resurrect erased data): the legacy claim re-reads the fence (sessions deleted after
    the entry are dropped; a pending reset is a retry); the privacy journal is re-checked against the payload's
    `journalSeq` (a reset completed after the upload began skips the batch); a batch whose payload is gone is skipped
    as erased (`eraseLogPayloads` deletes a user's unapplied payloads). A definitive refusal (403/409/413/422/400)
    skips the batch; anything else is a retry with the cursor unchanged. A read that sees a concurrent append as a gap
    is a retry too.
  - Payloads are deleted once their entry is applied; unreferenced payloads (a put whose append never happened)
    are collected after `ORPHAN_GRACE_MS` (1 h, by the store's own lastModified), so an in-flight admission's payload
    is never collected. Admission answers a batch id its window already holds without writing a payload.
  - Running: the consumer kicks the stream after each append (never awaited); every scan kicks every stream named in
    the registry `<keyRoot>/.ledger/log-streams/` (written when a writer first takes a stream and by the operator
    tool; hash-keyed pointers since slice 6), so a stream another process admitted is applied too.
- `ledger-log-stream-ops.ts` + `scripts/ledger-log-stream.ts` (dry run unless `--apply`):
  - `status`: legacy | log | closed, the head, the cursor, legacy batches pending and committed;
  - `switch`: refused while a legacy batch of the stream is pending (drain first) or the stream is not legacy; writes
    entry 0 with `legacyCutover = "committed=<n>"` and registers the stream;
  - `close`: refused until every entry is applied; writes `closed.json`. The consumer and `logModeGuard` both read it,
    so the stream is legacy again. One way: a closed stream is not reopened. A writer that had the stream cached may
    still append one in-flight entry; the materializer keeps applying the log.
- Tests:
  - `ledger-log-materialize.test.ts` (self-host edge, fs store):
    - equivalence: the same uploads on a legacy and a log-mode edge give byte-equal canonical objects and revision
      index, owners and rearm markers, the same batch-doc meaning (state, digest, origin, batch id; claim tokens are
      random) and the same per-upload outcomes; applied payloads are gone and every entry has its marker;
    - fault injection on every read-side write, failing before or after it: the same bytes;
    - two materializers racing on one stream: the same state, applied once;
    - privacy at apply time: deleted-after-entry, reset-after-upload and erased payload;
    - the sweeper applies a stream no kick reached;
    - the operator steps (drain refusal, dry runs, switch, close refusal, close, legacy again).
  - `ledger-log-emulators.test.ts`: the protocol on MinIO and Azurite (including the create-if-absent races) when
    their endpoints are set. fake-gcs-server ignores generation preconditions, so GCS runs only the sequential
    cases.

## Slice 4: admission order, the admission snapshot, and the slice-3 review fixes

Slice 2's review (lander, P2 1-4) and slice 3's (P2 1-4) found ordering and lifecycle gaps. Every one of them gates log
mode: no stream leaves tests before slice 4 lands. What slice 4 changes:

- **Replay before the gates** (`ledger-log-admit.ts`). Admission settles any unresolved entry, then answers a replay or
  the batch-id 409 from the stream's window and then from the legacy batch doc, before it reads the fence or runs a
  privacy or consent gate, as the legacy core does (`deliver.ts` answers its claim's replay first). A retry of a
  committed batch is never refused or held by a control that landed after it committed.
- **The legacy batch doc is read at admit.** A batch committed by the legacy path before the switch, or one the window
  evicted and the materializer committed, is a replay; the same id with another digest is the 409; a legacy claim still
  pending is a retry.
- **The revision rules at admit.** The claim's own `judge` (moved to module scope in `ledger-s3.ts`, unchanged) runs over
  the shards of the live records, so two records at one revision with another revision id and digest are refused as
  the legacy revision 409 and never logged.
- **The admission snapshot** (`LogBatch.admitted`: the journal seq, the fence's `resets`, each written session's epoch)
  is written on every batch. The materializer re-judges against it before it applies: a reset pending or running is a
  retry; one that completed since the append refuses the batch (`delivery_privacy_superseded`); a session whose epoch
  moved (deleted, maybe re-enabled) has its records dropped. Owner stubs are registered by the materializer, through
  the legacy claim, at apply time; a reset between the append and the apply needs no stub, because nothing of the batch
  is written and the snapshot refuses it.
- **One admission per stream at a time** in a process (a promise chain per stream), so concurrent admits of one stream
  never race for head+1; a LogFenced drops the cached state only if it is still the state that failed.
- **The payload is written only after every refusal point**, and carries no admission time (received_at is the entry's
  own `atMs`). Slice 5 adds a per-attempt nonce, so each attempt writes its own object.
- **Unresolved entries have an age bound** (`LOG_PENDING_MAX_AGE_MS`, 15 min). Past it an entry is never resent, only
  read back: landed applies, absent is dropped. The orphan collection (`ORPHAN_GRACE_MS`, 1 h) is longer, and it also
  skips any payload an entry names, so no entry can land naming a payload that was collected.
- **A final skip at apply writes a refused outcome** into the batch's receipt directory and retires its admitted one,
  so the upload's status never reads admitted for data that was not written. The consumer writes its admitted outcome
  before it kicks the materializer. Log lines name the stream, seq and batch by hash prefix.
- **The sweep is bounded**: `LOG_SWEEP_CONCURRENCY` streams at a time in the background, one sweep at a time, and a
  stream this process already applied is skipped unless its next entry exists (one HEAD). It also runs when upload
  promotion fails. A corrupt registry doc is skipped, a malformed payload is refused (skipped as `payload_invalid`, with
  its refused outcome), never a blocked stream.
- **The switch** requires the deployment-wide proof `<keyRoot>/.ledger/log-commit-on.json` (written by the operator
  once every replica runs with the flag on), waits past the longest legacy claim (`SWITCH_SETTLE_MS`) after entry 0 and
  re-checks: a legacy batch that committed or is pending in that window (the commit guard's HEAD race with entry 0)
  reports `legacy_raced_switch`. The CLI catches failures and prints a code only, and uses a per-run operator writer id.
  (Slice 5 replaces the CLI's bucket-name denylist with an allowlist and adds a guard inside the switch itself.)
- **Precheck parity**: the tenant segment and the delivery budget run first, as the legacy core's precheck. The log
  path takes no tenant slot: its per-stream concurrency is one. The mode is carried in `SelfhostIngestSettings`
  (`ledgerLogCommit`), parsed at config; the env is read only for a direct caller that omits it. An unreadable mode
  HEAD on the admission side is a `ledger_log_mode_unreadable` retry, logged by the consumer's retry line.

## Slice 5: a skipped batch stays refused, fresh payloads per attempt, take only to write

Commit-4's review (lander, P2 1-2, P3 3-7). Both P2s gate log mode: **no stream leaves tests before slice 5 lands.**

- **A replay of a skipped batch answers refused.** The materializer writes a skip record (`<stream>/s/<h(batchId)>.json`,
  the refusal code) BEFORE its refused outcome and before it retires the admitted one. The admission consumer, on the
  log path, reads the skip record before it writes anything (skipped: it finishes refused), and again AFTER it writes
  its admitted outcome (skipped: it writes refused and retires its own admitted). Because the skip record precedes the
  materializer's retirement, the re-read after our write sees every skip whose retirement could have run before our
  write, so every interleaving of a replay with a final skip ends refused. The inbox is retired only after that
  re-read; a failure anywhere in it retries the message. This also closes the cross-replica race that was Remaining 7.
- **A fresh payload per attempt.** The payload carries a per-attempt nonce, so every admission attempt writes its own
  object (a fresh `lastModified`): an identical re-put can no longer leave a stale object that the orphan collection
  takes between a retry's PUT and its append. One attempt's own retried PUTs are still idempotent (the same bytes).
- **Take only to write.** Admission opens the stream read-only (the cached owned state, or a fresh open that is not
  cached) for the replay check, the legacy doc, the fence, the journal and consent at admit. It takes the stream (the
  epoch entry, the registry doc) only after every refusal point, just before the payload, and looks at the window again
  after the take (another writer may have appended the batch since the open). Refused or replayed traffic writes no
  epoch entry and no registry doc.
- **Eviction never drops an in-flight stream**: the bounded state cache skips states whose admission chain is running.
- **The switch's allowlist**: `switchToLogMode` itself refuses unless the bucket carries the operator-written
  non-production marker (`<keyRoot>/.ledger/log-mode-nonprod.json`, `bucket_not_declared_nonprod`). The CLI requires
  `--i-know-this-is-not-prod` for `switch` and `close` with `--apply`, and still refuses production-like bucket names
  (prod, prd, live, hosted, customer). Status and dry runs stay allowed.
- **The sweep never blocks the scan loop**: the registry listing and the idle HEADs run in the background with the
  materializing, `LOG_SWEEP_CONCURRENCY` at a time.
- **Window eviction (documented, not changed).** A batch id evicted from the 4096-entry window and not yet committed by
  the materializer through the legacy path is appended again: a second entry for the same batch. Nothing is lost or
  doubled in the read-side state: the materializer applies both through the legacy core, and the second is a replay
  of the first's legacy batch doc. Only the log carries the duplicate entry.
- **Residual (P3 3, documented).** An entry this process left unresolved is settled (resent if young) before the
  current batch's gates run. That entry passed its own gates when it was built; a consent revocation since then is
  re-checked at apply by the materializer (slice 6, Remaining 1).

## Slice 6: the before-production items

Closes the Remaining items marked before production (1, 2) and items 9, 10 and 8, in the order below, each with tests.
Compaction (Remaining 5) stays out of scope beyond what the checkpoint start makes possible.

- **Consent at apply (Remaining 1).** `LogAdmitGates.consentCap` (the consumer passes the decoded inbox's cap) is
  written into the payload. The materializer validates it with the inbox's own validator (`inboxConsentCap`; a
  malformed cap is `payload_invalid`) and passes `deliverPrepared` a consent gate built exactly as the consumer's
  (`checkUploadConsent` with the cap as `operator` and `LogMaterializerDeps.uploadConsent`, which the self-host runtime
  wires from the same config). It runs in the admit phase only: after the legacy claim, before any canonical write, so
  a refusal leaves nothing written. The commit-phase re-check runs after the canonical writes, where a skip would strand
  them; it is skipped at apply. `UploadConsentRequired` (a 503 on the request path, where the inbox keeps the bytes for
  a redrive) is a final skip here: the batch's inbox is already retired, and holding the stream would block every later
  entry behind a consent that may never come. The person's upload reads refused (`upload_consent_required`). A
  re-upload after they confirm is a fresh admission only under a NEW batch id: the same batch id answers refused (the
  skip record), and the skip is sticky.
  - **A skip is sticky** (review P2-1). Before applying a batch the materializer reads its skip record; one that exists
    (this run or another materializer refused the batch for good) answers skipped whatever the gates say now, and the
    idempotent finishing steps (refused outcome, admitted retired) run again. So a run that wrote the skip and crashed
    before the entry's marker, or a second materializer replaying the entry after the skip record was written, never
    writes the batch after a consent is
    confirmed. The admit-phase gate (after the legacy claim, before any canonical write) reads the skip record once
    more, so a skip written by another materializer after this run's first read is a retry, and the next run finishes
    it as skipped. A batch whose payloads mix a capped and an uncapped one is `payload_invalid`.
  - **Residual window (not closed).** Two materializers applying the same entry whose consent answers DIFFER at the
    same moment (the consent changed between their two gate reads): B passes its admit gate (no skip record yet,
    consent allowed) and goes on to write; A's gate refuses, A writes the skip record and the refused outcome; B then
    commits. The batch is written while its skip record and outcome say refused. It needs two materializers on one
    stream inside one apply's duration (the consumer's kick and another replica's sweep) AND a consent change inside
    that window. Closing it needs a create-if-absent apply-intent record that the skip writer must lose to (or the
    reverse); not done in this slice. Under `INGEST_LEDGER=s3` today `INGEST_UPLOAD_CONSENT` is refused
  (SQL only), so in practice only the operator cap applies.
  - Tests (`ledger-log-materialize.test.ts`): the cap rides in the payload and refuses at apply with no consent store;
    a hold that lands between the append and the apply skips the batch while an earlier allowed batch is written; a
    consent store fault is a retry (no skip record, payload kept); a malformed cap, and capped and uncapped payloads
    in one batch, are `payload_invalid`; a skip survives a crash before the marker and a later consent confirmation;
    a second materializer whose consent allows the batch never writes it, whether the skip landed before its first
    read or between that read and its admit gate.
- **Start from a checkpoint (Remaining 2).** `readEntries(fromSeq)` always reads entry 0 (the mode marker, and the
  `legacyCutover` a checkpoint must match), then tries the checkpoint boundary at or below `fromSeq` and the one before
  it (deterministic keys, no listing of `c/`; a checkpoint PUT is best effort). The first that passes
  `verifiedCheckpoint` (the `openStream` rules, now one function: hash, epoch and epoch writer cross-checked against
  entry `seq`, the inbox cursor and window for a batches entry, the same `legacyCutover`, epoch at least 1, window
  bounded) is the replay's start; none verifying, or a store error reading one, replays from entry 0, as before. A
  materializer run therefore reads O(entries since its cursor), not O(stream length). What a checkpoint start does not
  see: a gap or a bad hash strictly between entry 0 and the checkpoint (`openStream` already had this property).
  - Tests (`ledger-log.test.ts`): no entry below the checkpoint is read; a missing boundary checkpoint falls back to
    the previous; every forgery (hash, epoch writer, epoch, cursor, cutover, epoch 0, seq) and malformed JSON fall back
    to entry 0 with the same entries; a store fault on the checkpoint falls back; a cursor read stays bounded.
- **No epoch entry per retry (Remaining 9).** `takeStream` returns the state unchanged when, after settling its
  pending entry, the epoch is already held by the same writer id (an ambiguous take resolved on the retry, or a
  process whose cached state was dropped). `LogStreams` writes the registry doc BEFORE the take, so a registry failure
  fails the attempt before any epoch entry; registering a stream that is then not taken only costs the sweeper an idle
  check (the stream is already in log mode).
  - Tests: the ambiguous-take test now ends with one epoch entry (`ledger-log.test.ts`); a registry PUT failing before
    or after landing writes no epoch entry and the retry takes once; the same writer id with its cache dropped appends
    without a take, and another writer still takes (`ledger-log-admit.test.ts`).
- **IAM in the stack (Remaining 10).** `stacks/aws/ledger-log-commit-policy.ts` writes the grants down as statements:
  the consumer (admission and the materializer, one process) GET/PUT on every stream object and the receipt outcomes,
  GET/PUT on the registry, DELETE on payloads and the admitted outcome; the operator tool (a role outside the stack)
  GET on the legacy batch docs, the log's control objects (entries, checkpoints, head hint, `closed.json`, the identity
  doc; never the payloads, which hold session records), the registry and the markers, LIST on the bucket
  (unconditioned: S3 answers 404 for a missing key only to a ListBucket holder, and a GET/HEAD carries no `s3:prefix`,
  so a prefix condition would turn every missing read into 403), PUT on entry 0, `closed.json`, the identity doc and the registry, DELETE on
  registry docs (the migration), KMS via S3 for those keys. `uploadAdmissionBucketStatements`
  appends the consumer's statements only with `ledgerLogCommit` on, and `assertUploadAdmissionInputs` refuses `on`
  (the stack mirrors `ledger_log_commit_unsupported`): default off, the rendered policy is byte-for-byte unchanged.
  The bucket's `OnlyTheEraserDeletes` deny would still refuse the consumer's deletes; the carve-out it needs is named
  (`LEDGER_LOG_DELETE_CARVE_OUT`) and left for the change that clears log mode for production.
  - Tests (`upload-inbox-infra.test.ts`): off is byte-for-byte unchanged and `on` is refused; every glob matches the
    keys the code writes (from the real key functions); with the statements rendered, the consumer reads and writes
    every log object, gets nothing else and no other principal does, and the eraser-only deny still refuses its
    deletes; the operator's grants are exactly the drain reads, the four writes and registry deletes.
- **A hash-only registry (Remaining 8).** `registerLogStream` writes the stream's identity doc
  (`<stream root>/stream.json`: org, workspace, user and origin ids) inside the user's admission directory first, then the
  registry doc `{v: 2, root}` at `.ledger/log-streams/<sha256(["log-stream-v2", root])[:40]>.json`. The root names the
  org and workspace as path segments (as every key under them does) and the user and origin only as hashes. The sweep
  resolves a pointer only when its root has the stream-root shape under this key root's admission inbox, its identity
  doc reads back, and the identity, the root and the registry key all name the same stream; a pointer whose identity
  doc is gone (the user's directory was erased) or that lies is skipped. Erasing the user's directory therefore leaves
  no raw user or origin id behind.
  - Migration: v1 docs (raw ids in the body, keyed by a hash of the ids) are still listed, once per stream even
    alongside a v2 doc. `migrateLogStreamRegistry` (CLI: `scripts/ledger-log-stream.ts migrate-registry --bucket <b>`,
    dry run unless `--apply`, which needs `--i-know-this-is-not-prod` and a non-production-like bucket name) registers
    each v1 stream in the v2 layout and then deletes its v1 doc. A v1 doc whose stream has no entry 0 any more (the
    user's directory was erased) is deleted WITHOUT re-registering (`dropped`), so the raw ids are never written back
    into an erased directory; one with ids admission refuses (`assertTenantSegment`, an invalid key) or a log that
    reads corrupt is left in place and counted `invalid`, never aborting the run. It is idempotent and resumable.
    The entry-0 check and the re-register are not atomic: an erasure of that user's directory that runs between them
    leaves the identity doc and pointer just written (the raw ids back in the directory), so run the migration while
    no erasure is in flight, or re-run it after one (the stream then reads dropped and the pointer is skipped). An
    `invalid` v1 doc also stays listed (its stream fails to materialize, and the sweep logs a retry for it every
    sweep): accepted, because only non-production runs wrote v1 docs; an operator deletes it by hand. Only the operator tool
    and non-production test runs ever wrote v1 docs (log mode never ran in production).
  - Tests (`ledger-log-materialize.test.ts`): the registry doc and key carry no user or origin id and the identity doc
    sits in the user's directory; after that directory is erased, nothing under `.ledger/` names the user or origin and
    the stream is no longer listed; a pointer to another root, outside the inbox, or copied under another key names
    nothing, including a copied pointer listed on its own and an identity doc that names another stream than its
    root; the migration's dry run, crash midway (listed once), apply and idempotent re-run, an erased stream's v1 doc
    dropped without writing into its directory, and an invalid v1 doc reported while the rest migrate. The CLI guard covers
    `migrate-registry` (`ledger-log-admit.test.ts`).

## Production pilot: one stream, switched deliberately

The owner pilots log-commit on ONE production stream (his own org) in parallel with the customer migration waves. The
operator runbook is [docs/ledger-log-pilot-runbook.md](../ledger-log-pilot-runbook.md). Nothing here lifts the hosted
runtime's or the stack's refusal of `INGEST_LEDGER_LOG_COMMIT=on`: until that separate change lands and the operator
writes the deployment-wide proof, every production switch refuses at its preflight (`log_commit_not_proven_on`).

- **The default refusal stays.** Without a production context, `switchToLogMode` still requires the bucket's
  non-production marker, and the CLI still requires `--i-know-this-is-not-prod` and refuses production-like bucket
  names. The production path is a separate, explicit opt-in.
- **Opt-in** (`ledger-log-stream-production.ts`). A production switch or close needs ALL of: the dormant switch
  `LEDGER_LOG_PRODUCTION_ENABLED` (false until a reviewed one-line flip; `production_dormant`); the CLI's `--production`
  flag (never with `--i-know-this-is-not-prod`; only on switch and close); the code sha actually running (Actions:
  `GITHUB_SHA`; locally a clean worktree whose HEAD is on `origin/main`); a plan (the same command without `--apply`);
  and the typed confirmation
  `LEDGER-LOG-<ACTION> BUCKET <sha256(bucket, keyRoot)> STREAM <sha256(keyRoot, org, workspace, user, origin)> SHA <sha> STATE <sha256(action, mode, entry-0 cutover)> EXPIRES <iso> PLAN <local | plan run>`.
  The plan derives the phrase from its own preflight, with an expiry 30 minutes out. The apply runs the preflight
  again, rebuilds the phrase with the expiry the phrase carries and refuses unless it is byte-equal, unexpired and at
  most one TTL ahead. The check is inside the operation (`productionGate` in `ledger-log-stream-ops.ts`), not only in
  the CLI. STATE binds the mode and the log instance, deliberately not the head or the legacy counts, which move every
  second on a live stream; every count-based check runs again at apply.
- **Preflight** (`streamPreflight`, read only, counts and verdicts only). Switch: the stream reads (no corrupt log),
  is legacy, has at least one committed legacy batch (`stream_not_found`: a mistyped id or an unused stream), its org
  has a valid fence-authority marker (org or deployment-wide, by the claim's own rules; a STOP refuses), the user's
  fence has no reset begun-but-unfinished or pending, the deployment-wide proof exists, and no legacy batch is
  pending. Close: the stream is in log mode with an epoch, and every entry is applied (cursor = head). Close needs no
  fence authority, so a rollback stays possible while admission is stopped. The report also says whether the stream is
  registered.
- **No in-flight conflicting writer.** Before the switch there is no log writer (only the operator writes entry 0).
  Legacy writers are the conflict: the switch re-lists the stream's legacy batch docs right before entry 0 (reading
  only new or changed docs) and refuses if one is pending; after the settle wait it lists again, and any doc that is
  new, changed or pending and is NOT a batch of the log is a legacy writer behind entry 0 (`legacy_raced_switch`).
  Before this change the check compared the committed count, which the materializer moves itself (it writes the
  legacy batch docs of logged batches), so a busy stream always reported a race; both the production and the
  non-production switch use the precise check now. After entry 0, the first admission writer takes the stream by the
  existing epoch protocol (an `epoch` entry; a stale writer is fenced at its next append), which needs no operator
  step. For close, a writer that had the stream cached may append the entry it had in flight; a production close
  waits the settle time and reports the stream again (`after`), so the receipt shows it.
- **Rollback** is the existing close, with the same guards (plan, phrase, preflight). Half-done states: a switch that
  died after entry 0, or reported `legacy_raced_switch`, leaves the stream in log mode (admission registers it on its
  first take); drain it (`status` until the cursor equals the head) and close it. A close that died after its marker
  is complete (`status` reads `closed`; a second close refuses with `stream_is_closed`). Close is one way: a pilot
  stream is never reopened.
- **Receipts** carry digests (bucket binding, stream identity, state, sha256 of the approving phrase), counts and
  verdicts. Never a bucket name, key or id; `measure` names an origin only by a 12-hex prefix of its key hash.
- **Measure** (`measureArrivalRate`, read only, allowed in production). Batch arrival rate per origin over a window:
  legacy batch docs by their listing lastModified (the commit rewrites the doc), log entries by their own `atMs`,
  counting each logged batch once (its materializer-written batch doc is left out). Reports the window average, the
  busiest minute, and whether it reaches 1.4 batches/s (where a stream's latency becomes visible in-region). It lists
  the user's batch docs and log objects (a page budget; `truncated` when it runs out) and GETs only entries inside the
  window.
- **The workflow** (`.github/workflows/ledger-log-pilot.yml`, dormant by the same constant). The org admission repair
  pattern: exact live main head, first attempt, the pinned dispatcher, the run's own name, green push CI, the protected
  `ledger-log-pilot` environment (main only; not created yet, so the gate refuses), and for switch and close a
  successful same-sha `plan-<action>` run under 30 minutes old whose artifact holds exactly the typed phrase. The
  stream is an environment secret (never a dispatch input), masked before anything prints; a read role serves status,
  measure and plans, a write role switch and close. The operator role's grants now include the preflight's control
  reads (`LEDGER_LOG_OPERATOR_PREFLIGHT_READS`: the fence-authority markers and the user's fence); nothing renders them.
- Tests: `src/session-ingest/__tests__/ledger-log-stream-production.test.ts` (every refusal and the happy paths of
  switch, close and measure, the CLI guard, the gate and the workflow's shape), the operator-grant cases in
  `stacks/aws/__tests__/upload-inbox-infra.test.ts`.

## Remaining

1. Closed in slice 6: the payload carries the inbox `consentCap`, and the materializer re-runs the consent gate at apply
   time (admit phase, after the legacy claim and before any canonical write) with that cap and its own
   `INGEST_UPLOAD_CONSENT` config. A refusal is a final skip (`upload_consent_required`, payload deleted), as the
   journal re-check is; a consent store fault is a retry.
2. Closed in slice 6: `readEntries(fromSeq)` reads entry 0 and then starts from the newest verified checkpoint at or
   below `fromSeq` (the same trust rules as `openStream`), falling back to entry 0 on any mismatch.
3. A privacy reset does not delete unapplied payloads by itself: the materializer skips them at apply time (the journal
   re-check and the snapshot) and then deletes them, so they live until the stream's next apply. Operator and session
   erase are refused under `INGEST_LEDGER=s3` today; when they are admitted, they must call `eraseLogPayloads`.
4. The GCS protocol run on a backend that honours generation preconditions (real GCS or a newer emulator). The local
   fake-gcs-server 1.52.2 also answered the adapter's create with `400 invalid uploadType`, so the GCS case did not run
   here.
5. Compaction and retention: entries, materializer markers (`m/`) and the stream registry grow forever. Metrics. Slice 6
   makes compaction possible (readers start from a verified checkpoint, Remaining 2) but does not implement it: it must
   keep entry 0, the checkpoint readers start from and everything after it, and stop while a checkpoint fails to
   verify. Registry pointers whose user directory was erased stay behind (they carry no raw id, and the sweep skips
   them at one GET each) until retention removes them.
6. Cost: with the flag on, each admission HEADs entry 0 and the close marker (in parallel), reads the legacy batch doc,
   the fence and the shards of its live records; each apply reads the fence once more. Since slice 6 each sweep reads
   two objects per registered stream (the registry pointer and the identity doc), and each apply reads the batch's skip
   record twice (before and at the admit gate) and, with a consent gate, the consent store once.
7. Closed in slice 5 (the skip record, read before and after the consumer's admitted write). Kept as a number so the
   references stay stable.
8. Closed in slice 6: a registry doc is a hash-keyed pointer to the stream root, and the ids live in the stream's
   identity doc inside the user's directory. v1 docs are still read; `scripts/ledger-log-stream.ts migrate-registry`
   rewrites them and deletes the originals.
9. Closed in slice 6: admission registers the stream before it takes it, and `takeStream` is idempotent for the
   writer that already holds the epoch, so a retry adds no epoch entry.
10. Documented in the stack in slice 6 (`stacks/aws/ledger-log-commit-policy.ts`), behind
    `transcriptStore.uploadAdmission.ledgerLogCommit`, which refuses `on` as the hosted runtime refuses the flag, so
    nothing renders and nothing is applied. Before production, the change that lifts both refusals must also carve the
    payload, admitted-outcome and registry deletes out of `OnlyTheEraserDeletes` (`LEDGER_LOG_DELETE_CARVE_OUT`) and
    grant the operator role (`ledgerLogOperatorStatements`, `ledgerLogOperatorKeyStatement`), which lives outside the
    stack today.
