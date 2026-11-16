# S3 ledger: numbered-log-entry commit

Status: proposed. Slices 1 (writer and reader), 2 (admission wiring) and 3 (materializer, operator tool) are
implemented behind `INGEST_LEDGER_LOG_COMMIT` (default off). With `on`, only a stream whose entry 0 exists is
admitted through its log; entry 0 is written only by the operator tool (`scripts/ledger-log-stream.ts switch`). No
stream should be switched in production before the items in Remaining marked "before production" are done.

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
loop. Linearity always held (the head + 1 create); this makes ownership explicit too.

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
other entries present reads as `log_open_missing`. Today `readEntries` replays from entry 0 and does not start from
a checkpoint, so until it can (planned), compaction must not delete any entry a reader may still replay through.
Once it can, entries below a materialized checkpoint can be deleted after a retention window, because the
checkpoint holds the dedupe window, the owner and the cursor. Payloads live under `ledger/log/o=<origin>/p/` in the
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
    tool), so a stream another process admitted is applied too.
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

## Remaining

1. Before production: the materializer does not re-check upload consent at apply time (the inbox `consentCap` is not
   in the payload). Carry it in the payload and re-run the consent gate in the replay.
2. Before production: `readEntries` replays from entry 0 on every materializer run, and the sweeper lists the whole
   registry every scan. Both need a start-from-checkpoint read and a dirty-stream set before streams are many or long.
3. A privacy reset does not delete unapplied payloads by itself: the materializer skips them at apply time (journal
   re-check) and then deletes them, so they live until the stream's next apply. Operator and session erase are refused
   under `INGEST_LEDGER=s3` today; when they are admitted, they must call `eraseLogPayloads`.
4. The GCS protocol run on a backend that honours generation preconditions (real GCS or a newer emulator). The local
   fake-gcs-server 1.52.2 also answered the adapter's create with `400 invalid uploadType`, so the GCS case did not run
   here.
5. The hosted Worker must refuse `on` before any hosted wiring. Compaction and retention, and metrics.
6. Cost: with the flag on, each admission HEADs entry 0 and the close marker (in parallel) instead of using the cached
   state, so a close is seen at once.
