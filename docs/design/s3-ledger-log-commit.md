# S3 ledger: numbered-log-entry commit

Status: proposed, first slice implemented behind `INGEST_LEDGER_LOG_COMMIT=off` (default). Nothing in the
admission path calls it yet, and `on` is refused at startup until that wiring lands.

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
the stream never reads as legacy. A store fault on any listing or on the conflict read-back is `LogUnavailable`
(retryable), never a raw store error.

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

## This slice

- `src/session-ingest/ledger-log.ts`: the keys, entry and checkpoint schema, validation, `enableLogMode`,
  `takeStream`, `appendBatches` (dedupe, create-if-absent, read-back, pending resend), `openStream` (from a
  verified checkpoint or entry 0), `readEntries`, `putPayload`, and the `INGEST_LEDGER_LOG_COMMIT` parse.
- `selfhost-config.ts`: `INGEST_LEDGER_LOG_COMMIT=on` is refused at startup; `off` changes nothing. The hosted
  upload-admission runtime (`upload-admission-hosted.ts`) refuses `on` too (`ledger_log_commit_unsupported`); the
  hosted Worker never runs the S3 ledger at all (it refuses `INGEST_LEDGER=s3`).
- A checkpoint is trusted only as far as its hash: one with another `legacyCutover`, an epoch below 1 or a window
  larger than `LOG_REMEMBERED_BATCHES` is ignored, and the open replays from entry 0.
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

## Remaining

1. Wire the admission consumer behind the flag. That means claim, gates and outcome per member, the legacy
   writers refusing log-mode streams, and per-stream single-writer ownership with actor or lease and takeover.
2. The materializer (manifests, revision index, search feed), with its cursor and checkpoints.
3. The same suite against the MinIO, GCS and Azure emulators. The conformance cases cover the primitive; the
   protocol suite should run there too.
4. A property and fault-injection suite comparing log admission against legacy admission on the same inputs:
   the same records visible, no loss, no duplicates.
5. Compaction and retention, the close marker for rollback, and metrics.
