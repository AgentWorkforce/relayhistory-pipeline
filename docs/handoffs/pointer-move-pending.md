# `pointer_move_pending`: why it never clears, and a default-off resolution

Branch `review/pipeline-handoff-sfmini-pmp-resolve`, on top of `6d88b638` (the
code deployed when this was written). Code, tests and docs only. Nothing here
deploys anything, and nothing enables the flag.

Line numbers marked `@6d88b638` are in the deployed code. Unmarked ones are on
this branch.

## 1. Findings

### Where the code comes from

There is exactly one producer:
`src/session-pipeline/pointer.ts:215 @6d88b638`:

```ts
if (stored && stored.manifest_key !== landed.key) {
  const otherLive = !stored.deleted && (await deps.reader.read(stored.manifest_key)) !== null;
  if (otherLive)
    throw new PipelineError("pointer_move_pending", "the session's other key is still live; retry after its erasure", true);
```

It is `retryable: true`. `processBatch` logs `pointer index failed` with
`error_code: pointer_move_pending` and fails every SQS message behind that key
(`consumer.ts:204-207 @6d88b638`).

### What it inspects

`indexLocked` runs under the session's pointer lock, a transaction-scoped
advisory lock on `scopeLockKey(scope)` (`lambda.ts:307-312 @6d88b638`,
`pointer.ts:67-69,124`). In order:

1. It re-reads the landed key's manifest (`pointer.ts:154`). The manifest reader
   checks the manifest's `session.*` fields and its `started_at` UTC date against
   the key (`packages/object-store/src/manifest.ts:523-541`, "d= is the UTC date of
   started_at").
2. A delete whose key is gone first probes ±45 days of `d=` dates (90 HEADs, nearest
   first) for the same session, and follows the nearest live sibling
   (`pointer.ts:127-161`).
3. It runs the normal upsert of the landed manifest (`pointer.ts:194-196`). That is
   `sessions.upsert_session_index`, fenced on revision only:
   `WHERE excluded.revision > s.revision`
   (`supabase/migrations/20261025000300_upsert_keep_on_null.sql:111`). The row's
   primary key is `(org, workspace, harness, session, machine)`
   (`20261025000000_sessions_index.sql:90`). The date, and so the key, is not part of
   it.
4. It reads the row's `manifest_key` and `deleted_at` (`pointer.ts:204-211`).
5. If the row names another key, it reads that key's manifest. If that manifest is
   live, the stage throws `pointer_move_pending` (`:212-215`). Otherwise it deletes
   the row (fenced on the old key) and re-upserts it from the landed key, and the
   outcome is `moved` (`:216-225`).

No ETag, epoch or chunk is compared on this path. The only per-key inputs are
"does the row name another key" and "is that key's manifest readable".

### What "move" means

The Neon exporter keys a session by the UTC date of its first record:
`startDate = new Date(unit.firstMs)` (`src/migration/neon-s3-sync/export-session.ts:135`),
with `started_at` set to the same instant (`:332`). When the first record moves,
for example when an earlier record arrives, the session's key moves to another
`d=` prefix. At the new key the exporter writes revision `(prior at that key ?? 0) + 1`,
so usually 1 (`:489-494`). The old key is queued for erasure as kind `prefix` with
reason `stale_prefix` (`src/migration/neon-s3-sync/sync.ts:281-289`). The exporter
never deletes it itself.

A move completes in the pointer stage when the pointer row names the new key. That
needs one of the following:

- the new key's revision is greater than the row's revision, in which case the upsert
  silently re-keys the row (step 3);
- the old key's manifest is no longer readable, in which case the stage re-keys
  (step 5), or the old key's delete event follows the sibling (step 2).

### Why it never completes on retry

The stage itself changes nothing in S3. A retry re-reads the same two live
manifests and reaches the same branch. With the new key at revision 1 and the old
key at revision N ≥ 1, the revision fence never lets the new key in. So the event
fails on every receive: 8 receives, then the DLQ. A redrive replays the same state.

The only exit is outside the stage: the old prefix must be erased. That erasure is
the operator-run, approval-gated, identity-allowlisted pass described next. Its
guards skip many entries, and a skipped entry stays queued without an error. The
exit therefore does not come within the retry window, and for some sessions it never
comes.

The golden fixture captured at `6d88b638`
(`src/session-pipeline/__tests__/fixtures/pointer-move-pending-off.golden.json`)
shows three more ways sessions get stuck:

- **Multi-hop.** A third live copy under another date fails too. So does its own
  delete event: the delete follows the nearest live sibling, and that sibling's row
  check is pending again (golden steps "multi-hop, third key open" and "third key's
  delete").
- **Reverse direction.** The row already names the newer key, and an event arrives
  for an older copy that is still live, for example a re-put or a sweep re-enqueue. If
  the older copy's revision is not greater, that event is pending forever (golden
  "stale key's put while the row names the newer key").
- **Revision arithmetic across keys** (a hazard, not a pending). A stale copy whose
  revision is greater than the row's silently re-keys the row back to the stale copy
  (golden "stale key with a higher revision"). Revisions restart per key, so this
  comparison means nothing across keys. The flag below does not change it, because the
  upsert runs before the move check. See Known limits.

The stage also has side effects on every pending attempt. Before the throw, the
upsert call has already updated `session_index_ext` (`last_trace_id`,
`last_revision`) and re-projected the session directory
(`src/hoopdb/index.ts:183-232 @6d88b638`). Each retry repeats them.

### What the narrow stale-prefix erasure changed

Main's `migration:neon-s3-erasure --stale-prefixes-only --only-identity <sha256>`
(origin/main `runbooks/neon-s3-sync.md:153-383`) erases only the listed sessions'
queued `stale_prefix` entries. It runs inside the session-prefix lease, and only
when both of these hold:

- (a) the old prefix holds none of the org's current exported or pending keys;
- (b) a current exported manifest of the same session exists that differs only in
  the date, came from a successful export, and is live in the bucket
  (origin/main `runbooks/neon-s3-sync.md:220-236`; `eraser.ts:320`).

It deletes every version of the old prefix, manifest first. After that,
`reader.read(old)` is null: the retried event takes the re-key branch
(`pointer.ts:216-225`), and the old key's delete event follows the sibling
(`:155-161`). That is the only thing the erasure did for the pointer. It made the
old manifest unreadable. The decision of which copy wins came from the exporter's
own state (guard b), not from the pointer stage.

## 2. Design: `SESSION_POINTER_RESOLVE_MOVE_PENDING` (default off)

| value | effect |
| --- | --- |
| unset, `""`, `false` | off (the default): `lambdaEnv`, the pointer deps and every call are as at `6d88b638` |
| `true` (exactly) | on: init adds `resolveMovePending: { head }` to the pointer deps (`pointerFlagDeps(env, store)`) |
| anything else | init fails, naming the variable (never the value) |

It is a pointer-stage flag and does not need `SEARCH_INDEX_ENABLED`. No stack,
runbook or workflow renders it.

When the flag is on, the stage acts only where it would throw today
(`pointer.ts:233-256`). `resolveMovePending` (`src/session-pipeline/move-resolve.ts`)
decides, still under the session's pointer lock, whether one live copy is provably
authoritative. The result is one of three outcomes:

| `move_resolution` | when | effect |
| --- | --- | --- |
| `resolved_complete` | the landed key is authoritative | the existing re-key (delete the derived row fenced on the old key, upsert the landed manifest), outcome `moved` |
| `resolved_skip` | the row's key is authoritative and the event is for a stale copy | nothing written; outcome `unchanged`; the message succeeds |
| `still_pending:<reason>` | no proof | the same `PipelineError("pointer_move_pending", …, retryable)` as today, carrying the reason |

The outcome is the `move_resolution` field on the existing `pointer indexed` line,
or on the existing `pointer index failed` line. The field is allowlisted with an
exact-value rule (`src/shared/log.ts`, `MOVE_RESOLUTION_VALUES`). A metric filter on
`$.move_resolution` gives one count per outcome. `error_code` stays
`pointer_move_pending`, so existing alarms, DLQ tooling and main's fast-retry
allow-list keep matching. The dedicated reason lives in `move_resolution`. With
the flag off the field never appears.

### The proof, and why each part is needed

The proof runs on the pair (landed L, stored S). The newer copy A is then checked
against every other live date sibling X.

| # | reason on failure | check | why |
| --- | --- | --- | --- |
| 1 | `not_date_move` | the keys differ only in `d=` (same prefix, ws, m, h, s) | only a date move is in scope; a machine or workspace move is not |
| 2 | `identity_mismatch` | the `96bulls` extension's `tenant_id` and `canonical_*` ids agree where both copies have them | the reader already ties each manifest's session fields and start date to its key |
| 3 | `writer_mismatch` | same `raw.owner`, `writer.delivery_instance_id` and first-epoch opener | one writer moved its own session; two writers' copies are not arbitrated here |
| 4 | `open_session` | both copies `raw.state: closed` | an open copy can still be appended to (a concurrent writer) |
| 5 | `epoch_mismatch` | identical epoch history (epoch, reason, opener, resumes) and `raw.epoch` | a truncate, rewrite, backfill or adoption between copies is a different lineage |
| 6 | `ambiguous_order` | `updated_at` strictly orders them; `raw.updated_at` does not contradict it | A is the newer copy; a tie has no winner |
| 7 | `not_covered` | A holds every older chunk (sha256 and length), or both carry the exporter's `origin_revisions` and A's is ≥ for every origin of the older copy | A has everything the older copy had, by content or by the source watermark |
| 8 | `multi_hop` | every other live sibling within ±45 days of either key passes 1-7 with A as the newer copy; at most 3 of them | a chain resolves only toward its single newest copy |
| 9 | `too_many_chunks`, `dest_chunk_missing`, `dest_chunk_mismatch` | every chunk A lists HEADs at A's prefix with the stored length and the `raw-sha256` metadata equal to the manifest's `raw_sha256`; at most 512 chunks | the destination copy is complete where it lives (no body is read) |
| 10 | `no_etag`, `concurrent_write` | a re-read of every copy returns the same ETag and version, and the row still names S with the same revision and is not deleted | nothing was published or re-indexed while the proof ran |
| - | `check_failed` | any error while proving | fail as today |

A note on check 7. For a real exporter date move, the old copy's chunks are almost
never byte-present at the new prefix. An earlier record shifts every offset, so every
chunk digest changes. A content check ("the stale prefix's objects are already at
the destination") would therefore almost never pass. The proof uses the content
check where it holds. Otherwise it uses the exporter's own per-origin watermark
(`exportWatermarks`), which is monotonic in the source.

### Safety argument

- **No customer data is deleted or written.** The resolution makes HEADs and
  manifest reads only. Its one write is the existing re-key of the derived
  `session_index` row, the same statements the stage runs today once the old key is
  gone (`pointer.ts:255-263`). The stale copy stays in S3 for the erasure job.
  Whenever proving it stale would need a delete or exporter state, the outcome is
  `still_pending`, as today.
- **The final state is the one the erasure would produce.** If the proof holds, A is
  the copy guard (b) would keep and the stale copy is the one it would erase. After
  that erasure, today's code also ends with the row on A. The resolution reaches that
  state before the erasure. A later erasure of the stale copy is then a no-op for the
  row: the tombstone is fenced on the key (`src/hoopdb/index.ts:62-84`), and the
  delete follows A.
- **Concurrency.** Pointer writers for the scope are serialized by the advisory lock
  the stage already holds. Writers outside the lock (an exporter or client publishing
  to S3) are detected by check 4 (open copies refused) and check 10 (an ETag or
  version change, or a row change). A publish that lands after check 10 is a new S3
  event, which the stage handles under the lock as usual.
- **Idempotent.** After `resolved_complete` the row names L, so a retry takes the
  normal path (`unchanged`). `resolved_skip` writes nothing and gives the same answer
  on retry. `still_pending` changes nothing beyond what today's pending attempt
  already did.

### Cost when on

The cost applies only on the path that throws today: one extra row read before the
proof and one after, about 90 HEADs for the sibling probe (both windows,
deduplicated), one HEAD per chunk of A (at most 512, 16 at a time), and a re-read of
each manifest. With the flag on, a put that does not hit a move makes the same calls
as with it off (test "a repeated put issues the same … calls").

## 3. Known limits

These are judged unsafe to automate, and stay pending or out of scope:

- **Deleting the stale copy.** That needs the exporter's state (current and pending
  keys, leases, obligations) and the privacy authority, and the owner's approval per
  run. It stays with the stale-prefix erasure.
- **Copies from two different writers** (client versus exporter, or two grants):
  `writer_mismatch`. There is no rule for which one wins.
- **Open copies, epoch changes, ties and uncovered copies:** these stay pending.
- **Siblings more than 45 days from both keys** are not seen. The stage's own probe
  has the same reach.
- **An A-to-B-to-A move where the exporter does not rewrite A** (unchanged content
  keeps the old `updated_at` and watermarks). At B's time the proof correctly picks B.
  If the session later moves back to A without A being rewritten, the row stays on B
  until B is erased. Today, the same happens whenever B's revision is greater than
  A's.
- **The cross-key revision hazard** (a stale copy with a greater revision re-keys the
  row back) happens in the upsert before the move check, with the flag on or off.
  Fixing it changes the flag-off path, so it is left for a separate change.
- **Only the code is covered.** The 22 stuck sessions were not inspected (no AWS
  access here), so it is not known how many would prove. Expect `still_pending`
  wherever the copies have different writers or are open, or wherever the older copy
  has no export marker and its chunks are not contained in the newer copy. The
  per-reason counts on `move_resolution` will show the split once the flag is on.

## 4. Tests

- `src/session-pipeline/__tests__/pointer-move-pending-off-golden.test.ts`, with
  `fixtures/pointer-move-pending-off.golden.json`, captured at `6d88b638` in the
  branch's first commit, before any resolution code. The flag is unset. The test pins
  every object-store call, every SQL statement and its parameters, the lock
  placement, the results, the log lines and the row, for 12 move steps. It passes
  unchanged on the feature commit.
- `src/session-pipeline/__tests__/pointer-move-resolve.test.ts` runs the real handler
  (`processBatch`) on PGlite with every migration and an fs store:
  - **Resolved:** `resolved_complete` by watermark and by content; `resolved_skip`
    (and its retry); a multi-hop chain; a stale hop's delete; idempotent retries
    followed by the old copy's erasure.
  - **Refused:** `dest_chunk_missing`; `dest_chunk_mismatch` (wrong length, missing
    digest metadata); `concurrent_write` (the old copy re-published mid-proof);
    `epoch_mismatch`; `open_session`; `writer_mismatch`; `not_covered` (watermark
    behind, origin missing); `ambiguous_order`; `multi_hop` (open third copy, newer
    third copy, too many copies); `check_failed`.
  - **Every case asserts:** the outcome on the real log line, the row unchanged on a
    refusal, the object listing byte-for-byte unchanged, and the same answer on retry.
  - **Also covered:** the flag grammar, the deps wiring, and the parity of the log
    value set with `MOVE_PENDING_REASONS`.
- `pointer-move-harness.ts` is the shared rig.
- One existing test was changed. `pointer-skip-origin.test.ts`'s source check now
  expects `...pointerFlagDeps(env, store),`.
