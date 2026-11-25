# Incremental search indexing: committed wake and revision-aware passes (design, M1)

Status: design for review. Code and plans only. No migration, deploy, flag or config change follows from this
document without a separate production go per step.

Goal: a committed session is searchable within about 30 s (p50), for every org, without relying on any single
notification arriving, and without ever indexing an uncommitted, aborted or unaccepted generation.

Acceptance fixture: `src/session-ingest/__tests__/publication-portable.pglite.test.ts`, test
"generation PUT before scoped finalization", with `PUBLICATION_REQUIRE_AUTOMATIC_SEARCH=1` (branch
`review/publication-postcommit` @ 14edcfed; integrated reproducer c05c905d; publisher contract 65712ab8). Today it is
red: after the committed wake is acknowledged, `early-final` is still partial.

## 1. What exists today (origin/main fefbc87c)

- **The publish already writes a durable committed wake.** `sessions.ingest_publication_publish` flips the generation
  live, compare-and-sets `ingest_publication_heads.pointer_revision`, calls `upsert_session_index`, and inserts
  `sessions.pipeline_jobs(stage='object-landed', work_key=manifest_key@version)`, all in ONE transaction
  (migration 20261105059000, loop 147-182). An aborted attempt inserts nothing. This is the wake: it is created
  atomically with successful finalization.
- **Nothing turns that wake into a search pass.** `src/hoopdb/indexer.ts` consumes `object-landed` and then
  `session-indexed` (pointer and directory write, summary fanout). No stage runs `indexSearchScope`.
- **The S3 path deliberately ignores managed publications.** `pointer.ts:103` answers
  `managed_publication_hint` for `/upload-generations/` keys (and :121-125 for managed `srv_` machines) before any
  read. So neither an S3 event nor a freshness re-send (`reconcileStaleProjections` → `processBatch`) ever reaches the
  search stage for a managed session. This is the red test.
- **Every search pass is a full re-read.** `indexSearchScope` (whole read, streamed or staged) reads every sibling
  manifest and diffs against the index. Nothing records which batches a complete pass covered.
- **Revision signals that exist:**
  - `session_index.revision` per machine row;
  - `ingest_publication_heads.pointer_revision`;
  - `session_directory.event_projection_epoch`, bumped by trigger on any `session_index` change;
  - `session_directory.event_projection_sources` (manifest_key → revision) plus `event_projection_ready`, written only
    by a complete pass. Together these are the persisted fingerprint of the last complete pass.
  - `alreadyCurrent` (search-index.ts:1305) already compares the live rows against those sources in SQL, without S3.

## 2. Design

### 2.1 Wake: a search job, created with the pointer write

Add a `pipeline_jobs` stage `search-index`. The `session-indexed` handler enqueues it in the SAME transaction as its
pointer and directory write, with `work_key = <org>/<ws>/<harness>/<canonical session>@<event_projection_epoch>`.

- Created only after the publish committed, because `session-indexed` exists only downstream of a committed
  `object-landed`. An aborted generation never creates one (fixture case a).
- Survives restarts and lost notifications. It is a row, claimed with a lease and retried with attempts; a crashed
  lease is reclaimed (fixture case b: a different authorized worker finds it after restart).
- Duplicates are harmless. Two wakes for the same epoch collapse in the claim (newest epoch wins per scope), and the
  pass itself is idempotent (2.3).
- No raw `upload-generations` PUT is ever indexed. `pointer.ts:103` stays as is.

Needs a migration: the `stage` CHECK constraint gains `search-index`, plus an optional partial unique index on
`(stage, work_key)` for open jobs. That is a production migration, so it needs its own go.

### 2.2 Catch-up that survives the loss of every wake

A periodic sweep (the existing reconcile tick) lists scopes where the committed state is ahead of the search state:

- Postgres search state: `partials()` already compares `projection_state` against the directory epoch, readiness and
  canonical id.
- ClickHouse state (hosted today) has no epoch. Add `source_epoch` to `search_ingest_session_state`, or compare
  `session_directory.event_projection_ready = false` with a live pointer as the stale predicate. The second needs no
  schema change, and the directory is authoritative.

The sweep enqueues the same `search-index` job, not a direct pass, so there is one execution path and one lease.

### 2.3 The pass: revision-aware, held when not accepted

The `search-index` handler:

1. resolves the scope's committed state under the existing locks (`livePointerSnapshot`: live keys, epoch, canonical
   id);
2. if the job's epoch is older than the live epoch, re-keys to live. A newer committed head always wins over the
   wake's;
3. if `alreadyCurrent` (sources equal the live rows and ready), acks as `unchanged`. A repeated hint is cheap;
4. otherwise runs `indexSearchScope`;
5. on `relayhistory_acceptance_missing`, HOLDS: no rows are published for the contended records, the state stays
   incomplete, and the job is retried with backoff. After N attempts it is parked (`dlq`) with the reason, visible
   in metrics. Exhaustion never marks the scope complete or ready, so it never grants visibility;
6. on erased (no live keys): erases only when the authoritative fence says erased (`invalidateDirectoryEvents` plus
   the erasure fence, including recreate races), never on an old-version delete or a pre-finalize missing pointer.

M2 ships steps 1-6 with a full-scope pass in step 4 (correctness first).

### 2.4 Incremental apply (M3)

Persist a per-scope watermark next to the directory sources: `search_indexed_batches` (scope, canonical id,
manifest_key, last indexed batch ordinal or chunk id, manifest fingerprint). In step 4:

- if the live manifest set is the watermark's set plus appended batches only (same fingerprint prefix), read only
  the new batches and upsert them. No index scan, no delete;
- on any rewrite, compaction, erasure or fingerprint mismatch, do the full pass and reset the watermark.

Expected effect: the typical live-session update (one new batch) costs one small S3 read plus one upsert, instead of
reading every manifest. Streamed scopes stop holding the scope for about 4 minutes per update, which also removes
most `scope_busy` collisions.

### 2.5 Fairness (M4)

The claim takes at most a fixed share (draft 50%) of one batch from a single org while other orgs have open jobs.
The publisher already round-robins per org (lander's fleet tests).

## 3. Hosted placement (open question; needs an answer before M2's deploy step)

Hosted indexing today is the SQS Lambda. Whether a hosted worker claims `pipeline_jobs` at all is unverified
(`stacks/aws/hardening.ts` lists an `indexer` principal for `src/hoopdb/indexer.ts`). Options:

- **(a)** The Lambda claims `search-index` jobs at the start of each invocation, bounded by the stream slot, and from
  a 1-minute schedule. No new service.
- **(b)** The existing indexer worker runs the search stage. It needs the ClickHouse/search deps that it does not
  have today.

Recommendation: (a). The search deps, locks and streaming budget already live in the Lambda.

## 4. Milestones

| M | Content | Exit | ETA (PT) |
|---|---|---|---|
| M1 | this design | reviewed by the lander and pipe-uploads-codex | Tue 1:30 AM |
| M2 | `search-index` stage, enqueue in session-indexed, handler (2.3) with a full pass, catch-up predicate, flag default off | the acceptance fixture is green with the flag; flag off is byte-identical | Tue about 9 AM |
| M3 | watermark plus incremental apply, with a bench on replayed scope sizes | equality: incremental equals a full pass on every fixture | Wed end of day |
| M4 | fairness cap, busy coalescing | no org above its share in the claim tests | Thu |

## 5. Production steps (each needs its own go, one at a time)

1. Apply the `search-index` stage migration (additive constraint change; rollback is to drop the stage after
   draining).
2. Deploy the flag-off build to the Lambda (backport or main); byte-identical behaviour.
3. Flag on for one internal org. Watch search age p50 and p90, `search-index` job age, parked holds, DB lock waits,
   Lambda errors. Rollback: flag off.
4. Flag on for all orgs, with the same stop lines.

## 6. Risks

- **Job volume.** One job per pointer write is about 1:1 with today's indexing events. The 73% unchanged re-sends ack
  at step 3 for one SQL read each.
- **Connection pool.** The pass takes the same locks as today. The claim adds one short transaction. It must stay
  inside the pool budget (docs/handoffs/indexing-batching-plan.md).
- **Two paths during rollout.** S3-event search and job search both run for non-managed sessions. They are idempotent
  against each other through `alreadyCurrent`.
