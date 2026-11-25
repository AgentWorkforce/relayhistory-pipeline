# Incremental search indexing: committed wake and revision-aware passes (design M1, rev 4)

Status: design for review. Code and plans only. No migration, deploy, flag, schedule or config change follows from
this document without a separate production go per step (§9). Rev 2 closed the nine findings of the M1 review. Rev 3 closes the seven rev-2 findings (B1-B7), marked inline.
(Both reviews: pipe-uploads-codex, 2026-11-25.)

- Acceptance fixture: `src/session-ingest/__tests__/publication-portable.pglite.test.ts`, test "generation PUT before
  scoped finalization", run with `PUBLICATION_REQUIRE_AUTOMATIC_SEARCH=1`.
- Sources: branch `review/publication-postcommit` @ 14edcfed; integrated reproducer c05c905d; publisher contract
  65712ab8.
- Red today: after the committed wake is acked, `early-final` is still in `partials()`. The positive control passes.

Line references are to origin/main fefbc87c unless stated.

## 0. Objectives [F1]

There are two clocks, both measured from the publish commit (the `ingest_publication_publish` transaction commit
time):

| Objective | p50 | p90 | p99 | Hard objective |
|---|---|---|---|---|
| commit → readable (pointer and directory row visible to an authorized peer) | ≤ 5 s | ≤ 15 s | ≤ 60 s | 100% within 15 min, or an alert |
| commit → searchable (search state complete for the committed epoch) | ≤ 30 s | ≤ 60 s | ≤ 5 min | 100% within 15 min, or an alert; held scopes (§4.5) are reported, never silently missing |

- **Scopes above the whole-read bound** (streamed; tonight about 20 per 25 min) have a separate objective:
  searchable ≤ 15 min p90. They are reported separately until M3 makes their updates incremental.
- **Not in the SLO:** sessions whose acceptance is unresolved (held by contract) and sessions under an erasure fence.
  Both are counted and alerted, never treated as searchable.
- **Measurement [B1]:** time zero is `ingest_publication_attempts.published_at` for the attempt that made the
  committed head.
  - That value is set by `clock_timestamp()` in the publish transaction's final UPDATE (migration
    20261105059000:192). It is a lower bound on the publish commit, within that transaction's remaining
    statements: milliseconds.
  - The authority transaction does NOT supply the clock (its `now()` is a different, later transaction's start).
  - At enqueue (§2), the authority transaction reads `published_at` for the head's current attempt and copies it into
    the job payload as `published_at`.
  - The pass computes `commit_to_searchable_ms = clock_timestamp() at its search-state write − published_at`.
  - So the object-landed queue delay, the Lambda batching and every retry are inside the measurement.
  - Test R5 asserts that a job delayed in the queue reports a duration that includes the delay.

**Budget for commit → searchable at p50** (the managed path, hosted):

| Step | Budget |
|---|---|
| publish commit → authority transaction (object-landed queue, S3 event, SQS delay 30 s, ESM window up to 20 s; measured 7-10 s only with the reverted 5 s / 2 s settings) | today about 35-60 s; this is a SEPARATE commit from the publish |
| wake enqueue, inside the authority transaction (§2) | 0 s after the authority commit |
| claim: the in-invocation follow-on (§5.1) | under 1 s; else the next invocation or the schedule |
| claim, worst case without traffic: the 1-min schedule | ≤ 60 s. This is the p99 driver, not p50 |
| pass: committed-epoch check plus whole read (median scope) | about 0.5-2 s (today: pointer 0.7 s, reconcile 0.4 s median) |
| total p50 | about 2-5 s after the authority transaction commits |

The authority transaction is a separate commit, made when the Lambda processes the publication's event: 30 s of
queue delay plus up to 20 s of batching window today. A trial of 5 s / 2 s on 2026-11-25 raised latency at saturated
concurrency and was reverted. So commit → searchable p50 is about 40-60 s until per-slot capacity rises (incremental
apply, M3) and the queue settings can then be lowered. The 30 s p50 objective therefore depends on M3, and on
re-running that batching trial afterwards. The 1-min
schedule is only the floor for lost-wake recovery, never the primary path; that is the correction to rev 1's
schedule claim.

## 1. What exists today

- **The publish is one transaction.** `sessions.ingest_publication_publish` (migration 20261105059000:147-182) flips
  the generation live, compare-and-sets `ingest_publication_heads.pointer_revision`, calls `upsert_session_index`,
  and inserts `pipeline_jobs('object-landed')`. An aborted attempt inserts nothing.
- **The managed authority path is one transaction.** `withPublicationAuthority`
  (`src/shared/publication-authority.ts:26-34`) runs `session_publication_lock` on `deps.publication.transact`'s
  transaction. `src/hoopdb/index.ts:154-157` then runs `upsertRawPointer` with `queue: postgresPipelineQueue(tx)`,
  on that same transaction:
  - rekey;
  - `upsert_session_index` / `ingest_operator_runtime_upsert`;
  - `session_index_ext`;
  - the identity repair;
  - `projectSessionDirectory`;
  - the fan-out INSERTs (365-368);
  - `armWakeup`;
  - `fanout_revision`.
- **The generic, non-managed path is NOT one transaction** (autocommit per statement; `index.ts:348-349` "a crash in
  between repeats the fan-out"), except for moves (`moveTransaction`, 68-83). Tombstones (105-140) return before
  the authority block. [F4]
- **No stage runs a search pass.** `pointer.ts:103` answers `managed_publication_hint` for `/upload-generations/`
  keys, so S3 events and `reconcileStaleProjections` re-sends never reach the search stage for managed sessions.
- **Hosted facts (read-only, 2026-11-25):**
  - `sessions.pipeline_jobs` is empty.
  - The Lambda connects as the runtime role, which holds SELECT/INSERT/UPDATE/DELETE on `pipeline_jobs` (granted by
    the migrator, `src/hoopdb/migrate-guards.ts:282`; verified with `has_table_privilege`).
  - The Lambda's fan-out is `droppedFanout` unless `PIPELINE_FANOUT=postgres` (`lambda.ts:481`).
  - There are about 600 live tenants (org × workspace).

## 2. The wake: a `search-index` job written inside the authority transaction [F4]

- **Managed sessions.** `upsertRawPointer`'s authority branch enqueues `search-index` through
  `postgresPipelineQueue(tx)`, inside the `withPublicationAuthority` callback (`index.ts:154-157`).
  - The enqueue goes BEFORE the `fanout_revision` gate (337-347), because that gate returns early on a same-revision
    redelivery. A redelivery re-asserts the wake, and §3's coalescing makes that free.
  - A retired or non-current publication (`current:false` becomes `publication_retired`) rolls the transaction back,
    so there is no job. Test R1c.
- **Non-managed sessions** keep today's S3-event search path unchanged. The catch-up (§6) covers them too. This
  design does not claim that the generic path is atomic.
- **Tombstones** do not enqueue from the pointer path. Erasure reaches search only through §4.4's fence predicate,
  driven by the catch-up or the erase job's own wake (out of scope; erase jobs already purge search).

**Job message [B2]:** a full `StageMessage` (`packages/pipeline/src/lib/pipeline/dlq.ts:58`), so the trace, DLQ and
replay tooling works unchanged:

```json
{ "v": 1, "stage": "search-index",
  "ctx": { "traceId": "<uuid, inherited from the session-indexed message>", "orgId": "...", "workspaceId": "...",
           "harness": "...", "sessionId": "<canonical session id>" },
  "workKey": "search␟<org>␟<ws>␟<harness>␟<canonical session id>",
  "revision": <pointer revision of this write, informational>,
  "payload": { "epoch": <session_directory.event_projection_epoch after this transaction's writes>,
               "clock": { "source": "publish", "at": "<ingest_publication_attempts.published_at, ISO>" } } }
```

- `epoch` is THE authority token for search (§4.4): one counter, the directory epoch, bumped by trigger on every
  `session_index` change, including tombstones.
- **Clock source [rev 4]:** `payload.clock = {source, at}`. It is never null and never silently missing.
  - `publish`: `ingest_publication_attempts.published_at` of the head's current attempt. Used by the managed enqueue,
    and by managed catch-up through the same lookup.
  - `pointer`: max(`session_index.indexed_at`) of the scope's live rows. Used by non-managed catch-up, which has no
    publication attempt. That is the pointer write time, a later origin than the upload commit.
  - `commit_to_searchable_ms` is emitted only for `source = publish`, and is labelled by source. `pointer` jobs report
    `pointer_to_searchable_ms`, a separate series excluded from the commit SLO and shown beside it.
  - Tests R5c (a managed catch-up job carries the publish clock) and R5d (a non-managed catch-up job carries the
    pointer clock and emits no commit metric).
- `revision` is carried for traces only and is never compared.

## 3. One row per scope: coalescing, claim, settlement [F3, B2, B3]

Rev 2's "open-row" partial index collided with retry settlement: a retried leased row sets `lease_id = NULL` and
re-enters the index next to the follow-up row. Rev 3 keeps EXACTLY ONE `search-index` row per scope, leased or not,
and records the wanted epoch in it.

```sql
CREATE UNIQUE INDEX pipeline_jobs_search_index_scope
  ON sessions.pipeline_jobs (work_key) WHERE stage = 'search-index';
```

### 3.1 Enqueue

Executable. All NOT NULL columns are supplied, and the shape is the same as `postgresPipelineQueue.insert`
(`queue.ts:134-150`). `$1` is the StageMessage jsonb above.

```sql
INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, machine_id, message, visible_at)
SELECT m->>'stage', m->>'workKey', (m->'ctx'->>'traceId')::uuid, m->'ctx'->>'orgId', m->'ctx'->>'workspaceId',
       m->'ctx'->>'sessionId', NULL, m, now()
  FROM (SELECT $1::jsonb AS m) x
ON CONFLICT (work_key) WHERE stage = 'search-index'
DO UPDATE SET
  message = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
                 THEN EXCLUDED.message ELSE pipeline_jobs.message END,
  -- [rev 4] A NEW epoch is new work and gets its own budget. The active lease is kept; settlement compares epochs.
  attempts = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
                  THEN 0 ELSE pipeline_jobs.attempts END,
  dlq = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
             THEN false ELSE pipeline_jobs.dlq END,
  -- An unleased row becomes visible now. A leased row keeps its lease; settlement (3.3) reopens it if the wanted
  -- epoch moved on during the pass.
  visible_at = CASE WHEN pipeline_jobs.lease_id IS NULL THEN least(pipeline_jobs.visible_at, now()) ELSE pipeline_jobs.visible_at END;
```

### 3.2 Claim

The generic claim (`queue.ts:229-241`) with `stage = 'search-index'`, unchanged. The claimer records the epoch it
read from `message.payload.epoch` at claim time: `claimed_epoch`.

### 3.3 Settlement

A stage-specific settlement, in the Lambda's search-job loop; not the generic one. The generic DELETE on `done` would
lose a follow-up wake that arrived during the pass. Each statement keys on `(id, lease_id)`.

- **done:**
  ```sql
  DELETE FROM sessions.pipeline_jobs WHERE id = $id AND lease_id = $lease
    AND (message->'payload'->>'epoch')::bigint <= $claimed_epoch;
  -- 0 rows (the wanted epoch moved on during the pass): reopen instead of deleting.
  UPDATE sessions.pipeline_jobs SET lease_id = NULL, visible_at = now(), attempts = 0
   WHERE id = $id AND lease_id = $lease;
  ```
- **retry** [rev 4]: claimed-epoch aware, lease-bound:
  ```sql
  UPDATE sessions.pipeline_jobs
     SET lease_id = NULL,
         visible_at = CASE WHEN (message->'payload'->>'epoch')::bigint > $claimed_epoch THEN now() ELSE now() + $backoff END,
         attempts   = CASE WHEN (message->'payload'->>'epoch')::bigint > $claimed_epoch THEN 0 ELSE attempts END
   WHERE id = $id AND lease_id = $lease;
  ```
  - If the wanted epoch moved during the attempt, the row reopens now with a fresh budget.
  - The single row cannot collide with anything.
  - Only this stage-specific settler is authoritative for `search-index`. The generic `postgresStageConsumer`
    settlement must never settle this stage (enforced by not registering the stage with the generic runWorker;
    test R6g).
- **exhausted** (held acceptance after 12 attempts, §4.5): the same claimed-epoch guard as `done`, in ONE
  transaction bound to the lease, so exhaustion can never discard a newer wake:
  ```sql
  -- a) Park the CLAIMED message only (the message as it was at claim time, $claimed_msg; epoch = claimed_epoch).
  INSERT INTO sessions.pipeline_dead_letters (stage, …, message, error_code)
    SELECT 'search-index', …, $claimed_msg, 'retries_exhausted'
      FROM sessions.pipeline_jobs WHERE id = $id AND lease_id = $lease;     -- 0 rows if the lease was lost: abort
  -- b) No newer wake: delete the row.
  DELETE FROM sessions.pipeline_jobs WHERE id = $id AND lease_id = $lease
    AND (message->'payload'->>'epoch')::bigint <= $claimed_epoch;
  -- c) A newer wake arrived during the lease: keep the row, with a fresh budget, visible now.
  UPDATE sessions.pipeline_jobs SET lease_id = NULL, visible_at = now(), attempts = 0
   WHERE id = $id AND lease_id = $lease;
  ```
  - If (a) matched 0 rows (the lease was lost), the transaction aborts and nothing is parked or deleted.
  - A later wake inserts a fresh row, so a newly accepted revision is not blocked by an old quarantine.
  - Test R4b: N exhausts after N+1 merged into the leased row. N lands in dead letters; the row survives with
    epoch N+1 and attempts 0.

### 3.3b Dead-letter replay [rev 4]

Replay of a `search-index` dead letter is stage-aware. It never uses the generic replay send, because that would hit
the unique scope row. One transaction:

1. mark the letter `replaying` with a new replay id (the existing `postgres-dead-letters.ts` states);
2. run the §3.1 upsert with the letter's message, plus `payload.replay_id`;
3. then one of:
   - **superseded:** the row already carries an epoch at or above the letter's (the upsert kept the row's message).
     Mark the letter `resolved` (reason `superseded`) in the SAME transaction. Newer work covers it, so it can never
     stay stuck in `replaying`;
   - **merged:** the row now carries the letter's epoch and replay id. The §3.3 `done` settlement resolves the letter
     by `last_replay_id = payload.replay_id`, in the same transaction as its DELETE. `exhausted` re-quarantines it
     (state back to `quarantined`).

Tests: R8a (superseded resolves at once), R8b (merged resolves on done).

### 3.4 Races

| Case | Outcome |
|---|---|
| wake N, then N+1, both before claim | one row, epoch N+1 |
| N leased, wake N+1 during the pass | the same row's message becomes N+1; `done` with claimed N fails the DELETE guard and reopens; N+1 runs next |
| N leased, the pass retries, wake N+1 arrives | the retry reopens the single row, now carrying N+1; no collision |
| N leased, then the worker crashes | the lease expires; the same row is reclaimed with the max epoch |
| unleased N at attempt 11, wake N+1 | the row carries N+1 with attempts 0 (new budget) |
| N leased at attempt 11, wake N+1, the attempt fails | the retry reopens now with attempts 0, carrying N+1 |
| N leased at attempt 12, wake N+1, N exhausts | N parked (claimed snapshot); the row stays with N+1, attempts 0 |
| late older wake (epoch below the row's) | the CASE keeps the higher epoch |

Tests R1a (coalescing), R1b (crash reclaim), R1d (done with a follow-up reopens), R1e (retry with a follow-up: one
row, no unique violation).

## 4. The pass [F6]

The `search-index` handler, in one `deps.transact` transaction for the decision, then `indexSearchScope` for the work.

### 4.1 Lock order (unchanged from `search-index.ts:636-643`)

1. scope lock `96bulls:search␟org␟ws␟harness␟session` (try; `scope_busy` leads to a retry with a short delay,
   coalesced);
2. `livePointerSnapshot` (keys, epoch, canonical);
3. canonical lock `96bulls:directory-events␟…␟canonical` (try);
4. then the fence read (§4.4), `FOR SHARE` on the fence row if one exists.

### 4.2 Committed-state guard

- If the scope has NO `session_index` row at all (live or tombstoned): ack `not_committed` and write nothing.
- This closes a hazard in today's code: `indexSearchScope` erases when live keys and siblings are empty, which
  includes "never committed". A wake can only exist after a commit (§2), but a catch-up or redelivery must not erase
  on absence. Test R3a.

### 4.3 Current check

If `alreadyCurrent` holds (`event_projection_sources` equals the live rows and `ready`): ack `unchanged`. This is one
SQL read; a repeated hint costs nothing else.

### 4.4 Erase only on authority [B5]

All reads happen in the pass's transaction, under the scope lock and then the canonical lock (§4.1). So nothing
can commit between the reads and the decision for this scope. One token is used: the directory epoch. No revision is
compared.

The pass erases search rows only when ALL hold:

- (a) no live `session_index` row for the scope, and no live canonical sibling;
- (b) either a `sessions.session_erasure_fences` row exists for `(org, ws, source, session)`, or at least one
  `session_index` row exists for the scope and every such row is tombstoned (`deleted_at IS NOT NULL`);
- (c) the directory epoch read under the lock is at or above the job's `payload.epoch`. The job never sees a future
  it has not been woken for; a lower epoch than the job's is impossible, because epochs are monotonic.

What happens in the other cases:

- **Recreate race:** a recreate inserts a live row and bumps the epoch, so (a) fails under the lock and the pass
  indexes the live generation.
  - A FENCED scope cannot be recreated at all: `session_erasure_fences` refuses DELETE, and a trigger refuses any live
    row for a fenced id. So "fence, then a newer live generation" is not a reachable state.
  - The recreate case that IS reachable is "all rows tombstoned, then a live row returns". Test R3c covers it.
- **Old-version delete or retired generation:** it leaves a live row (or a live sibling), so (a) fails.
- **Never-committed scope:** no row at all, so (b) fails and the pass acks `not_committed` (§4.2).

Tests R3a-R3d.

### 4.5 Acceptance held

- On `relayhistory_acceptance_missing`, the contended records are not published. The state stays incomplete and
  `ready` stays false.
- The job is retried with the registry backoff (30 s · 2^n, capped at 900 s), max attempts 12 (about 2.5 h).
- At exhaustion, the queue's existing DLQ path quarantines it into `pipeline_dead_letters` with `retries_exhausted`
  (`dlq.ts:352-371`).
- Exhaustion NEVER marks the scope complete or ready. A metric and an alert count held scopes. Test R4.

### 4.6 Work

`indexSearchScope` as today: whole read, streamed or staged. M2 is correctness first; M3 makes it incremental.

## 5. Hosted placement [F7]

### 5.1 Placement: the session-pipeline Lambda claims `search-index` jobs, three ways

1. **Follow-on:** after processing an SQS batch, the invocation claims up to K open `search-index` jobs (K = 5) and
   runs them within its remaining time, minus the stream margin. This includes the jobs its own authority
   transactions just committed. This is the p50 path.
2. **Schedule:** a new EventBridge rule every 1 min invokes the Lambda with `{searchJobs:true}`. It claims up to K
   jobs. This is the no-SQS-traffic recovery path and the p99 floor.
3. **Catch-up** (§6): the existing reconcile cadence enqueues jobs. It does not run passes itself.

### 5.2 Surfaces (infra and config, each needing a go)

- **DB role:** none new. The runtime role already holds the needed privileges on `pipeline_jobs` (verified).
- **Migration:** §7.
- **Stack** (`stacks/aws/session-pipeline.ts`):
  - the new rule `session-search-jobs-schedule` (rate 1 min), its Lambda permission, and a stage setting
    `searchJobs: { enabled, claim }`, default off, rendering `SEARCH_JOBS_ENABLED` and `SEARCH_JOBS_CLAIM`;
  - no new IAM on the function role. The schedule invokes through a resource policy like the existing rules.
- **Connection budget:** the claim is one short transaction and the pass uses the same locks as today. Per
  invocation, peak connections stay ≤ 2 (pass plus lock), inside HOOPDB_POOL_MAX 3. The schedule adds at most one
  concurrent invocation per minute, inside ESM concurrency 20 plus 1.
- **Recovery without SQS:** the schedule, path 2.

## 6. Catch-up that survives the loss of every wake [F5, B4]

Replaces the S3-reconcile-body freshness path for search. `reconcileStaleProjections` stays for non-search freshness
until retired.

- **One global keyset cursor over stale scopes**, not per tenant: `sessions.search_catchup_cursor`, a single row
  `(org_id, workspace_id, harness, session_id, lap, updated_at)`, where `session_id` is the directory's own key: in
  `session_directory` it IS the canonical session id (there is no `canonical_session_id` column). Each tick, in one transaction:
  ```sql
  SELECT d.org_id, d.workspace_id, d.harness, d.session_id AS canonical_session_id, d.event_projection_epoch
    FROM sessions.session_directory d
   WHERE d.event_projection_ready IS NOT TRUE
     AND EXISTS (SELECT 1 FROM sessions.session_index i WHERE <live row for the canonical scope>)
     AND (d.org_id, d.workspace_id, d.harness, d.session_id) > ($c_org, $c_ws, $c_harness, $c_canon)
   ORDER BY d.org_id, d.workspace_id, d.harness, d.session_id
   LIMIT $S;          -- S = 500
  ```
  - Each row gets the §3.1 enqueue. The cursor moves to the last row returned. Fewer than S rows means the cursor
    resets to the minimum and `lap` increments.
- **Fairness:**
  - The cursor always moves past every scope it returned, whether that scope is held, ready, or failed to enqueue.
    So a tenant whose first S scopes stay held cannot starve its later scopes, or other tenants.
  - Every stale scope is visited once per lap.
  - A held scope's re-enqueue is a no-op upsert while its row exists.
- **Lost-wake bound:** one lap = ceil(stale scopes / S) ticks. With 1-min ticks and fewer than 5,000 stale scopes,
  that is at most 10 min, plus the claim, plus the pass. That is inside the 15-min hard objective. More stale scopes
  than that raise an alert (lap duration over 10 min).
- **Existing defect, reported:** `reconcileStaleProjections` (`consumer-service.ts:716-725`) restarts from the first
  tenant each tick, capped at 4096. Filed separately; not relied on.
- **Tests:**
  - R2a: a lost wake converges in one lap;
  - R2b: S = 2 with 5 stale scopes in one tenant, the first 2 permanently held; all 5 enqueued within 3 ticks;
  - R2c: the cursor wraps and `lap` increments.

## 7. Closed-stage surface for `search-index` [F2]

Every closed list must gain the stage in ONE migration plus one code change. Otherwise recording, routing or DLQ
parking fails.

**SQL** (one migration, replacing the three CHECKs from 20261031000100:62-76):

- `pipeline_jobs_stage_check`;
- `pipeline_events_stage_check`;
- `pipeline_dead_letters_stage_check`;
- the partial unique index of §3;
- `sessions.search_catchup_cursor` (§6);
- `supabase/tests/pipeline_queue_test.sql:8`.

**TypeScript** (`packages/pipeline/src/lib/pipeline/` unless a path is given):

- `trace.ts:23-35` `PIPELINE_STAGES`;
- `registry.ts:22-31` `QUEUED_STAGES` and `registry.ts:47-114` `STAGE_REGISTRY` (binding, `next: []`, `maxAttempts: 12`,
  `implemented`);
- `packages/pipeline/src/env.ts:99` (the optional binding);
- `index.ts:114-123` and `:172-187`;
- `dlq.ts:40-56` (it loops over `PIPELINE_STAGES`; covered automatically, but tested);
- `queue.ts:54-61` (`checked()`);
- the worker stage lists `src/hoopdb/indexer.ts:38,51-64` (NOT added: the Lambda is the hosted consumer, so the
  indexer worker does not claim `search-index`; documented so it is not "missing").

**Observability:**

- `traces.ts:174-179,197`;
- `src/proof/aws/probes.ts:57` and `src/proof/aws/checks.ts:577-578` (add the stage to the pipeline probes);
- `src/smoke/e2e.ts:97`.

A test asserts that every closed list contains the same stage set (R6).

**Ordering hazard:** `dlq.ts:374-389` quarantines a job whose stage has no handler as `stage_not_configured`.
Therefore the consumer code (the handler, flag off) deploys BEFORE any writer enqueues (§9 step order).

## 8. Rollback ordering [F8, B6]

Two flags:

- `SEARCH_WAKE_ENQUEUE`: the writers, i.e. the authority-path enqueue and the catch-up;
- `SEARCH_JOBS_ENABLED`: the consumer, i.e. the follow-on claim and the schedule.

**Drain path** (the default; the consumer is healthy):

1. writers off (`SEARCH_WAKE_ENQUEUE=0`, catch-up off). The consumer stays ON;
2. drain: wait until `pipeline_jobs` has 0 `search-index` rows (held rows exhaust into dead letters within their
   backoff, about 2.5 h worst case, or use the park path for the remainder);
3. consumer off (`SEARCH_JOBS_ENABLED=0`);
4. roll the code back;
5. down migration (below).

**Park path** (the consumer is unhealthy):

1. writers off;
2. consumer off;
3. park, in ONE fenced transaction:
   ```sql
   LOCK TABLE sessions.pipeline_jobs IN SHARE ROW EXCLUSIVE MODE;
   INSERT INTO sessions.pipeline_dead_letters (stage, reason, message, …)
     SELECT 'search-index', 'rollback_park', message, … FROM sessions.pipeline_jobs WHERE stage = 'search-index';
   DELETE FROM sessions.pipeline_jobs WHERE stage = 'search-index';
   INSERT INTO sessions.search_rollback_audit (at, operator, parked) VALUES (now(), $operator, <count>);
   ```
   The parked messages stay replayable from dead letters once the consumer is back;
4. roll the code back;
5. down migration.

**Down migration:**

- It takes the same lock, refuses unless `search-index` has 0 rows in `pipeline_jobs`, then drops the index and the
  cursor table and restores the three CHECKs.
- It also refuses while the stage has rows in `pipeline_events` or `pipeline_dead_letters`, unless those are first
  archived. The CHECKs cannot be restored with rows that violate them.

## 9. Production steps (each needs its own go, one at a time)

1. Migration (§7): additive CHECK values, the partial index, the cursor table. Rollback is §8.5.
2. Deploy the consumer code to the Lambda (backport or main) with every flag off. Byte-identical behaviour.
3. Stack: the schedule rule disabled, plus the stage settings (off).
4. Writer enqueue on, consumer on, schedule on, for ONE internal org: watch both clocks, job age, holds, lock
   waits, errors and the pool. Rollback is §8.
5. All orgs, then the catch-up on.

Stop lines for 4 and 5 are the A+C set, plus: `search-index` oldest open job over 5 min; held scopes rising for
15 min.

## 10. Red tests (executable, red until M2) [all]

| ID | Asserts | Why red today |
|---|---|---|
| R0 | the acceptance fixture (exists) | no automatic search trigger |
| R1a | two wakes for epochs N and N+1 leave one open job, with epoch N+1 | stage and upsert absent |
| R1b | N leased, then the worker crashes: the same row is reclaimed carrying the max epoch | absent |
| R1d | N leased, wake N+1 during the pass, `done` with claimed N: the row reopens (not deleted), carrying N+1 | absent |
| R1e | N leased, wake N+1, the pass retries: exactly one row, no unique violation | absent |
| R1c | a retired or non-current publication rolls back, so there is no job | absent |
| R2a | a lost wake (the job deleted) converges through the catch-up within one lap | cursor absent |
| R2b | S = 2 with 5 stale scopes in one tenant, the first 2 permanently held: all 5 enqueued within 3 ticks | absent |
| R2c | the cursor wraps and `lap` increments | absent |
| R3a | a pass on a never-committed scope acks `not_committed` and erases nothing | today it erases on empty keys |
| R3b | an old-version delete with a newer live revision: indexes, no erase | absent |
| R3c | a recreate race (fence, then a newer live generation): live wins | absent |
| R3d | a stale queued epoch after a newer one is indexed: `unchanged` | absent |
| R4 | acceptance missing past max attempts: quarantined, scope not ready, never in a search result | absent |
| R4b | N exhausts after a wake N+1 merged into the leased row: only N is parked; the row survives with N+1 and a fresh budget | absent |
| R5 | the publish clock: a job delayed in the queue reports `commit_to_searchable_ms` that includes the delay, measured from `published_at` | absent |
| R7 | M3 milestone (not in the M1b file): a chunk with the same seq and a changed content sha256 forces a full pass. It lands as a red fixture at the start of M3, with the compaction and CAS cases | deferred to M3 |
| R1f | wake N+1 during N's exhausting attempt: no N+1 dead letter, the row survives with N+1 | absent |
| R6g | the generic runWorker / postgresStageConsumer is not registered for `search-index` | absent |
| R8a/b | stage-aware replay: superseded resolves at once; merged resolves on done | absent |
| R5c/d | catch-up clock: publish for managed, pointer for non-managed; no null clock | absent |
| R6 | every closed stage list has the same set, including `search-index` | absent |

These tests land on a separate head after this doc, with exact commands and outputs.

## 11. M3: true incremental apply [F9, B7]

Immutability is VERIFIED, not assumed. Each manifest chunk already carries `seq`, `epoch`, `start_offset` and the
content `sha256` (`packages/object-store/src/manifest.ts:53-62`).

- **Watermark:** `sessions.search_batch_watermark` (scope, canonical, manifest_key, last_seq, chain_digest), where
  `chain_digest_n = sha256(chain_digest_{n-1} ‖ seq_n ‖ epoch_n ‖ start_offset_n ‖ sha256_n)`.
  Content is bound, not just ids.
- **Append-only proof:** recompute the chain over the new manifest's chunks with `seq <= last_seq` and compare it with
  the stored digest. If they are equal, those chunks are byte-identical in content and order, and only chunks with
  `seq > last_seq` are read and upserted. If they differ, the chunks were mutated, compacted, rewritten or
  re-epoched: do a full pass and reset the watermark.
- **CAS:** `UPDATE … WHERE last_seq = $old_seq AND chain_digest = $old_digest`. A lost CAS retries the pass.
- **Erasure or a recreate with a new canonical id:** a full pass, then reset.
- **Tests:**
  - incremental equals full on every fixture;
  - R7: same seq, changed `sha256`, forces a full pass;
  - compaction forces a full pass;
  - a lost CAS retries.

## 12. Milestones

| M | Content | Exit | ETA (PT) |
|---|---|---|---|
| M1 rev 2 | this doc | reviewer closes F1-F9 | Tue about 1:30 AM |
| M1b | R1-R6 red tests on a separate head | red for the intended absence | Tue about 5 AM |
| M2 | the migration, consumer, writer, cursor and flags, all off | R0-R6 green; flags off byte-identical | Tue about 3 PM |
| M3 | watermark plus incremental apply | equality tests and a bench | Wed end of day |
| M4 | fairness tuning, busy coalescing | claim-share tests | Thu |

Tonight's operational changes (queue delay, window, pool, connect timeout) are NOT part of this branch's diff or
evidence.
