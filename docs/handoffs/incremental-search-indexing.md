# Incremental search indexing: committed wake and revision-aware passes (design M1, rev 2)

Status: design for review. Code and plans only. No migration, deploy, flag, schedule or config change follows from
this document without a separate production go per step (§9). Rev 2 closes the nine findings of the M1 review
(pipe-uploads-codex, 2026-11-25). The closures are marked [F1]..[F9].

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
- **Measurement:** the `search-index` job carries `committed_at` (the publish transaction time); the pass logs
  `commit_to_searchable_ms`. The budget test (§10, R5) asserts the accounting fields.

**Budget for commit → searchable at p50** (the managed path, hosted):

| Step | Budget |
|---|---|
| wake enqueue, inside the authority transaction (§2) | 0 s, same commit |
| claim: the in-invocation follow-on (§5.1) | under 1 s; else the next invocation or the schedule |
| claim, worst case without traffic: the 1-min schedule | ≤ 60 s. This is the p99 driver, not p50 |
| pass: committed-epoch check plus whole read (median scope) | about 0.5-2 s (today: pointer 0.7 s, reconcile 0.4 s median) |
| total p50 | about 2-5 s after the authority transaction commits |

The authority transaction runs when the Lambda processes the publication's event. After tonight's queue change
(delay 5 s, window 2 s) that is about 7-10 s after the PUT, so commit → searchable p50 is about 10-15 s. The 1-min
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

**Job payload**, `message` jsonb:

```
{ scope: {org, ws, harness, session}, canonical, epoch, committed_at }
```

- `epoch` = `session_directory.event_projection_epoch` after the transaction's writes. It is read in the same
  transaction, after `projectSessionDirectory`.
- `committed_at` = `now()` of the transaction.

## 3. Coalescing, claim and lease races [F3]

- **Scope identity:** `work_key = 'search␟' || org || '␟' || ws || '␟' || harness || '␟' || canonical_session_id`.
  - It uses the canonical id, not the alias, so aliases of one canonical session coalesce.
  - The epoch is in the message, NOT in the work_key, so successive epochs share one key.
- **Uniqueness:** a partial unique index:
  ```sql
  CREATE UNIQUE INDEX pipeline_jobs_search_index_open
    ON sessions.pipeline_jobs (work_key) WHERE stage = 'search-index' AND lease_id IS NULL AND NOT dlq;
  ```
- **Enqueue (coalescing upsert):**
  ```sql
  INSERT INTO sessions.pipeline_jobs (stage, work_key, org_id, workspace_id, session_id, message, visible_at)
  VALUES ('search-index', $wk, $org, $ws, $session, $msg, now())
  ON CONFLICT (work_key) WHERE stage = 'search-index' AND lease_id IS NULL AND NOT dlq
  DO UPDATE SET message = CASE WHEN (EXCLUDED.message->>'epoch')::bigint > (pipeline_jobs.message->>'epoch')::bigint
                                THEN EXCLUDED.message ELSE pipeline_jobs.message END,
                visible_at = least(pipeline_jobs.visible_at, EXCLUDED.visible_at);
  ```
  At most one OPEN (unleased) job exists per scope, and it carries the newest epoch. A leased job has left the
  index (`lease_id` is set), so a wake during a pass creates exactly one new open job: the follow-up.
- **Claim:** today's `queue.ts:229-241` SQL, unchanged (`FOR UPDATE SKIP LOCKED`, ordered by `visible_at, id`, with a
  lease).
- **Races and their outcomes:**

| Case | Outcome |
|---|---|
| epoch N leased, N+1 committed | N+1 inserts a new open row; N finishes; N+1 runs, finds the live epoch and is cheap if current (§4.3) |
| N leased, then the worker crashes | the lease expires (300 s); N is reclaimed; N+1 may already be open; both run; each resolves the LIVE epoch, so the result is identical and the second acks `unchanged` |
| N and N+1 both open | impossible: the upsert keeps one row with max(epoch) |
| stale N runs after N+1 is indexed | the pass compares against the live epoch, not the job's; it acks `unchanged` |

- **Tests:** R1a (coalescing), R1b (leased plus crash).

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

### 4.4 Erase only on authority

The pass erases search rows only when ALL of the following hold, checked under the locks above:

- (a) no live `session_index` row for the scope, and no live canonical sibling;
- (b) either a `sessions.session_erasure_fences` row exists for `(org, ws, source, session)`, or every
  `session_index` row of the scope is tombstoned (`deleted_at IS NOT NULL`) with a tombstone revision at or above
  the job's epoch;
- (c) no live row with a revision above the fence's or tombstone's revision. Recreate wins: if a newer live
  generation exists, index it instead.

An old-version delete, a retired generation, or a missing pointer before finalization satisfies none of (b). The
result is `not_committed` or a normal index. Tests R3a-R3d (old-version delete, retired generation, recreate race,
stale queued epoch).

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

## 6. Catch-up that survives the loss of every wake [F5]

Replaces the S3-reconcile-body freshness path for search. `reconcileStaleProjections` stays for non-search freshness
until retired.

- **Cursor:** a new table `sessions.search_catchup_cursor` with a single row `(last_org, last_ws, updated_at)`.
  - Each tick reads up to T tenants (T = 200) with `(org_id, workspace_id) > (last_org, last_ws)` in key order,
    wrapping at the end.
  - It writes the new cursor in the same transaction.
  - There is no 4096 cap and no restart from the first tenant, so every tenant is reached within
    ceil(tenants / T) ticks. With about 600 tenants and T = 200 that is 3 ticks.
- **Stale predicate, per tenant** (paged by session, at most S = 500 per tenant per tick; overflow resumes next lap):
  ```sql
  d.event_projection_ready IS NOT TRUE AND EXISTS (live session_index row for the canonical scope)
  ```
  This works for both ClickHouse and Postgres state, because the directory is authoritative.
- **Action:** the coalescing upsert of §3, which enqueues `search-index` directly. No S3 reconcile body, so
  `managed_publication_hint` is irrelevant.
- **Fairness:** at most S stale scopes per tenant per tick; the rest carry to the next lap.
- **Lost-wake bound:** one lap (3 min at a 1-min tick with about 600 tenants), plus the claim, plus the pass. That is
  inside the 15-min hard objective.
- **Existing defect, reported:** `reconcileStaleProjections` (`consumer-service.ts:716-725`) restarts from the first
  tenant each tick and processes only the first 4096. Tenants past that are never reached. That is harmless at about
  600 tenants, but it contradicts its own comment (702-703). The cursor above avoids it; the defect is filed
  separately.
- **Tests:** R2a (lost wake converges), R2b (tenants beyond one page, T = 2 with 5 tenants), R2c (the cursor wraps
  and is fair).

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

## 8. Rollback ordering [F8]

1. Flag off (`SEARCH_JOBS_ENABLED=0`). The Lambda stops claiming and the follow-on stops.
2. Stop the enqueues: the writer flag `SEARCH_WAKE_ENQUEUE=0` in the authority path (a separate flag, so the
   consumer can drain first), and the catch-up flag off.
3. Drain or park: let the open `search-index` jobs drain with the consumer on, OR park them
   (`UPDATE … SET dlq = true` for the stage, recorded) when rolling back the consumer too.
4. Roll the code back. The consumer and writer code stay tolerant of the stage existing.
5. Only when the stage has 0 rows in `pipeline_jobs` and no writer at any deployed version can enqueue it: the down
   migration drops the index and the cursor table, and restores the three CHECKs.
   - An old writer still running blocks step 5. The down migration checks for 0 rows under a lock and refuses
     otherwise.

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
| R1b | N leased, then the worker crashes: reclaimed; N+1 runs; final state equals the live epoch; second run `unchanged` | absent |
| R1c | a retired or non-current publication rolls back, so there is no job | absent |
| R2a | a lost wake (the job deleted) converges through the catch-up within one lap | cursor absent |
| R2b | tenants beyond one page (T = 2, 5 tenants) are all reached in 3 ticks | absent |
| R3a | a pass on a never-committed scope acks `not_committed` and erases nothing | today it erases on empty keys |
| R3b | an old-version delete with a newer live revision: indexes, no erase | absent |
| R3c | a recreate race (fence, then a newer live generation): live wins | absent |
| R3d | a stale queued epoch after a newer one is indexed: `unchanged` | absent |
| R4 | acceptance missing past max attempts: quarantined, scope not ready, never in a search result | absent |
| R5 | budget accounting: the job has `committed_at`; the pass logs `commit_to_searchable_ms` | absent |
| R6 | every closed stage list has the same set, including `search-index` | absent |

These tests land on a separate head after this doc, with exact commands and outputs.

## 11. M3: true incremental apply [F9]

- **Precondition, from the publisher contract (to confirm with the lander):** an accepted publication's batches are
  immutable once published. Each batch has a stable `batch_id` and a strictly increasing `seq` within its manifest,
  and a new generation's manifest lists the previous generation's batches unchanged, in order, then the new ones.
- **Watermark:** `sessions.search_batch_watermark` (scope, canonical, manifest_key, last_seq, chain_digest), where
  `chain_digest_n = sha256(chain_digest_{n-1} || batch_id_n)`.
  - The prefix relation is proven by recomputing the chain over the new manifest's first `last_seq` batches and
    comparing it with the stored digest.
  - The update is a CAS: `UPDATE … WHERE last_seq = $old AND chain_digest = $old`.
- **Append only:** read and upsert only batches with `seq > last_seq`. No index scan and no delete.
- **Everything else** (digest mismatch, compaction, rewrite, erasure, a recreate with a new canonical): a full pass,
  then the watermark is reset.
- **Tests:** incremental equals full on every fixture; mismatch forces a full pass; CAS loss retries.

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
