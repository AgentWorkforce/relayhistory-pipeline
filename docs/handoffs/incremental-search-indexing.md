# Incremental search indexing: committed wake and revision-aware passes (design M1, rev 9)

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
- **Measurement [B1]:** time zero is the publish clock (`payload.clock`, §2): `ingest_publication_attempts.published_at`
  of the latest current live update of the scope.
  - That value is set by `clock_timestamp()` in the publish transaction's final UPDATE (migration
    20261105059000:192). It is a lower bound on the publish commit, within that transaction's remaining
    statements: milliseconds.
  - The authority transaction does NOT supply the clock (its `now()` is a different, later transaction's start).
  - At enqueue (§2), the authority transaction reads `published_at` for the head's current attempt and carries it as
    `payload.clock = {source: 'publish', at}`.
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
    the migrator, `src/hoopdb/migrate-guards.ts:282`; verified with `has_table_privilege`). For `search-index` rows
    that DML is refused by the queue guard; the consumer goes through the two SECURITY DEFINER RPCs (§3.2, §3.3).
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
  "workKey": "search\u001F<org>\u001F<ws>\u001F<harness>\u001F<canonical session id>",
  "revision": <pointer revision of this write, informational>,
  "payload": { "epoch": <session_directory.event_projection_epoch after this transaction's writes>,
               "clock": { "source": "publish", "at": "<ingest_publication_attempts.published_at, ISO>" } } }
```

- `epoch` is THE authority token for search (§4.4): one counter, the directory epoch, bumped by trigger on every
  `session_index` change, including tombstones.
- **Clock source [rev 4]:** `payload.clock = {source, at}`. It is never null and never silently missing.
  - `publish`: for the canonical scope, the latest current live update:
    ```sql
    SELECT max(a.published_at)
      FROM sessions.ingest_publication_heads h
      JOIN sessions.ingest_publication_images i ON i.manifest_key = h.current_key
      JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token AND a.state = 'published'
      JOIN sessions.session_index p ON p.manifest_key = h.current_key AND p.deleted_at IS NULL
     WHERE h.state = 'live' AND h.org_id = $org AND h.workspace_id = $ws AND h.physical_source = $harness
       AND coalesce(p.canonical_session_id, p.session_id) = $canonical;
    ```
    Several live sibling heads (machines) take the max: the clock of the newest committed update the pass makes
    searchable. Used by the managed enqueue (one head; the same query) and by managed catch-up. Test R5e: two
    current heads give the later one's `published_at`.
    - The aggregate excludes deleted or retired heads and other-scope heads.
    - It never replaces the triggering head's own authority check: a sibling's valid timestamp cannot validate an
      unproven trigger (§2's `session_publication_lock` still gates the enqueue).
    - If the aggregate is NULL for a managed scope, FAIL CLOSED FOR SEARCH ONLY:
      - the authority transaction still commits the pointer and directory, so the session is readable;
      - a search-side defect never blocks upload or read;
      - the writer enqueues NO search-index job, and records the refusal [rev 7] as a `sessions.pipeline_events` row
        IN THE SAME authority transaction: `stage = 'search-index'`, `status = 'skipped'`,
        `error = {code: 'clock_unprovable', retryable: false}`, with the scope's org, ws, harness and session columns,
        `idempotency_key = trace:search-index:skipped:1:<manifest_key>`;
      - durable and committed with the pointer. It is observable through the existing trace and operator tooling over
        `pipeline_events` (the stage's skipped/error counts), plus the Lambda log metric. The alert fires on any
        `clock_unprovable` in 15 min;
      - the `pointer` clock is only for non-managed catch-up, never a fallback here.
      - Test R5g.
    - R5e covers the older head triggering, plus the exclusions.
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

[least privilege] The claim is `sessions.claim_search_jobs_v1(p_limit integer)`, SECURITY DEFINER,
`SET search_path = pg_catalog, pg_temp`, every relation schema-qualified. It takes only a page size and applies
server policy:

- the limit is clamped to 1..10; it claims only `stage = 'search-index' AND NOT dlq AND visible_at <= now()`,
  `FOR UPDATE SKIP LOCKED`;
- it counts the attempt and sets a fresh lease of the server's 300 s;
- it upserts the **claim receipt** and returns the STORED receipt `(job_id, lease_id, message, epoch, attempts)`.

**The claim receipt** is `sessions.search_job_claims (job_id PRIMARY KEY, lease_id, claimed_epoch, claimed_message,
claimed_attempts, claimed_at, lease_until)`: owner-only (REVOKE ALL from PUBLIC, anon, authenticated and service_role;
no grant), one row per leased job. A reclaim after expiry replaces it; every settlement deletes it in the settling
transaction. The receipt's attempt follows an owner-side raise of the leased row's attempts and survives a newer wake's
budget reset on the leased row (§3.4: N leased at attempt 12, wake N+1, N exhausts).

### 3.3 Settlement

A stage-specific settlement, in the Lambda's search-job loop; not the generic one. The generic DELETE on `done` would
lose a follow-up wake that arrived during the pass. Each statement keys on `(id, lease_id)`.

[least privilege] The settlement is `sessions.settle_search_job_v1(p_job_id bigint, p_lease uuid, p_outcome text)`,
SECURITY DEFINER with the same pinned search_path, returning `{applied, action}`. The SQL below is its body; the
caller supplies no epoch, message, attempt, scope or delay:

- outcomes are allowlisted: `done` | `retry` | `busy` (anything else raises 22023). `exhausted` is not a caller
  outcome: the server exhausts a `retry` at its max attempts (12) and backs off `min(900, 30 * 2^(attempt-1))` s
  otherwise; `busy` is bounded below;
- it loads the job row and the receipt `FOR UPDATE` and requires `stage = 'search-index'`, the row's lease and the
  receipt's lease both equal to `p_lease`, and the lease unexpired on the wall clock read after both row locks
  (`visible_at` and `lease_until` both after `clock_timestamp()`, never the transaction start).
  Otherwise it returns `{applied: false}` and changes nothing (lost, expired or replaced lease, another stage's id,
  a duplicate settlement);
- `$claimed_epoch`, `$claimed_msg` and the attempt below come ONLY from the receipt. So a lease holder cannot submit
  N+1 as its claimed epoch (and delete later work), or park a fabricated message;
- the letter's id, trace and scope columns come from the receipt's message (also on a re-death's conflict update);
  the letter id is computed in SQL from it (`sessions.search_letter_id_v1`, dlq.ts `deadLetterId`
  byte for byte, parity-tested);
- the row change, the receipt delete, the letter (park or §3.3b resolution) and the `pipeline_events` audit row are
  one transaction.

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
  -- Exactly the postgres-dead-letters.ts quarantine statement. [rev 6] The letter id is the repository's canonical
  -- deadLetterId('search-index', ctx, workKey) = 'search-index:' || encodeURIComponent(org) || ':' ||
  -- encodeURIComponent(ws) || ':' || workKey (dlq.ts:125-139). It is computed in TS and passed as $letter_id: ONE
  -- letter per scope, the same id the store and the operator and replay tooling use. Re-death refreshes the body and
  -- un-resolves the letter.
  INSERT INTO sessions.pipeline_dead_letters AS d
         (id, stage, trace_id, org_id, workspace_id, session_id, machine_id, message, first_dead_at, last_dead_at)
  SELECT $letter_id, 'search-index', j.trace_id, j.org_id, j.workspace_id, j.session_id, j.machine_id,
         $claimed_msg::jsonb, now(), now()
    FROM sessions.pipeline_jobs j WHERE j.id = $id AND j.lease_id = $lease   -- 0 rows if the lease was lost: abort
  ON CONFLICT (id) DO UPDATE SET message = EXCLUDED.message, state = 'dead', last_dead_at = EXCLUDED.last_dead_at,
         dead_count = d.dead_count + 1, resolved_at = NULL,
         session_id = COALESCE(EXCLUDED.session_id, d.session_id), machine_id = COALESCE(EXCLUDED.machine_id, d.machine_id);
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

**Bounded busy, contention and the reclaim cap [least privilege, M2].** All server-side, inside the claim and settle
functions, in their own transactions:

- **`busy`** (the pass found `scope_busy` or `canonical_scope_busy`; the Lambda sends it) never spends the 12-attempt
  budget and never parks. At the claimed epoch it gives back the attempt the claim counted (attempts = the receipt's
  attempt - 1), deletes the receipt, holds the job a fixed 30 s, and writes a durable `retrying` event
  (`scope_busy`, retryable; the idempotency key carries the lease id). If a newer epoch merged during the lease it
  reopens now at attempts 0 instead.
- **Contention state** lives outside the receipt in `sessions.search_job_contention` (owner-only, no grant): one row
  per contended job at its epoch (first and last contended, count, last escalation, `held_at`, `next_probe_at`). A
  done, a substantive retry, an exhaustion or a newer epoch clears it in the same transaction.
- **Escalation:** at 2 h of same-epoch contention, then at most hourly (`last_escalated_at`), a
  `scope_contention_escalated` event; the settle result carries `escalated`, `contended_for_s`, `contention_count`.
- **The 24 h cap (hold and hourly probe):** at 24 h the job is HELD (`held_at`, `next_probe_at` = now + 1 h; event
  `scope_contention_held`, once per hold). A held job is never ordinary work: `claim_search_jobs_v1` skips it.
- **The probe lane:** `sessions.claim_search_probe_v1()` (SECURITY DEFINER, pinned search_path, PUBLIC revoked,
  EXECUTE to the runtime through the service_role mirror, like the ordinary claim) returns at most ONE held job whose
  `next_probe_at` is due (oldest due first, then job id), and only if the fleet bucket has a token, which it spends.
  It leases the job like any claim, moves the next probe an hour on, and writes exactly one `scope_contention_probe`
  event per admitted probe in that transaction (its lease in the key), whether the probe then completes, is busy or
  is abandoned; a token rejection or a park writes none. A probe that finds the lock free completes normally, so a
  static scope recovers with no replay. A newer epoch releases the hold.
  `sessions.resume_search_contention_v1(job_id, operator)` (service_role only; search-index only; row locks; refuses
  while a lease is live) clears a hold and audits the operator text, `session_user`, `current_user` and the clock.
- **Fleet probe budget:** `sessions.search_probe_budget`, one owner-only row, a token bucket shared by every prober
  under `FOR UPDATE`. Defaults: refill **R = 6 per minute**, burst **C = 6**, initial balance **6 tokens**; tunable by
  an owner UPDATE of the row. Probes admitted over any interval are at most C + R x elapsed minutes.
- **Ordinary capacity is isolated:** the Lambda's `drainSearchJobs` runs up to K ordinary passes (SEARCH_JOBS_CLAIM),
  each claimed with `claim_search_jobs_v1(1)` just before it runs; probes are EXTRA and never take an ordinary pass.
  The SQS follow-on probes once after its ordinary passes if time remains; the scheduled invocation
  (`{"searchJobs": true}`) probes FIRST, then runs its K ordinary passes, so held scopes recover even when ordinary
  passes always run to the deadline. Hosted once-a-minute recovery is CONDITIONAL on that schedule rule being
  provisioned and enabled (a stack change with its own go); the handler test qualifies what an invocation does when
  invoked, not that the schedule exists.
- **Expired-lease reclaim cap** (a worker that dies or hangs; this may park): the persisted receipt counts consecutive
  abandoned leases at one epoch (`reclaim_count`); it resets on a newer epoch and on any settled pass (every
  settlement deletes the receipt). At exactly 6 consecutive same-epoch expiries the claim parks the previous claim's
  snapshot (code `expired_lease_reclaims`) and deletes the row, receipt and contention, atomically.
- The down migration refuses while any receipt or contention row exists, and drops both tables and the budget row.

### 3.3b Dead-letter replay [rev 4, rev 5]

[rev 6] The letter id is `deadLetterId('search-index', ctx, workKey)` (dlq.ts:125-139): one letter per scope, keyed
by the same work key as the job row, in the repository's canonical form. Resolution is
therefore keyed by SCOPE, not by a replay id carried in the message. No handle can be lost when a newer wake
replaces the row's message (the rev-4 race: replay N, wake N+1, done N reopens, N+1 completes without the replay id).

Replay, in ONE transaction:

1. `UPDATE pipeline_dead_letters SET state = 'replaying', replay_count = replay_count + 1, last_replay_id = $r,
   last_replay_at = now() WHERE id = $letter AND state = 'dead'`;
2. the §3.1 upsert with the letter's message. It coalesces with any newer row; epoch advance resets attempts.

Completion: the §3.3 `done` DELETE, in the same transaction:

```sql
UPDATE sessions.pipeline_dead_letters SET state = 'resolved', resolved_at = now(),
       resolution = CASE WHEN (message->'payload'->>'epoch')::bigint = $claimed_epoch THEN 'completed' ELSE 'superseded' END
 WHERE id = $letter_id AND state = 'replaying'
   AND (message->'payload'->>'epoch')::bigint <= $claimed_epoch;   -- the completed epoch covers the letter's
```

- **Superseded** (the row already carries a newer epoch when the replay runs): the letter resolves when that newer
  epoch completes. The predicate holds, because the completed epoch is at or above the letter's.
- **Exhausted again:** the quarantine statement (§3.3) refreshes the same letter to `dead`, so it is never left
  `replaying`.
- The M2 migration adds a nullable `resolution text` and the `search_stage_archive` table. Completion sets `completed` (equal epoch) or `superseded`
  (a newer epoch covered it); R8a asserts `superseded` and R8b asserts `completed`.

Tests:

- R8a: superseded resolves on the newer completion;
- R8b: merged resolves on done;
- R8c: replay N, claim N, wake N+1, done N reopens, N+1 done, letter resolved (the rev-4 race);
- R8d: re-death after replay refreshes the message and clears `resolved_at`.

All run through the REAL M2 entry points (§10).

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

1. scope lock `96bulls:search\u001Forg\u001Fws\u001Fharness\u001Fsession` (try; `scope_busy` leads to a retry with a short delay,
   coalesced);
2. `livePointerSnapshot` (keys, epoch, canonical);
3. canonical lock `96bulls:directory-events\u001F…\u001Fcanonical` (try);
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
- At exhaustion, the STAGE-SPECIFIC §3.3 exhausted settlement quarantines the claimed snapshot (never the generic
  `dlq.ts` path, which this stage never reaches).
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

- **DB role and boundary:** no new role. The runtime role's only stage surface is EXECUTE on
  `sessions.claim_search_jobs_v1` and `sessions.settle_search_job_v1` (granted to service_role in migration
  20261105062300 and mirrored onto the runtime role by the migrator, RUNTIME_APPLICATION_FUNCTIONS). It needs no
  search-index row DML: the queue guard refuses search-index writes from anyone but the schema owner, and the claim
  receipts are owner-only.
- **Operator-only, never runtime:** replay (`replaySearchDeadLetter`), catch-up (`catchUpTick`) and park
  (`parkSearchJobs`) are owner-run TypeScript over the guarded queue (owner credentials); the archive is
  `archive_search_stage_v1()`, EXECUTE to service_role only. A future scheduled catch-up needs its own scoped grant.
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

**`sessions.search_stage_archive`** [rev 8], created by the up migration and NOT dropped by the down migration:

```sql
CREATE TABLE sessions.search_stage_archive (
  batch_id    uuid        NOT NULL,
  kind        text        NOT NULL CHECK (kind IN ('letter', 'event', 'manifest')),
  source_id   text        NOT NULL,      -- letter id | event sha256 | 'manifest'
  row         jsonb       NOT NULL,      -- to_jsonb(source row) | {letters, events, sha256}
  archived_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (batch_id, kind, source_id)
);
CREATE UNIQUE INDEX search_stage_archive_one_manifest ON sessions.search_stage_archive (batch_id) WHERE kind = 'manifest';
-- Sealed and append-only [rev 9]: INSERT, UPDATE and DELETE are refused for every role but the schema owner (a
-- trigger raises 42501). The ONLY write path is sessions.archive_search_stage_v1(): SECURITY DEFINER, owned by the
-- schema owner, SET search_path = pg_catalog, pg_temp. It performs §8's whole archive (lock, refusals, insert rows
-- and manifest, delete) in one transaction, so no role can append to a completed batch or forge one.
-- Cleanup is a separate, explicitly approved owner migration.
REVOKE ALL ON sessions.search_stage_archive FROM PUBLIC, anon, authenticated, service_role;
GRANT SELECT ON sessions.search_stage_archive TO service_role;
REVOKE ALL ON FUNCTION sessions.archive_search_stage_v1() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.archive_search_stage_v1() TO service_role;   -- operator only, never the runtime role
```

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

**Drain path** (the default; the consumer is healthy). Its step 5 runs only once the stage is empty under the rule
below:

1. writers off (`SEARCH_WAKE_ENQUEUE=0`, catch-up off). The consumer stays ON;
2. drain: wait until `pipeline_jobs` has 0 `search-index` rows (held rows exhaust into dead letters within their
   backoff, about 2.5 h worst case, or use the park path for the remainder);
3. consumer off (`SEARCH_JOBS_ENABLED=0`);
4. roll the code back;
5. down migration (below).

**Park path** (the consumer is unhealthy):

1. writers off;
2. consumer off;
3. park, in ONE fenced transaction [rev 5]. It runs only with the consumer flag off, and the table lock fences
   any straggler:
   ```sql
   LOCK TABLE sessions.pipeline_jobs IN SHARE ROW EXCLUSIVE MODE;
   INSERT INTO sessions.pipeline_dead_letters AS d
          (id, stage, trace_id, org_id, workspace_id, session_id, machine_id, message, first_dead_at, last_dead_at)
   SELECT <deadLetterId per row, computed by parkSearchJobs in TS over the selected rows>, 'search-index', j.trace_id,
          j.org_id, j.workspace_id, j.session_id, j.machine_id, j.message, now(), now()
     FROM sessions.pipeline_jobs j WHERE j.stage = 'search-index'
   ON CONFLICT (id) DO UPDATE SET message = EXCLUDED.message, state = 'dead', last_dead_at = EXCLUDED.last_dead_at,
          dead_count = d.dead_count + 1, resolved_at = NULL;
   DELETE FROM sessions.pipeline_jobs WHERE stage = 'search-index';
   ```
   - The parked count is the INSERT's row count. It is logged with the operator and the time; there is no audit
     table.
   - The parked messages are replayable once the consumer is back.
   - Tests R9a-R9g (below): after a park, the down precondition does NOT hold.
4. roll the code back (writer and consumer code), with flags off;
5. STOP. The down migration is NOT run on this path: it refuses while parked letters exist. The stage's schema
   stays in place (inert without the code and flags) until the replay, resolve and archive path below empties it.

**Down migration [rev 6]: the additive migration STAYS until the stage is fully empty.** Parked letters are a
promise of replay, so the two cannot coexist with a down migration.

- After a park, the stage's letters and events still exist. The down migration's precondition is therefore NOT met,
  and it REFUSES. That is the expected state: the CHECK values, the unique index and the cursor table are inert
  without the code and flags, so keeping them costs nothing.
- **To reach down:** bring the consumer back, replay the parked letters until each is `resolved`, then run
  `archiveSearchStage` [rev 7]. The archive is durable and failure-safe. In ONE transaction, under
  `LOCK TABLE pipeline_dead_letters, pipeline_events IN SHARE ROW EXCLUSIVE MODE`:
  1. REFUSE (raise and roll back; nothing archived or deleted) if any `search-index` letter is not `resolved`, or if
     any `search-index` job row exists;
  2. `INSERT INTO sessions.search_stage_archive (batch_id, kind, source_id, row, archived_at)`: every `search-index`
     letter and event row as jsonb, with `batch_id` a fresh uuid;
  3. compute the manifest digest [rev 8] and insert one manifest row (`kind = 'manifest'`) with
     `{letters, events, sha256}`. Exactly:
     ```sql
     encode(sha256(convert_to(string_agg(jsonb_build_array(kind, source_id, row)::text, E'\n'
            ORDER BY kind, source_id), 'UTF8')), 'hex')
     ```
     It is taken over the batch's `letter` and `event` rows.
     - `row` is `to_jsonb(<source row>)`.
     - A letter's `source_id` is its letter id.
     - An event's `source_id` is `encode(sha256(convert_to(to_jsonb(<event row>)::text, 'UTF8')), 'hex')`.
       `pipeline_events` has no primary key, and the row's own content (idempotency key, timestamp, attempt) makes it
       unique.
     - `jsonb_build_array` makes the serialization unambiguous: no delimiter can collide.
  4. DELETE the archived letters and events.
  
  A failure at any step rolls back everything. `search_stage_archive` is created by the up migration and is NOT
  dropped by the down migration: it is an audit artifact, removed only by a separate, explicitly approved cleanup.
  Only after a successful archive does the precondition hold: 0 rows for the stage in `pipeline_jobs`,
  `pipeline_events` AND `pipeline_dead_letters`.
- **Events written by the stage:** settlement writes `pipeline_events` rows like every stage
  (`ok` / `retrying` / `dead_lettered` / `replayed` / `skipped`), so a used stage always has events. That is why
  the down precondition needs the archive.
- The down migration takes `LOCK TABLE … SHARE ROW EXCLUSIVE` on all three, checks all three counts are 0,
  refuses otherwise, then drops the runtime RPCs (claim and settle), the claim-receipt table, the index and the
  cursor table and restores the three CHECKs. It keeps the sealed archive.
- **Tests:**
  - R9a: after a park, down REFUSES (letters exist; events > 0 from the park's `dead_lettered` rows);
  - R9g [rev 8]: each guard on its own: job-only gives false; letter-only gives false; event-only gives false; all
    three zero gives true. A precondition that ignores any one table fails;
  - R9b: replay, resolve, archive:
    - jobs, events and letters all 0;
    - the archive holds every row plus one manifest;
    - the manifest sha256 AND the returned sha256 both equal a digest recomputed in-test, independently, from the
      PRE-archive source rows with the exact rule above;
    - the precondition holds;
    - another stage's seeded letter, job and REAL event survive;
  - R9c: archive with one letter still `dead` REFUSES, and nothing is archived or deleted (failure-safe);
  - R9d [rev 8]: archive with a `search-index` job row present REFUSES, and nothing changes;
  - R9e [rev 8]: an archived row cannot be updated or deleted by the runtime role (append-only);
  - R9f [rev 9]: a direct INSERT by a non-owner role (an append to a completed batch, or a forged batch) is refused
    with 42501; `archive_search_stage_v1` is SECURITY DEFINER with a pinned search_path, owned by the archive's
    owner, with no PUBLIC execute and service_role (operator) EXECUTE only, never the runtime role; every non-owner
    holds SELECT only on the table.
  - Least privilege (search-jobs-definer.pglite.test.ts, and search-jobs-definer.postgres.test.ts on a real server):
    an unprivileged LOGIN with no membership and only EXECUTE on claim and settle claims and settles; an expired or
    replaced lease, another stage's id, a duplicate settlement and a forged outcome (22023) change nothing; a
    forged epoch or message has no effect (the receipt decides); a settlement error rolls back whole; direct
    search-job or receipt DML, TRUNCATE, replay, catch-up, park and archive give 42501, also with the runtime role's
    own table privileges.

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
| R3c | a recreate race (every row tombstoned, then a newer live generation; a fenced scope cannot be recreated): live wins | absent |
| R3d | a stale queued epoch after a newer one is indexed: `unchanged` | absent |
| R4 | acceptance missing past max attempts: quarantined, scope not ready, never in a search result | absent |
| R4b | N exhausts after a wake N+1 merged into the leased row: only N is parked; the row survives with N+1 and a fresh budget | absent |
| R5 | the publish clock: a job delayed in the queue reports `commit_to_searchable_ms` that includes the delay, measured from `published_at` | absent |
| R7 | M3 milestone (not in the M1b file): a chunk with the same seq and a changed content sha256 forces a full pass. It lands as a red fixture at the start of M3, with the compaction and CAS cases | deferred to M3 |
| R1f | wake N+1 during N's exhausting attempt: no N+1 dead letter, the row survives with N+1 | absent |
| R6g | the generic runWorker / postgresStageConsumer is not registered for `search-index` | absent |
| R8a-d | stage-aware replay: superseded resolves on the newer completion; merged on done; the replay/new-wake race; re-death refreshes | absent |
| R5e | two current live heads: the clock is the later published_at | absent |
| R9a-g | park, then down refuses (letters and events present); replay, resolve, archive (durable, sha256 manifest), then the precondition holds; an archive with an unresolved letter refuses and changes nothing | absent |
| R5f | an OLDER live head triggers the authority enqueue while a newer sibling supplies max(published_at): the job clock is the newer one, and the trigger is still validated by its own authority | absent |
| R5g | a managed scope with a NULL or unprovable publication clock: no job, an error metric, never a pointer fallback | absent |
| R5c/d | catch-up clock: publish for managed, pointer for non-managed; no null clock | absent |
| R6 | every closed stage list has the same set, including `search-index` | absent |

**Production-path coupling [rev 5].** Every race and catch-up test calls the REAL M2 entry points:

- `enqueueSearchWake`;
- `settleSearchJob(done | retry | exhausted)`;
- `replaySearchDeadLetter`;
- `catchUpTick`;
- `runSearchJob`.

Each is imported from `src/session-pipeline/search-jobs.ts`. None runs the design's SQL in-test, so a migration
alone can never turn them green. One wiring test asserts that the Lambda consumer's search-job loop calls
`settleSearchJob`, and that the stage is absent from the generic runWorker registrations.

## 11. M3: true incremental apply [F9, B7]

Immutability is VERIFIED, not assumed. Each manifest chunk already carries `seq`, `epoch`, `start_offset` and the
content `sha256` (`packages/object-store/src/manifest.ts:53-62`).

- **Watermark:** `sessions.search_batch_watermark` (scope, canonical, manifest_key, last_seq, chain_digest), where
  `chain_digest_n = sha256(chain_digest_{n-1} ‖ seq_n ‖ epoch_n ‖ start_offset_n ‖ sha256_n)`.
  Content is bound, not just ids.
- **Exact chain encoding:** `chain_digest_(-1)` is 32 zero bytes. Every integer is an unsigned 64-bit big-endian
  value and `sha256_n` is its 32 decoded bytes, so each link hashes exactly 88 bytes. The storage-independent
  planner is `src/session-pipeline/search-incremental.ts`; malformed or incomplete evidence always selects a full
  pass, never an incremental one.
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
