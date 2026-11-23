# Skip a repeated size refusal before any manifest read

Branch `review/layered-sessions-pipeline-skip-before-read-246a`, on top of
`review/layered-sessions-pipeline-stream-backport-246a`. Code, tests and docs only.
Nothing here deploys anything, and nothing enables the flag.

## The flag

`SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED`, read once at Lambda init (`lambdaEnv` in
`src/session-pipeline/lambda.ts`). Only `true` and `false` are accepted:

| value | effect |
| --- | --- |
| unset, `""`, `false` | off (the default): the env object is exactly as without the flag |
| `true` (exactly) | on; needs `SEARCH_INDEX_ENABLED=1` |
| anything else, including `1`, `TRUE`, ` true`, `yes` | init fails |

A refused value (or `true` without `SEARCH_INDEX_ENABLED=1`) fails Lambda init for the
whole function, every stage, not only search. On, init adds `skipOverBound: true` to
the search stage's deps. It is off by default and enabled nowhere: no stack, runbook
or workflow in this repository renders it. A variable set by hand on the function is
dropped by a later stack deploy that does not carry it.

## What it does

Today a scope whose readable manifests sum past the whole-read bound
(`limits.maxScopeBytes` / `limits.maxScopeEvents`) is refused after every manifest
is read, and the refusal is recorded as a state row (`needs_full = 1`,
`scanned_rows = 0`, revision = highest readable manifest revision). Every later pass
at the same revision reads every manifest again to reach the same refusal.

With the flag on, `indexSearchScope` checks, after the scope lock, the live pointer
listing and the erase branch, and before `windowStart` and `readScope`
(`overBoundBeforeRead` in `src/session-pipeline/search-index.ts`). It returns early
only when all three hold:

1. **Pointer facts.** One query in the pass's transaction over the scope's live
   `sessions.session_index` rows: the sum of `raw_bytes` over rows with
   `scrubbed = true` is over `limits.maxScopeBytes`. The live row count must equal
   the listing the pass just took, else the normal pass runs. That check only
   notices a change in the number of live rows, not a swapped key or an in-place
   update; facts newer than the listing only make the skip more conservative, since
   the decision uses the facts query alone, one consistent statement.
   `raw_bytes` is a lower bound of what `readScope` sizes for the same manifest: the
   pointer stage writes `raw.raw_bytes` (`upsertRawPointer` in `src/hoopdb/index.ts`
   through `pointerFields`), the manifest reader's `parseManifest` refuses a manifest
   whose `raw.raw_bytes` is not the sum of `chunks[].bytes` (`validateManifest` in
   `packages/object-store/src/manifest.ts`), and `readScope` sizes a chunk as
   `max(bytes, stored_raw_bytes ?? 0)`. `upsert_session_index` overwrites `raw_bytes`
   on every applied revision (NULL becomes 0, still a lower bound). `scrubbed` is true
   only when the manifest carries `raw.scrub`, which is what `readScope` admits.
   `event_count` is **not** used: the upsert keeps the stored value when a writer sends
   NULL (migration `20261025000300`), so it can describe an older revision, and the
   event bound is therefore never used to skip.
2. **Stream guard.** With the streamed reconcile armed (`deps.stream`), the same lower
   bound times `STREAM_PASSES` must also exceed the stream byte budget, so the streamed
   path could not take the scope. A scope the streamed path could take is never skipped.
3. **Stored index state.** Only after 1 and 2 hold, one bounded ClickHouse read
   (`LIMIT 1`, 5 s timeout) of the scope's latest `search_ingest_session_state` row:
   it must have `needs_full = 1`, `scanned_rows = 0`, `revision` equal to the highest
   live pointer revision, and `synced_at` less than `SKIP_STATE_MAX_AGE_MS` (six hours)
   from the pass's time, in either direction; a missing or non-numeric time does not
   skip. This only shows that the state already says the scope owes a full pass at
   this revision, so the skip writes nothing the refusal would not. That the normal
   pass would refuse follows from conditions 1 and 2 alone; the row may have been
   written by any path (a whole-read refusal, a stopped streamed pass, or the streamed
   pass's own first row). A skip never rewrites the row, so a scope that keeps getting
   events takes a normal pass, which rewrites it, at least once per age bound.

## Failure paths

- A Postgres error in the facts query aborts the pass like any other Postgres error
  in it: the transaction rolls back and the event retries.
- A ClickHouse error or timeout in the state read, or an empty result, runs the
  normal pass.
- The state read runs inside the scope-lock transaction, with a 5 s timeout.

The early return reads no manifest and writes nothing (no state row, no delete, no
insert, no catalog). Its result: `complete: false`, `partial: true`, `manifests: 0`,
`skippedBeforeRead: true`, `reasons: ["scope_too_large", "skipped_before_read"]`.
The consumer handles it exactly as today's size refusal: no requeue, no failure (so
no DLQ), the same `search index partial` warning and a `search index reconciled`
line whose existing `reason` field reads `scope_too_large,skipped_before_read`. No
new log key.

## What it does not do

- It does not change the bound, the streamed path or what a normal pass writes.
- It never decides a scope fits: anything short of all three conditions is the
  normal pass, which reads and decides as before.
- No ClickHouse schema change, no Postgres object, no migration.
- Flag off: no new query and no new branch; the golden transcript
  (`src/session-pipeline/__tests__/search-index-golden.test.ts`) is unchanged.

## Known limits

- Pointer lag: the pointer row can lag S3 while a manifest event is in flight (a
  manifest just rewritten smaller, or just deleted). The skip can then repeat a
  refusal until the pointer event for the newer manifest has run; the repeat ends on
  its own with that event, which updates the row and runs its own pass.
- The skip relies on pointer events arriving (and on dead-letter redrive). A lost
  event is covered only by the age bound of condition 3, so the repeat can last up to
  that bound after the next event for the scope.
- While a scrubbed live pointer's manifest is missing or no longer admitted, the
  normal pass might index the remaining manifests insert-only, or throw; the skip
  does neither until the age bound runs out or the pointer event arrives.
- A skipped pass logs `manifests` 0, meaning not read, and carries none of the
  per-manifest reasons (`manifest_missing` and the like), because no manifest was
  looked at. The warning line keeps the plain size reason; the reconciled line
  carries `scope_too_large,skipped_before_read`.
- The state row is keyed by (org, session id) only, as `recordState` writes it; a
  matching row written by another scope with the same session id is accepted, but
  conditions 1 and 2 already show this scope would be refused, and the row the
  refusal would write is the same apart from `synced_at`.
- With an unscrubbed live pointer holding the highest revision, the recorded state
  revision (highest readable) never matches, so such a scope is never skipped.

## Tests

`src/session-pipeline/__tests__/search-index-skip.test.ts`, PGlite with every
Supabase and HoopDb migration and the real pointer stage; the live-count check is
tested by changing the scope's rows in the test's executor between the listing and
the facts query. Each condition, the strict bound, the scrubbed filter, the revision
comparison, the live-count check and the age bound has a mutation check: changing it
turns at least one test red.
