# Skip a repeated size refusal before any manifest read

Branch `review/layered-sessions-pipeline-skip-before-read-246a`, on top of
`review/layered-sessions-pipeline-stream-backport-246a`. Code, tests and docs only.
Nothing here deploys anything, and nothing enables the flag.

## The flag

`SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED`, read once at Lambda init (`lambdaEnv` in
`src/session-pipeline/lambda.ts`), parsed the way `SEARCH_INDEX_STREAMING_CH` is:

| value | effect |
| --- | --- |
| unset, `""`, `false` | off (the default): the env object is exactly as without the flag |
| `true` (exactly) | on; needs `SEARCH_INDEX_ENABLED=1`, else init fails |
| anything else (`1`, `TRUE`, ` true`, `yes`, …) | init fails |

On, init adds `skipOverBound: true` to the search stage's deps. It is off by default
and enabled nowhere: no stack, runbook or workflow in this repository renders it.

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
   the listing the pass just took, else the normal pass runs.
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
   it must have `needs_full = 1`, `scanned_rows = 0` and `revision` equal to the
   highest live pointer revision. A failed or empty read runs the normal pass.

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

- The pointer row can lag S3 while a manifest event is in flight (a manifest just
  rewritten smaller, or just deleted). The skip then repeats a refusal the normal
  pass might not make; the in-flight event updates the pointer row and runs its own
  pass after it, which sees the new facts.
- The state row is keyed by (org, session id) only, as `recordState` writes it; a
  matching row written by another scope with the same session id is accepted, but
  conditions 1 and 2 already show this scope would be refused, and the row the
  refusal would write is the same apart from `synced_at`.
- With an unscrubbed live pointer holding the highest revision, the recorded state
  revision (highest readable) never matches, so such a scope is never skipped.

## Tests

`src/session-pipeline/__tests__/search-index-skip.test.ts`, PGlite with every
Supabase and HoopDb migration and the real pointer stage. Each of the three
conditions has a mutation check: removing it turns at least one test red.
