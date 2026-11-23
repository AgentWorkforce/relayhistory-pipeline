# Skip a repeated acceptance-missing pass with an unchanged pointer set

This builds on the over-bound skip (`docs/handoffs/skip-before-read.md`). This change holds
code, tests and docs only. It deploys nothing, and nothing in it turns the flag on.

## The flag

`SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED` is read once at Lambda init (`lambdaEnv` in
`src/session-pipeline/lambda.ts`). It uses the same parse rule as
`SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED`:

| value | effect |
| --- | --- |
| unset, `""`, `false` | off (the default). The env object is exactly the same as without the flag. |
| `true` (exactly) | on. Needs `SEARCH_INDEX_ENABLED=1`. |
| anything else, including `1`, `TRUE`, ` true`, `yes` | init fails |

A refused value fails Lambda init for the whole function. So does `true` without
`SEARCH_INDEX_ENABLED=1`. With the flag on, init adds `skipAcceptanceMissing: true` to the
search stage's deps. This flag is independent of the over-bound flag.

No stack, runbook or workflow in this repository sets the flag. If someone sets it by hand
on the function, the next stack deploy that does not carry it drops it.

## What it does

A pass over a scope whose manifests share a record id from different origins, where no
record carries `accepted_seq`, refuses the fold with `relayhistory_acceptance_missing`. This
happens on both paths:

- **Whole read:** the pass inserts missing in-window rows, deletes nothing, writes no
  catalog, and records `needs_full = 1`.
- **Streamed:** the pass stops with that reason, writes nothing, and records `needs_full`.

Every repeat over the same manifests reads every manifest again and reaches the same
outcome.

With the flag on, `indexSearchScope` in `src/session-pipeline/search-index.ts` changes in
three ways.

1. **Listing.** The live pointer listing selects each row's read-determining facts
   (`livePointerFacts`). It is the same `WHERE` and `ORDER BY` as the key listing, so it is
   still one Postgres statement.
2. **Decision.** The decision runs after the scope lock, the erase branch and the over-bound
   skip, and before `readScope`:
   - The pass reads the stored `window_start`, using `windowStart`'s own read and default,
     so the window it uses is unchanged.
   - It computes `acceptanceDigest`.
   - It reads the scope's stamp: one bounded ClickHouse select, with a 5 s timeout.
   - It returns early only when the stamp meets all of these:
     - it is the latest row for the stamp name;
     - it parses as `{v: 1, reason: "relayhistory_acceptance_missing", digest}`;
     - its digest equals the current digest;
     - its `updated_at` is less than `SKIP_STATE_MAX_AGE_MS` (six hours, the over-bound
       bound) from the pass's time, in either direction.
3. **Settle.** After a pass that was not skipped, busy or deferred:
   - **Stamp.** The pass writes a stamp only when both of these hold:
     - the outcome is `complete: false` with reasons exactly
       `["relayhistory_acceptance_missing"]` (not partial, from either path);
     - every listed pointer was read as listed: same revision, same non-null content
       sha256, same etag as the manifest read returned.
   - **Clear.** Any other outcome writes a tombstone (empty value), but only when a stamp
     was seen or the stamp read failed. Erase also clears.

### The early return

The early return reads no manifest, takes no stream slot and writes nothing: no state row,
no insert, no delete, no catalog, no stamp. It returns this result:

```
{ manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: false, skippedBeforeRead: true,
  reasons: ["relayhistory_acceptance_missing", "skipped_before_read"] }
```

The consumer is unchanged. It acknowledges this result like today's outcome: no requeue, no
batch item failure, so the event never reaches the DLQ. It logs one `search index reconciled`
info line with `ok: false`. Its existing `reason` field reads
`relayhistory_acceptance_missing,skipped_before_read`.

There is no `search index partial` line, because the outcome is not partial. That matches a
normal acceptance-missing pass. No new log key is added.

### Digest

The digest is sha256 over JSON of these inputs:

- the salt `ACCEPTANCE_SKIP_VERSION` (`acceptance-skip-v1`). Bump it with any change to how a
  pass reads, folds or decides this outcome;
- the scope (org, workspace, harness, session);
- the window marker: `default:14d`, or `stored:<window_start>`;
- `limits.maxScopeBytes` and `limits.maxScopeEvents`;
- the stream budgets, or null when streaming is off;
- the sorted per-pointer tuples: manifest key, machine id, revision, content sha256,
  manifest etag, scrubbed.

All of these come from pointer rows, the scope, deployment config or the stored window. None
comes from S3.

### Carrier

The carrier is the existing ClickHouse `search_ingest_state` table: (name, value,
updated_at), `ReplacingMergeTree(updated_at)`, ordered by name. No DDL and no migration are
needed.

- **Row name.** `acceptance_skip/<sha256 of the scope tuple>`. That is one name per scope,
  and it is overwritten.
- **Writes.** Rows are written only with the flag on: a stamp, or a tombstone when a seen
  stamp must be invalidated. A complete pass with no stamp present writes nothing. Growth is
  bounded by the number of scopes that ever had an acceptance-only outcome. Merges collapse
  each name to its newest row.

Every reader of the table in this repository:

| reader | why it is safe |
| --- | --- |
| `readState` (`src/session-pipeline/vendor/search-index/reconcile.ts:91`) and its callers `windowStart` (:117), the tail lease (:656, :680), watermark (:708), revision floor (:713) and cursor (:721) | It matches an exact name (`window_start`, `tail_*`, `revision_floor`). No stamp name equals one of these, because of the prefix (tested). |
| `readAcceptanceStamp` (search-index.ts) | It is the new reader, flag on only. |
| `pipeline.test.ts:178` | It is a test stub that answers nothing for the table. |

Readers outside this repository that list the whole table, if any, are not covered here (see
"Before turning it on").

## Failure paths

- **Stamp read error or timeout.** The pass runs the normal read (fail open). It then clears
  the stamp, or stamps its own outcome.
- **Stamp write or clear error.** The pass result is unchanged. One `warn` line is logged,
  `search index acceptance stamp not written`, with `reason` set to
  `acceptance_stamp_write_failed` or `acceptance_stamp_clear_failed` and the usual error
  code fields. The line uses only existing allowlisted keys.
- **The `window_start` read.** It is the same statement `windowStart` runs. An error aborts
  the pass, as it does today.

## Safety

- **Any change to the pointer set changes the digest, so no skip.** That covers a pointer
  added or removed, a revision raised, an etag or content hash changed, or a scrubbed flip.
  A stored `window_start` set or changed, or a change to the limits or stream budgets, also
  changes it.
- **Stale stamps are harmless.** A stamp predicts the outcome for its exact digest. Two
  passes over the same digest fold the same manifest bytes under the same window and limits.
  A concurrent pass that wrote a stamp for an older pointer set never matches the current
  digest. The latest row by `updated_at` decides.
- **Erase and delete flows.** An acceptance-only pass never deletes, so a skip never keeps a
  row that the normal pass would remove. Erase (no live pointer) runs before the decision and
  clears the stamp. A pointer removal changes the digest.
- **A pointer row that disagrees with what S3 returned writes no stamp.** That covers a stale
  `manifest_etag`, an older revision or a null content hash. Such scopes are never skipped.

## Before turning it on

- The ClickHouse user the pipeline uses must be allowed to INSERT into
  `search_ingest_state`. Before this flag, the search stage only read that table (for
  `window_start`). Check the grant before turning the flag on.
- Confirm that nothing outside this repository lists `search_ingest_state` by prefix or
  without a name filter. If something does, it must ignore `acceptance_skip/` names.
- Accept the whole-read caveat below.

## Counting affected scopes from logs

- **Skips:** count `search index reconciled` lines whose `reason` is
  `relayhistory_acceptance_missing,skipped_before_read`. Distinct (`org_id`, `session_id`)
  on those lines gives the scopes.
- **Normal acceptance-only passes:** count lines whose `reason` is exactly
  `relayhistory_acceptance_missing`.
- **Stamp failures:** count `search index acceptance stamp not written`, split by `reason`.

## Rollback

- **Turn it off.** Unset the flag, or set it to `false`. Nothing reads stamps with the flag
  off, so leftover rows are inert.
- **Clean up (optional).** To remove leftover rows:
  `DELETE FROM search_ingest_state WHERE startsWith(name, 'acceptance_skip/')`. Turning the
  flag back on later is safe either way, because old stamps older than six hours never
  match.

## Limits

- **It is not a cure.** It saves only the repeated reads. The scope stays unindexed past the
  refusal until a writer supplies `accepted_seq` or the contending manifests change.
- **It helps only when repeats arrive with an unchanged pointer set.** Any pointer event that
  changes a pointer fact runs a full pass.
- **Whole-read caveat.** A skip does not re-insert rows that ClickHouse lost since the
  stamped pass, and does not refresh the session state row. A skip never refreshes the stamp,
  so a normal pass runs at least once per six hours for a scope that keeps getting events.
  That bounds this caveat.
- **Rolling default window.** With no stored `window_start`, the rolling default window moves
  forward without changing the digest. That only shrinks what a normal pass would insert.
  The outcome and reasons do not depend on it.
- **Stores without etags.** A rewrite of a manifest at the same revision and content hash
  needs a changed etag to be noticed. On a store that returns no etag, such a rewrite cannot
  change the fold, because the content hash is in the digest.
- **Skips can hide extra reasons.** A skip carries none of the reasons a normal pass might
  add. For example, it would not show `clickhouse_truncated`, or `manifest_missing` while a
  live pointer's manifest is gone from S3. Both are no-delete outcomes.

## Tests

The tests are in `src/session-pipeline/__tests__/search-index-acceptance-skip.test.ts`. They
use PGlite with every Supabase and HoopDb migration, the real pointer stage, an fs object
store, and a ClickHouse fake that models `search_ingest_state`. They cover:

- with the flag off: statement-level equality, and no stamp IO;
- stamping and skipping on both paths;
- each digest input;
- stale, future, malformed, other-reason and other-version stamps;
- read and write failures, with the write failure checked through the real logger;
- clear on other outcomes and on erase;
- the consumer's acknowledgement and log line;
- the env parse and wiring.

The golden transcript (`search-index-golden.test.ts`) is unchanged.
