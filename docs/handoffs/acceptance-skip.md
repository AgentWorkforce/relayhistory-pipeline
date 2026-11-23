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
   - **Tombstone time.** When a stamp was seen, the tombstone is written at the later of the
     pass's time and the stamp's time plus one millisecond. So it sorts above the stamp it
     clears even when the clock that wrote the stamp ran ahead.

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
  pass reads, folds or decides this outcome, including the acceptance fold and the transform
  outside this file (see "Before turning it on");
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
- **Stale stamps are harmless, under two conditions.** A stamp predicts the outcome for its
  exact digest. Two passes over the same digest fold the same manifest bytes under the same
  window and limits only while both of these are true:
  - every manifest writer raises the revision whenever a manifest's content changes;
  - the fold code is unchanged since the stamp was written.

  If either fails, a skip can repeat an outcome that a full read would no longer reach, for
  up to six hours (see "Before turning it on"). A concurrent pass that wrote a stamp for an older pointer set never matches the current
  digest. The latest row by `updated_at` decides.
- **Erase and delete flows.** An acceptance-only pass never deletes, so a skip never keeps a
  row that the normal pass would remove. Erase (no live pointer) runs before the decision and
  clears the stamp. A pointer removal changes the digest.
- **A pointer row that disagrees with what S3 returned writes no stamp.** That covers a stale
  `manifest_etag`, an older revision or a null content hash. Such scopes are never skipped.

## Before turning it on

Each item below must be true before the flag goes on.

1. **Fold or transform changes.** Any deploy that changes the acceptance fold or the
   transform (packages/pipeline `relayhistory-transform` and the projection) must either bump
   `ACCEPTANCE_SKIP_VERSION` in `src/session-pipeline/search-index.ts`, or purge the stamps.
   The digest does not see code changes. Without one of the two, a stamped scope can keep
   skipping for up to six hours after such a deploy.
   - Purge statement, run as a ClickHouse admin:
     `DELETE FROM <database>.search_ingest_state WHERE startsWith(name, 'acceptance_skip/')`.
   - The table is a plain `ReplacingMergeTree(updated_at)` ordered by `name`
     (`src/session-pipeline/vendor/search-index/schema.ts`). So this is a lightweight
     `DELETE`, the same form the stage already uses to erase rows from the events table.
     The mutation form `ALTER TABLE <database>.search_ingest_state DELETE WHERE
     startsWith(name, 'acceptance_skip/')` also works.
   - Both forms need the `ALTER DELETE` privilege on the table. The pipeline user does not
     need that privilege; the purge is an admin step.
2. **Manifest writers raise the revision.** Every writer of manifests must raise the revision
   whenever content changes. A manifest rewritten in place at the same revision leaves the
   pointer row as it was, so the digest still matches and a skip hides the change.
   - The pointer upsert keeps the existing pointer row unless the new revision is higher
     (`supabase/migrations/20261025000300_upsert_keep_on_null.sql`). So a rewrite at the same
     revision never reaches the pointer row.
   - The sync exporter in this repository writes the prior revision plus one
     (`src/migration/neon-s3-sync/export-session.ts`). It meets the condition.
   - Any other writer must be checked before the flag goes on.
3. **INSERT grant.** See "Grant" below.
4. **Readers.** See "Readers" below.
5. **The whole-read caveat.** Someone must accept it (see "Limits").

### Grant

On this code line no code path of the function writes `search_ingest_state`. The only INSERT
into it in this repository is `writeState` in
`src/session-pipeline/vendor/search-index/reconcile.ts`. Only the vendored tail reconcile
(`runTail` and its `tailPass`) calls it, and nothing in the function imports `runTail`. On the
base, `search-index.ts` imports only `chNow`, `deleteRows` and `windowStart` from that file.
This change adds `readState` and two constants, none of which write.

So the INSERT grant cannot be assumed. It is a prerequisite to the flag.

1. **Check first (read only).** An admin runs:
   - `SHOW GRANTS FOR <pipeline user>`
   - `SELECT * FROM system.grants WHERE user_name = '<pipeline user>' AND (table = 'search_ingest_state' OR table IS NULL)`

   A row with `access_type = 'INSERT'` on `<database>.search_ingest_state`, or on the whole
   database, means the grant is already there. Grants through a role show under the role's
   name; check `system.role_grants` for the user's roles.
2. **Grant, if missing:**
   `GRANT INSERT ON <database>.search_ingest_state TO <pipeline user>`
3. **SELECT is already needed.** The stage reads `window_start` from this table on every
   pass. With the flag on it also reads the stamp. Both are SELECTs on the same table.
4. **Who does it.** The grant is a ClickHouse admin step. It needs the owner's own approval.
   It is not part of any deploy of this code.
5. **Flag on without the grant.** Each stamp or tombstone INSERT is refused. The refusal is
   caught. The pass returns exactly what it would return with the flag off, nothing is
   thrown, and one warning line is logged per pass (`acceptance_stamp_write_failed` or
   `acceptance_stamp_clear_failed`). No stamp is ever stored, so every pass reads in full and
   no skip ever happens. The test "ClickHouse refuses every insert into the state table"
   covers this.

### Readers

- Every reader of `search_ingest_state` in this repository asks for one exact name:
  `readState` (`reconcile.ts:91`), called by `windowStart` (:117), the tail lease (:656,
  :680), the watermark (:708), the revision floor (:713) and the cursor (:721), and by the
  stage itself for `window_start`. The new `readAcceptanceStamp` also asks for one exact
  name. None of the fixed names starts with `acceptance_skip/` (tested).
- The read API's partial check reads `search_ingest_session_state`, not this table
  (`src/session-read-api/queries.ts:30`, used at :105 and :124).
- Readers outside this repository must be confirmed absent before the flag goes on. If one
  lists the table by prefix or without a name filter, it must ignore `acceptance_skip/`
  names.

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
- **Clean up (optional).** To remove leftover rows, run the purge statement under "Before
  turning it on". Turning the flag back on later is safe either way, because stamps older
  than six hours never match.

## Limits

- **It is not a cure.** It saves only the repeated reads. The scope stays unindexed past the
  refusal until a writer supplies `accepted_seq` or the contending manifests change.
- **It helps only when repeats arrive with an unchanged pointer set.** Any pointer event that
  changes a pointer fact runs a full pass.
- **Whole-read caveat.** A skip does not re-insert rows that ClickHouse lost since the
  stamped pass, and does not refresh the session state row, so `synced_at` in
  `search_ingest_session_state` does not move. A skip never refreshes the stamp. So a normal
  pass runs at least once per six hours only while the scope keeps receiving events. A scope
  that receives no more events gets no more passes, skipped or not. That bounds this caveat.
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
- every insert into the state table refused: flag-off results, one warning per pass, every
  pass reads in full;
- clear on other outcomes and on erase, and no tombstone on erase with no stamp;
- a tombstone written on a slower clock than its stamp;
- the consumer's acknowledgement and log line;
- the env parse and wiring.

The golden transcript (`search-index-golden.test.ts`) is unchanged.

## `contender_seats` on the pass lines

A pass skipped before read carries no `contender_seats`, because it read nothing. A pass
that read and ended with `relayhistory_acceptance_missing` always carries it: one of
`all_unseated`, `seated_unseated` or `both` for the refused record id groups (the
catalog-key fold may also have refused), or `catalog_key` when only the catalog-key fold
in `projectRelayhistorySession` refused. The stamp format, the digest and the skip logic
are unchanged. The words are defined in `docs/handoffs/stream-backport-build.md`.
