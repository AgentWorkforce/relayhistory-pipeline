# Backport flags on main

The hosted session-pipeline Lambda ran a backport line. It had three default-off features that main did not
have. They are ported here as behaviour, not as patches, because main's search stage and consumer have changed.

Each flag keeps the backport's variable name and the backport's accepted values. So an environment that
turned a feature on for the backport turns on the same feature here. Every flag is off by default. With a
flag unset, `""` or `false` (for `SESSION_PIPELINE_RETRY_VISIBILITY_S`: unset, `""` or `0`), the code runs
the same statements, reads, writes and log lines as the base commit 9d1adf84. The golden test
`src/session-pipeline/__tests__/search-index-forward-port-off-golden.pglite.test.ts` checks this against a
transcript recorded at 9d1adf84.

This change contains code, tests and docs only. It deploys nothing, and nothing in it turns a flag on.

## SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED

The backport commits are 1c931eff, 0c4fe2be and 247d9994.

### Grammar

| Value | Result |
| --- | --- |
| unset, `""`, `false` | Off. |
| `true` | On. Needs `SEARCH_INDEX_ENABLED=1`. |
| anything else | Init fails and names the variable. |

### What it does

A pass can end incomplete with exactly one reason, `relayhistory_acceptance_missing`, from the whole read
or the streamed read. It must also have read every pointer as listed: the same revision, content sha256 and
etag as the pointer row. Such a pass writes a stamp `{v: 1, reason, digest}` into `search_ingest_state`
under `acceptance_skip/<sha256 of the scope>`.

A later pass skips when it finds a matching stamp. A stamp matches when it is the latest row, it has the
same digest, and it was written less than six hours away from the pass's time, in either direction. The
skip happens before any manifest read. It returns `complete: false`, `skippedBeforeRead: true` and the
reasons `relayhistory_acceptance_missing, skipped_before_read`. It writes nothing.

Any other outcome writes a tombstone, but only if the pass saw a stamp or could not read it. Erasing the
scope also writes one. When the pass saw a stamp, the tombstone's time is the later of the pass's time and
the stamp's time plus 1 ms.

If the stamp read fails, the pass runs in full (fail open). If a stamp write fails, the pass logs one warn
line, `search index acceptance stamp not written`, and its result is unchanged.

### How main differs from the backport

- **Read set.** Main reads the canonical sibling union of a scope (`readCanonicalScope` over every live row
  with the same canonical session id), so the digest is built from that set. Each row contributes its key,
  session, machine, revision, content sha256, etag and scrubbed flag. The digest also covers:
  - the canonical id and the directory epoch, which the pointer trigger moves on every pointer change;
  - the window marker;
  - the read limits;
  - the stream budgets;
  - `READER_MAX_RULESET`.
- **Salt.** The salt is `acceptance-skip-v2`, so a stamp written by the backport line never matches here.
  The first pass after a deploy reads in full and writes a v2 stamp.
- **Facts listing.** With the flag on, the sibling listing selects each row's facts in the same statement,
  with the same filter and order, so it is still one statement. With the flag off, it is the original
  statement.
- **Read observation.** The pass observes what each manifest read returned through a wrapper around the
  reader. `readCanonicalScope` is unchanged.
- **What a skip leaves out.** On main, an incomplete whole read inserts no rows. It returns before the
  index scan, after `invalidateDirectoryEvents` and a `needs_full = 1` state row. So a skip leaves out only
  those two writes, which the stamped pass already made. The backport's caveat that "a skip does not
  re-insert lost rows" has nothing to apply to on main.
- **Where the skip applies.** Only on the S3-event pass on the hosted legacy ClickHouse binding. The
  function that decides this is `inlineSkipsApply` in `search-index.ts`. The skip does not apply to these
  passes:
  - **The managed search-jobs (M2) pass.** `runSearchJob` sets `committedGuard`, so the M2 pass never
    reads, writes or honours a stamp. Its pickup, lease, `held` (design §4.5), backoff, exhaustion and
    `ready` decisions are byte-identical with the flag on or off. When the M2 flags are off there is no M2
    pass at all. `search-jobs.ts` is not changed.
  - **The staged reconcile, a portable search adapter, and either output-facts store** (with
    `SEARCH_INDEX_OUTPUT_FACTS=1`, the flag does nothing).
- **Pre-read.** With the flag on, `SEARCH_INDEX_PREREAD_ENABLED` passes take the locked base path. Without
  that, the pre-read would read the manifests before the skip could decide.

### Conditions carried over from the backport

- **INSERT grant.** The pipeline's ClickHouse user needs INSERT on `search_ingest_state`. Without it,
  every stamp write logs a warn line, nothing is ever stamped, and every pass reads in full.
- **Bump the salt with code changes.** Bump `ACCEPTANCE_SKIP_VERSION` with any change to the fold, the
  transform or the projection. Otherwise purge the `acceptance_skip/` rows after every instance runs the
  new code.
- **Writers must raise the revision.** Every manifest writer must raise the revision when a manifest's
  content changes. The pointer upsert keeps a row at the same revision.
- **Readers outside this repository.** Any reader that lists `search_ingest_state` by prefix must ignore
  `acceptance_skip/` names.

### Rollback

Unset the flag, or set it to `false`. Leftover stamps are then inert.

## SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED

The backport commits are 14e9c00a and 95776f5b.

### Grammar

The grammar is the same as for the acceptance flag:

| Value | Result |
| --- | --- |
| unset, `""`, `false` | Off. |
| `true` | On. Needs `SEARCH_INDEX_ENABLED=1`. |
| anything else | Init fails and names the variable. |

The two skip flags are independent.

### What it does

The pass checks three conditions before any manifest read. If all three hold, it returns the size refusal
again without reading or writing anything. The result is `partial: true`, `skippedBeforeRead: true` and the
reasons `scope_too_large, skipped_before_read`. If any condition fails, the normal pass runs.

1. **Pointer sizes.** The summed `raw_bytes` of the live, scrubbed sibling pointers is over
   `maxScopeBytes`. The bound is strict.
2. **Stream guard.** With stream budgets (`SEARCH_INDEX_STREAMING_CH=1`), four times that sum is also over
   the stream byte budget, so the streamed path could not take the scope either.
3. **State row.** The scope's latest `search_ingest_session_state` row has `needs_full = 1`,
   `scanned_rows = 0` and the highest live sibling revision. It was written less than six hours away from
   the pass's time. This is one bounded read, made only after 1 and 2 hold. If the read fails, the normal
   pass runs.

The consumer handles the result like a size refusal. It logs one `search index partial` warn line with the
reason `scope_too_large,skipped_before_read` and one `search index reconciled` line. It does not requeue
the message and does not report a batch item failure.

### How main differs from the backport

- **Pointer set.** The sizes, revisions and count come from the same sibling-facts listing the read uses
  (the canonical union). The backport ran a separate aggregate query, so its "live count changed between
  listing and facts" check is not needed here.
- **Order of checks.** The skip runs after skip-current and before the acceptance stamp.
- **Where the skip applies.** The same `inlineSkipsApply` rule applies as for the acceptance flag. The
  managed search-jobs pass never skips. Its over-bound pass stays `held`, exactly as with the flag off.
- **Reasons.** On main, a pass refused by the stream budgets reports
  `scope_too_large, stream_read_bytes_budget`. Its skip reports `scope_too_large, skipped_before_read`, as on
  the backport.
- **Known gap.** A live scrubbed row that the read would still refuse is counted in the sum. Two such
  refusals are a key mismatch and a scrub ruleset above `READER_MAX_RULESET`. The normal pass for such a
  scope is incomplete as well, and it indexes nothing either.
