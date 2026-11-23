# Skip the sessionless per-origin bucket in the pointer stage

Branch `review/layered-sessions-pipeline-origin-bucket-skip-246a`, on top of
`247d9994`. A backport, adapted to this lineage, of main's `0b355260`. Code, tests
and docs only. Nothing here deploys anything, and nothing enables the flag.

## The flag

`SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED`, read once at Lambda init (`lambdaEnv`
through `pointerSkipOriginBucketFromEnv` in `src/session-pipeline/lambda.ts`). Only
`true` and `false` are accepted:

| value | effect |
| --- | --- |
| unset, `""`, `false` | off (the default): the env object and the pointer deps are exactly as without the flag |
| `true` (exactly) | on |
| anything else, including `1`, `TRUE`, ` true`, `yes` | init fails, naming the variable |

A refused value fails Lambda init for the whole function, every stage. It is a
pointer-stage flag, so unlike the search skip flags it does not need
`SEARCH_INDEX_ENABLED=1`. On, init adds `skipOriginBucket: true` to the pointer
stage's deps (`pointerFlagDeps`). It is off by default and enabled nowhere: no stack,
runbook or workflow in this lineage renders it. A variable set by hand on the
function is dropped by a later stack deploy that does not carry it.

## What it does

Records without a session id are written by the ingest writer into a per-origin
bucket whose key carries `h=_none` and `s=_origin` (`NO_SESSION_HARNESS` /
`NO_SESSION_ID` in `src/session-pipeline/pointer.ts`). Without the flag, that
bucket's manifest is indexed as if it were one session.

With the flag on, `indexPointer` returns `{ status: "ignored", reason:
"no_session_origin" }` for such a key right after the `not_a_session_key` check:
before the manifest-name check and before any object-store, org-resolver, lock or
database call. Put and delete events are treated alike.

The match is exact: the parsed harness must be `_none` and the parsed session id
must be `_origin`. `_none` with another session id, `_origin` under another
harness, case variants, and values with a prefix or suffix all take the existing
path.

In `processBatch` (`src/session-pipeline/consumer.ts`) an ignored outcome logs the
existing `object event ignored` line with the existing `reason` field and moves on:
the message is not a batch item failure, is not requeued, and feeds no search pass.
`reason` is an allowlisted log key without a value rule, so no log allowlist change
is needed.

## What it does not do

- Flag off: no new call, outcome, log key or deps field; every key, including
  `h=_none/s=_origin`, takes the existing path.
- Existing `_none/_origin` pointer, directory and search rows are left as they are.
  Their delete events are ignored too, so the flag removes nothing.
- Messages already in a dead-letter queue are not redriven or deleted by it.
- No SQL, no migration, no stack input, no change to `packages/`, the read API or
  the search stage.

## Caveat

The legacy seed proof and history lineage read these rows, so the flag stays off
wherever they still run; enabling it is an operator decision. Enabling is an
operator step outside this change.

## Tests

`src/session-pipeline/__tests__/pointer-skip-origin.test.ts`, pure fakes with
synthetic data: flag-on put and delete with deps that throw when touched; the
exact-match negatives reaching the org lookup; flag off or unset taking the existing
path; `processBatch` with an S3 put, an S3 delete and a reconcile body of the origin
key (no failure, no requeue, no search, the ignored line through the real log path);
the env grammar; the flag without `SEARCH_INDEX_ENABLED`; and the wiring from the
parsed env into the pointer deps (`pointerFlagDeps`, plus a source check that `init`
spreads it, since `init` itself needs AWS).
