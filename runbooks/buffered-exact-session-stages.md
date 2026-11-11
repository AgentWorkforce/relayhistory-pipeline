# Exact-session buffered stage lookup (047010)

`get_shared_session` supplies a source and session ID. When canonical pointers are absent,
its owner-buffered detail path locates visible stage objects in Postgres and reads their
content from S3. This is separate from transcript search in ClickHouse.

The 044090 detail function starts from the session-scoped claim index, but evaluates
per-record privacy predicates for every session group in each matching mixed-session
batch. Migration `20261105047010_buffered_exact_session_stages.sql` adds
`sessions.buffered_session_stages_for_target` with the same signature and output. It
resolves whole groups to the requested source/session before the per-record checks.
Stage-wide malformed/duplicate-ordinal validation still covers unrelated groups.
Scope, G2, head, generation, admission, deletion, reset, epoch, keep, native ownership
and ACK checks are retained. The gate-off fallback is unchanged.

This removes unrelated-group privacy work only; it does not cap the number of
requested-session records checked in one RPC. The separate default-off paged follow-up
(050050) owns that target-heavy work bound. An independent synthetic PGlite check with
all 6,939 records in the target session returned the correct 17 rows / 6,939 ordinals
in about 10.1 seconds. The mixed-batch regression asserts correctness/equivalence and
counts only, with no timing budget assertion or production latency guarantee.

For corrupt jsonb, query-plan evaluation can differ between error and empty output:
`target_groups` evaluates record/group resolution before judging, while unrelated epoch
entries may no longer be evaluated. Both paths fail closed; this change does not promise
identical error codes for every corrupt input or make malformed data visible.

The existing function remains callable. There are no new indexes or data writes in
this migration, and list/search behavior is unchanged.

## Source validation

Run from the repository root:

```sh
bun test src/session-ingest/__tests__/exact-session-stages.pglite.test.ts src/session-ingest/__tests__/exact-session-stages-equivalence.pglite.test.ts
bun scripts/sync-sessions-function-boundary.ts --check
bun scripts/check-engine-identifiers.ts
```

The tests install the migration and compare the new function against a frozen 044090
detail body through the real buffered-ingest fixture and read role. They cover reader
gate on/off, multi-session/provider batches, pagination of the unchanged list, other
users/organizations, stale G2, deletion, frozen deletion, publication heads, admission
changes, ACK changes, resets, missing/duplicate/malformed records and groups. A static
reverse-normalization test proves that the new group filter and join are the only body
changes. Migration preflight and read-role-only function ACLs are checked separately.

A disposable local PostgreSQL 15.17 query-core experiment used 17 mixed-session
claims with 170 groups, 6,939 records and 177 target ordinals across 17 target groups,
matching the incident aggregate shape with synthetic values. Results were identical:

| Plan mode | Frozen warm p50 / p95 | Narrowed warm p50 / p95 |
| --- | --- | --- |
| Default | 288 / 312 ms | 41 / 46 ms |
| Forced generic | 289 / 308 ms | 40 / 52 ms |

Each series used one cold and five warm calls. These are synthetic SQL-core timings;
they exclude the entry scope/G2 call, network, S3 and UI. They do not establish the
production slow request's plan or guarantee its latency. The production discriminator
is a bounded before/after plan and one actual normal read after an approved activation.

## Activation and inverse

Code integration does not activate the path. Migration and production flag changes
require the production operator's explicit authorization.

1. Slot 047010 follows the merged 047000 step and precedes the held erase migrations
   050000/050010/050020. It has no source-order dependency on that erase stack. A
   production apply reaching 047010 also applies the already-landed pending prefix:
   045000, 045010, 045030, 045040, 046000, 046010 and 047000. The DB actor must verify
   the CI plan contains exactly that reviewed pending prefix plus 047010, with no held
   erase step; an unexpected plan stops the apply. This source re-slot does not itself
   authorize that production prefix. In event-trigger boundary mode, refresh the reviewed
   `supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql` bootstrap and
   embedded manifest from the same image before migrate; its pinned body changes with
   the new RPC, and a stale installed boundary refuses validation. Follow the existing
   [function-boundary operator procedure](session-directory-search-worker-bootstrap.md#function-boundary).
   Then apply through the canonical migrator. Do not rewrite the landed 044090
   file or replace its function out of band. The new migration refuses an unexpected
   old function body, owner or attributes, and refuses a pre-existing target function.
2. Verify the new function's manifest, read-role ACL and migration sentinel. Keep the
   existing claim-session reader policy and backfill requirements satisfied; when that
   gate is off, the target RPC intentionally uses the frozen fallback.
3. Deploy the reviewed Worker with explicit `owner_buffered=on` and
   `owner_buffered_exact_session=on`, preserving the other reviewed settings. This
   renders `READ_API_OWNER_BUFFERED_EXACT_SESSION=on`. Only the exact string `on`
   selects the new RPC; absent/other runtime values select the existing function.
4. Bind one existing normal read to the actual new Reader version/config, retain its
   result and request correlation, and compare returned content/identity plus native
   status and backend trace. A source test or HTTP 200 alone is not feature success.

Inverse: an explicit deploy with `owner_buffered=on` and
`owner_buffered_exact_session=off` selects the existing function without changing data
or removing the new RPC. A separately authorized canonical rollback can use the generated
`supabase/rollbacks/20261105047010_buffered_exact_session_stages.down.sql` only after
disabling the flag; it requires the exact ledger checksum/body/owner and no later migration.
Default `profile`/`preserve` deploys preserve the live selection;
their `off` input is the historical unset convention, not an explicit reversal. To
change the selection, use explicit owner-buffered mode. No automatic retry through a
second RPC occurs after an error. Do not drop the existing GIN: the frozen fallback
still depends on it.
