# Exact-session buffered stage lookup (050030)

`get_shared_session` supplies a source and session ID. When canonical pointers are absent,
its owner-buffered detail path locates visible stage objects in Postgres and reads their
content from S3. This is separate from transcript search in ClickHouse.

The 044090 detail function starts from the session-scoped claim index, but evaluates
per-record privacy predicates for every session group in each matching mixed-session
batch. Migration `20261105050030_buffered_exact_session_stages.sql` adds
`sessions.buffered_session_stages_for_target` with the same signature and output. It
resolves whole groups to the requested source/session before the per-record checks.
Stage-wide malformed/duplicate-ordinal validation still covers unrelated groups.
Scope, G2, head, generation, admission, deletion, reset, epoch, keep, native ownership
and ACK checks are retained. The gate-off fallback is unchanged.

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

A disposable local PostgreSQL 15.17 query-core experiment used four batches with 50
sessions and five records per session (250 records per batch). Results were identical:

| Plan mode | Frozen warm p50 / p95 | Narrowed warm p50 / p95 |
| --- | --- | --- |
| Default | 79 / 101 ms | 18 / 31 ms |
| Forced generic | 72 / 103 ms | 12 / 17 ms |

Each series used one cold and eleven warm calls. These are synthetic SQL-core timings;
they exclude the entry scope/G2 call, network, S3 and UI. They do not establish the
production slow request's plan or guarantee its latency. The production discriminator
is a bounded before/after plan and one actual normal read after an approved activation.

## Activation and inverse

Code integration does not activate the path. Migration and production flag changes
require the production operator's explicit authorization.

1. Land predecessors 050000/050010 (#506) and 050020 (#514) before this assigned 050030
   slot, then apply through the canonical migrator. Do not rewrite the landed 044090
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
`supabase/rollbacks/20261105050030_buffered_exact_session_stages.down.sql` only after
disabling the flag; it requires the exact ledger checksum/body/owner and no later migration.
Default `profile`/`preserve` deploys preserve the live selection;
their `off` input is the historical unset convention, not an explicit reversal. To
change the selection, use explicit owner-buffered mode. No automatic retry through a
second RPC occurs after an error. Do not drop the existing GIN: the frozen fallback
still depends on it.
