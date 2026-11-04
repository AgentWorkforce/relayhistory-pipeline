# Pending upload fencing upgrade

Append migration `20261104030000_ingest_fence_ownership.sql` after the existing ingest ledger. It preserves non-fenced committed revisions, removes previously reinserted fenced replay entries, backfills session ownership, and replaces only ledger decisions. New tables contain tenant/session identifiers, with RLS and no direct runtime table grants. Existing function grants and authenticated binding identity are unchanged.

Claims record non-fenced session ownership before acknowledgement, including staged batches whose receipt commits no revisions. Reset enumerates both committed revisions and that ownership. Disable/reset remove ownership together with revisions. A fresh post-enable claim registers the session again. Source and session remain distinct; records without a session retain the prior non-reset semantics.

Claim, direct commit, fanout revision commit, selection, and reset acquire one short row lock keyed by `(org_id,workspace_id,user_id)` before fence decisions. Object reads/writes and queue publication remain outside those transactions. Direct commits exclude currently fenced revisions and rearm erasure for written sessions; fanout commits retain the same rearm behavior. Different users/workspaces do not share this lock. This serializes SQL decisions, not complete uploads.

## Operator prerequisites

Before applying this migration or enabling the upgraded writer, stop new ingest requests and drain/reconcile any pre-upgrade acknowledged staged receipts and pending claims. Their session membership was not recorded in SQL, so the migration cannot infer it from old empty-revision commits. Verify the pending queue/receipts directly; an empty revision ledger alone is insufficient. With a never-enabled dedicated writer, establish that no pre-upgrade receipts exist. Do not use unproven historical revision seeding as a substitute.

For event-trigger boundary mode, refresh the reviewed boundary bootstrap/embedded ACL manifest before migrate; the new owner-only helper changes the pinned body hash. The exact runtime/helper grants remain unchanged. The updated migrator refuses an older installed boundary body.

The migration and its backfill run transactionally through the existing migrator. No role creation, role mutation, extra credentials, S3 policy or Worker flag change is part of this change. Concurrent revision seeding remains an offline rollout operation, never an ingest-time activity.

Continue the existing erasure consumer and its rearm handling. A reset may finish while object IO is in flight; the late commit deliberately leaves revisions absent and enqueues another sweep. These tests prove queueing, not a deployed eraser's drain. Existing staged-root erasure/redrive policy remains a separate rollout check.

Rollback routing uses the approved ingest switch after stopping/draining active work. Retain the append migration and ownership rows; do not restore old function bodies or drop the new tables while uploads/fanout can still run. Never rewrite the applied original migration.

## Reproduction

Use only an owned disposable PostgreSQL cluster, as a role allowed to create disposable databases and test roles:

```sh
SESSION_INGEST_TEST_PG_URL=postgres://postgres@127.0.0.1:<owned-port>/postgres \
  bun test src/session-ingest/__tests__/ledger-races.postgres.test.ts
```

Each test creates and removes its own database and temporary filesystem object store. The real PostgreSQL suite proves disable/commit and reset/fanout interleavings, actual zstd bytes restored by re-upload, pending/staged reset coverage, tenant isolation, populated-ledger upgrade, and closed helper/table permissions. No SQL lock crosses the object-write barrier.

Set `INGEST_RACE_MUTATION=no_serialization`, `no_pending_ownership`, or `no_commit_filter` to run discriminating controls against a modified copy of the migration in that disposable database. The checked-in SQL is not edited by these controls.
