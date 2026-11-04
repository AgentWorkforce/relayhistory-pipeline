# Upload fence generation release

Apply the release union in append order: existing main auth migrations 20261104020000/20261104020100, then20261104021000 event metadata,20261104022000 ingest ledger,20261104030000 repair. The formerly unlanded event/ingest files retain their original statement bodies and provenance names in comments. No applied auth migration bytes change. Never adopt a database containing an unreleased old filename by guessing a checksum or marking it applied.

The repair preserves non-fenced committed revisions, removes previously reinserted fenced replay entries, and backfills session ownership. New tables contain metadata only and have RLS with no direct runtime table grants. Existing authenticated binding/role contracts stay unchanged; three internal helpers are owner-only and part of both exact ACL manifests.

Claims register pending session ownership before acknowledgement, including staged batches whose receipt commits no revisions. Reset enumerates that ownership as well as stored revisions. Claim, direct commit, fanout fence/commit, selection and reset use one short SQL row lock keyed by(org,workspace,user); S3 IO and queue publication stay outside those transactions. Different users/workspaces do not share the row lock.

Persistent per-session epochs survive re-enable. Disable increments the addressed epoch; reset increments each discovered session epoch. The append-only upload_resets count also fences a reset that discovered no ownership. Every claimed/resumed attempt receives a server UUID and immutable epoch/reset-count snapshot. A resume never replaces the snapshot used by an older nonce. Lookup uses the nonce primary key plus exact tenant/origin/digest and its original batch relationship.

Direct commits exclude any snapshot-invalid session from the replay index and enqueue erasure rearm. If a session changed generations and is now enabled, the batch remains pending and the Worker returns retryable 503; a new attempt rewrites it before acknowledgement. Unaffected records in a mixed batch retain their normal committed revision. Currently deleted sessions keep the existing acknowledge-as-deleted behavior.

Staged objects keep additive v1 for the transitional Neon replica. Only the opaque claim_token is added to the header; no raw client batch id is newly stored there. The immutable object key also binds the nonce. Upgraded fanout requires that token, validates it before writes even for no-session groups, and uses its original snapshot at commit. Old missing-token headers fail closed. A generation change before fanout skips the old session; a change during object IO leaves its revision absent and rearms erasure. A re-enabled fresh upload can therefore restore actual bytes instead of receiving a false replay_fast.

## Stable fence contract for offline seed capture

After ingest_lock_fences(org,ws,user), compare all scope fields exactly:

- ingest_fence_versions PK(org_id,workspace_id,user_id,origin_id,source,session_id), epoch bigint>0; missing row means 0. Versions are never removed on enable.
- upload_resets is append-only per(org_id,workspace_id,user_id,request_id); count(*)::text for the user scope is a monotonic reset epoch. Replaying the same request id adds nothing.
- Capture both reset count and exact source/session epochs under that lock and compare them under the same lock at seed commit. Keep decimal bigint strings/SQL comparisons exact.
- Known session ownership lives in ingest_session_ownership. Never delete/reset version rows, request rows or claim snapshots to bypass refusal.

Concurrent revision seeding is not an ingest operation. The separate reviewed S3-proven seed prerequisite must replace the old unproven boolean-session seed before activation. This repair changes no revision column or seed RPC.

## Operator prerequisites and rollback

Stop new ingress and drain/reconcile all pre-upgrade pending claims, acknowledged staged receipts and redrives before applying/enabling the new RPC contract. Older receipts never stored the necessary nonce/session ownership, so an empty revision ledger cannot establish that they are safe. Establish that no such receipts exist for a never-enabled dedicated writer. Retain immutable attempts until all associated fanout work is complete.

Migrate transactionally through the existing migrator. For event-trigger boundary mode, refresh the reviewed bootstrap/embedded manifest before migrate; the new owner-only helpers change the pinned body hash. A stale installed boundary refuses validation. No role creation/mutation, credentials, S3 policy, deploy or Worker switch is part of this source change.

Continue the reviewed erasure consumer and rearm handling. These tests prove rearm queueing, not a deployed sweep. Actual eraser consumption, continuous Neon dual-fences where applicable, staged-root erasure/redrive policy and seed proof remain operator gates. Routing new uploads is independent of the historical search/read backfill.

Rollback uses the approved ingress switch after stopping/draining active work. Keep the append migrations, version rows and attempts. Do not restore old function bodies/drop tables while old or new uploads/fanout can run.

## Reproduction

Use only an owned disposable PostgreSQL cluster:

    SESSION_INGEST_TEST_PG_URL=postgres://postgres@127.0.0.1:<owned-port>/postgres \
      bun test src/session-ingest/__tests__/ledger-races.postgres.test.ts

The actual PostgreSQL suite creates/removes its own databases and filesystem stores. It proves real disable/commit and reset/fanout barriers, re-uploaded zstd bytes, immutable same-batch resume snapshots, nonce/scope rejection, mixed-batch preservation, and populated released-main upgrade. That upgrade requires the reviewed 4ddee50 Git ancestor to be available and checks its original migration bytes.

Set REVIEW_RHC_REPLICA_MODULE to the frozen RHC265 at 0c41e7e neon-replica.ts module to additionally prove new-stage rebuild through its actual consumer, v2 rejection and missing-token refusal in upgraded fanout. No RHC source is edited.

The INGEST_RACE_MUTATION choices(without the space) are no_serialization, no_pending_ownership, no_commit_filter, no_epoch_check and no_nonce_binding. Each removes one fence from a disposable SQL copy; the harness refuses a mutation that changes nothing. Run them serially or with a bounded fixture concurrency; no server connection-limit changes are needed.
