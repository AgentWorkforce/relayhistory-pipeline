# Session-directory parity re-projection plan (not executed)

This is an operator plan, not an authorization to run it. Applying the schema,
writing Supabase or Neon, changing rollout markers, switching LIST traffic, or
deploying services requires Khaliq's separate explicit approval. Keep
relayhistory-cloud on Neon or Supabase shadow mode throughout the rebuild.

## Goal and invariants

Rebuild the derived Supabase session directory so it has trusted user IDs,
source RelayHistory session and machine IDs (including Kimi IDs containing
characters encoded in S3 paths), and project/catalog metadata. S3 remains the
content source of truth; Neon is a read-only fallback for identities or catalog
rows that predate the S3 metadata.

The run must preserve these invariants:

- never manufacture `unknown` as a user membership;
- never use unvalidated manifest metadata to redirect a physical pointer;
- never expose local paths as project keys or project IDs;
- never leave both a physical-ID and canonical-ID directory row live;
- never mark either directory rollout complete until the full verification and
  shadow window pass;
- use bounded, restartable batches and retain a recoverable snapshot.

## Preconditions and dry run

1. Deploy the code and additive canonical-ID migration only after approval, but
   leave list reads on Neon/shadow. Confirm the migration ledger, function ACLs,
   RLS, and Worker-role validation are green.
2. Pause directory projectors for the bounded rebuild window, or route their
   queue to a held/replayable state. Do not pause canonical S3 writes.
3. Take a recoverable snapshot of `session_index`, `session_index_ext`, the
   directory row, all directory companion tables, resurrection fences, and both
   rollout-marker tables. Record row counts and a snapshot identifier outside
   the database.
4. Produce a read-only mapping report keyed by the physical pointer primary key.
   For every live and tombstoned pointer, report physical and candidate canonical
   session/machine IDs, identity source, user source, and conflicts. A source ID
   is accepted only if the shared layout encoding recreates the physical ID.
5. Fail the dry run on a canonical key collision, divergent canonical IDs within
   one physical session scope, invalid/control-character IDs, more than 64 live
   machines, or conflicting trusted users. Resolve these rows explicitly; do not
   choose a winner automatically.

Trusted user precedence is authenticated `session_index_ext.user_id`, then a
validated migration export binding (`client.relayhistory_export.source_user_id`
equal to `96bulls.user_id`). For old sessions lacking both, use a read-only Neon
owner lookup. Leave the directory user set empty when no trusted source exists.

Canonical IDs come from validated `96bulls.canonical_*` fields, then the export
marker's `neon_session_id` / `neon_machine_id`. Older exports have
`neon_session_id` but may need a read-only Neon machine lookup. This explicit
fallback is required for older Kimi rows; do not infer an original colon from an
underscore.

## Bounded rebuild

Process stable `(org_id, workspace_id, harness, physical_session_id)` keyset
batches. Commit each logical session atomically and persist an external
checkpoint plus counts.

For each conflict-free logical session:

1. Populate the pointer's canonical columns and replace `unknown` only when a
   trusted real user was found. Fence updates to the exact current manifest key
   so a newer pointer cannot be overwritten.
2. Read every live manifest and verified raw chunk in the physical S3 scope.
   Normalize and project the complete record set. If anything is missing,
   unscrubbed, conflicting, unreadable, or over limit, record the reason and make
   no directory/catalog change.
3. Build project ID, safe project key/method, repository, branch, agents, models,
   and relationships from the same relayhistory projection. Use read-only Neon
   catalog rows only for sessions with no complete normalized S3 history.
4. In one transaction, lock the affected pointer and directory keys, delete the
   legacy physical-ID directory row and its cascade-owned companions, project the
   canonical directory row, then write its catalog. This order works with the
   projector's compatibility guard and prevents two live IDs.
5. Re-read the canonical row and companions before commit. Require exact machine
   and trusted-user sets, one row per canonical key, and a manifest pointer for
   every live machine.

Rate-limit batches by database latency, lock wait, queue age, and error rate.
Stop on any collision, unexpected deletion, dead-letter growth, sustained lock
pressure, or mismatch-rate increase. Resume only from the last committed
checkpoint.

## Verification and activation gate

After replaying held projector work, produce tenant- and harness-sliced reports:

- no duplicate physical/canonical logical sessions;
- no `unknown` directory user or user-companion row;
- every canonical directory machine maps to exactly one live physical pointer;
- Kimi and non-Kimi session IDs match the trusted source exactly;
- project ID/key/repository/branch and user filters match Neon for at least 95%
  of page-1 sessions, with every remaining mismatch classified;
- catalog, relationship, tombstone, fence, and erasure negative tests pass;
- pagination has no repeats, skips, or cursor loops;
- queue/DLQ and query-plan/SLO gates stay green.

Run Supabase list traffic in shadow for at least 48 hours, comparing keys,
ordering, filters, and hydrated fields with Neon. Only after all gates pass may
an approved operator set the catalog and directory rollout markers and change
`LIST_BACKEND`.

## Rollback

Before activation, stop projectors, restore the snapshot tables in a new
transaction or run the recorded inverse batch, replay held queue work, and keep
LIST on Neon. After activation, first return LIST to Neon, clear no rollout
marker destructively, and preserve mismatch evidence. Restore the derived tables
from the snapshot, validate counts and ACLs, then replay all pointer events since
the snapshot. Never rewrite shared Git history or mutate Neon during rollback.
