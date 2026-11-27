# Slot 062340: per-org erasure history for the 062320 release

Status: code only, on `review/org-erasure-history`. Slot `20261105062340_ingest_org_erasure_history` was confirmed by
layered-sessions-pipeline. Whether it ships in the same window as 062300 and 062320 is not decided yet. Applying it
to any real database needs Khaliq's explicit OK. 062320's bytes are unchanged.

## Why

062320's `bridge_complete` release compared `p_erasures` with a count of the surviving rows of two sources
(`operator_user_erasures`, `session_erasure_fences`). A surviving-row count can't prove that nothing happened in a
window:
- `ingest_apply_selection` deletes `upload_deletions` rows on a re-enable. A deletion and its re-enable inside the
  window leave no row: the "0 -> 1 a millisecond after an empty proof" race layered-sessions-pipeline reproduced.
- An update of an old row into the window keeps the count, and a delete plus an insert cancel out.
- 13 of the 15 erasure sources were not counted at all.

## What it adds

- **`sessions.ingest_org_erasure_history`**: insert-only. One row per INSERT, UPDATE or DELETE of a row of any of
  the 15 sources in `src/hoopdb/org-erasure-history-sources.ts` (the one shared list; a test pins the migration's
  triggers and its `source` CHECK to it). Each row holds `(erased_org, source, op, at, xid)`. An UPDATE that moves a
  row to another org is recorded for both orgs.
- **The recorder**: an AFTER ROW trigger for INSERT, UPDATE and DELETE on every source. It takes 062320's org key
  SHARED, for each org the row names and in key order, and holds it until the writer commits.
- **TRUNCATE refusal**: a BEFORE TRUNCATE trigger on every source refuses (55000) while any hold is not released.
  It takes SHARE on the holds table, so it serializes with an install.
- **`sessions.ingest_org_erasure_history_coverage`**: insert-only. A `started` row is written at apply, after every
  recorder exists, and an `ended` row at the down. Coverage runs from the last `started` row, only while no `ended`
  row follows it.
- **`sessions.ingest_org_erasure_history_bindings`** and **`_bind(org, epoch, cutoff)`**: the trusted FULL cutoff,
  bound once per epoch while sealed.
  - Bind takes (961996,1) SHARED, runs the slot checks, then takes the org key EXCLUSIVE, the same lock order as
    every 062320 lifecycle function.
  - It refuses (55000) a cutoff that coverage doesn't reach (b1's `history_not_covering_full_cutoff`), a cutoff
    after the seal, a second, different cutoff, and an epoch that isn't sealed.
- **062320's `_release` and `_status`, replaced.** For `bridge_complete`, the release also requires, all under the
  EXCLUSIVE key:
  - this epoch's binding
  - coverage of its cutoff
  - every one of the 30 source triggers present and enabled
  - `p_erasures` equal to the org's history rows at or after the cutoff

  Every other line of the release is 062320's. `_status` is STABLE, so it returns the hold, that count,
  `by_source`, `covered`, `recording` and the coverage start from one snapshot.

## Writer participation, per source

Every writer of every source participates through the recorder, whatever its entry, whether it is checked or not,
and for updates and deletes too. Nothing depends on a writer taking the key itself. These are the writer functions
per source, from the migrations at this head:

| Source | Writers (SQL) |
|---|---|
| operator_user_erasures | operator_user_erase_begin, _step, _count, _complete |
| session_erasure_fences | ingest_operator_erase_session |
| session_erasure_receipts, session_erasure_events | ingest_operator_erase_session, _fence, _record_step, _settle |
| session_erasure_runs | ingest_operator_runtime_begin, _authorize, _next, _release |
| session_erasure_scopes | ingest_operator_runtime_begin |
| session_erasure_writes | ingest_operator_runtime_write_begin, _write_failed, _write_finish |
| upload_deletions | ingest_apply_selection, ingest_apply_reset, ingest_lazy_records, ingest_lazy_user, ingest_turn_materialize_fence, operator_user_erase_purge |
| upload_resets | ingest_apply_reset, ingest_lazy_user, ingest_seed_fence_baseline, ingest_seed_fences, operator_user_erase_purge |
| upload_erasure_rearm | ingest_commit_batch, ingest_commit_revisions (admission: refused while held), operator_user_erase_purge |
| relayfacts_erasure_fences | relayfacts_user_erase_v1 |
| session_analysis_purge_fences | purge_session_analysis_for_owned |
| session_analysis_purge_authority | purge_session_analysis_for_owned, session_analysis_purge_grant |
| ingest_frozen_source_deletions, ingest_frozen_source_resets | ingest_privacy_authority_activate, ingest_privacy_authority_apply |

No runtime role holds a table privilege on any source, so every write runs as the owner inside one of these
functions or an owner session.

**Bypasses.** Only the owner can bypass the recorder: `ALTER TABLE ... DISABLE TRIGGER`, or `session_replication_role`,
which needs a superuser. The release checks that all 30 triggers are enabled in origin mode and refuses otherwise.
A superuser session in `replica` mode is outside this model, as it is for every trigger in the schema.

## Lock orders, proven on real PostgreSQL

`org-erasure-history.postgres.test.ts`, on 15, 16 and 17 in the pg-proof lanes, observes each of these from
`pg_locks`:
1. **Writer first:** the release waits for an in-flight write, then refuses, because the write is counted.
2. **Release first:** a write waits for the release, resumes after it, and is recorded after `released_at`.
3. **Row lock:** a writer holding a row lock blocks the release, with no deadlock. A second writer queued on that
   row also resolves.
4. **Down, then reapply:** a new coverage period starts. A write between them is never recorded, and a cutoff
   before the reapply is not covered (bind refuses).

The PGlite suite covers the single-connection semantics: the 0 -> 1 race, a transient row, a same-count
replacement, an update into the window, reassignment, an unrelated org, the bind refusals, a disarmed recorder,
TRUNCATE, immutability and runtime-role access.

## Residuals, stated

- **Timing margin.** `at` is the recorder's clock, not the commit time. A write recorded before the cutoff that
  commits after the FULL snapshot is invisible to both. The FULL margin (cutoff = FULL start minus margin, 15
  minutes in the bridge) must exceed the longest writer transaction, as it must for the window proof itself.
- **No history before apply.** History exists only from the apply, so every wave's FULL must start after the apply
  plus the margin. Bind enforces that.
- **The bridge must bind.** `review/backlog-ops-hold-bridge` refuses every release (`release_watermark_partial`)
  until it binds to this contract: `_bind` after the seal, then the status read (`erasures`, `by_source`, `covered`,
  `recording`) in the same statement as its window proof, and `p_erasures` from that read.

## Retention and erase policy

The three tables hold:
- an org identifier
- a source table name
- an operation
- timestamps and an xid

They hold no workspace, user, session, object key or content. They are kept as inert history with 062320's hold
history (062320 design, section 6) and are immutable: every UPDATE, DELETE and TRUNCATE is refused. A user or
session erasure does not need to touch them, because they name no user or session. Removing an org's rows, for
example on an org deletion, needs a separately reviewed owner procedure. That procedure is an open item for the
erase-policy owner (hard-delete-owner), not part of this slot.

## Rollback

The generated down, `supabase/rollbacks/20261105062340_ingest_org_erasure_history.down.sql`, is checksum-pinned and
owner-run. It:
- refuses while any hold is not released, or while a later slot is applied
- restores 062320's `_release` and `_status` exactly
- drops the 30 triggers and the slot's functions
- writes `ended` to coverage
- asserts that the retained shape and the removal both hold
- resets readiness to pending
- removes its ledger row

062320's down refuses while 062340 is applied, so the order is 062340, then 062320, then 062300.
