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
(047020) owns that target-heavy work bound. An independent synthetic PGlite check with
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

1. Slot 047010 follows merged 047000 and precedes the erase migrations
   050000/050010/050020. Keep this append-only order across all environments; do not
   insert a new file below any already-applied step. First rehearse the canonical plan,
   boundary refresh, apply and rollback against a qualified isolated dev target using
   synthetic data. This source PR is not dev/prod resource or apply authorization.
   Before production, obtain a fresh read-only ledger plan rather than assuming an old
   receipt still names the live tail. Catch up only the actually pending members of the
   separately reviewed landed range 045000–047000; then plan 047010 as a separate step.
   Require the reviewed pending inventory and no unapproved later erase migrations.
   The current all-pending release workflow must not be assumed to provide a cutoff:
   if it cannot express that bounded plan from the pinned source, stop for reviewed
   canonical workflow support. Renumbering alone does not limit an apply.
   In event-trigger boundary mode, refresh the reviewed
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
   Each schema apply changes the PASS generation: pair it with the existing coordinated
   ingest expected-generation rebind and fresh ingest health/readback before the next
   step. Obtain fresh current-generation G2 parity for the trial; a pre-apply G2 row no
   longer qualifies. Use the established operator generation-window procedure and its
   rollback plan, rather than changing the generation guard or reusing old proof.
3. Deploy the reviewed Worker with explicit `owner_buffered=on` and
   `owner_buffered_exact_session=on`, preserving the other reviewed settings. This
   renders `READ_API_OWNER_BUFFERED_EXACT_SESSION=on`. Only the exact string `on`
   selects the new RPC; absent/other runtime values select the existing function.
   Build the render from a fresh live configuration readback and require the exact-session
   flag to be its sole configuration difference, including all allowlists, bindings,
   origins, events, timeout, phase/concurrency and index flags. Explicit owner-buffered
   mode does not inherit every live-only child automatically: copy their reviewed values
   explicitly and refuse any collateral diff. Apply the same guard when reversing the flag.
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

## Paged stages (047020)

Migration `20261105047020_buffered_session_stages_paged.sql` adds
`sessions.buffered_session_stages_for_target_paged`: the 047010 arguments plus
`p_cursor jsonb DEFAULT NULL` and `p_page_claims integer DEFAULT 2`. It returns exactly one
envelope row per call: `stages` (a jsonb array of the rows
`buffered_session_stages_for_target` returns, same column names as keys and the same
order), `cursor` (NULL when exhausted), `has_more` and `snapshot_at`. 044090 and 047010
are unchanged and stay callable.

**What it bounds.** Each call expands at most `p_page_claims` candidate claims of the
requested source/session. The first CTE after `ctl` is a MATERIALIZED `page` that reads
the session's side rows through `ingest_claim_sessions_session_idx` and selects at most
`p_page_claims + 1` claims in keyset order (batch `committed_at`, `batch_id`, `stage_key`,
`stage_token`, all `COLLATE "C"`), strictly after the cursor. There is no OFFSET. Only the
first `p_page_claims` claims are expanded (records/epochs jsonb and every 047010 privacy
predicate). The extra claim only sets `has_more`. `p_page_claims` is clamped to 1..4 in
SQL, so out-of-range values are clamped rather than rejected.

**Page semantics.** The first page (`p_cursor` NULL) records `snapshot_at =
clock_timestamp()`. The returned cursor carries that snapshot. Later pages admit only
claims whose batch committed at or before it. The cursor is the position of the last
claim the page inspected, whatever the privacy predicates hid, so a run of fully
filtered pages still advances and ends. Concatenated pages equal the 047010 result in
the same order. A malformed or future cursor is rejected with 22023. Scope, 22023, the
control-head gate and the v2 reader gate are identical to 047010:
- With the v2 gate off, the first page returns 047010's fallback result in one envelope.
- A closed gate on a later page raises 55000, so a truncated traversal can never look
  complete.

**Worker.** `READ_API_OWNER_BUFFERED_STAGES_PAGED` is default off, and only the exact
string `on` enables it. It takes effect only on the owner-buffered path with
`READ_API_OWNER_BUFFERED_EXACT_SESSION=on`. Otherwise the reader uses the 047010 or 044090
function exactly as before, and the deploy render refuses `on` without
`owner_buffered=on` and `owner_buffered_exact_session=on`.

When the flag is on, every stage read is a fresh complete traversal from cursor NULL with
a new snapshot. This covers the first read, the not-found refresh and the recheck. The
Worker:
- asks for 2 claims per page (`BUFFERED_PAGED_STAGES_PAGE_CLAIMS`);
- concatenates `stages` across pages;
- uses the existing single-RPC statement budget for the whole traversal, setting each
  page's `statement_timeout` to whatever remains of it.

The traversal fails closed, never returning a partial stage set, on any of these:
- a page error;
- a malformed envelope;
- snapshot drift between pages;
- a repeated stage;
- a cursor that did not advance;
- more than 64 pages;
- deadline overrun;
- request abort.

The outcome matches the single RPC's failures:
- On the first read, it is answered as main (unverified).
- After a candidate was found, it is a 503 `buffered_fenced`.
- If the recheck's fresh traversal differs from the stages read, the existing changed
  outcome applies: the result is hidden and answered as main.

**Validation.** Run from the repository root:

```sh
bun test --timeout 600000 ./src/session-ingest/__tests__/stages-paged.pglite.test.ts
bun test --timeout 600000 ./src/session-read-api/__tests__/owner-buffered-stages-paged.test.ts
bun scripts/sync-sessions-function-boundary.ts --check
```

**Activation and inverse.** Code integration does not activate anything. Applying the
migration and changing the production flag each require the production operator's
explicit authorization.

1. Apply 047020 after the exact-session reader 047010 (it sorts directly before 047020)
   through the canonical migrator, refreshing the function boundary bootstrap first as for
   047010. The migration refuses unless the exact owned 047010 reader function is present and
   no paged function exists.
2. Verify the new function's manifest entry, read-role ACL and migration sentinel.
3. Deploy with explicit `owner_buffered=on`, `owner_buffered_exact_session=on` and
   `owner_buffered_stages_paged=on`. This renders `READ_API_OWNER_BUFFERED_STAGES_PAGED=on`.
   `profile`/`preserve` deploys keep the live value and refuse an explicit `on`.

Inverse: an explicit deploy with `owner_buffered_stages_paged=off` returns to the 047010
single RPC without touching data. A separately authorized rollback can then use
`supabase/rollbacks/20261105047020_buffered_session_stages_paged.down.sql`, but only after
the flag is off. It drops only the paged function and its own ledger row.
