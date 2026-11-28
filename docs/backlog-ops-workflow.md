# Backlog ops workflow (0005 backfill, export and drain)

`.github/workflows/backlog-ops.yml` is the only way the 0005 backlog tools run against production. Production
operations do not run from a laptop or a host. The tools are:

- the fence backfill, `scripts/upload-fence-backfill.ts`;
- the accepted-stage export, `scripts/accepted-stage-export.ts`;
- the S3 drain, `scripts/buffered-drain.ts`.

See [decision 0005](decisions/0005-s3-native-upload-ledger.md), "The buffered backlog". The contract is in
`src/session-ingest/backlog-ops.ts`, the gate in `scripts/backlog-ops-github.ts`, and the credential step in
`scripts/backlog-ops.ts`.

## Modes and order

| Mode | Writes | Binds (typed `confirmation`) |
| --- | --- | --- |
| `backfill-dry` | nothing | none |
| `backfill-execute` | fences, owner stubs, revision shards | `DRY <backfill-dry run>` |
| `backfill-refresh-dry` | nothing (fences and ownership sections only) | none |
| `backfill-refresh-execute` | fences, owner stubs (no revisions section) | `DRY <backfill-refresh-dry run>` |
| `backfill-catchup-dry` | nothing (all three sections, revisions since t) | `full_backfill_run` (no confirmation) |
| `backfill-catchup-execute` | fences, owner stubs, the revision shards of rows updated since t | `DRY <backfill-catchup-dry run> FULL <backfill-execute run>` |
| `export-dry` | nothing | none |
| `export-execute` | accepted sidecars, per-org completion | `DRY <export-dry run> BACKFILL <backfill-execute run>` |
| `drain-dry` | nothing | none |
| `drain-execute` | canonical layout, index, re-arm and drained markers | `DRY <drain-dry run> BACKFILL <backfill-execute run> EXPORT <export-execute run>`, or with a refresh `DRY <drain-dry run> BACKFILL <backfill-refresh-execute run> FULL <backfill-execute run> EXPORT <export-execute run>` |
| `presence-dry` | nothing (dry only; there is no `presence-execute`) | none |
| `marker-dry` | nothing (per org: the 062320 hold, attestations, sweeper, bound evidence; the wave id) | `AUTHORITY`, `BACKFILL`, `FULL`, `RESHARD` (no confirmation) |
| `marker-execute` (held) | the per-org fence-authority markers, under each org's enforced hold | `DRY <marker-dry> BACKFILL .. FULL .. RESHARD .. AUTHORITY .. WAVE <wave_id>` |

The confirmation is exactly `<MODE uppercase> <sha> DRY <id> [BACKFILL <id>] [FULL <id>] [EXPORT <id>]`, for example
`DRAIN-EXECUTE <sha> DRY 111 BACKFILL 222 FULL 444 EXPORT 333`, `BACKFILL-REFRESH-EXECUTE <sha> DRY 555` or
`BACKFILL-CATCHUP-EXECUTE <sha> DRY 666 FULL 444`. `FULL` is the `full_backfill_run` input: optional on
`drain-execute`, required on both catch-up modes, refused on every other mode.

`presence-dry` is outside the sequence: a diagnostic for one session, described in
[Session presence (presence-dry)](#session-presence-presence-dry).

Run them in this order, all at the same `main` commit, after the scope's Worker HTTP admission stop:

1. `backfill-dry`
2. `backfill-execute` (the FULL run; about 3-5 hours scoped)
3. `export-dry`
4. `drain-dry`
5. `backfill-refresh-dry`
6. `backfill-refresh-execute` (the refresh: fences and ownership only)
7. `export-execute`, binding the FULL `backfill-execute` as `BACKFILL`
8. `drain-execute` with `DRY` the drain-dry, `BACKFILL` the refresh, `FULL` the full run, `EXPORT` step 7

The drain needs a fresh backfill, because a control (a deletion or reset) made after a backfill is not in the fence.
A full `backfill-execute` now takes longer than the drain's 3 hour window (its revisions section reads and
compare-and-swaps millions of shards; even a no-op rerun reads them all), so the drain binds a fresh refresh beside
the full run it refreshes. Without `FULL`, the drain's rule is unchanged: its `BACKFILL` must be a full
`backfill-execute` under 3 hours old with every section complete.

### The two-phase cutover: FULL while live, then a catch-up

The refresh order above needs the scope stopped for the whole FULL run (hours). The catch-up keeps the stop window
short: the FULL run happens while the org still uploads through SQL, and only a short catch-up runs after the stop.
Per org (scope `BACKLOG_OPS_ORGS` to it), at one `main` commit:

1. **Phase A, while the org is live:** `backfill-dry`, then `backfill-execute` (the FULL run, hours).
2. **Stop:** stop the org's Worker HTTP admission (`INGEST_HTTP_ADMISSION_STOPPED_ORGS`), verify it took effect
   (`http_admission_stopped` log lines, no new SQL admission for the org) and wait until the Worker's admission queue
   has drained. All within 24 hours of the FULL run's dispatch.
3. **Phase B, the catch-up:** `backfill-catchup-dry` and `backfill-catchup-execute`, both with `full_backfill_run` the
   phase A run. They run all three sections: fences and ownership in full (as a refresh), the owned lazy slots in full,
   and the `ingest_record_revisions` rows updated at or after t only.
4. `export-dry`, `drain-dry`, `export-execute` (binding the FULL run as `BACKFILL`; created after the catch-up), then
   `drain-execute` with `BACKFILL` the catch-up, `FULL` the phase A run, `EXPORT` the export.
5. The fence-authority marker (decision 0005, rollout step 4), as before.

**t** is the FULL run's `created_at` minus `BACKLOG_OPS_CATCHUP_MARGIN_MS` (15 minutes), computed by the gate from
the GitHub API, never an input. The gate writes it as the recheck step's output `revisions_since_ms`, which the
workflow hands only to the credential step (`BACKLOG_OPS_REVISIONS_SINCE_MS`), and the tool gets it as
`--revisions-since-ms`. The FULL run's tool starts after its `created_at` (gate, approval), so t is before its first
read; the margin also covers clock skew between GitHub and the database.

**Why it converges.** Every revision upsert stamps `updated_at` (the insert default `now()`; every `ON CONFLICT DO
UPDATE` sets `now()` or `clock_timestamp()`). A revision row updated at or after t is re-applied by the catch-up; a
row last updated before t existed when the FULL run began, so the FULL run read it as it still is. Fences, ownership
and the owned lazy slots (which have no custody timestamp: `initialized_at` is the insert time, and a custody move
rewrites `current_*` in place) are re-read in full after the stop, which also covers slots the FULL run's block
windows missed while the org was live. Every write is monotone (fence changes are idempotent, owner stubs keep their
oldest generation of the current epoch, revision merges never lower and drop what a fence has forgotten), so FULL plus
the catch-up leave the same ledger, as readers see it, as one full run after the stop
(`upload-fence-backfill.pglite.test.ts`, "catch-up").

**Cost.** No index serves `updated_at` (the primary key and the session index both lead with the org), so the catch-up
still walks the scope's `ingest_record_revisions` primary key in keyset windows of `--row-page-size` rows (each page
returns its matching rows and its last key, so a window with no match still advances); only the S3 work (fence reads
and shard compare-and-swaps) shrinks to the changed rows. With the `slots_read` input set to `org` (opt-in, any backfill
mode, default off) a scoped run reads the owned slots once per org through the `org_id` index (one short read-only transaction with a server-side cursor fetched to the end before any S3
work; measured about 6 s for an org with ~270k slots on a 48 GB heap), and their merges of what is already stored read
each shard once and write nothing. Expect seconds to minutes of SQL for a wave-sized scope plus the S3 writes of the
changes.

**Receipts and bindings.** The catch-up's receipt must show all three sections `complete`, `failed` 0 in each, and
`revisions_since_ms` (a number) equal to the gate's t; a catch-up execute's DRY must have recorded the same t, so
both followed the same FULL. FULL must be a successful `backfill-execute` at the same commit, under 24 hours old,
with exactly the catch-up's scope (never an unscoped run), created before the catch-up's DRY. A backfill receipt that
is not a catch-up must not carry `revisions_since_ms`, so a since-filtered run never stands in for a full one.

**The drain.** A drain with `FULL` binds a catch-up execute as its `BACKFILL` by the same rules as a refresh (below):
under 3 hours old, created after `FULL`, exactly the drain's scope, the export created after it. The run-name and
the receipt's `mode` say which of the two it is. A catch-up gets no fingerprint comparison with `FULL`: the two differ
by design (the org uploaded during the FULL run), and the catch-up re-read everything that could have changed.
Instead its `revisions_since_ms` must equal t computed from the drain's `FULL`, so it followed that run.

**Residual risks.** A revision upsert stamped by a transaction that began more than the margin before the FULL run's
dispatch and committed after the FULL run read that row's window would be missed by both runs; ingest transactions
are short (bounded by their leases), far below 15 minutes. An SQL writer that still moves a row after the stop (the
catch-up reads after the stop and is not re-checked by a fingerprint) is the same assumption every full run makes.

### The refresh and its staleness rule

With `FULL`, the gate requires:

| Bound run | Mode | Age | Order | Receipt |
| --- | --- | --- | --- | --- |
| `BACKFILL` | `backfill-refresh-execute` | under 3 h | after `FULL` | `fences` and `ownership` complete, `revisions` `not_run` |
| `FULL` | `backfill-execute` | under 24 h | before `BACKFILL` | every section complete |
| `EXPORT` | `export-execute` | under 24 h | after `BACKFILL` (the export records the fence views it read) | as for every export |

The refresh has its own modes, so its run-name (`backlog ops backfill-refresh-execute <sha>`) and its receipt's
`mode` both differ from a full run's. It can never satisfy a full-run binding (`export-execute`'s `BACKFILL`, the
drain's `FULL`, or the drain's `BACKFILL` without `FULL`), and a full run can never satisfy a refresh binding.
`revisions` `not_run` is accepted only in a refresh mode's receipt; every other backfill receipt needs all three
sections complete.

**Why a refresh is enough.** After the scope's Worker HTTP admission stop, no new upload reaches SQL for those orgs,
so nothing should write a revision, an ownership row or an owned lazy slot for them (admission, and the SQL publish
and seed paths that follow from what it admitted, write them), and the full run's revision shards and owner stubs stay
valid for its 24 hour binding window. If any writer still moves one of those rows, the fingerprint below changes and
the drain refuses. Only the controls can still move (deletions and resets), and they reach the drain only through the
fences, which the refresh re-runs together with ownership. **But a control is not invisible to the fingerprint:**
`ingest_apply_selection` and `ingest_apply_reset` also DELETE the affected rows from `ingest_record_revisions` and
`ingest_session_ownership` (and clear slots). So a deletion or reset in scope between FULL and the refresh changes the
fingerprint and the drain refuses `backlog_ops_backfill_stale`: run a new full backfill-execute (and a new export after
the refresh) before draining. Fail-safe, and the operator should expect it.

**The fingerprint proves the rest is unchanged.** Every backfill run (dry and execute, full and refresh) first reads,
in one `REPEATABLE READ READ ONLY` transaction, an input `fingerprint` of its scope, all non-negative safe integers:

| Field | Value |
| --- | --- |
| `revisions_count` | `ingest_record_revisions` rows |
| `revisions_max_updated_ms` | the latest `updated_at` of those rows, epoch milliseconds (every revision upsert sets it); 0 when none |
| `revisions_hash` | sum of `hashtext` over each revision row's content (ids, revision, revision id, digest, owner, source, session), normalised into [0, 2^40); catches a rewrite that leaves the count and the latest `updated_at` unchanged |
| `ownership_count`, `ownership_hash` | `ingest_session_ownership` rows, and the sum of `hashtext` of each row's key, in [0, 2^40) |
| `slots_owned_count`, `slots_revision_sum` | owned `ingest_lazy_native_slots` rows, and the sum of `current_revision` modulo 2^53 |
| `slots_hash` | the sum of `hashtext` of each owned slot's key and current custody (owner, source, session, revision, digest; a NULL column hashed as a fixed sentinel), in [0, 2^40) |

Before any credential is read, the credential step requires `FULL`'s and the refresh's fingerprints to be equal, field
by field, over the same org scope. A difference, or either receipt without a fingerprint (for example a full run from
before this field), refuses the drain with `backlog_ops_backfill_stale`: run a new full `backfill-execute`. The
counts catch an insert or delete, the max `updated_at` an upsert, and the hashes a row swapped or rewritten at an
equal count. `hashtext` is a server-internal hash, stable within one server but not a cross-version contract: the
fingerprint is compared only between two receipts at the same commit from the same database, under 24 hours apart,
and is never stored outside the receipts. The sums wrap at their modulus, so a change of an exact multiple of it is
not seen.

To drain in slices, give `max_objects` and rerun `drain-execute`. Each rerun skips what earlier runs wrote, using
the create-only `drained` markers. The workflow never passes `--checkpoint-file` or `--start-after`.

## Org scope

A run can be limited to some orgs with the environment **secret** `BACKLOG_OPS_ORGS`, set on both
`session-schema-production` (dry runs) and `session-backlog-execute` (executes): comma-separated lowercase org UUIDs,
no spaces, at most 64. Unset or empty runs every org, as before. It is a secret rather than a variable so this public
repository's logs mask the ids; the receipt records only the scope's id (`all`, or `orgs_` and 32 hex of a sha256 of
the sorted list).

- The workflow passes each org to the tool as `--org`. The tools' own rules are unchanged: the drain still writes
  nothing for an org without its fence-authority marker.
- An execute refuses unless its dry run has the same scope. A bound backfill or export must have the same scope or
  be unscoped, **except the drain's BACKFILL and FULL, which must have exactly the drain's scope**: an unscoped
  backfill (for example an all-orgs pre-warm) says nothing about a scoped org's admission stop or freshness, so it
  can never satisfy a scoped drain. A receipt from before scoping counts as unscoped. So change the secret only between complete
  sequences, and set it on both environments to the same value.
- An unscoped backfill reads `ingest_lazy_native_slots` in heap block windows (one sequential pass) instead of walking
  the primary key, which fetched every slot at random. With `slots_read=org` (opt-in, default off) a scoped one reads each
  org once through the `org_id` index (`SLOTS_ORG_CURSOR`); an org above `SLOTS_ORG_READ_MAX_ROWS` (4M slots, a memory guard: buffered rows times ~350 B) falls back to the windows. A slot rewritten during the pass can be
  missed, so run a scoped backfill-execute after the scope's HTTP admission stop (decision 0005's cutover order).
- **Pacing and the slots fingerprint (`slots_read=org`).** A scoped `slots_read=org` run reads each org's owned slots
  once: the run-start fingerprint transaction then reads only the revision and ownership values, and the three slots
  values (`slots_owned_count`, `slots_revision_sum`, `slots_hash`) are summed from the per-org cursor rows (the server
  computes each row's `hashtext`; the tool sums and normalises exactly as `FINGERPRINT_SLOTS`, so the values equal
  what that query gives over the same rows). **Snapshot caveat:** those three values come from each org's own cursor
  snapshot, taken later than (and per org apart from) the one run-start snapshot of the revision and ownership values;
  they describe exactly the slots the run processed. The drain's FULL-versus-refresh equality keeps its meaning (an
  owned slot changed between the two runs' reads still moves a value), and a FULL and a refresh may differ in
  `slots_read`: over unchanged rows both methods give equal values. An org above
  `SLOTS_ORG_READ_MAX_ROWS` still has its cursor read to the end for its fingerprint (rows dropped, not buffered)
  before its rows are processed through the windows. A fences-only CLI run (no slot pass) keeps `FINGERPRINT_SLOTS`.
  The optional `slot_pace_ms` input (backfill modes with `slots_read=org` only; digits, 0 to 5000; empty or 0 is
  unpaced and the argv unchanged) passes `--slot-pace-ms`: the tool sleeps that long between the cursor's fetches,
  which shrink from 5000 to 2000 rows when paced (`--slot-fetch-rows`, 100 to 50000, CLI only), so a multi-million
  slot org is read as a stream of small index bursts instead of one IO-saturating sweep. Rows are still buffered to
  the end and processed after the transaction closes (S3 work inside it would stretch the snapshot further). A paced
  read holds its read-only transaction open longer by design, so it is bounded: an org whose paced read would pass
  30 minutes (`SLOTS_ORG_PACED_READ_MAX_MS`) is rolled back and fails closed (`failed_codes.slot_read_too_long`,
  ownership and revisions incomplete, so the receipt binds nothing; it is never retried through the heap windows).
  Choose the pace so the largest org fits: about 2.9M slots at 2000 rows a fetch is ~1450 fetches, so 500 ms is
  ~12 min of pauses plus the reads. Unpaced reads are not bounded by it. The 30-minute bound is per org; the job
  (`backlog-ops.yml`, `timeout-minutes: 355`) must fit the SUM over the scope's orgs of each paced read plus the
  revisions pass and the S3 work, so size the pace for the whole scope, not just the largest org (six orgs of ~2.9M
  slots at 500 ms is over an hour of pauses alone).
- An unscoped backfill run while orgs still admit (a pre-warm) certifies no org that was not stopped before it
  began. Each org's cutover takes its own scoped backfill-execute after its stop.
- A scoped drain-execute WITHOUT `full_backfill_run` binds its backfill with the 3 h freshness check only: no
  fingerprint and no proof that the backfill began after the stop. An unscoped backfill is accepted there too. Keeping
  that safe is the operator's order: bind only a backfill that began after the scope's stop. A follow-up gate change
  will require the drain's BACKFILL and FULL receipts to carry exactly the drain's scope.

## Path B: no-head parity (per org, default off)

Decision 0005 step 0b, "Path B". While Neon is the privacy authority, the export cannot prove that a user has no
control head. That user's buffered stages then stay unpublished (`head_unproven`). For orgs on an allow-list only,
Path B accepts the SQL reader's own no-head proof without the authority:
- the user has no control receipt on any epoch;
- a `no_head` `owner_buffered_parity` row that still matches the digests recomputed at the verdict;
- that row is at most the maximum age old.

A row older than that is `head_unproven_parity_stale`, and a fenced authority still wins (`privacy_fenced`). Each
stage published this way gets a proof record under `.drain/accepted-proofs/v1/` (execute only); its sidecar and
the drain are unchanged.

**Configuration (never a dispatch input: run inputs and logs of this public repository are public).**
- `BACKLOG_OPS_NO_HEAD_PARITY_ORGS`: an environment **secret** in the same format as `BACKLOG_OPS_ORGS`
  (comma-separated lowercase org UUIDs). Every org in it must also be in the run's `BACKLOG_OPS_ORGS` scope, so a
  scoped run is required (`backlog_ops_no_head_parity_scope`). Set it identically on both environments, or on
  neither.
- `BACKLOG_OPS_NO_HEAD_PARITY_MAX_AGE_SECONDS`: an environment **variable** of digits, 60 to 21600 (6 h). It is
  required exactly when the list is set (`backlog_ops_no_head_parity_max_age`). Set it identically on both
  environments.
- The workflow passes both values to the tool step for export and drain modes only. Every other mode refuses
  either one (`backlog_ops_no_head_parity_unexpected`).
- The export receives the list only on its stdin pipe, never in argv or a log, and each org is masked. It receives
  the age as `--no-head-parity-max-age`.
- Unset, every argv, stdin payload, summary and receipt is byte-identical to before.
- The receipt records `no_head_parity: nhp_<32 hex>`, a hash of the sorted list and the age, and only when the list
  is set.
- Binding is refusal-only:
  - an `export-execute` refuses unless its DRY recorded the same id
    (`backlog_ops_bound_dry_no_head_parity`);
  - a `drain-execute` refuses unless the EXPORT it binds recorded the id of its own environment's values
    (`backlog_ops_bound_export_no_head_parity`);
  - a receipt without the field counts as unset;
  - the drain tool gets neither value, and what it publishes is unchanged.
- The parity age is checked at each run's own verdict. An execute re-checks it on its own and never inherits the
  dry run's result, so a parity row that ages out between the dry run and the execute is refused at the execute.

**Residual risk, stated plainly:** a Neon-only deletion in the census-to-drain window is readable until the eraser
removes it. Concretely, a deletion that reached Neon only (never the Worker) between the parity census and the drain
is published and readable until the Neon-mode eraser removes it (10 min nominal, 48 h SLA). The proof records
add nothing readable. They live under `<keyRoot>/.drain/`, which the eraser never targets: its erase prefixes are
under the org's `v1/` tree, and its dot-segment check only refuses `.` and `..` segments. So, like the sidecars, the
proof records outlive a user erase. Each one holds the org id in its key, plus the stage's sha256 and the max age,
the same class of data as the existing sidecars.

**Per-org procedure.** Every step is required, one org at a time.
1. Once, for the allow-listed orgs, compare Neon's control heads with Supabase's receipts. Any user with a Neon
   head but no Supabase receipt is out: drop the org from the list, or stop.
2. Confirm that `INGEST_S3_FENCE_MIRROR=on` and that the Neon-mode eraser is running.
3. Confirm that the drain's target bucket and key root equal those of the running Neon eraser. If they differ,
   drained objects are outside erasure coverage. Do not proceed.
4. Immediately before each export, run a fresh bulk parity with the written_at refresh ON.
   - **Prerequisites:** a fresh `bulk-plan` run in which every buffered owner is g1-equal, and the typed phrase.
     A real run with the refresh on is an operator step and needs the owner's explicit OK to the lane that runs it.
   - **How:** the owner-buffered-parity workflow's `bulk-write` with the input `refresh_written_at=on` (default
     `off`). Its typed phrase must be the plan's phrase followed by exactly ` REFRESH written_at`, and must have no
     token when the input is off. A mismatch either way refuses with `owner_parity_confirmation`. Outside the
     workflow, use the operator's `--refresh-written-at on`, which is refused (`usage`) with any census source
     but Neon.
   - **Why it is needed:** the bulk write never rewrites an unchanged proof row (the census is read-only). So only
     with the refresh do the rows of a stable backlog get a current `written_at`. Without it they keep their first
     write time and Path B refuses them as `head_unproven_parity_stale`.
   - **What the refresh does:** it touches `written_at` only, under the row's full-tuple CAS. The bulk receipt
     counts the rows it refreshed (`refreshed`, a field present only when the refresh is on, kept in a refused
     receipt too).
   - **A concurrent change during the run:** a row that another writer moved since its census is refused for that
     tenant only (`proof_refresh_cas_conflict`) and counted (`tenantRefused`, shown in the receipt's `mismatch`).
     The run carries on with the other tenants, then ends refused with `owner_parity_bulk_write_partial`, exit 1,
     and skips the readback. Rerun it after a fresh `bulk-plan`.
   - **Timing:** run the export soon enough that every row is inside the maximum age at the dry run, and again at
     the execute. The execute re-checks the age itself.
5. Run `export-dry`, then `export-execute` (the same list and age), then `drain-dry` and `drain-execute`, as in
   [Modes and order](#modes-and-order).
6. After the org's drain, run a Neon re-census. Check the list it produces against both the object store and the
   search index. Any hit blocks the next org.

**Open item:** this repository does not confirm that an erase also removes search-index rows created between the
census and the drain. Search-index removal depends on the S3 ObjectRemoved -> search-index predicate-delete path.
The eraser does not delete search-index rows itself. Until that is confirmed, step 6's search-index check is the
control.

## Authority (reshard-dry, authority-dry, authority-execute)

The last step of decision 0005's per-org cutover is writing the org's fence-authority marker
(`96bulls/.ledger/fence-authority/<org>.json`, `ledger-s3.ts` `validOrgFenceAuthority`). Three modes
cover it (`src/session-ingest/backlog-authority.ts`, run by `scripts/backlog-authority.ts`). All three
need a scoped run (`BACKLOG_OPS_ORGS`; `backlog_ops_scope_required` otherwise), because their verdicts
are per org. Their receipts carry only org hashes (`authority-evidence.ts` `evidenceHash("org", ...)`),
never an org id.

**authority-execute is held.** `BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED` is `false`, so the dispatch
refuses it with `backlog_ops_authority_held` before any GitHub, S3, Cloudflare or database read. The
tool refuses its `execute` again (`authority_execute_held`). It stays off until the evidence it relies on
passes review: the review of `authority-evidence.ts` found that observation alone cannot rule out a
transient admission between reads, and an enforced per-org writer hold is being designed. Until then
the dry modes' output is diagnostic only.

| Mode | Role | Binds | What it does |
| --- | --- | --- | --- |
| `reshard-dry` | dry | optional `backfill_run`: the completed catch-up execute it verifies after (its receipt records `after_run`) | The revision reshard's verify (`revision-reshard.ts`, `verify: true`) per scope org, against the revision home: with `BACKLOG_OPS_LEDGER_*` set, the ledger bucket routed as the edge routes it (fences from the transcripts bucket), after the tool fork guard (`assertToolLedgerHome`). Receipt: `verify_passed` overall and, per org hash, `verify_passed` with the counts. Dry only: `reshard-execute` refuses (`backlog_ops_reshard_dry_only`). |
| `authority-dry` | dry | nothing | The unbound live evidence (`authorityEvidence` without a baseline). Exit 0 only for `authority_evidence_unbound`, the state `authorityBaseline` takes. Receipt: the report, snake_case. |
| `backfill-catchup-dry/-execute` with `authority_run` | as before | `AUTHORITY <authority-dry>` | The catch-up runs `authorityBaseline` inside its input fingerprint's REPEATABLE READ READ ONLY transaction (`--authority-baseline`; the evidence on stdin). This is a SAMPLED fingerprint baseline: the backfill's later pages and slot reads use their own snapshots, so it does not attest the inputs the backfill actually read. Its receipt carries `authority_baseline` and, from the credential step, `authority_run` and `full_run` (the exact FULL). Without `authority_run` it is byte-identical to before. |
| `authority-execute` | execute | `DRY <authority-dry> BACKFILL <catch-up execute> FULL <backfill-execute> RESHARD <reshard-dry>` | Re-reads the evidence bound to the catch-up's baseline (must be `authority_evidence_corroborated`), then the sweeper (below), then, per org, the enforced writer fence and the marker, written create-only. |

The order is the evidence module's own: stop and drain the org, then `authority-dry`, then the
catch-up with `authority_run`, then `reshard-dry` with `backfill_run` set to that catch-up (its gate
requires the catch-up completed, so the verify runs after it), then `authority-execute`. The
execute requires its RESHARD to name its BACKFILL (`after_run`, `backlog_ops_reshard_not_after`).

```
AUTHORITY-EXECUTE <sha> DRY <authority-dry> BACKFILL <catch-up> FULL <full> RESHARD <reshard-dry>
BACKFILL-CATCHUP-EXECUTE <sha> DRY <catch-up dry> FULL <full> AUTHORITY <authority-dry>
```

Bindings (checked before any credential, the same way every other bound run is):

- The gate checks every bound run: same sha, success, first attempt, the dispatcher, its mode from its
  run-name, one receipt. Age limits: the catch-up (BACKFILL) under 3 h (`BACKLOG_OPS_FRESH_BACKFILL_MS`);
  DRY, FULL and RESHARD under 24 h. Ordering: FULL and DRY were created before the catch-up, and RESHARD
  after it. A catch-up's AUTHORITY was created before that catch-up's own DRY. The gate computes t from
  FULL as for every catch-up, and the credential step checks that the catch-up recorded it.
- The credential step: every receipt has exactly this run's scope (never `all`) and the ledger target
  this environment gives (`backlog_ops_bound_<kind>_ledger`). Every receipt of a backfill, drain, reshard or
  authority run records the store-home pair `home`: a hash of the Locker bucket and region and the ledger
  target, never `none`. The field is new on backfill and drain receipts, and no earlier binding reads it.
  Every authority-execute binding, FULL included, must carry this run's `home`
  (`backlog_ops_bound_<kind>_home`), so a FULL from before this field must be run again. Then: DRY must be `authority_evidence_unbound`
  over exactly the scope's org hashes, each refused for `sql_unbound` alone. The catch-up's
  `authority_run` must equal DRY and its `full_run` must equal FULL's run id exactly (t alone could alias
  two FULL runs created in the same second), and its baseline must be `authority_baseline_ready` over
  exactly the scope. RESHARD must have `verify_passed` for exactly the scope, each org with missing, invalid and
  failed at 0.

Inside the job, `authority-execute` does these steps in order, and writes nothing unless the first two
pass for every org:

1. It reads the bound evidence (`authorityEvidence` with `bound` set to the catch-up's baseline).
2. It reads the sweeper's serving spec and builds `freshSweeperPass`'s expectation from it. Every org
   needs a pass under 6 h old (`BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS`).
3. Evidence is corroboration only: it cannot rule out an admission between its reads or after the
   last one, so it never authorises a marker by itself. For each org, immediately before its write, the
   run re-checks that the report is under 5 minutes old (`BACKLOG_OPS_AUTHORITY_REPORT_MAX_AGE_MS`) and
   that the org's verdict is ok. It then asks the enforced writer fence (`writerFence`): a durable per-org
   hold that every admission path honours, bound to this run's baseline. That fence is being designed and
   is not in this tree, so the tool has none and refuses before any write (`writer_fence_unavailable`).
   With a fence that holds, the run writes the marker with `If-None-Match` and reads it back.
   ANY object already there is a `conflict`, never overwritten and never taken as this run's, and the
   run fails (`authority_incomplete`). An earlier or delayed write cannot be told from this run's until
   the epoch-bound hold protocol exists, so a rerun after a partial write fails closed.

Authority receipts are the only receipts that may hold lists. Each mode has an exact schema
(`backlog-authority-schema.ts`) covering every key, every list and its bound, every hash and digest
position, and every code from a closed set. The credential step applies it before the receipt is
persisted or logged. A summary that does not conform becomes `{"refused": "backlog_ops_receipt_schema"}`
and the run exits 1. A refusal summary keeps its code only when the mode's closed refusal map lists
it; otherwise it becomes `internal_error`, or `backfill_refused` for a catch-up. A backfill section's
`failed_codes` keeps only its known keys and sums the rest under `other`.

Every backfill receipt (backfill-dry/-execute, refresh, catch-up) is closed the same way. A FULL
backfill-execute is an authority binding, and every backfill mode shares its producer. The exact
backfill schema accepts every real summary (tested against PGlite runs). A refusal keeps only a code
the backfill itself throws (enumerated from its source), a digits-only `sqlstate_<5 digits>`, or
`db_connection_lost`; anything else becomes `backfill_refused`. Before this, `redactedReason` let any
code-shaped error message through.

### Verifying a sweeper pass (sweeper-pass-verify; read only, CLI)

`scripts/backlog-sweeper-verify.ts` (contract: `src/session-ingest/sweeper-pass-verify.ts`) checks one org's
latest re-arm sweeper pass receipt without writing anything. It is a CLI, not a dispatch mode: the workflow
never runs it.

    bun scripts/backlog-sweeper-verify.ts --bucket <locker> --region <r> < expectation.json

The expectation (one JSON line on stdin, a closed shape) is the serving sweeper as the operator established it
independently of the store: the org, the sweeper's org scope (or null), its transcript and ledger homes, its
namespace and CronJob, the ONE image digest it is pinned to, and optionally its git sha and the Job and Pod names
and UIDs. Invalid input is refused before any credential is read or request made. Run it with the dry
(read-only) role.

It reads exactly two objects with signed GETs (`latest.json` and the receipt it names) through `s3GetOnly`, a
frozen `{ getObject }` that is not an object store. It never loads the authority path. It applies
`freshSweeperPass`'s validation and match, with authority-execute's window (`BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS`,
6 h, which the caller cannot change) and the future skew, on its own clock. It also requires: the exact digest
(a git sha never stands in), a Job name of that CronJob with a Job UID, a Pod name of that Job with its own Pod
UID, and no Lambda name.

A PASS also needs trusted evidence that the sweeper wrote the exact pointer and receipt versions read (see
`docs/aws-stage.md`, D5: LastModified strictly inside a documented enforcement interval, or a writer record).
Neither this tree nor the S3 read API has such a source: body fields, user metadata, ETag, version id and the bucket
policy do not identify the writer. So the production run refuses every pass that meets every other check with
`writer_unverified`. That is the expected result until a trusted source exists. A provider seam exists, but no
input can reach it.

Output is one JSON line, the closed summary (`validSweeperPassSummary`), for success and for every refusal. It
holds the mode, verdict and a fixed reason code. It also holds hashes of the scope and expectation, the sha256 of
both objects' bytes, a hash of each version read (when S3 reports a version id), the pass times and the checks.
Unavailable fields are null. It never contains an org, key, bucket, identity, digest, ARN, version id or error
text. Exit 0 only when the verdict is `verified`.

### The sweeper's serving spec (proposal; not built)

The GitHub job has no kubectl. It therefore reads what the CURRENT serving sweeper is from
`96bulls/.ledger/rearm-sweeper/serving-spec.json` in the transcripts bucket (`BACKLOG_OPS_BUCKET`, next
to the sweeper's cursor and `passes/`):

```json
{"v":1,"kind":"rearm-sweeper-serving-spec","observedAtMs":0,"namespace":"<ns>","cronJobName":"<name>",
 "cronJobUid":"<uuid>","suspended":false,"orgs":["<org>"],"bucket":"<INGEST_REARM_BUCKET>",
 "ledgerBucket":null,"imageDigests":["sha256:<64 hex>"]}
```

`orgs` may be `null` (every org), `ledgerBucket` a bucket name, and `gitShas` an optional list.

An in-cluster observer writes it from the live CronJob: its uid, `spec.suspend`, the container's
`INGEST_REARM_ORGS`, `_BUCKET` and `_LEDGER_BUCKET`, and its image, which must be pinned by digest.
The observer runs under its own IAM role, which may put only that key. A bucket-policy deny keeps
every other principal off the key, and keeps the observer off the rest of `.ledger/rearm-sweeper/`.
The observer, its IAM and that policy are an infrastructure follow-up. Until they exist,
`authority-execute` refuses with `sweeper_spec_missing`.

The execute refuses when any of these does not hold: the spec is valid; it is under 1 h old and not in
the future; `suspended` is false; its `bucket` and `ledgerBucket` equal this run's own protected
variables (`sweeper_home_mismatch`); and its `orgs` cover the scope.

Prerequisites before the hold can be lifted (none is code on this branch):

- A read-only Cloudflare token (Workers Scripts:Read, Queues:Read), set as the environment SECRET
  `BACKLOG_OPS_CLOUDFLARE_TOKEN` on both backlog-ops environments. The workflow passes it to the tool
  step for authority modes only, and the tool gets it on stdin only.
- The execute role needs `s3:GetObject` on `96bulls/.ledger/rearm-sweeper/*` and
  `96bulls/.ledger/fence-authority/*`, and `s3:PutObject` on `96bulls/.ledger/fence-authority/*`. The
  dry role needs `ListBucket` and `GetObject` on the revision home's `96bulls/.admission-inbox/*`.
- The serving-spec observer, described above.
- The epoch-bound per-org writer hold, replacing the boolean `writerFence` seam. It must be durable and
  reversible, and the admission paths must take conflicting locks on it. Baseline, catch-up, marker and
  replay must bind to one fence epoch. The hold must outlive an ambiguous or delayed marker PUT, and must
  reconcile one by epoch.
- An independently authenticated serving spec (the observer above, with its bucket-policy deny).
- A reviewed commit that flips `BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED`.

### The empty-erasure-window proof and bridge-dry

The gating criterion layered-sessions-pipeline accepts for an org to reach `bridge_complete` without
the erase manifest is `erasure_window_empty` (`src/session-ingest/erasure-window.ts`). `bridge-dry` is
a scoped mode that binds `full_backfill_run`; the gate computes its cutoff t from FULL, exactly as it
does for a catch-up. For each org, the tool itself reads one statement, in a READ ONLY transaction, so
the caller never supplies the set:

- **Every erasure-lane source in the schema.** The sources are `operator_user_erasures`, the five
  `session_erasure_*` tables, `upload_deletions`, `upload_resets`, `upload_erasure_rearm`,
  `relayfacts_erasure_fences`, `session_analysis_purge_fences` and `_authority`, and
  `ingest_frozen_source_deletions` and `_resets`.
  - A row counts as in the window when its timestamp is at or after t, or when it is still in
    flight, whatever its age.
  - A table with no timestamp counts every row of the org (fail closed).
  - A test fails the build if an erasure-like table in the migrations is left unclassified. The
    excluded singletons (an eraser heartbeat and a purge cursor) are listed with their reasons.
- **The status, in the same snapshot.** The 062320 status (hold, epoch, timestamps) and its erasure
  watermark come from the same statement.

The proof is `erasure_window_empty` only when every count is 0, the hold is live in `sealed` or
`marker_attempted`, and the read came after the seal. Anything else is `erasure_window_nonempty` or
`erasure_window_unproven`, and the org stays HELD for the manifest or for reconciliation.

A positive set never releases. A user erase still needs `bytes_verified` and the writer-generation
proof, and a session erase needs evidence from the operator-erase replay-target manifest (its own slot,
still to be assigned by the slot registry; 062330 belongs to the buffered-search-feed fix).

The receipt has an exact schema (kind `bridge`) and carries org hashes only.

The proof binds the hold epoch as a typed hash (`epoch_hash`), never the raw UUID. The receipt also
records `full_run`, the exact FULL its cutoff came from.

An empty proof is corroboration only, never permission to release. A count of surviving rows cannot
prove that no event happened. A row deleted again leaves the count unchanged: `ingest_apply_selection`
removes an `upload_deletions` row on re-enable, for example. And today's 062320 watermark counts
surviving rows of only two tables.

So on this head `releaseOnEmptyWindow` refuses unconditionally (`release_watermark_partial`),
whatever the proof or its declared `watermark_kind`, and takes no watermark argument. A future release
is a separate, reviewed consumer of the follow-up erasure-history slot in the 062320 owner's lane. It
needs:
- verified history coverage from the FULL cutoff through the release;
- serialized writer checks;
- a comparison of the dry receipt's `full_run` with its selected FULL.

With slot 062340 (review/org-erasure-history @ adc8f8f5), the hold status also carries two things:
- `erasure_history`, the history fence: `fence_at`, which is always the hold's `installed_at`, plus
  `covered`, `recording` (a continuity check since the apply), `coverage_started_at` and `by_source`.
  `erasures` counts the history rows for the org since `fence_at`.
- `selected_full`, the FULL bound by the post-seal `_bind`: `run`, `started_at`, `started_ms` and
  `evidence_sha256`. It is null until `_bind` runs.

`bridge-dry` reads both in the same statement as the per-source counts. Each of these checks is
independent and has its own closed reason:
- `fence_at` equals the hold's `installed_at` exactly, in SQL, at stored precision
  (`history_cutoff_mismatch`).
- The bound FULL is this run's, compared only to `selected_full` and never to the fence
  (`history_full_mismatch`):
  - its `run` equals the dispatch's `full_backfill_run`;
  - its `started_ms` equals the FULL's start as the gate knows it, its created_at, which is the
    cutoff plus the 15-minute margin;
  - its `evidence_sha256` equals the sha256 of the bound FULL receipt's exact downloaded bytes. The
    credential step computes that digest and hands it to the tool on stdin.
- `installed_at` < start ≤ `sealed_at`, with the start built exactly from `started_ms` in SQL
  (`history_full_order_invalid`).
- `covered` and `recording` hold (`history_not_covering_cutoff`, `history_not_recording`), and
  `by_source` sums to `erasures`, which is 0 for an empty window.

**Contract for the seal and bind callers.** No caller exists at this head. Whoever writes the post-seal
bridge step (authority-execute, or the cutover runbook) must keep to these values exactly, and that step's
review must check them. A mismatch fails closed (`history_full_mismatch`).
- `_seal(org, epoch, evidence)`: `evidence` is the lowercase hex64 sha256 over the exact bytes of the
  bound FULL backfill-execute's `backlog-ops-receipt.json` artifact, the file as downloaded and not
  re-encoded. SQL stores it in the `sealed` event and never hashes anything itself.
- `_bind(org, epoch, full_run, full_started_ms, full_evidence)`:
  - `full_run` is that FULL's GitHub run id;
  - `full_started_ms` is that FULL run's GitHub created_at in integer ms, the same value the gate derives
    (the cutoff plus `BACKLOG_OPS_CATCHUP_MARGIN_MS`);
  - `full_evidence` is the same digest that was passed to `_seal`, byte for byte.

  SQL requires install < start ≤ seal, so a FULL dispatched before the install committed is refused
  (55000).

The credential step computes that same digest for `bridge-dry` from the file's raw bytes, read without
decoding. It refuses if those bytes differ from the text it parsed and checked.

Separately, persistence still requires each proof's snapshot `cutoff_ms` to equal the gate-derived FULL
cutoff. Without 062340 (062320 alone) the proof is a snapshot only. A test pins `ERASURE_SOURCES` to
062340's `ORG_ERASURE_HISTORY_SOURCES` once that file is on the branch.

`bridge-execute` is HELD in code (`BACKLOG_OPS_BRIDGE_EXECUTE_ENABLED`), and its tool refuses
(`bridge_release_held`) until slot 062320 lands.

### The hold bridge (code only)

`src/session-ingest/hold-bridge.ts` covers the bridge side of the per-org admission hold:
`docs/design/org-admission-hold.md`, sections 6.1 and 6.2, slot `20261105062320`. The replay of a
positive erasure set is not wired to a mode. Only the empty-window path above is (`bridge-dry`).

- **Lifecycle adapter (`holdSql`).** It holds the slot's owner-only functions in one table, so a
  rename is one edit. Each runs alone in a READ COMMITTED transaction:
  - `_status(org)`: read-only. It returns the live (or latest released) hold and the org's erasure
    record count.
  - `_install(org, request)`, `_seal(org, epoch, evidence)` and `_marker_attempt(org, epoch, evidence)`.
  - `_release(org, epoch, reason, evidence, detail, erasures)`. For `bridge_complete`, `erasures` is
    the count the replay settled on. The function recomputes it under the exclusive key and refuses if
    an erasure committed since.
- **Erasure set (`ERASURE_SET_SQL`, read-only).** It reads `operator_user_erasures` and
  `session_erasure_fences` for the org, since FULL minus the margin.
- **Replay (`replayErasures`).** It replays until the count is unchanged across a pass that found
  nothing new:
  - A user erase maps to that user's S3 fence reset (`resetUploadedSessions`, keyed by the request id),
    then `eraseGenerationLogPayloads` at the request's bound generation: only payloads of batches admitted
    below it, in every stream of the user. Never the unconditional `eraseLogPayloads`: a retry after release
    finds the reset known (a no-op) but an unconditional erase would still delete post-release uploads.
  - A user erase replays only once the SQL erase reached `bytes_verified`. That is the artifact lane's
    own byte receipt, a separate prerequisite. Until then the replay returns `erasure_bytes_unverified`
    and writes no receipt.
  - Even after `bytes_verified`, a user erase also needs a proven generation
    (`src/session-ingest/user-erase-generation.ts`). Its generation is the user's S3-fence reset ordinal
    that the request's own begin CAS bound (`FenceDoc.resetOrdinals`, written only by the S3 ledger's
    `applyReset`; copies of SQL's resets by the backfill or the control mirror get none and are never
    proven). Two separate checks:
    - **Pre-effect**, before any write: the erase was recorded strictly after the hold's install (one
      before it is `generation_pre_install`, one within 5 s of it `generation_ambiguous`; timestamps
      only ever refuse), the hold is sealed with its FULL bound (started after the install, by the seal),
      the SQL erase ran against this key root, no reset is pending, R is either not yet begun in the fence
      or begun with its ordinal, and the evidence port `admissionFencedBefore` shows the SQL, direct and
      fallback admission paths were all fenced before R. **That port is not built**: until it is, every
      user erase stays `erasure_generation_unproven`.
    - **One store.** The bridge builds the user erase's effects itself from its own store and key root
      (the S3 ledger's reset, the scoped payload erase); nothing is injected, and the proof functions
      take that same store, so the reset, the erase, both receipts and every re-verification hit one
      store. The store and its bucket come from one config (`bridgeStore`, which builds the store and
      records its bucket; there is no other setter): a store not built by it has no bucket and every
      user erase on it refuses. The binding and the receipt carry that bucket, and it must equal the
      SQL erase's bucket and key root (`eraseDestination`) before any effect.
    - **Post-effect**, after the reset and the scoped payload erase: a create-only receipt at
      `96bulls/.ledger/hold-bridge/<org>/<epoch>/generations/<hash of the request>.json` (hashes, counts,
      the key root and epoch only) once every byte class of the user is empty below the generation:
      no old owner stub the reset missed; every present log payload in every stream classified, with
      only one positively bound at or above the generation kept (an orphan written before a crash,
      applied residue, or an unsnapshotted or older batch's payload refuses); every re-arm marker swept
      (a done record for its exact bytes: a paused pre-reset write that commits after the reset is
      re-armed, and holds the proof until swept), no publication generation and no admission-inbox body
      for the user. Those two roots are judged over EVERY machine under the workspace, not only the
      machines the known origins derive: a machine of the user must hold nothing, another user's must be
      attributed by the `machineOwners` port, and an unattributed machine refuses
      (`generation_machine_unattributed`). In every class a key that does not parse refuses
      (`generation_unreadable`); nothing is skipped. A retry re-derives the receipt and must match it; so does
      `bridgeCompleteVerdict`. The erasure receipt is written only after it.
    - Open, stated: the `admissionFencedBefore` evidence source and the `machineOwners` attribution
      source (absent, any other user's machine in the workspace refuses); the publication-generation
      check lists the org's whole `upload-generations/` prefix and refuses on any malformed key there; a session re-enabled after release makes a later
      re-derivation refuse (the gate runs before release, so it only blocks a post-release retry).
  - A session erase maps to the S3 fence deletion (`setSessionUploads`, `enabled: false`) of every
    target in a complete erase manifest: every alias, for every owner, at every recorded origin, the
    turns origin included. S3 owner stubs can be partial: the SQL erase unions owners from ownership,
    index and directory rows over the alias closure. The erase-lane review therefore requires a durable
    target manifest captured under the erase lock before destructive cleanup.
  - No such manifest exists yet. Every session erase replays to `session_manifest_missing` (or
    `session_coverage_partial`) and writes no receipt, so it blocks `bridge_complete`. This fails
    closed. A cross-lane dependency remains open: the erase lane has to record that manifest. Its design
    belongs to the erase-lane owner (layered-sessions-pipeline, with the 050000/050010 author). This
    code only consumes it and refuses on incomplete evidence. Historic erases recorded without a
    manifest stay blocked unless an independent completeness proof is supplied.
  - Each replay writes a create-only receipt at
    `96bulls/.ledger/hold-bridge/<org>/<epoch>/erasures/<hash of the request>.json`. A retry finds it
    and re-applies nothing.
- **`bridge_complete` (`bridgeCompleteVerdict`).** Only with the hold `marker_attempted` at this
  epoch, and every erasure of the re-read set receipted for that epoch. After any marker attempt the
  only release is `bridge_complete` (`releaseAllowed`).
- **Waves (`waveOrgs`).** An org whose hold is `marker_attempted` is stuck and never joins a new
  wave.
- **Alarms (`holdAlarms`).** A hold sealed for over 60 min, or attempted for over 24 h without
  `bridge_complete`, pages the owner named by configuration (`BACKLOG_OPS_HOLD_ALARM_OWNER`, an Agent
  Relay name; for this rollout, claude-cloud-cloud-b1). The 60-minute bound exists because publication
  for the org is refused while it is sealed. The alarm carries the org's hash only.

## Marker publication (marker-dry, marker-execute) and org waves

`marker-execute` is **held in code** (`BACKLOG_OPS_MARKER_EXECUTE_ENABLED`, refused `backlog_ops_marker_held` before
any GitHub read). It writes nothing until a reviewed commit flips it, and that flip, installing and sealing a hold, and
every dispatch against production each need Khaliq's explicit OK. Contract: `src/session-ingest/marker-publication.ts`;
tool: `scripts/backlog-marker.ts`.

**Authority is the 062320 hold, not the evidence.** While an org's hold is `sealed` or `marker_attempted`, slot 062320
refuses every admission (RH042) and publication for it, and the state machine only moves forward (a released hold
never comes back). The bound live evidence (authority-evidence.ts) stays corroboration. Per org, `marker-execute`:

1. reads the hold: a live hold at the epoch `marker-dry` recorded, `sealed` (or `marker_attempted` on a retry). Else
   that org is refused before any write (`hold_absent`, `hold_released`, `hold_not_sealed`, `hold_epoch_changed`).
2. checks every marker field's attestation and that no marker this epoch cannot own is already there;
3. commits `_marker_attempt(org, epoch, sha256(final catch-up receipt bytes))`. **This is the point of no return:**
   after it only `bridge_complete` releases, so any later failure leaves the org held, and a rerun retries it;
4. opens a READ COMMITTED READ ONLY transaction and takes the org key SHARED (`_check(org, 'erasure')`, never refused;
   erasures still proceed). It re-reads the hold, which must be `marker_attempted` at the same epoch. While this
   transaction is open, no `_release` (EXCLUSIVE) can commit: a concurrent release waits for the publication. A
   release that committed before the lock refuses the org (`hold_released_before_put`) with nothing written;
5. GETs the marker. If absent, it PUTs create-only (`If-None-Match`) and reads it back. If present, it adopts it only
   when this epoch wrote it (below); otherwise `marker_conflict`, never overwritten. Each S3 call is bounded at 60 s
   (a timeout is `marker_put_unknown`: the org stays held and a retry adopts or writes);
6. re-reads the hold in the same transaction after the PUT. Still `marker_attempted` at the epoch proves the hold was
   live from before the PUT to after it. If the transaction was lost, a fresh read that still shows the hold live
   is the same proof. A released hold is `marker_hold_lapsed` and needs the hold owner.

**Attestations** (every field of `validOrgFenceAuthority`; any missing refuses `attestation_missing`):

| Field | Attested by |
| --- | --- |
| `fenceBackfill`, `ownershipBackfill`, `revisionBackfill` | the section complete with failed 0 in BOTH the FULL and the final catch-up |
| `controlMirror: on` | the bound evidence: every serving Worker version has `INGEST_S3_FENCE_MIRROR=on` |
| `rearmSweeper: live` | `freshSweeperPass` for the org against the serving sweeper's spec |
| `revisionLayout: 4096` | the bound reshard-dry verified that org (missing, invalid and failed 0) |
| `workerHttpAdmission: off` | the bound evidence (stop, queue drained, SQL quiet, turns on Neon) AND the hold enforcing it |

**Publication identity.** Every PUT stamps the object's metadata `marker-publication` with
`sha256(org, epoch, attempt evidence)`. The attempt evidence is the sha256 of the final catch-up receipt's bytes. Its
durable side is the epoch's one `marker_attempted` event. `_marker_attempt` returns early on a repeat without checking
evidence, so after the attempt the tool re-reads that event and refuses (`hold_attempt_evidence_mismatch`) unless it
carries this run's bound evidence.

**Retry and adoption.** A rerun binds the same `marker-dry` and wave. `_marker_attempt` is idempotent at the same
epoch. An existing marker, on the first GET or on the If-None-Match conflict path, is adopted only when all of these
hold:
- it is valid for this org;
- its stored identity equals the one recomputed from the live epoch and that epoch's durable attempt evidence, which
  equals this run's;
- the hold is `marker_attempted` at the approved epoch;
- its `writtenAtMs` lies between the epoch's first attempt minus 2 min (runner versus database clock) and now plus 2 min;
- no other epoch of the org ever recorded a marker attempt.

Anything else is a conflict.

**Bindings and order** (scoped runs only, all at one commit, every scope exact):

| Mode | Binds | Confirmation |
| --- | --- | --- |
| `marker-dry` | `AUTHORITY` the authority-dry, `BACKFILL` the catch-up that took the baseline (under 3 h), `FULL` its FULL, `RESHARD` a reshard-dry after the catch-up | none |
| `marker-execute` | the same four, plus `DRY` the marker-dry (created after the reshard-dry) and the input `wave_id` | `MARKER-EXECUTE <sha> DRY <dry> BACKFILL <catch-up> FULL <full> RESHARD <reshard> AUTHORITY <authority-dry> WAVE <wave_id>` |

The marker-execute's DRY must have recorded exactly the same four runs (`backlog_ops_bound_marker_runs`), and its
`wave_id` must equal the input (`backlog_ops_bound_marker_wave`).

### Org waves

A wave is one scoped sequence over up to 64 orgs (`BACKLOG_OPS_ORGS`): FULL, catch-up, reshard verify, marker, drain.
It is unchanged for one org. The per-org tools give each org its own verdict, at most `--concurrency` orgs at once
(`src/session-ingest/org-wave.ts`). The marker defaults to 8 and the reshard verify to 4; at most 16. One org's
refusal or error never stops or changes another's.

**Per-org runs (`per_org`).** With the dispatch input `per_org` set to `true` (scoped runs only), the credential step
runs the unchanged tool once per scope org as a single-org scoped run. This applies to the backfill modes (FULL,
refresh, catch-up), export and drain.
- Each child's summary goes through its mode's own checks: countsOnly, and the exact backfill or catch-up schema.
- The receipt summary is `{kind: "wave", org_count, ok_count, per_org: [{org_hash, exit_code, summary}]}`. Its hashes
  must be exactly the scope's, each once, and `ok_count` is derived. A dry run exits 0 when at least one org succeeded
  (it shows the eligible subset). An **execute exits nonzero unless every org completed**. Its receipt still keeps
  every slice, and nothing binds a failed run.
- **Retry of the outstanding orgs** (`previous_run`, a per-org execute only; confirmation `... PREVIOUS <run> PER_ORG`).
  The gate checks the previous run: this execute at this sha, failed, first attempt, by the dispatcher, created after
  every run this one binds, under 3 h old, one unexpired receipt. It writes that run's creation time as
  `previous_created_ms`. The credential step checks the previous receipt: a wave receipt of exactly this mode, sha,
  run, scope, ledger, home and bindings (the receipt's `bindings`), not complete.
  - Completed orgs are carried with `carried_ms`, the time they really ran. Only the outstanding orgs run, and the
    drain budget is split over those.
  - Consumers apply their freshness rule to a carried slice from its `carried_ms`.
  - A carried FULL slice moves that org's catch-up `t` to `carried_ms` minus the 15-min margin, so the catch-up never
    reads from a later retry's `t`.
- A run that binds a wave receipt checks it per org, on that org's view: the slice as a single-org receipt, or a
  non-wave receipt of exactly the wave's scope re-scoped to the org with its per-org lists sliced. The unchanged
  per-receipt checks run on each view: home, ledger, since, fingerprint, baseline. The run's 3 h / 24 h freshness and
  order checks still apply.
- An org whose bindings fail is refused alone (its slice records the code) and no child runs for it.
- A catch-up child with `AUTHORITY` gets only its org's evidence and records its org's baseline. A marker merges the
  passing orgs' baselines and refuses the others `binding_refused`. A reshard-dry needs one org of a wave catch-up to bind.
- Evidence hashes (the marker attempt's, the bridge's) stay sha256 over the bound file's exact bytes, never a view.
- The export and drain executes hold a one-at-a-time lease, so their children run sequentially. Other modes run 2
  at a time, so size the job's 355-minute timeout for the sum.
- An execute's confirmation ends with ` PER_ORG` exactly when `per_org` is set (`BACKFILL-EXECUTE <sha> DRY <id> PER_ORG`),
  so the approval names the run's shape. Without it the confirmation is unchanged.
- The drain's `max_objects` stays a per-run cap: a wave splits it across its orgs (shares sum to exactly the budget; a
  budget below the org count refuses `backlog_ops_wave_max_objects`).
- A green wave run can still have refused orgs: the step logs `backlog_ops_wave_result ok_count=<n> org_count=<m>
  complete|incomplete`, and the receipt's `ok_count` says the same.
- Path B is refused in a per-org run.
- Without `per_org`, every receipt is byte-identical to before. A consumer that does not check per org (bridge,
  authority-execute) refuses a wave receipt.

**The approval is pinned to the wave.** `marker-dry` records `wave_id`, a hash over the commit, the scope id, the four
bound runs and each READY org's hold epoch hash. `marker-execute` names it in its confirmation, so the environment
approval covers exactly those orgs at exactly those epochs. The execute recomputes it (`marker_wave_mismatch`), acts on
those orgs only (any other scope org is `not_in_wave`), and refuses an org whose epoch moved. `marker-dry` succeeds
when at least one org is ready (`marker_wave_partial` names how many); `marker-execute` succeeds only when every wave
org is published or adopted. Otherwise `marker_wave_incomplete`, and a rerun retries.

The wave-level prerequisites stay wave-level: the authority-dry and the catch-up's baseline must pass for every
scope org (the evidence module's own rule). Drop an org that fails them and rerun the sequence for the smaller scope.

**Logs.** No org id is a dispatch input, part of a run-name, or in a log line or receipt. `wave_id` is a hash, the
scope reaches the tools only from the secret, every scope org is masked (`::add-mask::`) before any tool runs, no
`run:` script expands an expression (`${{ }}`), and the marker tool prints only its counts-only line. Refusals carry
fixed codes, never messages.

**reshard-verify** (read only): the workflow's `reshard-dry` now verifies the orgs in a bounded parallel wave
(`reshard-wave.ts`; same verify, same receipt). For an operator check outside the workflow:
`bun scripts/backlog-reshard-verify.ts --bucket <locker> --region <r> < {"orgs": [...]}`. Run it with the read-only
role; the orgs come on stdin, never argv. It binds nothing.

**Not built here:** installing and sealing a hold (`_install`, `_seal`, and 062340's `_bind`) as a mode, and the
bridge release (`bridge-execute`, held). A marker needs the org's hold already sealed.

## Gates

Every run passes the same gates as the guarded schema release:

- the repository id, `refs/heads/main`, and the exact live main head;
- first attempt only;
- the pinned dispatcher, as both actor and triggering actor;
- the mode's protected environment, with deployments from `main` only: `session-schema-production` for a dry run,
  `session-backlog-execute` for an execute (derived from the mode, never an input);
- green push `ci` and `pg-proof` at the commit;
- inside the protected job, the recorded approval for that environment.

Every execute additionally needs all of the following:

- **Dormant switch:** `BACKLOG_OPS_EXECUTE_ENABLED` in `src/session-ingest/backlog-ops.ts` was `false` until a
  reviewed commit flipped it to `true` for the backlog run; flip it back the same way to make executes dormant again.
  A dry run never needs it.
- **Bound runs:** each one is a successful first attempt of this workflow at the same commit, by the pinned
  dispatcher, of the expected mode, under 24 hours old, with exactly one unexpired receipt artifact.
- **Receipts, checked before any credential is read:**
  - every bound receipt exited 0;
  - the backfill's `sections` are all `complete` (a refresh's: `fences` and `ownership` complete, `revisions`
    `not_run`; a catch-up's: all three complete, `failed` 0 in each, and `revisions_since_ms` equal to the gate's t). A backfill-execute therefore cannot pass while any user is `needs_review`: the backfill leaves that
    section incomplete until the user is resolved, and every later execute stays blocked until then;
  - with `FULL`, the refresh's input fingerprint equals the full run's (`backlog_ops_backfill_stale` otherwise); a
    catch-up as the drain's `BACKFILL` is checked by its t instead (`backlog_ops_bound_backfill_since` otherwise);
  - the export has `failed = 0`, `fence_late = 0` and `orgs.incomplete = 0`.
    When `failed` is not 0, the export receipt's `failed_codes` names the causes (closed codes only, summing to
    `failed`): `store_<code>` for an object-store error, `fence_<cause>` for a user whose S3 fence could not be read,
    `export_proof_mismatch`, `export_lock_lost`, `no_head_parity_unlisted`, `supersede_<cause>`, or `internal`.
    The export retries every object read, list and write of the run (not the lease's acquire or release, and not a
    response body that fails mid-stream) on `transient` (a throttle, 408/429 or 5xx) up to
    `EXPORT_TRANSIENT_ATTEMPTS` (5) times with backoff, and a failed lease renewal likewise, so `store_transient` in
    a receipt means S3 stayed unavailable across all of them. Any other code is never retried. A lost lease
    (`export_lock_lost`) stays a hard stop.
- **The tools' own refusals:**
  - the export refuses while the v2 claim-sessions reader gate is off (`export_claim_sessions_gate_off`);
  - the drain refuses beside a live lease at `96bulls/.drain/lock.json`, and writes nothing for an org without a
    fence-authority marker.

Dispatches share the concurrency group `guarded-session-schema-production` with every other guarded production
operator (the schema apply, the generation overlap/rebind, the 058000 index prebuild, owner-buffered parity and
admission renewal), so a backfill or export never reads the owner database mid-apply or mid-rebind, and one backlog
operation runs at a time. The cost: a running backlog operation (a drain slice can take up to the 355-minute job limit) holds that
group, so a schema apply or rebind queues behind it. Use `max_objects` to keep drain slices short when one is due.

## Credentials

The **database** login is the environment secret `SUPABASE_MIGRATION_OWNER_URL`, the same one the schema release
uses. It is passed only to the backfill, export and `presence-dry` modes; the drain needs no database. It is checked
against the pinned release connection and handed to the tool in one normal form (`sslmode=verify-full`). The tools
pin TLS to the Supabase CA themselves. The URL reaches the tool only on its stdin pipe, as `{"dbUrl": …}` (for
`presence-dry`, `{"dbUrl": …, "session": …}`). Every tool runs every page in a READ ONLY transaction.

**AWS** access is a GitHub OIDC token, exchanged inside the credential step for a role session through STS
`AssumeRoleWithWebIdentity`, for at most 6 hours; the step refuses before the tool starts unless the session has at
least 5 hours left. Dry modes use the read-only role and executes use the writer role. The session lives only in the
tool's environment.

The credential step is the only step given the database secret or the role session, and the only one that requests
the OIDC token. It is **not** the only step that could request one: GitHub exposes `ACTIONS_ID_TOKEN_REQUEST_*` to
every step of a job with `id-token: write`. So only the operate job has `id-token: write`, its actions are
`actions/checkout` and `actions/download-artifact` (GitHub's own) plus the third-party `oven-sh/setup-bun`, which is
pinned to a full commit SHA (v2.2.0) rather than a mutable tag. The separation that matters is in IAM: the write role trusts only the `session-backlog-execute` environment's subject, which no other job uses.

Both environments' protected variables pin the target (set the same five on each): `BACKLOG_OPS_BUCKET`, `BACKLOG_OPS_REGION`,
`BACKLOG_OPS_KMS_KEY_ARN` (a key ARN, `key/<uuid>` or `key/mrk-<id>`, not an alias), `BACKLOG_OPS_DRY_ROLE_ARN` and `BACKLOG_OPS_EXECUTE_ROLE_ARN`.
They are shape-checked: the key must be in the region, and both roles must be in the key's account and distinct.

### The relocated upload ledger

After the ledger moves to its own bucket ([ledger-relocation.md](ledger-relocation.md)), set two more protected
variables on both environments: `BACKLOG_OPS_LEDGER_BUCKET` (the edge's `INGEST_LEDGER_BUCKET`) and
`BACKLOG_OPS_LEDGER_REGION` (its region). Set both or neither: one alone refuses (`backlog_ops_target_ledger_bucket`,
`backlog_ops_target_ledger_region`) before any credential, and so does a ledger bucket equal to `BACKLOG_OPS_BUCKET`.
The credential step hands them to the backfill and the drain as `INGEST_LEDGER_BUCKET` / `INGEST_LEDGER_REGION`. Those
tools then write the ledger's own keys (ownership stubs, revision shards, re-arm markers) to that bucket. They send no
encryption header there, so the bucket's SSE-S3 default applies. They keep the fences, the staged batches, their
markers, the layout and the lease in `BACKLOG_OPS_BUCKET`. Before the first write they refuse unless
`BACKLOG_OPS_BUCKET`'s marker names exactly that ledger bucket (`ledger_home_mismatch`: a variable pointing at another
environment's ledger refuses here) and the ledger bucket's own marker names it (`ledger_home_missing`,
`ledger_home_moved`), and they check again before a ledger write once a minute has passed since the last good check
(a refusal then stops the run at its next boundary with that code, a failed receipt: the drain before its next batch
group, after releasing its lease, logging `drain_lease_release_failed` if that release fails; the backfill before its
next SQL page, slot window or org read, or after its current merge pass). The
export touches no ledger key and does not take them.

What a refusal mid-run looks like:
- The page or merge pass in flight finishes first. Its remaining ledger writes are refused by the guard without a
  request, but the backfill's store retry may still try each of them up to 5 times with backoff (the ledger library
  can report the refusal under a retryable store code), so the stop can take that long, writing nothing.
- The drain's failed stdout line is `{"refused": "<code>"}`, plus `"lease_release": "drain_lease_release_failed"` when
  its lease release also failed (one stderr line says the same). The lease then runs out its 15-minute TTL, and a
  following drain or export waits for it.
- A drain that completes but cannot release its lease still exits 0. Its summary carries
  `"lease_release": "drain_lease_release_failed"` (and the stderr line), and the lease is held until its TTL, so the
  next dispatch within those 15 minutes refuses with `drain_locked`.

**Set them as environment variables only.** `vars.*` falls back to a repository or organisation variable of the same
name when the environment has none, and the workflow cannot tell the scopes apart. So never define
`BACKLOG_OPS_LEDGER_*` at repository or organisation scope: define them on both `session-schema-production` and
`session-backlog-execute`. What still holds if one leaks in: one variable without the other refuses
(`backlog_ops_target_ledger_*`); a bucket that is not the one `BACKLOG_OPS_BUCKET`'s marker names refuses
(`ledger_home_mismatch`) before any write; and an execute refuses unless its bound dry run (and, for the drain, its
bound backfills) recorded the same ledger target (`backlog_ops_bound_<kind>_ledger`).

The backfill and the drain (and the export, which never reads the marker, so for it the variable does nothing) run with
`INGEST_LEDGER_HOME_GUARD=on`, always: the credential step adds it to the tool's environment in code (the step's own
environment never reaches the tool). So a backfill or drain dispatched before these variables are set, once the
relocation has written "moved" into `BACKLOG_OPS_BUCKET`'s marker, refuses with `ledger_home_moved` instead of writing a
stale copy. Every backfill mode runs the ownership section (a refresh runs fences and ownership), so every backfill
and drain dispatch checks the marker.

Receipts are counts only. Every leaf is a number, a boolean or a short lowercase code; any other string, such as an
id, a key or a URL, refuses the receipt. They are kept for 30 days as the `backlog-ops-receipt` artifact. Each also
records `ledger`: `none`, or `ledger_<32 hex>` (a hash of the ledger bucket and region a backfill or drain was given;
never the name, since receipts are echoed to this public repository's logs). A backfill or drain execute refuses unless
the backfill and drain receipts it binds recorded the same value. A receipt from before the field reads as `none`, so
a dry run (or backfill) made before this check existed binds only an execute with no ledger bucket: once
`BACKLOG_OPS_LEDGER_*` are set, re-run the dry runs and backfills an execute will bind. The field also changes every
receipt with the variables unset (`"ledger": "none"`), and with the guard on the tools also re-read the marker during
a run, so the unset path is not byte-identical to before; it behaves the same.
The hash is a plain SHA-256 of a fixed tag, the bucket name and the region, truncated, and deliberately unsalted (a
salt would be one more secret to hold and rotate). It hides the name from a casual reader of the logs but does not
protect it from anyone who can guess it: a guessed name and region can be confirmed against a receipt. Treat the
ledger bucket's name as discoverable, as bucket names are generally; nothing relies on it being secret.

## Session presence (presence-dry)

**Purpose.** Support diagnosis of ONE session whose dashboard shows "No conversation recorded yet": it counts how
far the session's rows got through ingest, the owner buffer, custody and publication. It reads the owner database
only, in one `REPEATABLE READ READ ONLY` transaction that always ends in `ROLLBACK`. It writes nothing, reads no S3
object and binds no other run. The tool is `scripts/session-presence.ts` (`src/session-ingest/session-presence.ts`).

**Inputs.** The `presence-dry` mode, the exact `main` sha, no `confirmation`, and no bound run ids. It runs in the dry
environment `session-schema-production` with the dry (read-only) AWS role, which it does not use (it reads no S3).
The **database** login is `SUPABASE_MIGRATION_OWNER_URL`, the migration owner, not a read-only database role. The
run is read-only only because the tool's connection sets `default_transaction_read_only=on` (beside the owner
config's `idle_in_transaction_session_timeout`) and every count runs in a `REPEATABLE READ READ ONLY` transaction
that checks `SHOW transaction_read_only` and always rolls back. All gates above apply unchanged. `BACKLOG_OPS_ORGS`,
if set, scopes every count to those orgs (receipt `scope` `orgs_<n>`, else `all`).

**Scope it where you can.** Unscoped runs (no `BACKLOG_OPS_ORGS`) are allowed, but most counts are then sequential
scans of large tenant-first tables (ownership, claim sessions, lazy slots, revisions, heads, generations and
`session_index`) on the production primary. Set `BACKLOG_OPS_ORGS` to the session's org whenever you know it, so the
counts use their org-leading indexes.

**Time bounds.** Each statement has a 60 s `statement_timeout` and a 5 s `lock_timeout` (a count that waits on DDL
refuses the run with `sqlstate_55p03`). The whole transaction has a 180 s budget (`--total-budget-ms 180000`). The
final generation marker read keeps a reserve equal to the statement timeout: the counts and the distinct-org check
share the budget minus the statement timeout (120 s), and the final marker keeps up to the full 60 s statement
timeout, so a count cut short by the budget still leaves the marker observable. No statement starts with less than
1 s of its share left, and each statement's timeout is lowered to what is left of its share, so the
`REPEATABLE READ` snapshot is held for about 3 minutes at most. Both values are fixed in the workflow's argv
(`BACKLOG_OPS_PRESENCE_TIMEOUT`).

**The session id is a secret.** The owner sets the environment **secret** `SESSION_PRESENCE_SESSION` on
`session-schema-production` immediately before the dispatch and **removes it immediately after** the run. The value
is one lowercase UUID or a `[A-Za-z0-9_-]{8,128}` token. The workflow passes it only to the credential step and only
for `presence-dry`. That step hands it to the tool on its stdin pipe beside the database URL. It never goes into argv,
a log line or the receipt. The tool's own refusals pass through only as `session_presence_*`, `backlog_ops_*` or
`sqlstate_<code>`; any other error is `internal_error`. The refusals are fixed codes and never contain the value:

- `backlog_ops_presence_session_missing`: the secret is unset or empty.
- `backlog_ops_presence_session_invalid`: any other shape.
- `backlog_ops_presence_session_unexpected`: another mode saw the secret non-empty, so remove it after the run.
  This is defense in depth: the workflow expression already blanks the secret for every other mode, so it fires only
  if that expression is changed or bypassed.
- `backlog_ops_presence_dry_only`: the dispatch named `presence-execute`, or an execute path reached the presence
  tool.
- `backlog_ops_bound_presence_receipt`: a presence receipt was bound as a dry, backfill or export run.

**Receipt.** Every value is a count, `null` when that count hit the 60 s statement timeout or the 180 s budget, or a
fixed code. There are no ids, keys, URLs, digests or content. With `BACKLOG_OPS_ORGS` unset, most counts scan their
table (the tables are keyed tenant-first). Each one is bounded by the timeout and the budget.

| Field | Counts |
| --- | --- |
| `ownership` | `ingest_session_ownership` rows naming the session (1 is normal) |
| `claims` | accepted upload claims (`ingest_claim_attempts`) whose records name the session |
| `claim_sessions` | `ingest_claim_sessions` rows (the v2 reader's session index, written only once its policy enables writes) |
| `buffered_stages` | owner-buffered stage rows of those claims that the buffered reader could serve. This is an upper bound, because the reader's runtime gates are not evaluated |
| `outbox`, `outbox_buffered` | publication outbox rows of those batches, in any state and still `buffered` |
| `lazy_slots` | `ingest_lazy_native_slots` currently owned by the session |
| `revisions` | `ingest_record_revisions` of the session |
| `heads`, `published_heads` | `ingest_publication_heads` rows, and the published ones (`live` or carrying a current pointer) |
| `dispositions` | `heads_building`, `heads_live`, `heads_retired`: heads per state (the fixed `state` enum) |
| `published_objects` | live objects of live publication generations |
| `canonical_pointers`, `canonical_pointers_live` | `session_index` pointers the read API looks for first, and the undeleted ones |
| `generation_stable` | whether the publication marker (heads and canonical pointers of the session) was identical when read first and last. This is a snapshot / pooler-consistency check, not a change detector: two reads in one `REPEATABLE READ` transaction see the same snapshot, so they can differ only if the pooler did not hold the transaction on one backend. A publish during the run does not change it |
| `complete`, `incomplete_reason` | `true` and `none`, or `false` with one of the codes below |

**`incomplete_reason` codes.** When more than one applies, the first in this list is reported. The counts are still
emitted.

1. `generation_unobserved`: a marker read timed out, or the budget ran out before the last one.
2. `generation_unstable`: the first and last marker reads differed, so the counts did not come from one snapshot
   (the pooler did not hold the transaction on one backend). Rerun; do not read the counts.
3. `multi_ownership`: more than one ownership row, or the session id appears under more than one org in the tables
   counted. Read the counts with care, because they span every owner. Scope with `BACKLOG_OPS_ORGS` to split them.
4. `count_timeout`: a count or the distinct-org check hit the 60 s statement timeout, so it is `null` or unobserved.
5. `budget_exhausted`: the counts' share of the 180 s budget (the budget minus the statement timeout) ran out, so a
   count or the distinct-org check was skipped or cut short, and those counts are `null`.
   Scope the run with `BACKLOG_OPS_ORGS`.

**Reading it against "No conversation recorded yet":**

| Shape | Meaning |
| --- | --- |
| everything 0 | Nothing arrived for this session id, or it arrived under another org (check `BACKLOG_OPS_ORGS`) or another id. Look at the uploader. |
| `ownership` 1, `claims` > 0, `buffered_stages` > 0, `published_heads` 0 | An unpublished buffered stage: accepted and buffered, never published. The page can show it only through the owner-buffered reader (flag, allowlist and admission), and after that only through the drain. |
| `buffered_stages` 0, `outbox` > 0, `outbox_buffered` 0, `published_heads` 0 | The buffer left the outbox without a publication. Check the drain and publisher receipts. |
| `published_heads` > 0, `canonical_pointers_live` 0 | Published, but there is no live canonical pointer for the read API. Check the index step, or a tombstone if `canonical_pointers` > 0. |
| `canonical_pointers_live` > 0 | The data is published and indexed. Look at the read path and access (org, workspace, user), not ingest. |
| `heads_retired` > 0 and no live head | The session's publication was retired (deleted or reset). |

## IAM

The provider, both roles, the two key statements and the five variables on `session-schema-production` were applied
by hand with Khaliq's OK on 2026-11-13 at 17:49Z. **The write role's trust and the `session-backlog-execute`
environment are not done yet** (see "Still to apply" below). The two key statements are reverted by the next
`aws-sessions-prod` stack deploy until the backlog-ops key phase (`transcriptStore.backlogOpsRoles`) lands and is
set in the stage inputs; until then, re-add them after any stack deploy.

The provider and both roles can be declared in the `aws-sessions-prod` stack by adoption in place
(`transcriptStore.adoptIdentities`, docs/aws-stage.md "Adopt identities"); the capture and the deploy refuse unless
a read-only live diff shows the declaration equals what is live.

No existing principal covers the drain's writes: the transcripts key policy grants key use only to named principals,
and the srv-writer and writer policies are prefix-scoped. So the workflow needs, in the sessions-prod account:

1. **An OIDC provider** for `https://token.actions.githubusercontent.com`, audience `sts.amazonaws.com`.
2. **`96bulls-aws-sessions-prod-backlog-ops-read`**, trusted only for
   `token.actions.githubusercontent.com:sub` =
   `repo:AgentWorkforce@252050005/relayhistory-pipeline@1437714333:environment:session-schema-production`
   (the repository's immutable-id subject, from `gh api repos/AgentWorkforce/relayhistory-pipeline/actions/oidc/customization/sub`
   on 2026-11-13; the name segment follows the repository's current name, so recheck it after a rename) and `aud` =
   `sts.amazonaws.com`. `MaxSessionDuration` is 6 h. It may only `s3:ListBucket` the transcripts bucket,
   `s3:GetObject` under `96bulls/*`, and `kms:Decrypt` on the transcripts key via `s3.us-east-1.amazonaws.com`.
3. **`96bulls-aws-sessions-prod-backlog-ops-write`**, trusted only for
   `repo:AgentWorkforce@252050005/relayhistory-pipeline@1437714333:environment:session-backlog-execute` (and `aud` =
   `sts.amazonaws.com`), never the schema environment's subject. It may do everything the read role does,
   plus `s3:PutObject` under `96bulls/*` with `s3:x-amz-server-side-encryption = aws:kms` and the transcripts key,
   and `kms:GenerateDataKey` via S3. It gets no `DeleteObject`: the bucket policy's `OnlyTheEraserDeletes` already
   denies it, and nothing in the tools deletes.
4. **Two key-policy statements** on the transcripts key, naming the two roles. The key policy delegates no key use
   to IAM:
   - `BacklogOpsReadDecryptsViaS3`: `kms:Decrypt`;
   - `BacklogOpsWriteEncryptsViaS3`: `kms:GenerateDataKey` and `kms:Decrypt`.

   Both are conditioned on `kms:ViaService` = `s3.us-east-1.amazonaws.com`.

5. **The ledger bucket**, once the ledger is relocated (above). The roles use the same session for both buckets.
   The ledger bucket is in another account, so access is cross-account, and cross-account access needs **both
   sides**: the ledger bucket's bucket policy must grant the two roles, **and** each role's own identity policy must
   allow the same actions on the ledger bucket. Either side alone gets `access_denied`.

   - **Identity side (this repository).** The grants are exactly `backlogOpsLedgerStatements`
     (`stacks/aws/adopt-identities.ts`). Under `<root>/.admission-inbox/*/ledger/*` the read role needs
     `s3:GetObject`, and the write role `s3:GetObject` and `s3:PutObject` with no `s3:x-amz-server-side-encryption`
     condition, since the tools send no encryption header there. Both need `s3:GetObject` on
     `<root>/.ledger/fence-authority/.ledger-home.json`, and `s3:ListBucket` on the bucket with **no condition**. S3
     answers a `GetObject` or `HeadObject` of a missing key with 404 only when the caller holds `s3:ListBucket` for
     that very request, and such a request carries no `s3:prefix`, so a prefix-conditioned List never applies there:
     every missing shard, owner stub or marker would read as a 403 (`access_denied`) and every real run would fail.
     The stack refuses any condition on this List and requires it whenever a ledger `GetObject` or `PutObject` is
     granted. It refuses, whatever its condition, any Deny that could cover `s3:ListBucket` (an Action pattern such
     as `s3:ListBucket`, `s3:List*`, `s3:*` or `*`, or a `NotAction` none of whose entries matches `s3:ListBucket`;
     one naming it or a pattern such as `s3:List*` is exempt) on the ledger bucket (a Resource pattern such as `*`,
     `arn:aws:s3:::*` or one with a policy variable, or any `NotResource`), any malformed Deny, and any Deny Action /
     NotAction entry with a policy variable or a non-ASCII character. Other Denies, such as a protective `s3:DeleteObject` on the ledger keys, pass. The usual
     `Deny s3:*` on `Resource: "*"` with `aws:SecureTransport` = `false` is caught by this: re-scope its Resource to
     exclude the ledger bucket (name the buckets and object ARNs it should cover) rather than removing it. No
     `DeleteObject`: neither tool deletes a ledger key. No KMS grant: the bucket uses SSE-S3. The
     adopted roles' pasted policies (`transcriptStore.adoptIdentities.backlogOpsRoles`) may carry these grants only
     with `backlogOpsRoles.ledgerBucket` = `{ "bucket": "<ledger bucket>", "keyRoot": "96bulls" }` set in the
     private stage inputs; the stack refuses any other statement on that bucket, and without the input it refuses
     every statement on it, exactly as before (docs/aws-stage.md "Adopt identities"). Order: add the grants to the
     live roles (an IAM change: Khaliq's explicit OK), then paste the updated live policies and set the input, so
     the live diff stays clean.
   - **Resource side (not this repository).** The ledger bucket's policy granting the two role ARNs the same actions
     on the same resources lives with that bucket's infrastructure, in its own repository. It must grant
     `s3:ListBucket` on the bucket to these principals **unconditioned** too: cross-account, S3 needs the List
     allowed on both sides before it answers a missing key with 404, so a prefix-conditioned List on the bucket side
     keeps every missing shard, stub and marker a 403 however the roles are set. **Check the live bucket policy
     before the first real run.**
   - **Key names are visible.** With the unconditioned List, both roles can enumerate every key *name* in the ledger
     bucket (`GetObject` and `PutObject` stay key-scoped, so not the contents). That is acceptable only because the
     bucket is dedicated to the ledger ([ledger-relocation.md](ledger-relocation.md)); its owner must keep it that
     way.
   - **Known scope gap: log-commit payloads.** The `GetObject` / `PutObject` pattern
     `<root>/.admission-inbox/*/ledger/*` also matches the log-commit streams' payloads under
     `ledger/log/<stream>/p/`, which hold session records. Once `INGEST_LEDGER_LOG_COMMIT` is used with the relocated
     ledger, both roles could read them and the write role could overwrite them, though neither tool touches them.
     Log-commit is off in production today. Narrowing the grants (to the ledger's `batches/`, `revisions/`,
     `owners/` and `rearm/`) before turning it on there is a code change, not just a policy edit: the validator
     accepts only `<root>/.admission-inbox/*/ledger/*` (`backlogOpsLedgerResources` / `backlogOpsLedgerStatements` in
     `stacks/aws/adopt-identities.ts`), so those must change first.
   - **Precondition: Object Ownership = `BucketOwnerEnforced` (ACLs disabled) on the ledger bucket.** The write role
     puts objects cross-account with no ACL. Under any other ownership setting those objects are owned by the
     roles' account, and the edge, in the bucket's own account, may be unable to read them. Check it before the
     first execute against the ledger bucket (it is also that bucket's infrastructure, not this repository's).

   Both roles also need `s3:GetObject` on the marker key in the transcripts bucket (the forced guard, and the
   Locker-side marker check described above); `96bulls/*` already covers it.

### Applied (2026-11-13)

All three steps below were applied on 2026-11-13 (see the dated note in step 1); they are kept as the reference for
recreating the environment. Re-check them with a readback before an execute, since environment settings live outside
the repo.

1. Create the GitHub environment `session-backlog-execute` with deployments from `main` only (one branch policy,
   `main`, type branch), as on `session-schema-production`. The gate refuses an environment without that branch
   policy. Required reviewers are optional: with the pinned dispatcher (`trustedDispatcher`), the gate accepts an
   environment with only the branch policy and no approval, exactly as on `session-schema-production`. The real
   control is the pinned dispatcher identity plus the write role's IAM trust in this environment's subject.
   (Created 2026-11-13 with the `main` branch policy, the secret and the five variables; the write role's trust was
   narrowed to this subject the same day.)
2. Set on it the environment secret `SUPABASE_MIGRATION_OWNER_URL` (the same value as on `session-schema-production`;
   backfill-execute and export-execute need it) and the same five `BACKLOG_OPS_*` variables.
3. Change the write role's trust to the `session-backlog-execute` subject above (before 2026-11-13 it trusted the
   `session-schema-production` subject, like the read role; narrowed that day, per step 1's note).

Dry runs need none of this and can run now.
