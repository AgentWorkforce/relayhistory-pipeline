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
     The stack refuses any condition on this List, requires it whenever a ledger `GetObject` or `PutObject` is
     granted, and refuses, whatever its condition, any Deny whose Action could cover `s3:ListBucket`
     (`s3:ListBucket`, `s3:List*`, `s3:*`, `*`, or any `NotAction`) and whose Resource could cover the ledger bucket
     (a pattern such as `*` or `arn:aws:s3:::*`, or any `NotResource`). Other Denies, such as a protective
     `s3:DeleteObject` on the ledger keys, pass. No `DeleteObject`: neither tool deletes a ledger key. No KMS grant: the bucket uses SSE-S3. The
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

### Still to apply (before the first execute)

1. Create the GitHub environment `session-backlog-execute` with deployments from `main` only (one branch policy,
   `main`, type branch), as on `session-schema-production`. The gate refuses an environment without that branch
   policy. Required reviewers are optional: with the pinned dispatcher (`trustedDispatcher`), the gate accepts an
   environment with only the branch policy and no approval, exactly as on `session-schema-production`. The real
   control is the pinned dispatcher identity plus the write role's IAM trust in this environment's subject.
   (Created 2026-11-13 with the `main` branch policy, the secret and the five variables; the write role's trust was
   narrowed to this subject the same day.)
2. Set on it the environment secret `SUPABASE_MIGRATION_OWNER_URL` (the same value as on `session-schema-production`;
   backfill-execute and export-execute need it) and the same five `BACKLOG_OPS_*` variables.
3. Change the write role's trust to the `session-backlog-execute` subject above (it currently trusts the
   `session-schema-production` subject, like the read role).

Dry runs need none of this and can run now.
