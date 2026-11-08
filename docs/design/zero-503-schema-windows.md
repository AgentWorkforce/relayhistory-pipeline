# Zero-503 schema windows: bounded generation overlap

Status: code and tests only (draft PR). Nothing is applied, deployed, opened or rebound by this change.
Applying migration `20261105053020`, opening a window on a real database, and any Worker deploy each still
need Khaliq's explicit OK in his own words.

## 1. The failure mode

Every schema window so far (three, including G4 on 2026-11-08, which measured 23/23 uploads failing with
`503`, SQLSTATE `55000` at claim) refused **every** upload from the first readiness-marker write until the
generation rebind. That was not a deploy-ordering mistake. Four exact-equality checks cause it, and no
Worker ordering avoids them.

Line numbers are on `origin/main` before this change.

### 1.1 Where the generation comes from

- `src/hoopdb/migrate.ts:335-363` `generation()`: sha256 over every migration file and every phase-2
  verifier. Any new migration produces a new generation.
- `src/hoopdb/migrate.ts:381-382` `MARKER_PENDING_SQL`, written at `:521` under the migrator advisory lock
  before the first step: the singleton row `sessions.__96bulls_role_validation('session_directory_search_worker')`
  becomes `(G_next, 'pending')`. That row is the only record of the old PASS, and this write overwrites it.
- `src/hoopdb/migrate.ts:596-597`: the row becomes `'pass'` only in the last phase-2 verifier transaction.

### 1.2 Worker side

- `src/session-ingest/worker.ts:182`: buffered mode requires `INGEST_EXPECT_GENERATION` (one hex64 value).
- `src/session-ingest/worker.ts:251` → `src/session-ingest/ledger-sql.ts:190-192`: every buffered RPC payload
  carries `generation: INGEST_EXPECT_GENERATION`. That covers claim (`:248`), stage, commit, ordered
  control, selection and reset.
- `stacks/cloudflare/session-ingest.ts:210-218,273`: the stack binds exactly that one value.

### 1.3 Database side: the fence

| # | Check | Where | Equality |
|---|---|---|---|
| F1 | `sessions.ingest_require_generation(p_expected)` | `supabase/migrations/20261104050000_buffered_upload_receipts.sql:58-66` | caller generation = marker generation, and marker `status='pass'` |
| F2 | `sessions.ingest_lazy_guard(p_org)` | `supabase/migrations/20261104055000_lazy_current_custody.sql:423-439` | `ingest_lazy_policy.generation` = PASS generation, and the org's `ingest_buffer_admission.schema_generation` = PASS generation |
| F3 | `sessions.ingest_lazy_begin` | `supabase/migrations/20261105041000_ingest_privacy_authority.sql:826-840` (`:837`), admission insert/check `:893-898` | caller generation = policy generation, and new admissions are written at the caller generation |
| F4 | `sessions.ingest_lazy_attempt` | `supabase/migrations/20261104055000_lazy_current_custody.sql:587` | the attempt's `schema_generation` (the caller's at claim) = policy generation |

F1 runs first in every buffered RPC: claim
(`supabase/migrations/20261105043850_ingest_acquisition_owner.sql:1150-1151`), stage (`:1180`), commit
(`:1278`), the lazy begin, Babysitter evidence (`20261105042000:275,…`) and buffered GC (`043850:1443,1592`).
F2 runs from `ingest_require_buffer_admission` (`055000:514-519`) right after F1 in claim (`043850:1152`).
The rebind (`src/hoopdb/ingest-generation-rebind.ts:29,33,40-47`) is the only thing that moves the policy and
the admissions, and it requires PASS for the target.

### 1.4 What that does to a window

| Interval | Worker at G (old) | Worker at H (new) |
|---|---|---|
| marker `(H, pending)`: the whole apply | F1 refuses (not PASS) | F1 refuses (not PASS) |
| marker `(H, pass)`, policy still G | F1 refuses (G ≠ H) | F1 passes; F3 refuses (policy G ≠ H) |
| after the rebind (policy H) | F1 refuses | admitted |

There is no Worker generation that is admitted between the pending write and the rebind. The outage lasts
from `migrate.ts:521` until the rebind commits. Today that span covers the full apply, the phase-2 verifiers,
the Worker PUT and any other runtime replacement. All of it is a 55000 at claim.

### 1.5 Why "Worker PUT first, backward-compatible migrations" is not enough

That ordering is part of the answer, but it cannot reach zero by itself. Evidence:

- Interval 1 refuses every caller generation. F1 accepts only `status='pass'`, and the migrator writes
  `pending` before any step. No Worker value, and no amount of migration compatibility, passes F1 while the
  apply runs.
- Interval 2 refuses both generations. G fails F1, and H fails F3/F4 because the policy stays at G until the
  rebind.

Deploying an `{H, G}` Worker first and running the rebind immediately after PASS shrinks the outage to
"apply plus rebind", but the apply interval stays. The database fence has to admit the previous generation
for a bounded, owner-recorded period. That is the change below. It is designed to be used *with* the
Worker-first ordering.

## 2. The overlap protocol

### 2.1 Database: one owner-opened, time-bounded window (migration `20261105053020`)

**NEW SLOT 053020.** It is the next free slot after the landed `043870`: `043880` and `043890` are taken, and
`049000`/`050000`/`051000` are named design lanes. The registry keeper should confirm it. Note that any
later migration must sort after `053020` once `053020` is applied (`sequencePreflight`, lane prefix rule).

The migration creates `sessions.ingest_generation_overlap_windows`, an append-only record with these rules:
- Columns: `from_generation`, `to_generation`, `admit_during_apply`, `approval_sha256`, `opened_by`,
  `opened_at`, `expires_at`, `closed_at`, `closed_reason`.
- Expiry is capped at 4 hours by a CHECK constraint.
- A unique partial index allows at most one open window.
- A trigger blocks every UPDATE except the single close, and blocks DELETE.
- RLS is on and every grant is revoked.

It also adds four owner-only functions. None of them is granted to a runtime role, and the generator-checked
ACL manifest shows `<owner>:EXECUTE` only.

- `ingest_generation_overlap_open(p)`:
  - Requires `current_user` to be the function owner (42501 otherwise, even when EXECUTE has been granted).
  - Takes the migrator advisory lock `(961996,1)`.
  - Requires exactly `{from, to, ttlSeconds, admitDuringApply, approvalSha256}`: explicit hex64 values,
    `from ≠ to`, TTL from 60 s to 14400 s, and a boolean.
  - Requires the marker to be PASS for `from` and the lazy policy to be at `from`.
  - Refuses a `to` that was ever a window's `from`, and a `from` that a rebind already retired. This is the
    no-downgrade rule.
  - Sweeps expired windows (closes them as `expired`) and refuses while another window is open.
- `ingest_generation_overlap_close(p)`:
  - Owner only. Reason is `owner` or `rebind`. A rebind close requires PASS and the policy to be at `to`.
  - Takes the open row `FOR UPDATE`, so it waits for every in-flight transaction the window admitted.
- `ingest_generation_overlap_admits(g, marker_gen, marker_status)`, the one predicate. It returns true only
  if all of these hold:
  - a window is open and unexpired;
  - the window's `from` = g, and the window's `to` = the marker's current generation;
  - the marker is `pass`, or it is `pending` and the owner chose `admitDuringApply`;
  - the caller is a runtime ingest role (`bulls96_session_ingest`, `bulls96_babysitter_retention`).

  It takes the window row `FOR SHARE`. Owner and operator paths (seat initialize, policy set) never use the
  window.
- `ingest_generation_overlap_immutable()`: the append-only trigger.

It replaces two owner-only helpers. Signatures, owner and ACL are unchanged, and the generated rollback
restores the exact prior bodies.

- F1 `ingest_require_generation`: exact PASS equality as before, **or** `admits(caller, marker)`.
- F2 `ingest_lazy_guard`: policy = PASS as before, **or** `admits(policy generation, marker)`. The org
  admission must equal the policy generation, which matches the old predicate when no window is open.

Locking:
- The exact-PASS path locks exactly as before: the marker row `FOR SHARE`, so a later pending write waits
  for in-flight PASS callers.
- A window-admitted transaction locks the **window** row `FOR SHARE` instead, and reads the marker without a
  lock. The migrator's own marker writes (the pending upsert and the phase-2 PASS update inside the
  hardening transaction) therefore never wait on, or deadlock with, live uploads. The real-PG test proves
  this with `lock_timeout=1s`, and a mutation that re-adds the marker lock fails it with `55P03`.

F3, F4, the claim's sticky `schema_generation`, privacy seals, the custody clock, epochs and the
contiguity/ledger checks are **unchanged**. While a window is open, uploads therefore keep running entirely
at G: the policy, admissions and attempts all stay G-consistent until the rebind moves them together.

Properties:
- **No wildcard.** The window names two explicit generations. The predicate compares for equality only.
- **Bounded.** Only the window's single `from` is ever added. The unverified pending `to` is never admitted.
  A window cannot bridge to any generation other than the marker's.
- **No downgrade.** A retired `from` can never come back.
- **Owner-only.** Opening, closing and reading are owner-only.
- **Auto-expiry.** The CHECK, the trigger and the read-time `expires_at > clock_timestamp()` together mean
  the window cannot be extended.
- **No window, no change.** With no open window the fence is byte-for-byte as strict as before.

### 2.2 Rebind closes the window atomically

`src/hoopdb/ingest-generation-rebind.ts` changes as follows:
- When the overlap table exists, it locks the open window `FOR UPDATE` *before* the policy, which matches
  the ingest lock order (window → policy) and so cannot deadlock with an admitted upload. The real-PG test
  proves this.
- It refuses a window that is not exactly `(from → to)`.
- It moves the policy and admissions as before.
- It closes the window (`rebind`) in the same transaction and reports `overlapClosed`.
- Before `053020` exists, it behaves exactly as before.

### 2.3 Worker: an explicit set of at most two generations

- `INGEST_ACCEPT_GENERATIONS` (`src/session-ingest/worker.ts`, `acceptedGenerations`):
  - Unset means `INGEST_EXPECT_GENERATION` only, which is today's behaviour.
  - Otherwise it is 1 or 2 comma-separated, distinct, lowercase hex64 values that must include
    `INGEST_EXPECT_GENERATION`.
  - Buffered mode only.
  - Anything else throws: `*`, a third value, whitespace, uppercase, or a list without the expected value.
- Stack input `sessionIngest.overlapGeneration` (`stacks/cloudflare/session-ingest.ts`): one other explicit
  hex64 value, different from `expectedGeneration`, buffered only. It renders
  `INGEST_ACCEPT_GENERATIONS=expected,overlap`.
- `sqlLedger` (`src/session-ingest/ledger-sql.ts`):
  - A call that starts an operation (claim, ordered control, selection, reset) is sent with one accepted
    generation. Only when the database refuses it with `55000` is the same call retried once with the other
    generation.
  - Each call is one statement, so a refused call wrote nothing.
  - Stage, commit and Babysitter evidence reuse the generation their claim was admitted with (the database
    binds the attempt to it).
  - An isolate-wide hint records which generation was last admitted, so the steady state is one call. The
    hint orders the attempts; it never authorizes one.
  - The Worker can only *offer* its two values. The database decides.

## 3. Rollout order

### 3.1 Shipping this change without the outage it fixes

`053020` is additive and accepts every old caller. It creates new objects, and the two replaced helpers
reduce to the old predicate while no window exists. But applying it is itself a schema window: `generation()`
moves, and the window machinery does not exist until the migration commits. **Its own window therefore
cannot be zero.** It can be made as short as the apply.

1. Merge (code only).
2. Worker deploy of this code with `expectedGeneration = G_cur` and `overlapGeneration = G5`, where G5 is
   the generation of the source containing `053020`. Against the unchanged database G_cur is admitted
   first. G5 is offered only after a 55000.
3. Apply `053020` **alone**, the smallest possible apply, with the normal approval. Uploads are refused
   from the pending write until step 4, as today. The Worker PUT is no longer inside this interval.
4. Run the rebind `G_cur → G5` immediately after PASS. The step-2 Worker switches to G5 on its next request
   with one internal retry. No Worker PUT is needed to recover.
5. At leisure, Worker deploy with `expectedGeneration = G5` and no overlap.

Alternative: fold `053020` into the next window that is needed anyway. It is the last window with an outage
either way.

### 3.2 Every later window (G → H): zero 503 from the fence

1. **Worker first.** Deploy with `expectedGeneration = G` and `overlapGeneration = H`. There is no behaviour
   change while the database is at G.
2. **Open.** Immediately before the apply, run
   `bun scripts/ingest-generation-overlap.ts open --from G --to H --ttl-seconds <apply+runtime+margin> --approval-sha <approval.json sha256> --admit-during-apply`
   (or `--no-admit-during-apply`, see §4). The approved owner-apply can instead open it under its own lock
   through `generationOverlapIntegration` (`src/hoopdb/ingest-generation-overlap.ts`). That fixed entry is a
   separate reviewed change, and the stock CLI is not modified here.
3. **Apply.** Use the normal owner-apply. Marker `(H, pending)` → `(H, pass)`. Uploads continue at G
   throughout.
4. **Other runtime replacements.** GC and publisher, as today. The GC at G is also admitted during the
   window (same role). The publisher and history stay on exact PASS and pause, as today.
5. **Rebind.** `ingest-generation-rebind.ts --from G --to H --execute` moves the policy and admissions and
   closes the window in one transaction. The `{G,H}` Worker's next G call gets 55000, retries at H, and is
   admitted.
6. **Worker cleanup.** Deploy with `expectedGeneration = H` and no overlap.

If the window expires before step 5, the fence fails closed: today's outage, never a widening. Choose a TTL
that covers steps 3–5. The maximum is 4 h.

## 4. Limits and residual risks

- **Compatibility is a precondition, not a guarantee.** While the window is open, a G Worker runs against
  each intermediate prefix of H's migrations (with `admitDuringApply`) and against H's final schema. Every
  such schema must keep G's RPC contract: function signatures, `ingest_exact_keys` shapes and result shapes.
  The owned-fixture rehearsal (`scripts/pg-proof/window*-rehearsal.ts` pattern) must prove old-Worker
  uploads across the full union before the window is approved. If a migration breaks the contract, use an
  expand/contract pair across two windows. `--no-admit-during-apply` limits exposure to the
  phase-2-verified final schema, at the cost of a refusal interval as long as the apply.
- **Requests that straddle the rebind instant.** A batch claimed at G before the rebind commits and staged
  or committed after it gets one 55000/40001. The attempt is bound to G, and F4 now sees policy H. The
  client retry is a fresh claim at H. Exposure is bounded by the claim-to-commit time of in-flight requests
  (seconds), not by the window. Previously the exposure was the whole window.
- **Lock waits are not fence refusals.** DDL in the apply, and the phase-2 hardening (which takes
  AccessExclusive locks on live tables), can still delay uploads past Worker or Hyperdrive timeouts. Those
  are lock/timeout errors (`55P03`/`57014`), not 55000. With `admitDuringApply=true` the apply runs
  **against live upload traffic**, which today's outage accidentally avoided:
  - each migration's `lock_timeout='5s'` can trip on a busy table;
  - the hardening transaction can lose a deadlock to an upload's table locks. The migrator retries `40P01`
    3 times (`migrate.ts` `ROLE_CHECK_DEADLOCK_ATTEMPTS`), then fails, which leaves the marker pending and
    the window admitting G until it expires.

  Rehearse the exact union under synthetic concurrent uploads before choosing `true`. If that is not
  rehearsed, `false` is the conservative choice: refusals then last only from the pending write to PASS.
  The Worker PUT, the GC replacement and the rebind wait are no longer inside the refusal interval.
- **Scope.** Only the two runtime ingest roles are bridged. The canonical publisher and history paths keep
  exact PASS. The non-lazy (pre-policy) admission path keeps exact PASS. Production uses the lazy policy,
  and the rebind already requires it.

## 5. Tests

- `src/session-ingest/__tests__/generation-overlap.pglite.test.ts` (PGlite, every migration, the real
  ledger and `deliverBatch`):
  - Status quo without a window: every upload is refused from the pending write until the rebind. This
    reproduces the outage.
  - A G Worker keeps admitting through `(H, pending)` and `(H, pass)`, and `{H,G}` admits throughout.
  - A single-H Worker is refused before the rebind.
  - A dry-run rebind rolls the close back. The real rebind closes the window (`rebind`).
  - After the rebind, G is refused and H and `{H,G}` admit. Attempts are recorded at the generation each
    claim was admitted with.
  - `admitDuringApply=false`: G is refused during pending and admitted after PASS.
  - The window bridges only `from → marker`. A third generation is never admitted.
  - Owner close and expiry both refuse G again. `open` sweeps an expired window.
  - Owner-only: the ingest role cannot execute, read or insert, and a granted non-owner still gets 42501.
    The owner calling the fence at G during a window is refused.
  - No wildcard or malformed input (22023 for `*`, `%`, from = to, bad TTL or boolean, extra keys).
    Readiness must be PASS for `from`. One open window at a time. Append-only (no extension, no retarget,
    no delete). The 4 h CHECK holds. A mismatched window refuses the rebind. No downgrade (H → G refused).
    A forward window H → I is allowed.
  - Worker configuration parsing and the `sqlLedger` set validation.
- `src/session-ingest/__tests__/generation-overlap.postgres.test.ts` (real PostgreSQL 17, opt-in via
  `SESSION_INGEST_TEST_PG_URL`):
  - the same end-to-end protocol;
  - an in-flight admitted claim held open across a concurrent rebind: the rebind waits (no deadlock, no
    cut-off), commits after it, closes the window, and G is then refused while H admits;
  - an upload admitted during the apply held open while the phase-2 PASS write runs under
    `lock_timeout=1s`: the write does not wait.
  - An expired window for another pair does not block a rebind. This one is in the PGlite suite.
- `src/hoopdb/__tests__/sessions-function-boundary.test.ts`: the lane adds four owner-only functions, every
  role verifier is byte-identical with and without the lane, and the generated rollback contains the exact
  050000/055000 bodies.
- `src/hoopdb/__tests__/migration-profile.test.ts`: the down/reapply chain. 043870 refuses under 053020,
  053020's down restores the strict fences and drops its objects, and the next migrate reinstalls it.
