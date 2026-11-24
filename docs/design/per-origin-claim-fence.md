# Per-origin claim fence: one user's machines stop serializing on each other

Status: code, migration file and tests only. Nothing is applied, deployed or switched on by this change.
Applying migration `20261105062010`, opening an overlap window, the rebind and every Worker deploy (including
turning on `INGEST_ORIGIN_CLAIM_FENCE`) each still need Khaliq's explicit OK in his own words.

## 1. The failure

The hosted buffered ingest (Cloudflare Worker → Hyperdrive → Supabase Postgres) runs three SQL calls per delivery:
claim (`sessions.ingest_claim_buffered_batch`), stage prepare (`ingest_prepare_buffer_stage`) and commit
(`ingest_commit_buffered_batch`). Each one calls `sessions.ingest_lock_fences(org, workspace, user)`, which takes
the user's single `sessions.ingest_fence_owners` row `FOR UPDATE` (body last replaced by `20261105058010`).

A user with several desktop installs has one origin per machine, but only one fence row. So machine B's claim waits
for machine A's whole claim or commit transaction. Accepted deliveries take 2-9 s end to end, and the claim gives up
after `INGEST_CLAIM_LOCK_WAIT_MS` (≤ 2 s, `55P03`) or the role's 5 s `statement_timeout` (`57014`). In production
about a quarter of deliveries failed this way, even with the lock wait at 2000 ms.

Product decision (2026-11-16): ordering **across** one user's machines is not required. Ordering **within** one
origin stream is.

## 2. What the per-user row protects

An inventory of every writer that takes the row, grouped by what it needs from the claim path.

| Holder | How it takes the row | What it needs | After this change |
|---|---|---|---|
| Buffered claim, stage prepare, commit (and the staged ACK trigger and lazy custody calls inside them) | `ingest_lock_fences` | mutual exclusion with per-user controls; serialization of the same origin's batches | **origin-scoped calls**: row `FOR SHARE` plus a per-origin advisory lock. Flag-off calls: unchanged `FOR UPDATE` |
| Selection, reset and ordered controls (`ingest_set_session_uploads`, `ingest_reset_uploaded_sessions`, `ingest_apply_selection`, lazy control receipts) | `ingest_lock_fences` | a stable per-user view: no claim may run between a control's read and its write | unchanged `FOR UPDATE` (conflicts with `FOR SHARE`) |
| Privacy/seed/custody paths, publication and history claims, GC finish, canonical claim/commit/revisions/fan-out, self-host erase | `ingest_lock_fences` | same as controls | unchanged `FOR UPDATE` |
| Publication pointer guard, buffered GC claim | the row directly, `FOR UPDATE NOWAIT` | must not interleave with a claim of the user | unchanged; `NOWAIT` fails fast against `FOR SHARE` exactly as against `FOR UPDATE` |
| Operator user erase (begin, step, complete) | `(961996,50000)` advisory **exclusive**, then the row `FOR UPDATE` | no ingest transaction of the user overlaps | unchanged. Every claim still `INSERT`s the row first, so `20261105050010`'s statement trigger takes `50000` **shared**; claims still run the RH041 erase check after their row lock |
| First-seen user adoption (`ingest_lazy_user`: seal, baseline, cold privacy rows) | `INSERT ... ON CONFLICT DO NOTHING RETURNING` on the row, before the lock | exactly one adopter per user | unchanged: a second inserter waits for the uncommitted first insert, then reads the committed seal (test O4) |

What the claim path itself writes, and what serializes it once two origins of one user can overlap:

- Keyed by origin (`ingest_batches`, `ingest_claim_attempts`, `ingest_session_ownership`, `ingest_fence_versions`,
  `upload_deletions`, `ingest_custody_records`, stage intents, outbox): the new per-origin advisory lock.
- Native custody slots (key `org, workspace, origin, record`): unchanged, the `(org, ws, origin)` advisory lock of
  `043500`/`043800` and the slot rows.
- Receipt seat numbering: unchanged, the global seat clock (`NO KEY UPDATE` for staged ACKs, `KEY SHARE` for claims).
- Acquisition owners (per session, cross-origin): unchanged, their own advisory locks.
- Per-user state the claim only **reads** (control receipt versions, `upload_resets` count, seals, the RH041 ledger):
  written only by holders that keep `FOR UPDATE`, so it cannot change under an in-flight `FOR SHARE` claim.

Nothing on the claim path writes a per-user row that is not keyed by origin, except first-seen adoption (above).
Different users already run this exact path concurrently; two origins of one user now look the same to every lock
below the fence row.

## 3. Design (migration `20261105062010_ingest_origin_claim_fence.sql`)

Three new entry points, `SECURITY DEFINER`, `EXECUTE` to `bulls96_session_ingest` only, each with the same input
and result as the function it wraps:

- `sessions.ingest_origin_claim_buffered_batch(p)` → `ingest_claim_buffered_batch(p)`
- `sessions.ingest_origin_prepare_buffer_stage(p)` → `ingest_prepare_buffer_stage(p)`
- `sessions.ingest_origin_commit_buffered_batch(p)` → `ingest_commit_buffered_batch(p)`

Each one calls `ingest_origin_claim_enter(p)` (owner-only), then the unchanged function, then
`ingest_origin_claim_leave` (owner-only). `enter`:

1. checks the role and derives `(org, workspace, user, origin)` with `ingest_batch_key(p)`, which is the same
   validation the wrapped function does (22023 on malformed input, before any lock);
2. takes `pg_advisory_xact_lock(hashtextextended(['96bulls:ingest-origin-claim-v1', org, ws, user, origin]))`.
   This is the per-origin serialization, taken before any other lock;
3. inserts a **mark** `(pg_current_xact_id(), [org, ws, user])` into the owner-only
   `sessions.ingest_origin_claim_marks`. `leave` deletes it. An error rolls the insert back.

`sessions.ingest_lock_fences` (owner-only, replaced) takes the user's row `FOR SHARE` when the current transaction
holds a mark for exactly this `(org, ws, user)`, and `FOR UPDATE` otherwise. Its INSERT and RH041 check are
byte-for-byte `058010`.

`ingest_lock_fences` stays SECURITY INVOKER (as in `058010`) while `ingest_origin_claim_marks` is owner-only with
RLS on, so the lock reads the mark with its caller's rights. Every caller today runs as the owner (the SECURITY
DEFINER wrappers and the owner paths). A future SECURITY DEFINER function owned by another role that calls
`ingest_lock_fences` gets 42501 on the mark read: it fails closed and never bypasses the exclusive lock.

The mark's key is the top-level transaction id. `leave` deletes it before the wrapper returns, so sequential calls in
one transaction are fine. A wrapper nested inside another wrapper in the same transaction would raise 23505 (no such
caller exists).

Why a mark and not a GUC or the call stack: a runtime role can set any custom GUC, take any advisory lock, and
(through `DO` + `EXECUTE`) put arbitrary text into `PG_CONTEXT`. None of these can make a control take `FOR SHARE`.
The mark table has RLS on, every grant revoked, and is written only by `enter` after it has taken the origin lock.
It has no `org_id`/`workspace_id`/`user_id` columns, so `ingest_lazy_user`/`ingest_lazy_begin`'s "prior authority"
scans and the user-erase inventory never see it. Rows exist only inside an in-flight call.

### Lock order

- Origin-scoped claim/stage/commit: origin advisory → `(961996,50000)` S (fence-row INSERT trigger) → fence row S
  → seat clock → `(org,ws,origin)` native advisory → slot rows.
- Controls: `50000` S → fence row X. Erase: `50000` X → fence row X.

The new key is only ever taken first, and only by `enter`. A flag-off call (or a mixed fleet during rollout) never
takes it. So it adds no cycle. Re-entrant `ingest_lock_fences` calls inside one claim find the row already
share-locked by the same transaction and do not wait. Nothing on the origin-scoped path takes the fence row
`FOR UPDATE`, so there is no shared-to-exclusive upgrade between two claims.

### Generation overlap interaction

With the marker at PASS (every normal window), the generation fences are unchanged. With `admitDuringApply` (off by
default and on the production CI path), `044030` admits a pending-marker call only when the outermost `sessions`
frame is one of its five listed entry points. The three new names are not listed, so during such an apply they fail
closed (`55000`), like any other caller. **Keep the flag off during any schema apply** that might use
`admitDuringApply`. Otherwise every flag-on claim, stage and commit gets a burst of 55000 for the length of the apply.

## 4. Worker flag (default: current behaviour)

- `INGEST_ORIGIN_CLAIM_FENCE=on` (any case; buffered acceptance only, refused otherwise) → `IngestSettings.originClaimFence`
  → `sqlLedger(..., { originClaimFence: true })`. Claim, stage prepare and commit call the origin-scoped names.
  Everything else (controls, selection, reset, Babysitter evidence, canonical mode) is unchanged.
- Unset (default): the per-user functions, byte-identical SQL.
- **42883 fallback**: if the database lacks `062010` (not yet applied, or rolled back), the origin-scoped call fails
  with `42883` before it runs. Only a 42883 whose text names that entry point itself (`function
  sessions.ingest_origin_…(`) falls back. A 42883 from inside a body, for example a missing inner function, is
  rethrown as `ledger_sqlstate_42883`, as before. On a fallback the ledger retries the per-user function once and
  keeps using it for 60 s in that isolate, then tries the origin-scoped function again. Inside the claim lock-wait
  transaction the failed attempt is rolled back first. Each switch logs one structured warning: route
  `ingest.origin_fence_fallback`, `sqlstate` 42883, `stage` the absent entry point, and `fallback_until_ms`. The
  database message is never logged. So the flag can never turn a missing migration into an outage, and a real
  missing-function bug is not masked. Every other error passes through unchanged.
- Stack: `sessionIngest.originClaimFence: "on"` renders the var (buffered only). Production CI: a cutover control like
  the claim lock controls (`PRIVATE_INGEST_ORIGIN_CLAIM_FENCE` protected-environment variable →
  `privateIngestCutoverFromEnv`), so the release guard lets a deploy add it at the live phase. The variable accepts
  only `on` or unset/empty, so no value keeps the binding with the flag off, and removing the rendered var has no
  operator unbind today (the operator refuses the delete row with `native_binding_target`); removal needs a
  separately reviewed operator change.

## 5. Release: generation overlap, then the flag

`062010` is a schema change, so `generation()` moves. It ships under the zero-503 protocol
(`docs/design/zero-503-schema-windows.md` §3.2, production CI path). Never move readiness without a window: that is
the 2026-11-13/16 outage. G is the live PASS generation, and H is the generation of the release target that includes
`062010` (the guarded apply plan's `targetGeneration`; if later slots are released together, H covers all of them).

1. **Land** (code only).
2. **Worker G+H.** Reviewed source change: `PRIVATE_INGEST_OVERLAP = {from: G, to: H}`, tracked profile
   `overlapGeneration: H`, `expectedGeneration` stays G. Deploy through `deploy-private-ingest-worker.yml` at the live
   phase (`<phase>` → `<phase>-overlap`, confirmation `... OVERLAP <H>`). Leave `PRIVATE_INGEST_ORIGIN_CLAIM_FENCE` unset.
3. **Open the window** right before the apply: `ingest-generation-overlap.yml` `open`, G → H, TTL covering the apply
   plus rebind (max 4 h), `admitDuringApply` off.
4. **Apply** through `apply-session-schema.yml` (plan, then apply with the typed confirmation; `target` = the
   `062010` step, or a later one if bundled). With `admitDuringApply` off, uploads get 503 + Retry-After from the
   pending write until PASS, as long as the apply. The Worker PUT and the rebind wait are outside that interval.
5. **Rebind** G → H: `ingest-generation-overlap.yml` `rebind-dry-run`, then `rebind` bound to it. This closes the
   window. The G+H Worker's next call retries at H.
6. **Worker H**, then dormant pair (two later reviewed commits, as in §3.2 steps 3-4).
7. **Flag on.** Set `PRIVATE_INGEST_ORIGIN_CLAIM_FENCE=on` in the protected environment and deploy (a cutover control
   at the live phase; plan and deploy bind it through the staged inputs digest). Watch the Worker tail: the claim
   `55P03`/`57014` share and the retry share of `POST /v1/delivery/batches` should fall. Steps 6 and 7 can be one
   deploy, but separate deploys keep the generation move and the behaviour change independently revertible.

Rollback:

- **Flag:** removing the var restores the per-user path immediately, and the schema stays compatible both ways.
  On the private-ingest release there is no operator unbind for `INGEST_ORIGIN_CLAIM_FENCE` today: unsetting the
  variable produces a plan whose delete row the operator refuses (`native_binding_target`), so this rollback
  needs a separately reviewed operator change that admits that unbind first, then unset the variable and deploy.
- **Schema:** `supabase/rollbacks/20261105062010_ingest_origin_claim_fence.down.sql` (checksum-pinned, migrator
  lock, refuses while a later step is applied) restores `058010`'s exact `ingest_lock_fences` and drops the new
  objects. A flag-on Worker then falls back on `42883`.

## 6. Limits

- **The global seat clock still serializes staged ACKs** (`NO KEY UPDATE`, every user). Two machines' commits that both
  allocate seats still queue on it, briefly (the ACK's seat loop), as two different users' commits do today. Claims
  do not wait on a staged ACK's `NO KEY UPDATE` of the clock (their `KEY SHARE` is compatible with it); they can wait on
  a control's `FOR UPDATE` of it, and test O3 proves a held commit does not block another origin's claim. A claim with the
  SAME origin key can wait indirectly: the commit wrapper takes the origin advisory lock before the ACK queues on the
  clock. How much of the production claim 57014 share that explains has not been established; see the runbook section
  "Claim 57014 convoy" (`runbooks/history-620-operations.md`) for the dated observations.
- **Per-user slots** (`ingest_acquire_slot`, `maxPerUser` = 4 in production) still cap a user's concurrent
  deliveries. A user with more than 4 active machines queues there (429, no DB lock), not on the fence.
- **Canonical mode** and the self-host server are unchanged (per-user `FOR UPDATE`).
- A per-user control (selection, reset, consent/privacy authority, erase) still waits for every in-flight claim of
  the user and blocks all of them. That is intended, and these operations are rare.
- **Starvation.** A steady stream of overlapping `FOR SHARE` claims from several machines can delay a per-user
  `FOR UPDATE` control of that user (selection, reset). PostgreSQL can grant a new `FOR SHARE` alongside existing share
  holders without queueing behind a waiting `FOR UPDATE`, so in theory overlapping claims could keep the control
  waiting. In practice it is bounded by the per-user slot cap (4 concurrent deliveries) and seconds-long claims, and
  the control's own `lock_timeout` surfaces it as a retryable 55P03. The operator
  erase is not exposed to this: it takes `(961996,50000)` exclusive first, and every claim needs that lock shared
  (the fence-row INSERT trigger) before its row lock.
- **Vacuum.** Every origin-scoped call inserts and deletes one row in `ingest_origin_claim_marks`. The table stays
  tiny but churns, which autovacuum handles. Concurrent `FOR SHARE` holders of one fence row create MultiXacts;
  watch `mxid_age` and the autovacuum multixact freeze if a single user is extremely hot.

## 7. Tests

- `src/session-ingest/__tests__/origin-claim-fence.postgres.test.ts` (real PostgreSQL, opt-in
  `SESSION_INGEST_TEST_PG_URL`; added to `pg-proof.yml`, matrix 15/16/17):
  - O1: two origins of one user claim concurrently; the per-user function queues the second (`55P03`): the production failure.
  - O2: the same origin serializes on the origin lock and proceeds after the first commits; under a short lock wait it gives up (`55P03`).
  - O3: a held origin-scoped commit (staged ACK) does not block another origin's claim.
  - O4: a new user's first claims from two machines: one seal adoption; a rolled-back first lets the waiter adopt.
  - X1: per-user control locking (`ingest_lock_fences` without a mark) and origin-scoped claims exclude each other in both directions; other users are unaffected.
  - X2: operator user erase begin waits for an in-flight claim, an open erase blocks a claim from another machine, then RH041 for every origin.
  - X3: a mark downgrades only its own tenant in its own transaction. Under a mark for U, `ingest_lock_fences(V)` is exclusive, and without a mark of its own a transaction is exclusive.
  - D1: flag-on end-to-end deliveries of two machines concurrently, replay through the per-user path, no mark left.
- `src/session-ingest/__tests__/origin-claim-fence.pglite.test.ts`: ACLs and SECURITY DEFINER, mark table owner-only
  (no forgery), no tenant columns, malformed input refused before any mark, flag-on routing of claim/stage/commit,
  RH041 unchanged, `42883` fallback (also inside the lock-wait transaction), one structured warning per switch, hint
  expiry after 60 s (fake clock), an inner-body 42883 not swallowed, other errors not retried, Worker env parsing.
- `src/hoopdb/__tests__/sessions-function-boundary.test.ts`: lane ACLs and the generated rollback restoring `058010`'s exact body.
- `src/hoopdb/__tests__/migration-profile.test.ts`: `062010` rolls back first (062000's down refuses under it), restores the exact body, drops its objects, refuses a rerun; the migrator reapplies it.
- `stacks/cloudflare/__tests__/session-ingest.test.ts`, `private-ingest-cutover.test.ts`: the stack input and the cutover control.
