# Factory acquisition owner (dormant)

The spec is `docs/factory-acquisition-owner-design.md` (PR #314). It defines one owner per enrolled `(org, workspace, source, session)`, and only a writer that presents that owner's grant may upload or publish the session. This branch ships the database contract, the edge refusal and the role. Everything is **dormant behind a default-off flag**: with the flag off, applying the migration changes no answer for any source and every hooked path takes the same locks and failure paths as on main, except for the residual costs listed under "Flag" below. The migration is therefore not literally zero-behaviour-change, but it adds no new refusal, row lock or advisory lock while the flag is off. Nothing here is applied, flagged, enrolled, deployed or given a login by this repository.

## Pieces

**Migration `20261105052000_ingest_acquisition_owner.sql` (provisional registry slot 052000; the registry owner may renumber it).**
- It adds ten tables:
  - the `ingest_acquisition_switch` singleton, with `install_enabled = false`;
  - the `ingest_acquisition_hooks_switch` singleton, with `enabled = false` (the hooks flag, below);
  - the `ingest_acquisition_enrollment_gate` singleton;
  - `ingest_acquisition_enrollment`, which is empty;
  - owner fence rows;
  - grants and grant origins;
  - the revoke-before-install and revoked-lineage fences;
  - the idempotency ledger of operations.
- RLS is on for every new table, and every table and function is revoked from `PUBLIC`.
- Control functions (`SECURITY DEFINER`):
  - `ingest_acquisition_install`, `_revoke` and `_rebind_origin` answer `{outcome, ...}` or raise `acquisition: <code>` (design §3).
  - `ingest_acquisition_status` returns an array.
  - Every bigint is a decimal string, and absent optional fields are JSON `null`.
- Owner-only functions, granted to **no role**:
  - `ingest_acquisition_enroll_begin` and `_enroll_complete` (design §4.4, §4.8);
  - `ingest_acquisition_set_hooks(boolean)`, the flag setter;
  - `ingest_acquisition_hooks_live()`, the flag read used by the internals.
- Hooks (design §5): eight current function bodies are re-created with only the acquisition calls inserted.
  - From `20261104055000`: `ingest_claim_batch_mode`, `ingest_claim_buffered_batch`, `ingest_prepare_buffer_stage` and `ingest_commit_buffered_batch`.
  - From `20261104060000`: `ingest_commit_batch`, `ingest_publication_claim`, `ingest_publication_publish` and `ingest_publication_guard_pointer`.
  - With the flag off, every hook-side internal returns before taking any lock: no enrollment-gate row lock, no advisory lock, no admission. A request carrying an `acquisition` key is refused as malformed (`22023`), exactly as on main.
  - With the flag on, a source with no enrollment row behaves as on main except for a shared row lock on the enrollment gate, which is busy (`40001`) only while an owner-run `enroll_begin` waits.
  - `acquisition-owner.postgres.test.ts` runs the same workload three times (flag off, flag on with nothing enrolled, no migration) and proves byte identity of every ledger table, minus the new columns, plus every RPC result. A separate test proves that with the flag off, writes proceed while the gate row is locked.
- Columns added to existing tables (constant defaults, no rewrite):
  - `ingest_claim_attempts.acquisition jsonb DEFAULT '[]'`;
  - `ingest_publication_heads.acquisition_epoch bigint DEFAULT 0`.
  - Their CHECKs are added `NOT VALID` (enforced for every new or updated row), so the `ACCESS EXCLUSIVE` lock is never held for a table scan. The file sets `SET LOCAL lock_timeout = '5s'`, so the migration fails fast instead of queueing live traffic behind it. It stays one transaction.
  - Operator step, no slot, after apply (needs the same OK as the apply): `ALTER TABLE sessions.ingest_claim_attempts VALIDATE CONSTRAINT ingest_claim_attempts_acquisition_check;` and `ALTER TABLE sessions.ingest_publication_heads VALIDATE CONSTRAINT ingest_publication_heads_acquisition_epoch_check;` (`SHARE UPDATE EXCLUSIVE`; it does not block reads or writes).

**Flag (`sessions.ingest_acquisition_hooks_switch.enabled`, default `false`).**
- Set it only with `SELECT sessions.ingest_acquisition_set_hooks(true|false)`, as the owner. No role may call it.
- The setter takes `ACCESS EXCLUSIVE` on the one-row switch table, so it waits for every transaction that read the flag. It answers `55000 hooks_not_quiescent` if they do not finish within 5s. No hook can straddle a flip.
- `enroll_begin` refuses `55000 hooks_disabled` while the flag is off, and holds the flag row `FOR SHARE` while it runs.
- Turning the flag off is refused with `55000 hooks_enrolled` while any enrollment row exists. Data alone never arms a hook, and an enrolled source is never silently unprotected.
- Residual cost while the flag is off:
  - every hooked call reads the flag, taking `ACCESS SHARE` on the switch table; this waits only while the owner flips the flag (at most the setter's 5s) or runs DDL on that table;
  - a caller at `REPEATABLE READ` or `SERIALIZABLE` reads the flag row `FOR SHARE` (a row lock), so a flag flipped after its snapshot raises PostgreSQL's native `40001` instead of returning a stale value. Under `READ COMMITTED` (every ingest RPC) there is no row lock.

**Roles.**
- `bulls96_acquisition_installer` is created `NOLOGIN NOINHERIT` by `supabase/bootstrap/session_ingest_roles.sql` and its RDS and Cloud SQL variants.
- The migration gives it `USAGE` on `sessions` and `EXECUTE` on install, revoke, rebind and status only. It gets no table right and no enrollment right.
- `bulls96_session_ingest` gets nothing new.
- `acquisition_installer_role.sql` grants LOGIN. `acquisition_installer_verify.sql` checks the role; it is generated by the function-boundary lane.

**Edge (design §5.6).**
- Any acquisition refusal of a batch is answered with HTTP 409 `{error:{code:"acquisition_refused",...},refused:[{source,sessionId,code}]}`.
- It is never `delivery_conflict`, another 4xx, 429 or a partial 2xx.
- See `src/session-ingest/acquisition-refusal.ts`. The parser is closed: anything other than our exact message, SQLSTATE and bounded DETAIL stays the ledger's 503.
- `GET /v1/delivery/limits` is unchanged: this branch adds no field to it. How Desktop learns that a source is enrolled is left to the rollout PR that arms the flag.
- The publication ticket carries an optional `acquisitionEpoch`, which is absent for unenrolled sources and whenever the flag is off, so the ticket is unchanged.

## What is dormant, and the OK that unlocks each step

Each step needs Khaliq's separate, explicit OK (AGENTS.md). They are listed in order; none is implied by an earlier one.

1. **Apply the migration** to a real database.
   - Unlocks: nothing user-visible. The flag is off, install refuses `disabled`, and no source is enrolled.
   - Run the role bootstrap first; otherwise the migration fails closed with `42704`.
   - If it answers `55P03` (lock_timeout on a hot table), retry at a quieter time; nothing was applied.
   - Afterwards, run the two `VALIDATE CONSTRAINT` statements above.
2. **Installer login.** Run `acquisition_installer_role.sql`, with `acquisition_installer_verify.sql` before and after.
   - The password comes from 1Password into memory, never onto disk.
   - Unlocks: the Factory binding service / History exchange route (design §8 Q1-Q3, not built) can call install, revoke, rebind and status.
   - While the switch is off, install still refuses `disabled`. Revoke can still record revoke-before-install targets.
3. **Enable the switch** (`UPDATE sessions.ingest_acquisition_switch SET install_enabled = true`, as the owner).
   - Unlocks: installs for fresh sessions.
   - Nothing is enforced until a source is enrolled. Until §7's drain-receipt producer ships, handover returns `handover_not_ready`.
4. **Arm the hooks.** As the owner, `SELECT sessions.ingest_acquisition_set_hooks(true)`.
   - Unlocks: the hook-side acquisition steps (gate row lock, advisory locks, admission) for every hooked call, and `enroll_begin`. Unenrolled sources still answer as on main.
   - The `acquisition` request key is accepted from here on (empty or absent for unenrolled sources).
5. **Enroll `(org, workspace, droid)`.** As the owner, run `ingest_acquisition_enroll_begin`, then `ingest_acquisition_enroll_complete`.
   - Unlocks: enforcement for that tuple. Every known session is backfilled `unknown` and refused, and every write needs the current grant.
   - Prerequisites (design §8 Q6, Q8, P1, P2):
     - Desktop ships a `droid` source with install support and handles the 409;
     - the issuer is built;
     - no non-`srv_` `droid` pointer writer exists, because the pointer guard refuses one.
   - `enroll_begin` waits for in-flight writers. It answers `enrollment_not_quiescent` if they do not drain within `lock_timeout`.

## Rollback

- **Dormant installation** (flag off; nothing enrolled, installed, revoked or enabled): run `supabase/rollbacks/20261105052000_ingest_acquisition_owner.down.sql`. It is generated by the lane.
  - It restores the eight hook bodies byte-for-byte from their source migrations.
  - It drops the new tables, functions and columns, and removes the ledger row.
  - It refuses with `55000` in any of these cases:
    - a later migration is applied;
    - the applied checksum is not its own;
    - the install switch is on;
    - the hooks flag is on (turn it off with `set_hooks(false)` first; that is refused while anything is enrolled);
    - any acquisition row exists;
    - any head has `acquisition_epoch <> 0`;
    - any attempt has an acquisition snapshot.
- **After anything was enrolled or installed**, the owner rows are the fence (design §4.6). The operational rollback is:
  - set `install_enabled = false`;
  - enroll nothing new;
  - leave the hooks flag on (it cannot be turned off while an enrollment row exists).
  - Deleting an enrollment row is an owner data change, not a rollback, and needs its own OK.

## Where the provisional slot appears

If the registry renumbers `20261105052000`, change it in all of these places:
- the migration and rollback file names;
- `MIGRATION` in `scripts/sessions-function-boundary/lanes/acquisition-owner.ts`;
- `src/hoopdb/migration-sentinels.ts`;
- the migration header comment;
- the comment in `supabase/bootstrap/acquisition_installer_role.sql` and `session_ingest_roles.sql`;
- `src/hoopdb/__tests__/sessions-function-boundary.test.ts` and `migration-profile.test.ts`;
- `runbooks/session-directory-search-worker-bootstrap.md`;
- `MIGRATION` in `src/session-ingest/__tests__/acquisition-owner.postgres.test.ts`;
- this file.

Then run `bun scripts/sync-sessions-function-boundary.ts`. The generated rollback embeds the migration checksum.
