# Supabase incremental upgrade: generation 1d99ccc8 → eff8b3be

Target: production Supabase project `pyamkrhoyszgzirqqffq` (PostgreSQL 17.11), database
`postgres`. Plan for review only. **Nothing in this document has been run.** Every step marked
**W** is a write. Each write needs Khaliq's OK, under AGENTS.md, in the session that runs it. The
executor is cloud-handoff (per Khaliq's decisions, `supabase-upgrade-decisions.md`).
helm-chart-chief-claude and the migration lane review this exact text before any write.

## 1. Starting state (read-only audit at fe5a2da, 2026-11-02T01:28Z)

The database is already activated. It was not created by this plan.

- **Activation:** generation `1d99ccc8…94718`, ledger 17/17, marker `pass`. It was activated on
  2026-11-01 at 17:56Z from `b47c1e6` with `HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke`.
- **Roles:**
  - `bulls96_runtime`: LOGIN, holds USAGE on `sessions` and SELECT on the ledger and the marker
    only. The #116 runtime application ACL is **not** installed.
  - `session_directory_search_worker`: present.
  - Both carry the #106 `postgres` creator edge.
- **Data:** about 127k rows in `sessions`. The catch-up loop writes as `postgres`.
- **`postgres` global function default:** `{postgres=X/postgres}`, installed by the earlier
  activation. This is the project-wide default revoke.
- **Event triggers:** none of ours. Six platform triggers exist, all owned by `supabase_admin`.
- **supautils:** `privileged_role=supabase_privileged_role`. `postgres` inherits it (inherit t, set
  t). `log_statement=ddl`, and `pgaudit.log=none`.
- **Backups:** PITR is off. The latest physical backup (05:31Z) predates the activation.

Why the #121 Lambda cannot run today: it connects as `bulls96_runtime`, and that role has no
EXECUTE on `upsert_session_index` and no table DML grants. Those grants are the #116 runtime
application ACL. Only the migrator installs it (`installRuntimeApplicationAcl`, `migrate.ts`).

## 2. Code to run

Use origin/main at **`ab3ee7f`** (the #137 merge) or later. It contains #125 `9dca95f`, #139
`fe5a2da` and #137.

**#137 is required.** Before #137, the exact-ACL drift check ran only in `event-trigger` mode, so a
Stage A migrate under `global-default-revoke` from `fe5a2da` or `84a962c` would skip it. From
`ab3ee7f` it runs in every non-local mode.

#137 changed only `migrate.ts`, `migrate-guards.ts`, tests and a comment in the trigger bootstrap
header (outside the function body, so the frozen prosrc sha is unchanged). It changed no migration
or verifier, so the generation is unchanged. So:

- `bun src/hoopdb/migrate.ts --generation` must print
  `eff8b3be46565b57ad8fa5e4a066dbb6d1fe608313d9c1680bfbcd484add259b`. On any other value, stop.
- Run it from a clean worktree at `ab3ee7f` or later with `bun install --frozen-lockfile`.
- Rebuild the images to deploy afterwards from `ab3ee7f` or later. Images from `fe5a2da` have the
  same generation, but their migrator lacks the drift check outside event-trigger mode, so do not
  use them to run the migrator.
- `--activation-check` gives no read-only preview of the drift check: the drift check runs only
  inside the migrator's validation transaction. That is a known gap; a follow-up will add the
  preview. So the first evidence of drift is a refusal at A2, which rolls back and leaves the
  marker `pending`.

The only migration still pending is `20261101050000_session_directory_list.sql` (#124). The
`20261030020000` ledger row on production must already equal `b0ddc7de…0758ce`, because
`b47c1e6` includes #106. So the #106 checksum alias does not apply; P4 checks this.

## 3. Why two stages

The migrator cannot install the #116 ACL on its own. Every migrate applies all pending steps, then
installs the runtime ACL and runs the exact-ACL drift check (every non-local mode from #137
onwards), all in one validation transaction. So:

- **Stage A** runs the new code under the boundary that is **already proven** on this database
  (`global-default-revoke`). It applies #124 and installs the #116 ACL. This unblocks #121 without
  waiting for the event trigger.
- **Stage B** installs the event trigger and switches the boundary mode. Its migrate is a no-op
  that re-proves the catalog under `event-trigger`.

Stage B depends on Stage A, but Stage A does not depend on Stage B. Stage A can land alone, and
Stage B can follow in a separate window.

## 4. Pre-flight (read-only; run immediately before Stage A)

Run every block as `BEGIN READ ONLY; SET LOCAL statement_timeout='5s'; SET LOCAL
lock_timeout='1s'; … ROLLBACK;` with role `postgres`. Use TLS `verify-full` with the pinned CA. Do
not print any connection string or secret.

| id  | check                                                                                                                                           | expected                                                                                                                                                       |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P1  | `SELECT count(*), max(applied_at) FROM sessions.__96bulls_migrations`                                                                           | 17. Nothing applied after 2026-11-01T17:56:13Z                                                                                                                 |
| P2  | `SELECT name, generation, status FROM sessions.__96bulls_role_validation`                                                                       | `session_directory_search_worker`, `1d99ccc8…`, `pass`                                                                                                         |
| P3  | `SELECT name FROM sessions.__96bulls_migrations WHERE name LIKE '%20261101050000%'`                                                             | 0 rows                                                                                                                                                         |
| P4  | `SELECT sha256 FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261030020000_session_directory_short_summary.sql'`                   | `b0ddc7de8c4cff9454638c52563f37f1e1380ed577e2a7535f67ddc2c0b758ce`                                                                                             |
| P5  | every ledger sha256 against the file at `ab3ee7f`                                                                                               | 17/17 equal                                                                                                                                                    |
| P6  | P12 baseline md5s from the fe5a2da audit (roles, memberships, default_acl, db_role_settings, database ACL, schemas, extensions, event_triggers) | unchanged. On any difference, stop and re-audit                                                                                                                |
| P7  | `pg_locks` for advisory `(961996,1)`, and non-idle sessions other than ours                                                                     | 0 advisory locks. Only the catch-up loop (until W1)                                                                                                            |
| P8  | `bun src/hoopdb/migrate.ts --activation-check` with `HOOPDB_DB_MODE=supabase HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke` (read-only)   | exit 0, `satisfied=true`. Only `supabase_admin` under `excluded_platform_superusers`. Every `sessions` CREATE role covered. `migration_checksum_aliases` empty |
| P9  | the #121 Lambda, the Worker and every reader are scaled to zero or paused; the catch-up loop is still running                                   | recorded by the executor                                                                                                                                       |

## 5. Stage A: new code, #116 runtime ACL (boundary unchanged)

| step | kind | action                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | stop if                                                            |
| ---- | ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| A0   | R    | **Restore point (Khaliq decision 2).** Run `pg_dump --schema=sessions --format=custom` as `postgres`. It only reads the database. Write it only to tmpfs. Record its sha256 and size, then upload it to the location Khaliq names: **an open decision. The dump holds tenant session metadata, so it must go to a KMS-encrypted bucket, not to a laptop disk.** Record that S3 can rebuild the index (the backfill script with the done logs) as the second restore path.                                                                                   | dump fails; sha not recorded                                       |
| A1   | W    | Pause the catch-up loop: `touch ~/.agent-tmp/supa-prod/CATCHUP_STOP`. Wait for "stopped" in `catchup.log`, then re-run P7.                                                                                                                                                                                                                                                                                                                                                                                                                                  | no "stopped" line within 15 minutes                                |
| A2   | W    | Run `bun src/hoopdb/migrate.ts` from `ab3ee7f` with `HOOPDB_DB_MODE=supabase`, `HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke`, `HOOPDB_RUNTIME_ROLE=bulls96_runtime`, `HOOPDB_RUNTIME_ROLE_LOGIN=true` and `HOOPDB_SUPABASE_SHIM=false`. `HOOPDB_ROLE_VERIFY` stays the default (true). The migrator writes the marker `pending` first; applies `20261101050000`; installs the #116 runtime ACL; runs phase 2, the runtime allowlist and the exact-ACL drift check; then writes the marker `pass` for `eff8b3be`, all in one validation transaction. | any non-zero exit. A refusal leaves the marker `pending`: go to §8 |
| A3   | R    | Post-checks: ledger 18/18. Marker `pass` for `eff8b3be`. `--activation-check` (same env) exits 0. No `sessions` function is executable by PUBLIC. `bulls96_runtime` holds exactly the #116 ACL (EXECUTE on `RUNTIME_APPLICATION_FUNCTIONS`, `RUNTIME_APPLICATION_TABLE_PRIVILEGES`, sequences, RLS policies), and the migrator's own validation passed. P6 diff: only `sessions` objects and `bulls96_runtime` grants changed, with no change to roles, memberships, default ACLs or event triggers.                                                        | anything else                                                      |
| A4   | W    | Restart the catch-up loop **on the new code**. The old loop's worktree is `b47c1e6`. If its handler path checks the schema verdict, it is now not-ready against `eff8b3be`. So restart it from an `ab3ee7f` worktree: `rm -f CATCHUP_STOP`, then the documented `CATCHUP_SINCE=<A1 stop time minus 2 min>` restart. Confirm one pass logs `processed ≈ to_process` and no errors.                                                                                                                                                                           | errors in its first pass                                           |
| A5   | W    | Re-enable the #121 Lambda on images with generation `eff8b3be`. Confirm that one real object-landed event indexes as `bulls96_runtime` and that RLS still isolates tenants (the handoff's `rls-check.sql`).                                                                                                                                                                                                                                                                                                                                                 | permission errors                                                  |

**Duration and locking (A2):**

- `ALTER TABLE sessions.session_directory ADD COLUMN …` adds eight columns. Seven of them have
  CHECK constraints, so the table is scanned once to validate them (about 127k rows).
- `CREATE INDEX session_directory_tenant_project_text_trgm_idx` is a GIN trigram index over the
  same rows, built non-concurrently.
- Both hold locks that block writes to `session_directory` for their duration, expected to be
  seconds. Writers are paused (A1, P9), so nothing waits on them.

**Not in scope:** the #124 catalog backfill. After A2 the list RPC refuses until a catalog backfill
runs (`docs/session-directory-list.md`, "Backfill"). That is a separate data write that needs its
own OK.

## 6. Stage B: event-trigger boundary

| step | kind | action                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | stop if                                                                                    |
| ---- | ---- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| B0   | R    | Repeat P6 and P7, with the Stage A results as the new baseline.                                                                                                                                                                                                                                                                                                                                                                                                                                    | drift                                                                                      |
| B1   | W    | Pause the catch-up loop (A1) and the #121 Lambda.                                                                                                                                                                                                                                                                                                                                                                                                                                                  | —                                                                                          |
| B2   | W    | As `postgres`: `psql -v ON_ERROR_STOP=1 -X -v runtime_role=bulls96_runtime -d postgres -f supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql`. It is one transaction. It creates schema `hoopdb_boundary`, the trigger function (owned by `postgres`, SECURITY INVOKER) and `CREATE EVENT TRIGGER hoopdb_sessions_function_boundary`, then runs `ALTER EVENT TRIGGER … ENABLE ALWAYS`.                                                                                              | 55000 "CREATE EVENT TRIGGER was refused" (rolled back; nothing created) or any other error |
| B3   | R    | Run `--activation-check` with `HOOPDB_FUNCTION_BOUNDARY_MODE=event-trigger`.                                                                                                                                                                                                                                                                                                                                                                                                                       | not `satisfied`                                                                            |
| B4   | W    | Run `bun src/hoopdb/migrate.ts` with the A2 env and `HOOPDB_FUNCTION_BOUNDARY_MODE=event-trigger`. No step is pending, so it only re-validates (phase 2, runtime ACL and drift check) and re-marks `pass` for `eff8b3be`.                                                                                                                                                                                                                                                                          | non-zero exit                                                                              |
| B5   | W\*  | Behavioural probe, **rolled back**. Run `BEGIN; CREATE FUNCTION sessions.zz_boundary_probe() RETURNS int LANGUAGE sql AS 'SELECT 1'; SELECT has_function_privilege('public','sessions.zz_boundary_probe()','EXECUTE');` and expect `f`. Then run `GRANT EXECUTE ON FUNCTION sessions.zz_boundary_probe() TO PUBLIC;` and expect 42501, which aborts the transaction. Finish with `ROLLBACK;`. Nothing persists. \* It is still DDL on production, so it needs its own OK. Skip it if not approved. | probe executable by PUBLIC, or the GRANT accepted                                          |
| B6   | W    | Switch every deployment's `HOOPDB_FUNCTION_BOUNDARY_MODE` to `event-trigger` (Helm values and the external Supabase path). Then restart the catch-up loop and #121 as in A4/A5.                                                                                                                                                                                                                                                                                                                    | —                                                                                          |
| B7   | R    | Final checks: `--activation-check` satisfied. The trigger row is `evtenabled='A'`, owned by `postgres`, with function owner `postgres` and prosrc sha equal to the frozen constant. Each `sessions` function ACL equals the manifest. The platform event triggers are unchanged.                                                                                                                                                                                                                   | anything else                                                                              |

**supautils behaviour.** Read from source at supautils `5de3de1`. The production version is NOT
OBSERVED:

- **Who may create it:** `create_event_trigger` accepts the current role when
  `has_privs_of_role(current_user, supautils.privileged_role)`. `postgres` inherits
  `supabase_privileged_role`, so it qualifies.
- **Owner:** supautils creates the trigger as the superuser, then runs
  `alter_owner(trigname, current_user_id, ALT_EVTRIG)`. The trigger is therefore owned by
  `postgres`, which is what the gate requires (`evtowner` = the migration owner).
- **Function owner:** it must **not** be a superuser ("Non-superuser owned event trigger must
  execute a non-superuser owned function"). Ours is owned by `postgres`.
- **ENABLE ALWAYS:** supautils does not intercept `ALTER EVENT TRIGGER`, so plain PostgreSQL
  ownership rules apply. `postgres` owns the trigger and may alter it.
- **When it is skipped:** the trigger does not fire when the current role is a superuser or a
  reserved role (`supabase_admin`, `service_role`, `authenticated`, `anon` and the others listed in
  the audit). Reserved roles have no CREATE on `sessions`, and `supabase_admin` is the platform
  trust root.
- **CREATE EXTENSION:** privileged-role triggers do not fire for it. The post-migrate drift check
  and the live PUBLIC check remain.
- **If production's supautils differs:** a refusal at B2 rolls back. An owner other than `postgres`
  makes B3 fail closed. Either way, stop and re-review. Neither case needs a live experiment.

## 7. Decision for Khaliq: the `postgres` global function default

Event-trigger mode covers `postgres` without the global default: the gate's creator coverage
accepts the migration owner in event-trigger mode. So after B7 the global default could be removed
with `ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT EXECUTE ON FUNCTIONS TO PUBLIC`, which
restores PostgreSQL's built-in default.

**Recommendation: keep it.**

- It is already in place, and the audit found no non-`sessions` consumer that depends on PUBLIC
  EXECUTE for new `postgres` functions. `public` is empty, and Supabase's API roles get their
  function access from the platform's schema-scoped defaults, which this does not change.
- It is defence in depth for the cases the trigger cannot see: supautils skips, CREATE EXTENSION,
  and a disabled trigger.
- Removing it is a second project-wide privilege change, for no gain.

If Khaliq chooses to remove it, that becomes step B8 (W). It needs its own OK, and the P12
default-ACL md5 must be re-recorded afterwards.

## 8. Failure and rollback

- **The A2 or B4 migrator refuses:** the validation transaction rolls back. The marker stays
  `pending` for the new generation, so readers are not-ready, and the services are already paused.
  Record the `error_code` and the evidence JSON. Fix the cause in code, review it, and run again.
  Do not edit catalogs by hand.
- **Undo #124 after A2 succeeded:** `supabase/rollbacks/20261101050000_session_directory_list.down.sql`
  (needs its own OK), then remove the ledger row by the runbook's documented procedure. **The
  repository has no procedure for re-applying after a down** (dossier B10). Prefer forward fixes.
- **Undo Stage B:** run `DROP EVENT TRIGGER hoopdb_sessions_function_boundary; DROP SCHEMA
hoopdb_boundary CASCADE;` as `postgres`, then set the mode back to `global-default-revoke`. The
  global default is still in place unless §7 removed it. Needs its own OK.
- **Data loss:** restore `sessions` from the A0 dump, or rebuild from S3 with the backfill and
  catch-up scripts. The index is derived data.

## 9. Evidence to record per step

UTC timestamp, executor, the exact command (with secrets redacted), exit code, and the JSON from
`--activation-check` or the migrator. Afterwards, re-take the P12 baseline md5s. Never record a
connection string, password, SCRAM verifier or token.
