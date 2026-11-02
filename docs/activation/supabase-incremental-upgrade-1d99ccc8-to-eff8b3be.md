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
    only. The #116 runtime application ACL is **not** installed. **Superseded by §1a:** it was
    applied by hand at 02:04Z, except for the 5 #124 objects.
  - `session_directory_search_worker`: present.
  - Both carry the #106 `postgres` creator edge.
- **Data:** about 127k rows in `sessions`. The catch-up loop writes as `postgres`. **Superseded by
  §1a:** the loop has been stopped since 02:02:14Z.
- **`postgres` global function default:** `{postgres=X/postgres}`, installed by the earlier
  activation. This is the project-wide default revoke.
- **Event triggers:** none of ours. Six platform triggers exist, all owned by `supabase_admin`.
- **supautils:** `privileged_role=supabase_privileged_role`. `postgres` inherits it (inherit t, set
  t). `log_statement=ddl`, and `pgaudit.log=none`.
- **Backups:** PITR is off. The latest physical backup (05:31Z) predates the activation.

Why the #121 Lambda cannot run today: it connects as `bulls96_runtime`, and that role has no
EXECUTE on `upsert_session_index` and no table DML grants. Those grants are the #116 runtime
application ACL. Only the migrator installs it (`installRuntimeApplicationAcl`, `migrate.ts`).
**Superseded by §1a:** the ACL was applied by hand, minus the 5 #124 objects that A2 adds, and the
Lambda is disabled and must be redeployed (A5).

### 1a. Changes made after the audit (cloud-handoff, 2026-11-02)

Recorded in `cloud-handoff-to-migration-lane.md`. Not run by this plan:

- **02:02:14Z, catch-up loop stopped.** `CATCHUP_STOP` was touched and the running pass was
  TERM'd. The interrupted pass is `2026-11-01T23:41:28`, which was re-covering every manifest
  (127,829). Nothing writes to `sessions` now.
- **02:03:24Z, read-only `pg_dump --schema=sessions -Fc`** (about 40 MB). It sits on linuyc's local
  disk under `~/.agent-tmp/supa-prod/dumps/`. That conflicts with the A0 rule (§5): move it to the
  encrypted location, then delete the local copy.
- **02:04:27Z, #116 runtime ACL applied by hand** as `postgres`, in one transaction of 71
  statements: the `installRuntimeApplicationAcl` constants at `84a962c`, minus the 5 objects that
  #124 creates (the tables `session_directory_agents` and `session_directory_relationships`, the
  function `session_directory_project_text(text,text,text)`, and their 2 policies).
  - It ran an in-transaction post-check: 134 privilege checks and 23 policies, all passing.
  - Approval: recorded at the time as Khaliq's pre-approval, relayed by self-hosted-lead
    (`tonight-search-cutover.md`). **Khaliq then confirmed it directly to helm-chart-chief-claude on
    2026-11-02: the 02:04Z apply was approved and stays.**
  - Snapshot at 02:07Z: `~/.agent-tmp/bulls96_runtime-grants-now.txt` on linuyc (cloud-handoff).
- **#121 Lambda:** the deployed image is built from `cc37755`, which **fails the §2 image gate**. Its
  handler has no schema-verdict gate. The SQS mapping and the reconcile rule are disabled.
- **New images (main `7d7229b`, a descendant of `ab3ee7f`, generation `eff8b3be`):**
  - GHCR `ghcr.io/agentworkforce/96bulls-services@sha256:6f22ea17dff336e1431664542759fd27958649388b90a0b2f0881883caa7c6b2`;
  - ECR `500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services@sha256:5bb6075240afa02a1b1797a81c07bffa029954eac7803d702ecba20db35d4db9`.

**Effect on A2.** The migrator's runtime pre-check (`runtimeRoleViolations`, phase `pre`) refuses
privileges **outside** the allowlist, but it does not require the full set. Missing grants are
checked only in phase `post`. So the hand-applied subset passes the pre-check, and
`installRuntimeApplicationAcl` adds the 5 #124 objects. The post-check and the drift check then see
exactly the code's ACL. If the hand-applied set contains anything outside the code's constants, A2
refuses before any write. P10 checks for that.

**Out of scope:** the read API role `bulls96_read_api` (#135). Its bootstrap fails on Supabase PG17,
because `ALTER ROLE … NOSUPERUSER` needs a superuser. It rolled back and created nothing. That is a
code fix for a separate PR, followed by its own plan.

## 2. Code to run

Use origin/main at **`ab3ee7f`** (the #137 merge) or later. It contains #125 `9dca95f`, #139
`fe5a2da` and #137.

**#137 is required.** Before #137, the exact-ACL drift check ran only in `event-trigger` mode, so a
Stage A migrate under `global-default-revoke` from `fe5a2da` or `84a962c` would skip it. From
`ab3ee7f` it runs in every non-local mode.

#137 changed only `migrate.ts`, `migrate-guards.ts`, tests and a comment in the trigger bootstrap
header (outside the function body, so the frozen prosrc sha is unchanged). It changed no migration
or verifier, so the generation is unchanged. So:

- **Code gate:** the generation alone cannot tell the disallowed `fe5a2da`/`84a962c` code from
  allowed code, because all of them are `eff8b3be`. So, in the worktree that runs every command
  below:
  - `git merge-base --is-ancestor ab3ee7ff21e88c497fee862015408f9ad911e048 HEAD` must exit 0;
  - `git status --porcelain` must be empty;
  - `bun install --frozen-lockfile` must succeed;
  - `bun src/hoopdb/migrate.ts --generation` must print
    `eff8b3be46565b57ad8fa5e4a066dbb6d1fe608313d9c1680bfbcd484add259b`.

  If any one fails, stop.

- **Image gate (A5, B6):** deploy only image digests whose `org.opencontainers.image.revision`
  label is a commit that has `ab3ee7f` as an ancestor (check it with the same `merge-base`
  command). Record each digest. Images built from `fe5a2da` or `84a962c` are not allowed, even
  though they show the same generation.
- `--activation-check` now previews the drift check read-only (with `HOOPDB_RUNTIME_ROLE` set):
  `function_acl_drift_extra` lists grants the migrator would refuse (exit 1), and
  `function_acl_missing` lists grants it would add (exit 0).

**Environment for every migrator command** (names only; values come from 1Password through
`op run` or tmpfs, are never printed and are never written to disk):

- **E-A (Stage A, A2/A3/P8):**
  - `HOOPDB_URL` (role `postgres`, TLS `verify-full` with the pinned CA);
  - `HOOPDB_DB_MODE=supabase`;
  - `HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke`;
  - `HOOPDB_RUNTIME_ROLE=bulls96_runtime`;
  - `HOOPDB_RUNTIME_ROLE_LOGIN=true`;
  - `HOOPDB_SUPABASE_SHIM=false`;
  - `HOOPDB_ROLE_VERIFY` unset (default true);
  - `HOOPDB_ROLE_BOOTSTRAP` unset (it is refused in supabase mode);
  - `HOOPDB_RUNTIME_WORKLOAD_ROLE` unset;
  - `HOOPDB_FUNCTION_BOUNDARY_APPROVED` unset (it is refused).
- **E-B (Stage B, B3/B4/B7):** E-A with `HOOPDB_FUNCTION_BOUNDARY_MODE=event-trigger`.

Without `HOOPDB_RUNTIME_ROLE`, `--activation-check` returns `satisfied=false`. That is a spurious
stop, so always use the full set.

The only migration still pending is `20261101050000_session_directory_list.sql` (#124). The
`20261030020000` ledger row on production must already equal `b0ddc7de…0758ce`, because
`b47c1e6` includes #106. So the #106 checksum alias does not apply; P4 checks this.

## 3. Why two stages

The migrator cannot install the #116 ACL on its own. Every migrate applies all pending steps, then
installs the runtime ACL and runs the exact-ACL drift check (every non-local mode from #137
onwards). It does this in separate transactions: each pending migration commits on its own, with
its ledger row, and then the validation transaction runs phase 2, the runtime ACL, the drift check
and the marker `pass` (§8 covers the state after a partial failure). So:

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

| id  | check                                                                                                                                           | expected                                                                                                                                                                                      |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P1  | `SELECT count(*), max(applied_at) FROM sessions.__96bulls_migrations`                                                                           | 17. Nothing applied after 2026-11-01T17:56:13Z                                                                                                                                                |
| P2  | `SELECT name, generation, status FROM sessions.__96bulls_role_validation`                                                                       | `session_directory_search_worker`, `1d99ccc8…`, `pass`                                                                                                                                        |
| P3  | `SELECT name FROM sessions.__96bulls_migrations WHERE name LIKE '%20261101050000%'`                                                             | 0 rows                                                                                                                                                                                        |
| P4  | `SELECT sha256 FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261030020000_session_directory_short_summary.sql'`                   | `b0ddc7de8c4cff9454638c52563f37f1e1380ed577e2a7535f67ddc2c0b758ce`                                                                                                                            |
| P5  | every ledger sha256 against the file at `ab3ee7f`                                                                                               | 17/17 equal                                                                                                                                                                                   |
| P6  | P12 baseline md5s from the fe5a2da audit (roles, memberships, default_acl, db_role_settings, database ACL, schemas, extensions, event_triggers) | unchanged. On any difference, stop and re-audit                                                                                                                                               |
| P7  | `pg_locks` for advisory `(961996,1)`, and non-idle sessions other than ours                                                                     | 0 advisory locks. No other non-idle session (the loop has been stopped since 02:02:14Z)                                                                                                       |
| P8  | `bun src/hoopdb/migrate.ts --activation-check` with env **E-A** (read-only), after the §2 code gate                                             | exit 0, `satisfied=true`. Only `supabase_admin` under `excluded_platform_superusers`. Every `sessions` CREATE role covered. `migration_checksum_aliases` empty                                |
| P9  | the #121 Lambda, the Worker and every reader are scaled to zero or paused; the catch-up loop has been stopped since 02:02:14Z (§1a)             | recorded by the executor                                                                                                                                                                      |
| P10 | `bulls96_runtime` grants: a read-only dump in the same format as the 02:07Z snapshot                                                            | identical to the 02:07Z snapshot `~/.agent-tmp/bulls96_runtime-grants-now.txt` (linuyc), and every entry inside the code's `RUNTIME_APPLICATION_*` constants at `7d7229b`. On any extra, stop |

## 5. Stage A: new code, #116 runtime ACL (boundary unchanged)

Run the steps in table order: **A1, then A0, then A2–A5**. A1 and the dump itself were already done by cloud-handoff (§1a). A1 is verify-only, and A0 only moves the existing dump.

| step | kind | action                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | stop if                                             |
| ---- | ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------- |
| A1   | R    | **Already done (02:02:14Z, §1a). Verify only.** No `catchup-loop.sh` process exists (`ps -eo pid,pgid,args \| grep '[c]atchup-loop.sh'` is empty), `CATCHUP_STOP` is present, and `pg_stat_activity` shows no session other than ours. Record the interrupted pass id `2026-11-01T23:41:28` and its `since` from its `catchup.log` start line; A4 needs it. If the loop is found running again, use the 30-minute STOP-then-TERM procedure from revision 13442d4 of this plan.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | a loop process or another writer is present         |
| A0   | W    | **Restore point.** A 02:03:24Z dump already exists, taken after the loop stopped. Nothing has written to `sessions` data since; the 02:04Z ACL change is grants only. Move it, do not re-take it: upload it to a **separate private bucket or prefix with its own admin-decryptable KMS key**. Khaliq names it; that is an **open decision**. It must **not** go to the transcript bucket: noncurrent versions there expire after 1 day, the eraser sweeps `96bulls/`, and its KMS key denies admin decrypt (self-hosted-lead). Verify the uploaded object's size and sha256 against the local file, then delete the local copy under `~/.agent-tmp/supa-prod/dumps/` and record the deletion. The second restore path is a rebuild from S3.                                                                                                                                                                                                                                                                                                                       | upload fails; sha mismatch                          |
| A2   | W    | Run `bun src/hoopdb/migrate.ts` from the gated worktree (§2) with env **E-A**. What it does: writes the marker `pending` under its advisory lock; applies `20261101050000` **in its own transaction**, which commits with ledger row 18; then, in the validation transaction, revokes PUBLIC, installs the #116 runtime ACL, and runs phase 2, the runtime allowlist and the exact-ACL drift check; then writes the marker `pass` for `eff8b3be`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | any non-zero exit: go to §8 (partial-failure state) |
| A3   | R    | Post-checks: ledger 18/18. Marker `pass` for `eff8b3be`. `--activation-check` (same env) exits 0. No `sessions` function is executable by PUBLIC. `bulls96_runtime` holds exactly the #116 ACL (EXECUTE on `RUNTIME_APPLICATION_FUNCTIONS`, `RUNTIME_APPLICATION_TABLE_PRIVILEGES`, sequences, RLS policies), and the migrator's own validation passed. P6 diff: only `sessions` objects and `bulls96_runtime` grants changed, with no change to roles, memberships, default ACLs or event triggers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | anything else                                       |
| A4   | W    | **Restart the catch-up loop on its current code.** `backfill-pointers.ts` does not check the schema verdict, and `catchup-loop.sh` hard-codes its `b47c1e6` worktree (`W=`). It writes as `postgres` through `upsert_session_index`, which works against the new schema: the new #124 columns have defaults, and the catalog columns stay empty, as they do for every row until the separate catalog backfill. So **do not** edit `W=` in this window. Run `rm -f CATCHUP_STOP`, then the documented restart with `CATCHUP_SINCE` set to the **`since` of the interrupted pass** (from its `catchup.log` start line). If A1 ended with a clean `stopped` line, use the **start of the last completed pass minus 2 minutes** instead. Never use T minus 2 minutes: an interrupted pass may have been re-covering a window that began much earlier (the 23:41Z pass re-covered from 19:38Z). Confirm one pass logs `processed ≈ to_process` and no `error:` counts. Moving the loop to new code is not needed: per the handoff, it retires once #121 is reconciling. | errors in its first pass                            |
| A5   | W    | Redeploy the #121 Lambda from main `7d7229b` (the ECR digest in §1a; its revision label descends from `ab3ee7f`), replacing the `cc37755` image. Record the digest. Then re-enable its SQS mapping and reconcile rule. Confirm that one real object-landed event indexes as `bulls96_runtime` with no permission error, and that RLS still isolates tenants (the handoff's `rls-check.sql`). The handler has no schema-verdict gate, so it must not be re-enabled before A3 passes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | permission errors; the image fails the gate         |

**Duration and locking (A2):**

- `ALTER TABLE sessions.session_directory ADD COLUMN …` adds eight columns. Seven of them have
  CHECK constraints, so the table is scanned once to validate them (about 127k rows).
- `CREATE INDEX session_directory_tenant_project_text_trgm_idx` is a GIN trigram index over the
  same rows, built non-concurrently.
- Both hold locks that block writes to `session_directory` for their duration, expected to be
  seconds. Writers are paused (A1, P9), so nothing waits on them.

**Other lanes (not in this plan):** #137's exact-ACL drift check now runs at every non-local
migrate. Before the next migrate of the AWS-proof CNPG or the GCP Cloud SQL databases, their owners
run `--activation-check` read-only there, with `HOOPDB_RUNTIME_ROLE` set: its
`function_acl_drift_extra` must be empty (§2).

**#121 ownership:** cloud-handoff owns the #121 Lambda. Its SQS mapping is paused today because of
a secret-format bug, and it stays paused until A2 marks `pass`. Its image must pass the §2 image
gate before A5.

**Not in scope:** the #124 catalog backfill. After A2 the list RPC refuses until a catalog backfill
runs (`docs/session-directory-list.md`, "Backfill"). That is a separate data write that needs its
own OK.

## 6. Stage B: event-trigger boundary

| step | kind | action                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | stop if                                                                                     |
| ---- | ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| B0   | R    | Repeat P6 and P7, with the Stage A results as the new baseline.                                                                                                                                                                                                                                                                                                                                                                                                                                                         | drift                                                                                       |
| B1   | W    | Pause the catch-up loop (A1) and the #121 Lambda.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | —                                                                                           |
| B2   | W    | As `postgres`: `psql -v ON_ERROR_STOP=1 -X -v runtime_role=bulls96_runtime -d postgres -f supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql`. It is one transaction. It creates schema `hoopdb_boundary`, the trigger function (owned by `postgres`, SECURITY INVOKER) and `CREATE EVENT TRIGGER hoopdb_sessions_function_boundary`, then runs `ALTER EVENT TRIGGER … ENABLE ALWAYS`.                                                                                                                   | 55000 "CREATE EVENT TRIGGER was refused" (rolled back; nothing created) or any other error  |
| B3   | R    | Run `bun src/hoopdb/migrate.ts --activation-check` with env **E-B** (the §2 code gate first).                                                                                                                                                                                                                                                                                                                                                                                                                           | not `satisfied`, or the trigger owner is not `postgres` (go to §8 "Stage B owner mismatch") |
| B4   | W    | Run `bun src/hoopdb/migrate.ts` with env **E-B**. No step is pending, so it only re-validates (phase 2, runtime ACL, drift check) and re-marks `pass` for `eff8b3be`.                                                                                                                                                                                                                                                                                                                                                   | non-zero exit                                                                               |
| B5   | W\*  | Behavioural probe, **rolled back**. Run `BEGIN; CREATE FUNCTION sessions.zz_boundary_probe() RETURNS int LANGUAGE sql AS 'SELECT 1'; SELECT has_function_privilege('public','sessions.zz_boundary_probe()','EXECUTE');` and expect `f`. Then run `GRANT EXECUTE ON FUNCTION sessions.zz_boundary_probe() TO PUBLIC;` and expect 42501, which aborts the transaction. Finish with `ROLLBACK;`. Nothing persists. \* It is still DDL on production, so it needs its own OK. Skip it if not approved.                      | probe executable by PUBLIC, or the GRANT accepted                                           |
| B6   | W    | Switch the **migrator's** `HOOPDB_FUNCTION_BOUNDARY_MODE` for prod Supabase to `event-trigger`: the env file or helper that runs `migrate.ts` and `--activation-check` against this project. Only the migrator reads this variable; no service, loop or Lambda does. Do not change selfhosted or Cloud SQL stacks (event-trigger is refused there). A future Helm migrate Job aimed at prod Supabase must set it too. Then restart the catch-up loop (A4 procedure, with the interrupted pass's `since`) and #121 (A5). | —                                                                                           |
| B7   | R    | Final checks with env **E-B**: `--activation-check` is satisfied. The trigger row has `evtenabled='A'`, `evtowner` = `postgres`, function owner `postgres`, and a prosrc sha equal to the frozen constant. Every `sessions` function ACL equals the manifest (the B4 drift check passed). The six platform event triggers are unchanged.                                                                                                                                                                                | anything else                                                                               |

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
- **If production's supautils differs:** a refusal at B2 rolls back and creates nothing. A trigger
  that B2 committed with an owner other than `postgres` makes B3 fail closed; recover with §8 "Stage
  B owner mismatch". Neither case needs a live experiment.

## 7. Decision for Khaliq: the `postgres` global function default

Event-trigger mode covers `postgres` without the global default: the gate's creator coverage
accepts the migration owner in event-trigger mode. So after B7 the global default could be removed
with `ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT EXECUTE ON FUNCTIONS TO PUBLIC`, which
restores PostgreSQL's built-in default.

**Recommendation: keep it.** self-hosted-lead concurs, and records that Khaliq approved keeping it.

- It is already in place, and the audit found no non-`sessions` consumer that depends on PUBLIC
  EXECUTE for new `postgres` functions. `public` is empty, and Supabase's API roles get their
  function access from the platform's schema-scoped defaults, which this does not change.
- It is defence in depth for the cases the trigger cannot see: supautils skips, CREATE EXTENSION,
  and a disabled trigger.
- Removing it is a second project-wide privilege change, for no gain.

If Khaliq chooses to remove it, that becomes step B8 (W). It needs its own OK, and the P12
default-ACL md5 must be re-recorded afterwards.

## 8. Failure and rollback

- **Stage A is one-way for code, and the old code is already shut out.** Since the 02:04Z hand
  ACL, the `b47c1e6` migrator already refuses (`runtime_role_unsafe`: `bulls96_runtime` holds
  grants that the old allowlist rejects). After A2 the same holds. Never point an older image's migrator at this database again. Recovery is always
  forward, with fixed code at `ab3ee7f` or later.
- **A2 fails after `20261101050000` committed** (for example `function_acl_drift` or
  `runtime_role_unsafe` in the validation transaction): the ledger has 18 rows and #124's DDL is in
  place. The validation transaction rolled back. The hand-applied #116 ACL from 02:04Z **stays in
  place**: only the 5 #124 objects' grants are missing, because the rolled-back transaction would
  have added them. The marker is `pending` for `eff8b3be`. Readers are not-ready, and the services are already paused. Record the
  `error_code` and the evidence JSON. Fix the cause (code through review, or an ACL difference that
  Khaliq approves), then re-run A2. It skips the applied step and re-runs only the validation. Do not
  edit catalogs by hand.
- **A2 fails inside `20261101050000`:** that transaction rolls back. The ledger stays at 17, and the
  marker is `pending` (written first). Fix, then re-run A2.
- **B4 refuses:** the same as a validation failure. Nothing is applied, and the marker is `pending`
  until it is fixed and re-run.
- **Undo #124 after A2 succeeded:** apply `supabase/rollbacks/20261101050000_session_directory_list.down.sql`
  (needs its own OK). **There is no documented procedure for removing its ledger row, nor for
  re-applying after a down** (dossier B10). Removing the row would need a separately reviewed
  procedure. Prefer forward fixes.
- **Stage B owner mismatch** (B2 committed, but `evtowner` is not `postgres`): if the owner is a role
  whose privileges `postgres` has (for example `supabase_privileged_role`), `postgres` can run
  `DROP EVENT TRIGGER hoopdb_sessions_function_boundary; DROP SCHEMA hoopdb_boundary CASCADE;`.
  Check `pg_has_role('postgres', <owner>, 'USAGE')` first. If the owner is a superuser, `postgres`
  cannot drop it; open a Supabase support request. Meanwhile the trigger still enforces the boundary
  for non-superusers, and the mode stays `global-default-revoke`, so the gate keeps passing on the
  global default. Either way, stop and re-review before retrying.
- **Undo Stage B** (owner is `postgres`): run the same `DROP EVENT TRIGGER …; DROP SCHEMA
hoopdb_boundary CASCADE;` as `postgres`, then set the mode back to `global-default-revoke`. The
  global default is still in place unless §7 removed it. Needs its own OK.
- **Data loss:** restore `sessions` from the A0 dump, or rebuild from S3 with the backfill and
  catch-up scripts. The index is derived data.

## 9. Evidence to record per step

UTC timestamp, executor, the exact command (with secrets redacted), exit code, and the JSON from
`--activation-check` or the migrator. Afterwards, re-take the P12 baseline md5s. Never record a
connection string, password, SCRAM verifier or token.

## 10. Outcome (executed 2026-11-02 by cloud-handoff; evidence relayed per step)

Khaliq gave the GO directly to helm-chart-chief-claude on `e2cbfb8` (`supabase-upgrade-go-e2cbfb8.md`):
A then B, a new private S3 bucket for the dump, keep the global default, run B5.

| step | result (UTC)                                                                                                                                                                                                                            |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P    | 02:16–02:21Z. The code gate passed at `7d7229b` (descends from `ab3ee7f`, clean tree, frozen install, generation `eff8b3be`). P1–P10 passed: P4 `b0ddc7de`, and P10 equal to the 02:07Z snapshot with no extras.                        |
| A1   | Verify-only, about 02:20Z. No loop or backfill process and no other client session. Interrupted pass `2026-11-01T23:41:28`, since `19:38:00`.                                                                                           |
| A0   | The 02:03:24Z dump was moved to `s3://96bulls-aws-sessions-prod-db-dumps-941433935742/supabase-pyamkrhoyszgzirqqffq/sessions/` (private, versioned, own KMS key, TLS-only). sha256 `0b55f653…` matched, and the local copy was deleted. |
| A2   | 02:25:00–18Z, exit 0. Applied `20261101050000`, skipped 17, validation passed for `eff8b3be`.                                                                                                                                           |
| A3   | Ledger 18, marker `pass`, 0 PUBLIC `sessions` functions. Runtime ACL 145/145: 22 functions, 28 tables, 25 `bulls96_trusted_backend` policies, none grantable. Globals unchanged.                                                        |
| A4   | Restarted 02:26:52Z with since `19:38:00`. It processed 6,717 of 9,565 with no errors, then B1 interrupted it as intended.                                                                                                              |
| A5   | #121 redeployed from `d350661` (on main, descends from `ab3ee7f`): 362 indexed, 0 errors, DLQ 0.                                                                                                                                        |
| C\*  | `bulls96_read_api` created from #135 `035fc53` SQL. Not in this plan; Khaliq approved it in cloud-handoff's session and confirmed it to helm-chart-chief-claude. No function grants.                                                    |
| B0   | Passed. The only delta is Stage C: role +1, its #106 creator edge, 2 settings, and `sessions` USAGE.                                                                                                                                    |
| B1   | 02:39–02:41Z. Loop and backfill exited, and the SQS mapping and reconcile rule were disabled.                                                                                                                                           |
| B2   | 02:40:56Z, bootstrap sha256 `a693776bf54b…` (equal to `7d7229b`). The trigger is `A`, owned by `postgres`; its function is owned by `postgres` and is SECURITY INVOKER. 7 event triggers in total.                                      |
| B3   | `--activation-check` (E-B) satisfied: tags exact, enabled `A`, owner equals the migration owner, creators covered, no aliases.                                                                                                          |
| B4   | 02:42:00–18Z, exit 0. 0 applied, 18 skipped, validation passed under `event-trigger` (the drift check passed).                                                                                                                          |
| B5   | The probe was not PUBLIC-executable. `GRANT … TO PUBLIC` was refused with 42501 by the trigger. Rolled back, nothing persisted.                                                                                                         |
| B6   | The prod migrator env (`hoopenv.sh`) now uses `event-trigger`; gcp/eks are untouched. Loop restarted with since `19:38:00`. #121 is on: 119 indexed, 137 unchanged, 0 errors, DLQ 0.                                                    |
| B7   | Satisfied. prosrc sha `8e59640c…` equals the frozen constant. 0 PUBLIC functions. Platform triggers md5 `7643ddb1` equals the baseline. P12 re-taken.                                                                                   |

\* Stage C ran outside this plan.

Follow-ups:

- Merge #135's bootstrap exactly as it ran (`035fc53`).
- Add a read-only drift-check preview to `--activation-check`.
- The #124 catalog backfill (needs its own OK).
- Update `supabase/README.md` and `docs/supabase-access.md`, which still say "nothing applied".
