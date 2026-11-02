# Supabase upgrade: generation eff8b3be → 5ed11913 (#127 session briefs)

Target: production Supabase project `pyamkrhoyszgzirqqffq`, database `postgres`. This is a plan for
review only; **nothing here has been run.** Every **W** step is a write and needs Khaliq's OK, under
AGENTS.md, in the session that runs it. cloud-handoff executes. helm-chart-chief-claude and the
migration lane review this exact text first.

## 1. Starting state

The state after `supabase-incremental-upgrade-1d99ccc8-to-eff8b3be.md` §10 (2026-11-02):

- generation `eff8b3be…`, ledger 18/18, marker `pass`;
- `HOOPDB_FUNCTION_BOUNDARY_MODE=event-trigger`, and the trigger `hoopdb_sessions_function_boundary`
  is enabled `A`, owned by `postgres`, with prosrc sha `8e59640c…`;
- the #116 runtime ACL is exact for `bulls96_runtime`;
- `bulls96_read_api` exists (Stage C, #135);
- the #121 Lambda runs from `d350661`, and the catch-up loop is running.

## 2. What #127 changes (main `475d503`, the #127 merge)

- **One new migration,** `20261102000000_session_briefs.sql`:
  - 3 tables (`session_briefs`, `session_brief_wakeups`, `session_brief_attempts`), RLS enabled,
    with `ALL` granted to `service_role` only;
  - 2 SECURITY DEFINER functions: `session_brief(text,text,text)`, whose EXECUTE goes to
    `session_directory_search_worker`, and `session_briefs_forget()`, a trigger function that is
    owner-only;
  - 4 `AFTER` triggers on the populated tables `session_index` and `session_directory`.
- **#116 runtime ACL:** the 3 new tables are added to `bulls96_runtime`'s table privileges and RLS
  tables. The migrator grants them.
- **Phase 2 verifier:** the Worker's approved RPCs now include `session_brief`.
- **Function boundary:** the manifest gains 2 entries. The trigger's embedded manifest and its
  `v_secdef` list gain the 2 functions (their tag set is unchanged), and the frozen prosrc sha
  becomes **`52ff269fc6d1149ad6a92dd5b5dc1f0f1dc7d69fa538bbdef08a1579f667fe21`**.
- **Generation:** `5ed119133e36047c942ac8c59410872c46a13d0ca0beb87c2df66bbd35a6d15f`.

**Why the order matters:** the trigger installed on production still embeds the **old** manifest,
`v_secdef` and prosrc. A migrate from the new code before U2 refuses at the function-boundary gate
(`function_boundary_unproven`: `prosrc_exact=false`) **before any write**. Only raw SQL applying the
new DDL past the gate would reach the trigger and be aborted with 42501. So the trigger function is
replaced first (U2), and only then does the migrator run (U4).

**One-way for code from U2.** After U2 the live prosrc is `52ff269f…`, so an `eff8b3be` (`7d7229b`)
migrator refuses the gate. Run the migrator only from the §3-gated code from U2 onwards.

**Briefs stay off.** `SESSION_BRIEF_ENABLED` must be exactly `"true"` to arm wakeups, and
`OPENAI_API_KEY` must be set for any model call (`src/session-brief/config.ts:60,79`). Neither is
set by this plan, so no brief is generated and no model is called.

## 3. Code and image gates

- **Code gate** (the worktree that runs every command):
  - `git merge-base --is-ancestor 475d503 HEAD` must exit 0 (`b1be32f` or later is fine);
  - `git status --porcelain` must be empty;
  - `bun install --frozen-lockfile` must succeed;
  - `bun src/hoopdb/migrate.ts --generation` must print `5ed11913…d15f`, the full value from §2.

  If any one fails, stop.

- **#121 bundle gate** (U6b): #121 is an Alchemy zip bundle, not an image. Its gate is the
  **source commit** the bundle is built from, which must descend from both `475d503` (#127) and
  `8a72ca1`. Check with `git merge-base --is-ancestor` for each, and record the commit and the bundle
  sha256.
  - The new bundle's projector reads `sessions.session_briefs`
    (`src/hoopdb/session-directory-projector.ts:55`) and has no verdict gate, so it **must not be
    deployed before U5 passes**.
  - Fallback: the current `d350661` bundle keeps working after U4, so #121 can be re-enabled on it
    unchanged.
- **Env E-B**, unchanged from the previous plan:
  - `HOOPDB_URL` (`postgres`, `verify-full`);
  - `HOOPDB_DB_MODE=supabase`;
  - `HOOPDB_FUNCTION_BOUNDARY_MODE=event-trigger`;
  - `HOOPDB_RUNTIME_ROLE=bulls96_runtime`;
  - `HOOPDB_RUNTIME_ROLE_LOGIN=true`;
  - `HOOPDB_SUPABASE_SHIM=false`;
  - unset: `HOOPDB_ROLE_VERIFY`, `HOOPDB_ROLE_BOOTSTRAP`, `HOOPDB_RUNTIME_WORKLOAD_ROLE` and
    `HOOPDB_FUNCTION_BOUNDARY_APPROVED`.

  This is the env in cloud-handoff's `hoopenv.sh`. Values never go on disk or into output.

## 4. Pre-flight (read-only)

Each check runs in `BEGIN READ ONLY … ROLLBACK`.

| id  | check                                                                                                   | expected                                                                                                                                                                                                                                                        |
| --- | ------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Q1  | ledger count; latest `applied_at`                                                                       | 18, latest 2026-11-02T02:25:18Z; nothing for `20261102000000`                                                                                                                                                                                                   |
| Q2  | marker                                                                                                  | `pass`, `eff8b3be…`                                                                                                                                                                                                                                             |
| Q3  | the trigger row and its function's prosrc sha (normalized as the gate does)                             | `A`, owner `postgres`, sha `8e59640c…`                                                                                                                                                                                                                          |
| Q4  | `--activation-check` with E-B (from the new code). It is read-only and includes the #147 drift preview. | Expected **before U2**, because the live trigger is still the old one: `function_boundary_satisfied=false`, with `prosrc_exact=false`. `function_acl_drift_extra` and `function_acl_missing_refused` empty. `function_acl_missing` lists only the #127 objects. |
| Q5  | the P12 baseline md5s against the B7 re-take (`~/.agent-tmp/supa-upgrade/p12-final.out`)                | equal                                                                                                                                                                                                                                                           |
| Q6  | `pg_locks` advisory `(961996,1)`                                                                        | 0                                                                                                                                                                                                                                                               |

Any difference other than the one expected in Q4: stop.

## 5. Steps

| step | kind | action                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | stop if                                                                                                                            |
| ---- | ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| U0   | W    | Pause writers: the catch-up loop (STOP, then the 30-minute time box, then TERM of its process group, as in the previous plan's A1; record the interrupted pass's `since`), plus the #121 SQS mapping and reconcile rule. Then re-run Q6, and confirm `pg_stat_activity` shows no other client.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | another writer remains                                                                                                             |
| U1   | W    | Restore point: a read-only `pg_dump --schema=sessions -Fc` written to tmpfs. Upload it to the dumps bucket (`96bulls-aws-sessions-prod-db-dumps-941433935742`), verify size and sha256, then delete the local copy.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | upload or sha mismatch                                                                                                             |
| U2   | W    | Replace the trigger function: as `postgres`, `psql -v ON_ERROR_STOP=1 -X -v runtime_role=bulls96_runtime -d postgres -f supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql`, from the gated worktree. One transaction: `CREATE OR REPLACE` of `hoopdb_boundary.sessions_function_boundary()` with the new manifest and `v_secdef`. The existing trigger is accepted only if its definition matches exactly (`A`, same tags), so the trigger itself is unchanged.                                                                                                                                                                                                                                                                                                                      | any error; the script rolls back                                                                                                   |
| U3   | R    | `--activation-check` with E-B.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `function_boundary_satisfied` is not true, `prosrc_exact` is not true (sha `52ff269f…`), or any `extra` or `missing_refused` entry |
| U4   | W    | `bun src/hoopdb/migrate.ts` with E-B. It applies `20261102000000` in its own transaction (ledger row 19). The validation transaction then installs the #116 ACL on the 3 new tables and runs phase 2 (Worker `session_brief` EXECUTE), the runtime allowlist and the exact drift check, and marks `pass` for `5ed11913`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | non-zero exit (see §6)                                                                                                             |
| U5   | R    | Post-checks with exact expected values (from the PG17 replay):<br>- ledger 19; marker `pass` for `5ed11913`;<br>- `--activation-check` (E-B) satisfied, with `function_acl_drift_extra`, `function_acl_missing_refused`, `function_acl_missing` and `function_acl_public_revoked` all empty;<br>- 0 PUBLIC `sessions` functions;<br>- 6 Worker approved RPCs (phase 2);<br>- 28 `bulls96_trusted_backend` policies and 31 runtime tables (28 plus the 3 #127 tables);<br>- the trigger `A`/`postgres`, sha `52ff269f…`; the 6 platform triggers unchanged.<br>P12 re-taken. The only delta is `sessions` +9 relations (3 tables, their indexes and keys) and +2 functions, plus 4 triggers and the runtime grants on the new tables. Roles, memberships, db settings and default ACLs are unchanged. | anything else                                                                                                                      |
| U6a  | W    | **Restart the catch-up loop**, still on its current `b47c1e6` code: it has no verdict gate and works against `5ed11913`. Set `CATCHUP_SINCE` to the interrupted pass's `since`, or, after a clean `stopped` line, to the start of the last completed pass minus 2 minutes. Confirm that its first pass logs no errors. **Retire this loop** (STOP, with no restart) **before `SESSION_BRIEF_ENABLED` is ever set**: its old code does not arm brief wakeups.                                                                                                                                                                                                                                                                                                                                         | errors                                                                                                                             |
| U6b  | W    | **#121** (its own OK): either re-enable the SQS mapping and reconcile rule on the existing `d350661` bundle (no deploy), or deploy a bundle that passes the §3 #121 gate. A new bundle is deployed **only after U5**. Confirm that one event indexes as `bulls96_runtime`, plus the RLS check.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | errors; the bundle fails the gate                                                                                                  |
| U6c  | —    | **Readers:** none is redeployed by this plan. `src/session-read-api` (#135) has **no** schema-verdict gate, so U4 does not take it down. Redeploying it from main would also ship unrelated changes (#151 `72a1cf7`), so that is a **separate decision**. Any verdict-gated reader is not-ready from U4 until it runs code descended from `475d503`.                                                                                                                                                                                                                                                                                                                                                                                                                                                 | —                                                                                                                                  |

**Locking (U4):** the 4 `CREATE TRIGGER` statements on `session_index` and `session_directory` take
a SHARE ROW EXCLUSIVE lock for milliseconds. The new tables and indexes are empty. Writers are
paused (U0).

**Out of scope:** enqueuing briefs for historical sessions. That needs its own OK and a budget, and
the handoff notes about 120k sessions.

## 6. Failure and rollback

- **U2 fails:** it rolled back, and the old trigger function stays. Stop and re-review. Do not
  migrate.
- **U4 fails after `20261102000000` committed:** the ledger has 19 rows and the DDL is in place. The
  validation rolled back, so the 3 new tables lack runtime grants, and the marker is `pending` for
  `5ed11913`. Readers are not-ready, and writers are paused. Fix the cause forward, through review,
  then re-run U4.
- **U4 fails inside the migration:** it rolled back. The ledger stays at 18 and the marker is
  `pending`. Fix, then re-run.
- **Undo #127 (needs its own OK; in this order):**
  1. Revert #121 to the `d350661` bundle first, if a #127 bundle was deployed. The new projector
     reads `session_briefs`.
  2. Apply `supabase/rollbacks/20261102000000_session_briefs.down.sql`.
  3. Re-run the trigger bootstrap from `7d7229b` to restore prosrc `8e59640c…`.
  4. **The ledger row `20261102000000` survives the down.** The `eff8b3be` migrator then refuses
     (`ledger_unknown_step`), and the `5ed11913` gate fails with 55000, so **no code can mark the
     marker `pass`**. Verdict-gated readers stay down. Removing that ledger row needs a separately
     reviewed procedure, which does not exist yet. So this rollback restores the schema but not
     readiness. Prefer forward fixes.
- **Data:** restore from the U1 dump, or rebuild the index from S3.

## 7. Evidence per step

The same as the previous plan §9: the UTC time, executor, command (redacted), exit code and JSON.
After each W step, send one line to helm-chart-chief-claude and the migration lane.
