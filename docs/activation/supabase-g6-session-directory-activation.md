# G6 session directory: Supabase production activation dossier (#38 + #96)

**Status: read-only planning document. Nothing in it has been run against any database.**

- Target: Supabase project `pyamkrhoyszgzirqqffq` (named as the production `sessions`
  target in `docs/relayhistory-supabase-migration.md` and `docs/relayhistory-t0-runbook.md`).
- Source of truth: `origin/main` at `28a8f0d` (merge of #96). #38 merged earlier as `9688d63e`.
- Prepared from the repository only. No connection was made to any real or remote
  database. No credential, `op` command or 1Password item was read. Nothing was deployed.
- Every value that can only come from the live target is labeled **NOT OBSERVED**. None
  of those values is inferred. Each one lists the exact read-only query or command that
  would fill it once Khaliq authorizes that read directly.
- Where the repository does not say something, this document says **unknown**.

Authorization model (`AGENTS.md`): applying migrations to any real database, deploys,
credentials and IAM changes each need Khaliq's separate, explicit OK in his own words.
That includes the read-only preflight audit in section 4. The bodies of the #38 and #96
merge commits say the same: "NOT authorized by this merge: applying any migration,
deploying, or activating rollout."

## 0. Evidence record format

Every observation made under an authorized audit goes in a row of this table. One table
per audit session, kept with the activation record:

| id  | query or command (verbatim) | expected | observed     | timestamp (UTC) | operator | connected as (role name only) |
| --- | --------------------------- | -------- | ------------ | --------------- | -------- | ----------------------------- |
| P1  | `SHOW server_version_num;`  | see 1.2  | NOT OBSERVED | —               | —        | —                             |

Rules for the record:

- Record role names, object names, counts, booleans and hashes. Never record a connection
  string, host password, token, JWT, secret value or 1Password field value.
- Paste result rows verbatim. If a query errors, record the SQLSTATE and message.
- One authorization covers only the queries it names. Do not add queries during an audit.

### Fields that are NOT OBSERVED

| field                                                                                                                                     | status                                                        | how to fill it once Khaliq authorizes it (read-only) | section |
| ----------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- | ---------------------------------------------------- | ------- |
| Server version                                                                                                                            | **NOT OBSERVED**                                              | P1                                                   | 1, 4    |
| Live role identity of the bootstrap and migrator connections                                                                              | **NOT OBSERVED**                                              | P2                                                   | 3, 4    |
| Live runtime HOOPDB identity (the role the services connect as)                                                                           | **NOT OBSERVED**                                              | RT1 (or R1 + R2)                                     | 3.4     |
| Whether that runtime identity can run the live validator read-only                                                                        | **NOT OBSERVED**                                              | RT2, RT3, RT4, RT5                                   | 3.4     |
| Effective privileges (role attributes, ACLs, default ACLs, RLS, dependencies and ownership for each role involved)                        | **NOT OBSERVED**                                              | P2, P3, P3b, P4, A1 to A10                           | 3, 4.1  |
| Installed migration state: the `sessions.__96bulls_migrations` ledger                                                                     | **NOT OBSERVED**                                              | P6, P7                                               | 2, 4    |
| Installed migration state: the `sessions.__96bulls_role_validation` marker                                                                | **NOT OBSERVED**                                              | P8                                                   | 4, 8    |
| Installed migration state: any Supabase CLI ledger                                                                                        | **NOT OBSERVED**                                              | P9                                                   | 2, 9    |
| Lock observations (live sessions, held locks, migrator advisory lock, table sizes)                                                        | **NOT OBSERVED**                                              | P14, P15, P16, P17                                   | 6       |
| Runtime secret target (which Kubernetes Secret and key, and which 1Password item, the services use)                                       | **NOT OBSERVED**                                              | R1, R2                                               | 3.4     |
| Backup and PITR availability for the project                                                                                              | **NOT OBSERVED**                                              | S1                                                   | 7.1     |
| Orchestrator readiness behavior for ingest and workers (which probe path, what traffic does on 503)                                       | **NOT OBSERVED**                                              | O1                                                   | 6.4     |
| Deployed reader images, their revisions (#96+ or pre-#96) and the generation they expect                                                  | **NOT OBSERVED**                                              | D1, D2, D3                                           | 6.4     |
| How the runtime Secret is synced from 1Password (resource kind and `itemPath`)                                                            | **NOT OBSERVED**; the repo defines no sync resource (unknown) | R1c to R1g                                           | 3.4     |
| `authenticator` memberships, exposed schemas, `sessions` `USAGE`, per-function effective `EXECUTE`, default ACLs and event triggers (B15) | **NOT OBSERVED**                                              | Y1 to Y7                                             | 4.2     |

## 1. Target and observed server version

### 1.1 Observed version

**Server version: NOT OBSERVED.** Observing it needs Khaliq's explicit OK for a read-only
audit. The query is P1 in section 4:

```sql
SHOW server_version_num;
SELECT version();
```

The repository's PostgreSQL evidence (`docs/evidence/session-directory-plans.md`) comes
from local Homebrew PostgreSQL 15.19 and 17.11 and PGlite 18.3. None of it is evidence of
the production server's version.

### 1.2 Why the version matters

Phase 1 (`supabase/bootstrap/session_directory_search_worker.sql`) creates the role only
when the caller is a superuser, or a CREATEROLE role on a server older than 160000:

```sql
IF NOT v_me.rolsuper
   AND NOT (v_me.rolcreaterole AND current_setting('server_version_num')::int < 160000) THEN
  RAISE EXCEPTION USING ERRCODE = '42501', ...
```

On PostgreSQL 16+, a CREATEROLE creator that is not a superuser is automatically granted
ADMIN OPTION on the role it creates. The migration's role guard
(`20261030020000_session_directory_short_summary.sql`) refuses any member of the role
other than a NOINHERIT `authenticator` without ADMIN OPTION, and on 16+ also refuses any
grant with `inherit_option`. So the bootstrap refuses that caller up front, with `42501`.

The runbook (`runbooks/session-directory-search-worker-bootstrap.md`, "Supabase (hosted)")
states: "On Supabase, `postgres` is not a superuser." Whether the production `postgres`
role has `rolsuper` or `rolcreaterole` is itself NOT OBSERVED (P2).

The version also changes three checks at run time: phase 1, the migration guard and phase
2 each read `pg_auth_members.inherit_option` only when `server_version_num >= 160000`.

### 1.3 Decision options by observed version

| observed `server_version_num`            | bootstrap caller                                              | what the repo says happens                                                                         | options for Khaliq                                                                                                                                                                                                                                                                                                                                                             |
| ---------------------------------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `< 160000` (PostgreSQL 15)               | `postgres` with CREATEROLE (P2 must show `rolcreaterole = t`) | Creating the role grants `postgres` nothing; phase 1 creates it to spec.                           | (a) Approve running phase 1 as `postgres`. (b) Defer G6 activation.                                                                                                                                                                                                                                                                                                            |
| `>= 160000` (PostgreSQL 16, 17 or later) | `postgres` (not a superuser)                                  | Phase 1 refuses with `42501`. The runbook calls this "an open blocker, not a step to work around." | (a) Have a superuser run phase 1 through Supabase support. Who does it, and how, is unknown. (b) Make an explicit, reviewed change to the role spec, and to the guard and verifier, so they accept the creator's ADMIN grant. The repo has no such change, and its safety is unknown. (c) Defer G6 activation. Do not grant ADMIN by hand, and do not edit the guard in place. |
| any                                      | a real superuser                                              | Phase 1 creates the role with no membership.                                                       | Applies only if a superuser connection is available and approved. Availability on this project is unknown.                                                                                                                                                                                                                                                                     |

## 2. Exact migration inventory and order

### 2.1 What #38 and #96 changed (from `git show --stat 9688d63e 28a8f0d`)

| file                                                                                          | added or changed by                                              | role                                     |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------- |
| `supabase/migrations/20261030010000_session_directory.sql`                                    | #38 (added)                                                      | migration, expand                        |
| `supabase/migrations/20261030020000_session_directory_short_summary.sql`                      | #38 (added); also changed by #72 (`0904999`) and #96 (`28a8f0d`) | migration, expand                        |
| `supabase/migrations/20261031001000_session_directory_reads_expand.sql`                       | #38 (added); also changed by #72                                 | migration, expand only                   |
| `supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql`                  | #38; changed by #72 and #96                                      | rollback (not run by the migrator)       |
| `supabase/rollbacks/20261031001000_session_directory_reads_expand.down.sql`                   | #38                                                              | rollback (not run by the migrator)       |
| `supabase/bootstrap/session_directory_search_worker.sql`                                      | #96                                                              | phase 1 (not ledgered)                   |
| `supabase/bootstrap/session_directory_search_worker_verify.sql`                               | #96                                                              | phase 2 and the live gate (not ledgered) |
| `supabase/bootstrap/session_directory_search_worker_mark_pass.sql`                            | #96                                                              | external verifier (not ledgered)         |
| `src/hoopdb/migrate.ts`, `src/shared/db.ts`, `src/shared/worker.ts`, `src/alleyoop/ingest.ts` | #96                                                              | runner, marker, live gate, `/readyz`     |

### 2.2 What `bun src/hoopdb/migrate.ts` would run, in order

`plan()` in `src/hoopdb/migrate.ts` sorts `supabase/migrations/*.sql` and
`src/hoopdb/migrations/*.sql` separately by name and runs:

1. `shim:supabase_roles` (`supabase/tests/supabase_shim.sql`), when `HOOPDB_SUPABASE_SHIM`
   is on. **It defaults to on.** The file says: "Never apply this to a Supabase project."
   This step goes through the ledger path: it is recorded as `shim:supabase_roles`.
2. `bootstrap:session_directory_search_worker` (phase 1), when `HOOPDB_ROLE_BOOTSTRAP` is
   on. It defaults to the value of `HOOPDB_SUPABASE_SHIM`. It runs every time and is never
   ledgered.
3. Every `supabase/migrations` file whose name is not in the ledger, as
   `upstream:<file>`, in name order.
4. Every `src/hoopdb/migrations` file whose name is not in the ledger, as
   `96bulls:<file>`, in name order.
5. `verify:session_directory_search_worker` (phase 2, then `'pass'` in the same
   transaction), when `HOOPDB_ROLE_VERIFY` is on (default on). It runs every time and is
   never ledgered.

Each migration file runs in its own `BEGIN` … `COMMIT`, with a ledger insert of its
sha256. An applied file whose sha256 differs fails the whole run ("changed after it was
applied"). A ledger name starting with `vendor:` fails the run with `legacy_schema`.

Full set at `28a8f0d`, with sha256 values computed offline from the repo files (they
match what `migrate.ts` computes over the same UTF-8 text):

| #   | ledger name                                                    | sha256              | origin                       | expand or contract                                                                          | applied on target? |
| --- | -------------------------------------------------------------- | ------------------- | ---------------------------- | ------------------------------------------------------------------------------------------- | ------------------ |
| 1   | `upstream:20261025000000_sessions_index.sql`                   | `6bfdb4c3…8874a3`   | relayhistory-cloud #171, #28 | base schema                                                                                 | NOT OBSERVED       |
| 2   | `upstream:20261025000100_repo_identity.sql`                    | `3b1d74f6…28c13a`   | relayhistory-cloud #177      | base                                                                                        | NOT OBSERVED       |
| 3   | `upstream:20261025000200_scrub_quarantine.sql`                 | `97ec2f03…0c7f4398` | relayhistory-cloud #185      | base                                                                                        | NOT OBSERVED       |
| 4   | `upstream:20261025000300_upsert_keep_on_null.sql`              | `17539224…5a649910` | relayhistory-cloud #185      | base                                                                                        | NOT OBSERVED       |
| 5   | `upstream:20261025000600_pipeline_queue.sql`                   | `41f16006…44177042` | relayhistory-cloud #210      | base                                                                                        | NOT OBSERVED       |
| 6   | `upstream:20261030000000_relayhistory_machines_compat.sql`     | `22fe48a3…29f227`   | #28                          | expand                                                                                      | NOT OBSERVED       |
| 7   | `upstream:20261030010000_session_directory.sql`                | `2427948e…48fa2f8`  | **#38**                      | expand                                                                                      | NOT OBSERVED       |
| 8   | `upstream:20261030020000_session_directory_short_summary.sql`  | `c223b77e…5918de4`  | **#38 / #72 / #96**          | expand (it also replaces two of #38's indexes and narrows `authenticated` to column grants) | NOT OBSERVED       |
| 9   | `upstream:20261031000000_migration_assertion_ledger.sql`       | `0683aca3…20d40689` | #43                          | not G6                                                                                      | NOT OBSERVED       |
| 10  | `upstream:20261031000100_manifest_readiness.sql`               | `1ff02258…242242d6` | #51 / #52                    | not G6                                                                                      | NOT OBSERVED       |
| 11  | `upstream:20261031001000_session_directory_reads_expand.sql`   | `6957cd17…20ec15`   | **#38 / #72**                | **expand only**                                                                             | NOT OBSERVED       |
| 12  | `upstream:20261101000000_migration_ingest_audit_base_read.sql` | `44813425…98acea`   | #79                          | not G6                                                                                      | NOT OBSERVED       |
| 13  | `upstream:20261101000200_claimless_window_activations.sql`     | `6dde3342…bd5d1a`   | #52                          | not G6                                                                                      | NOT OBSERVED       |
| 14  | `96bulls:0001_96bulls_observability.sql`                       | `b9d059c4…a430a8`   | 96bulls                      | not G6                                                                                      | NOT OBSERVED       |
| 15  | `96bulls:0002_session_ext_and_summaries.sql`                   | `2fee02ab…25568`    | 96bulls                      | not G6                                                                                      | NOT OBSERVED       |
| 16  | `96bulls:0003_fanout_ledger.sql`                               | `cccdd28c…18c7`     | 96bulls                      | not G6                                                                                      | NOT OBSERVED       |
| 17  | `96bulls:0004_summary_pointers.sql`                            | `fe351f30…94b2e94`  | 96bulls                      | not G6; **destructive** (see 6.3)                                                           | NOT OBSERVED       |
| —   | `verify:session_directory_search_worker` (not ledgered)        | `b974b099…c3a968f`  | #96                          | phase 2                                                                                     | —                  |

Full sha256 values for the G6 files:

```text
20261030010000_session_directory.sql                 2427948e076846d9d6aaf48ec775f718eac57da512f1a9a09cfff516848fa2f8
20261030020000_session_directory_short_summary.sql   c223b77e4c875cbc36499e6ca1c8e521b564ebf25facd87313b3e58335918de4
20261031001000_session_directory_reads_expand.sql    6957cd17a52ac225df91330e02514253ae7ffd772418e4b02b522d929a20ec15
session_directory_search_worker.sql (phase 1)        0c9c4aadf8430a48019e1a237f3facec527b54f7ac8c2b2bcb2a1af1ab3a8b1d
session_directory_search_worker_verify.sql (phase 2) b974b0994adc854696d2b657dc757da753069c4e5a3716f4f51014932c3a968f
session_directory_search_worker_mark_pass.sql        02808cd13657ef0bd1584a7e848d671232f1c434422a334d3f027fd6060940ba
```

Migration-set generation at `28a8f0d`, computed offline with the algorithm of
`generation()` in `src/hoopdb/migrate.ts`:
`d48ab0224b0e95364a5d031d33c236ab65b7229d4ffc7ce9ddeccabcddb5ef86`. Before it is relied
on, it must be confirmed with `bun src/hoopdb/migrate.ts --generation` from the exact
image the readers run. That command reads only local files and makes no connection.

**Scope warning.** `migrate.ts` has no way to apply only the G6 files. Against a target
whose `__96bulls_migrations` ledger is missing or partial, it applies every unledgered
file above, rows 1 to 17 included, in that order. The ledger contents are NOT OBSERVED
(P6, P7), so which files a production run would apply is unknown. See blocker B3, and
section 6.3 for the impact of each file.

### 2.3 What each G6 file does

**`20261030010000_session_directory.sql` (#38, expand).**
`CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;`. Creates
`sessions.session_directory` (primary key `(org_id, workspace_id, harness, session_id)`),
`sessions.session_directory_users` and `sessions.session_directory_machines` (foreign keys
to the directory, `ON DELETE CASCADE`), and 13 non-concurrent `CREATE INDEX` statements,
two of them GIN `public.gin_trgm_ops` (`session_directory_project_trgm_idx`,
`session_directory_metadata_trgm_idx`). Creates `sessions.session_directory_rollout`, a
singleton seeded with `completed_at` NULL. Enables RLS on all four tables, with
`authenticated` read policies through `sessions.jwt_org_id()`,
`sessions.jwt_workspace_ids()` and `sessions.member_of(...)`, which come from
`20261025000000_sessions_index.sql`. `REVOKE ALL ... FROM PUBLIC, anon, authenticated`,
then `GRANT SELECT ... TO authenticated` and `GRANT ALL ... TO service_role`. No trigger,
backfill or write path.

**`20261030020000_session_directory_short_summary.sql` (#38, #72, #96, expand).**
In order:

1. `DO $role_guard$`. It fails closed before any DDL: `42704` if
   `session_directory_search_worker` is absent, or `55000` naming every violation of the
   spec in 3.1. It never creates or alters the role.
2. `CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;`
3. `ALTER TABLE sessions.session_directory ADD COLUMN short_summary …`, plus four
   provenance columns with CHECKs, then `ADD CONSTRAINT
session_directory_short_summary_provenance` and
   `session_directory_short_summary_not_on_tombstone`.
4. Functions `sessions.session_directory_search_text(...)` (IMMUTABLE),
   `sessions.session_directory_tombstone_clears_summary()` with its `BEFORE INSERT OR
UPDATE` trigger, and `sessions.session_directory_search_query(text)`.
5. `DROP INDEX sessions.session_directory_metadata_trgm_idx;` then `CREATE INDEX
session_directory_tenant_search_trgm_idx … USING gin (org_id, workspace_id,
search_text public.gin_trgm_ops)`. Next, `DROP INDEX
sessions.session_directory_project_trgm_idx;` then `CREATE INDEX
session_directory_tenant_project_trgm_idx … USING gin (org_id, workspace_id,
project_id public.gin_trgm_ops)`. Both builds are non-concurrent.
6. `GRANT USAGE ON SCHEMA sessions TO session_directory_search_worker;`
7. `CREATE TABLE sessions.session_directory_search_token_policy`: a singleton with RLS
   on, `REVOKE ALL … FROM PUBLIC, anon, authenticated, service_role`, and **no row
   seeded**.
8. `sessions.search_session_directory(...)`: SECURITY DEFINER with a pinned
   `search_path`. `EXECUTE` is revoked from PUBLIC, `anon`, `authenticated`,
   `service_role`, every other grantee present at creation and `relayhistory_app`, then
   granted to `session_directory_search_worker` only.
9. `REVOKE SELECT ON sessions.session_directory FROM authenticated;` then a column-list
   `GRANT SELECT (...)` that excludes `search_text` and the `short_summary*` columns.

**`20261031001000_session_directory_reads_expand.sql` (#38, #72, expand only).** Header:
"ADDITIVE ONLY: it revokes nothing, adds no search index … and does not change
sessions.search_session_directory." It creates:

- `sessions.session_directory_resurrection_fences` (foreign key to the directory);
- the trigger function `sessions.session_directory_record_fences()` (SECURITY DEFINER),
  and an `AFTER INSERT OR UPDATE OF deleted_at` trigger on `sessions.session_directory`;
- `sessions.session_directory_worker_scope(text)` (not SECURITY DEFINER; EXECUTE revoked
  from everyone, the worker role included);
- `sessions.session_directory_pointers(text, text, text)` and
  `sessions.session_directory_readiness()` (SECURITY DEFINER; EXECUTE to
  `session_directory_search_worker` only).

### 2.4 Expand now, contract later

Everything above is expand. The contract step (in `docs/session-search-index-contract.md`,
"Direct surface: expand then contract") is **not in the repository**. It is a later,
separate forward migration that "revokes direct SELECT on `session_directory`, `_users`,
`_machines`, `session_index` and `session_directory_rollout` from `authenticated` and from
the static reader's role." Its prerequisites: "#226 cuts over to the RPCs behind a flag
with shadow parity, and logs prove zero direct reads over an agreed window." Plan and
consumer inventory: #87 (`docs/session-directory-consumers.md`).

### 2.5 Where the bootstrap and verify steps sit

```text
[Khaliq OK] -> P-queries (section 4, read-only)
  -> phase 1: supabase/bootstrap/session_directory_search_worker.sql      (privileged; never ledgered)
  -> phase 1 checks (runbook "Checks")                                     (read-only)
  -> migrate.ts: advisory lock 961996,1 -> marker ('pending', GEN)
       -> each unledgered migration in its own transaction (rows 1..17 as needed; 7, 8, 11 are G6)
       -> phase 2 verify + marker 'pass' in one transaction                (when HOOPDB_ROLE_VERIFY on)
     unlock
  -> [if HOOPDB_ROLE_VERIFY=false] mark_pass.sql with -v generation=GEN    (external verifier)
  -> post-apply evidence (section 8)
```

Phase 1 must succeed before the run that first applies `20261030020000`, or that file
fails with `42704` and rolls back (files before it in the same run stay committed).
Phase 2 runs only after the whole set is applied, because its allowlist includes the
`20261031001000` RPCs. If it ran after `20261030020000` alone, it would fail with
"approved RPC missing".

## 3. Role and bootstrap identity

### 3.1 The worker role spec (phase 1, the guard and phase 2 all enforce it)

| item                                        | required value (from the SQL)                                                                                                                                                                            |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                                        | `session_directory_search_worker`                                                                                                                                                                        |
| attributes                                  | `NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION` (`rolcanlogin`, `rolinherit`, `rolbypassrls`, `rolsuper`, `rolcreaterole`, `rolcreatedb` and `rolreplication` all `f`) |
| `pg_db_role_setting`                        | exactly one row: `setdatabase = 0`, `setconfig = ARRAY['statement_timeout=2s']`. `2000ms`, `2000`, any extra setting and any per-database row are refused.                                               |
| membership (as member)                      | none                                                                                                                                                                                                     |
| members (of it)                             | none until the separately approved binding. The only allowed member is `authenticator`, with `rolinherit = f`, `admin_option = f`, and on 16+ `inherit_option = f`.                                      |
| owned objects                               | none (`pg_class`, `pg_proc`, `pg_namespace`, `pg_type`, `pg_database`)                                                                                                                                   |
| explicit privileges before `20261030020000` | none, except `USAGE` on `sessions`, which the migration grants                                                                                                                                           |

Phase 1 creates the role with:

```sql
CREATE ROLE session_directory_search_worker
  NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER ROLE session_directory_search_worker SET statement_timeout = '2s';
```

### 3.2 Who must run each step

| step                          | must run as                                                                                                                                                                                                   | live identity on target               |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| phase 1 (create)              | a superuser; or, only below 160000, a CREATEROLE role                                                                                                                                                         | **NOT OBSERVED** (P2)                 |
| phase 1 (role already exists) | any role that can read `pg_roles`, `pg_db_role_setting` and `pg_auth_members` (verify only)                                                                                                                   | **NOT OBSERVED**                      |
| migrations (`migrate.ts`)     | the schema owner. The T0 runbook (A1) describes "a schema-owner role for migrations". It must be able to create in `sessions`, create extensions and grant to `authenticated`, `service_role` and the worker. | **NOT OBSERVED** (P2, P5)             |
| phase 2 and `mark_pass.sql`   | any role that can read the catalogs ("the migration owner (`hoop`, or Supabase `postgres`) is enough"). `mark_pass.sql` also needs UPDATE on `sessions.__96bulls_role_validation`.                            | **NOT OBSERVED**                      |
| live gate (`schemaVerdict`)   | the service's own `HOOPDB_URL` role                                                                                                                                                                           | **NOT OBSERVED** (RT1 to RT5, R1, R2) |

### 3.3 Least-privilege allowlist that phase 2 enforces

From `supabase/bootstrap/session_directory_search_worker_verify.sql`, the direct aclitems
naming the role must be exactly:

- `USAGE` on schema `sessions`;
- `EXECUTE` on
  `sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer)`;
- `EXECUTE` on `sessions.session_directory_pointers(text, text, text)`;
- `EXECUTE` on `sessions.session_directory_readiness()`.

None may be `WITH GRANT OPTION`. A missing approved RPC, or any function with one of those
names and another signature, fails. Any other `pg_shdepend` reference to the role in any
database of the cluster (ownership, ACL, policy, initial privilege) fails. PUBLIC grants
are ignored. On failure it raises `55000` naming every violation, and changes nothing.

### 3.4 Runtime HOOPDB identity, the live validator, and the runtime secret target

#### What the live validator needs

The live gate (`schemaVerdict`, `src/shared/db.ts`) runs as the role in `HOOPDB_URL` on
every ingest `/readyz`, every worker `/readyz` and before every worker claim. It is one
transaction with these statements, in this order:

1. `SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY`
2. `SET LOCAL statement_timeout = <HOOPDB_ROLE_GATE_TIMEOUT_MS>`, default 2000, allowed
   1..10000
3. `SELECT name FROM sessions.__96bulls_migrations`
4. `SELECT generation, status FROM sessions.__96bulls_role_validation WHERE name = $1`
5. the full text of `supabase/bootstrap/session_directory_search_worker_verify.sql`
6. statement 4 again

So the runtime role needs:

- `USAGE` on schema `sessions`;
- `SELECT` on `sessions.__96bulls_migrations` and `sessions.__96bulls_role_validation`;
- `SELECT` on `pg_roles`, `pg_auth_members`, `pg_db_role_setting`, `pg_shdepend`,
  `pg_namespace`, `pg_proc` and `pg_database`, the catalogs the exact text of
  `session_directory_search_worker_verify.sql` reads. It does not read `pg_default_acl`:
  the migration guard in `20261030020000` does, but the validator does not. The runbook's
  list ("… `pg_database` and `pg_default_acl`") is broader than the file;
- `EXECUTE` on `aclexplode(aclitem[])`, `pg_describe_object(oid, oid, integer)`,
  `to_regprocedure(text)` and `to_regnamespace(text)`, which phase 2 calls.

The runbook calls the catalogs "PUBLIC-readable". Whether that holds on this project is
NOT OBSERVED.

**Gap at `28a8f0d`:** `migrate.ts` creates both tables as the migrator and runs
`REVOKE ALL ON sessions.__96bulls_role_validation FROM PUBLIC`. No file on `main` grants
`SELECT` on either table to a runtime role. Unless the runtime role is the migrator, or
has access some other way (ownership, membership or default privileges; `BYPASSRLS` does
not grant table privileges), the gate fails with `42501`. It then reports
`role_validation_failed`, and `/readyz` returns 503.

**Pending, not merged: draft PR #99** (`fix/g6-supabase-mode-runtime-grants`; code at
`960ed64`, the round-6 head, still a draft under review; its exact-head CI run and artifact
IDs are recorded in the PR #99 body). It changes `src/hoopdb/migrate.ts` and adds
`src/hoopdb/migrate-guards.ts`:

- `HOOPDB_DB_MODE` becomes required (`local`, `selfhosted` or `supabase`).
- In `supabase` mode it refuses the shim (`shim_forbidden`) and a forced bootstrap
  (`bootstrap_forbidden`).
- `HOOPDB_RUNTIME_ROLE` becomes required. In **both `selfhosted` and `supabase`** the
  runtime role must be a dedicated role, not the migrator (`runtime_role_unsafe`).
  `HOOPDB_RUNTIME_ROLE_LOGIN` must be set explicitly there, and an optional
  `HOOPDB_RUNTIME_WORKLOAD_ROLE` names the one role allowed to be a member of it.
- The migrator checks an exact least-privilege allowlist for that role before any write
  and again after the last step. It then grants the role `USAGE` on schema `sessions`
  and `SELECT` on the ledger and the marker, nothing else.
- In `selfhosted` it adds `supabase/bootstrap/hoopdb_runtime_role.sql`, a privileged
  bootstrap that creates the role (no password, nothing granted) or verifies it. In
  `supabase` the role is **operator-created**, and the migrator only verifies it. The
  file notes that a PostgreSQL 16+ project has the same creator-ADMIN caveat as the
  Worker.
- Before any write, it adds authority preflights: `mixed_migration_authority`,
  `unledgered_schema` and `ledger_gap`.
- In `selfhosted` and `supabase` it adds a function-boundary gate before any write
  (`function_boundary_unproven`), selected by `HOOPDB_FUNCTION_BOUNDARY_MODE`
  (`global-default-revoke` is the only implemented mode; the old
  `HOOPDB_FUNCTION_BOUNDARY_APPROVED` name is refused). The catalog must show:
  - the migration owner's global function default (`pg_default_acl`,
    `defaclnamespace = 0`, `'f'`) is exactly `{<self>:EXECUTE}`: no other grantee, no
    grant option;
  - every role with effective `CREATE` on `sessions` (membership, schema owner and
    superusers included) has its own exact default, even before it has created anything;
  - no `sessions`-scoped function default grants another role, and every `sessions`
    function is owned by the owner.

  `bun src/hoopdb/migrate.ts --activation-check` prints that evidence (names and
  booleans) and exits 1 when it is unproven. On Supabase nothing installs the control,
  so supabase migrate stays refused until an operator decides it (B15). The live gate
  (readiness and worker claims) checks existing functions only; the default-ACL and
  creator evidence is enforced by the migrator and `--activation-check`.

- Real PostgreSQL 15 and 17 proofs (`scripts/pg-proof/role-proof.ts`, CI `pg-proof`)
  cover each refusal, both poisoned-default variants (bootstrap refused with
  `pg_default_acl` unchanged), and the behaviour: after the bootstrap, functions
  created later in `sessions` are not executable by PUBLIC, `authenticated` or the
  runtime role.

The generation hashes only the migration files and the phase 2 verifier. #99 (at `960ed64`)
changes no migration file, but it does edit a comment line in
`supabase/bootstrap/session_directory_search_worker_verify.sql` (sha256
`670246b8cf99e2a3882a9b4b05c8c1ed32aa18c6da1382d24c96f452282be69a`). So **if #99 merges
as it stands, the generation changes** from `d48ab022…b5ef86` (`main` `b00d4f3`) to
`dd2ca0d4ce1cff4bb9e4017bf7e728b7abc7ff02300bdf59857d552531ba7d07`, computed with
`bun src/hoopdb/migrate.ts --generation` at `960ed64` (offline, no connection), and every
reader needs an image built with it. Recompute it from the exact merged image.
`hoopdb_runtime_role.sql` and `hoopdb_function_default_revoke.sql` are not part of the
hash. `main` is now `b00d4f3`; a `git diff 28a8f0d b00d4f3` over `supabase/`,
`src/hoopdb/migrations/`, `src/hoopdb/migrate.ts` and `src/shared/db.ts` is empty, so
the migration set, the runner and the gate on `main` are unchanged. Until #99 merges,
this dossier describes `main`'s migration set as of `28a8f0d`, and the gap above stands
(blocker B6).

#### Proving that the runtime identity can run the validator read-only

**Observed runtime identity: NOT OBSERVED. Observed validator result for that identity:
NOT OBSERVED.** Each query below is read-only and needs Khaliq's explicit OK. Results go
in this evidence table:

| id  | query / command                                                    | expected                                | observed     | timestamp (UTC) | operator | connected as |
| --- | ------------------------------------------------------------------ | --------------------------------------- | ------------ | --------------- | -------- | ------------ |
| RT1 | identify the runtime role from live connections (admin connection) | one `usename` per service               | NOT OBSERVED | —               | —        | —            |
| RT2 | runtime role attributes                                            | not superuser, not `BYPASSRLS`, `LOGIN` | NOT OBSERVED | —               | —        | —            |
| RT3 | privilege matrix for the runtime role                              | all `t`                                 | NOT OBSERVED | —               | —        | —            |
| RT4 | the live validator, run as the runtime role, read-only             | completes; `ROLLBACK`                   | NOT OBSERVED | —               | —        | —            |
| RT5 | the service's own `/readyz` body                                   | `{"ok":true}`, or a typed reason        | NOT OBSERVED | —               | —        | —            |

**RT1: identify the role without reading any secret.** `openHoopDb` sets
`application_name` to `OTEL_SERVICE_NAME`. The repo's defaults are `alleyoop-ingest`,
`alleyoop-courier`, `hoopdb-indexer`, `bob-costas` and `referee`; `alchemy.run.ts` sets
`OTEL_SERVICE_NAME` to the service name. What the production chart sets is unknown. Run
on an admin connection:

```sql
SELECT usename, application_name, count(*) AS connections
  FROM pg_stat_activity
 WHERE datname = current_database()
   AND application_name IN ('alleyoop-ingest', 'alleyoop-courier', 'hoopdb-indexer',
                            'bob-costas', 'referee')
 GROUP BY 1, 2
 ORDER BY 1, 2;
```

If no service is connected yet, RT1 is empty, and R1 plus R2 identify the role instead.

**RT2: attributes** (any connection; `<runtime role>` is the RT1 or R2 name):

```sql
SELECT rolname, rolsuper, rolbypassrls, rolinherit, rolcanlogin, rolcreaterole, rolcreatedb
  FROM pg_roles WHERE rolname = '<runtime role>';
```

**RT3: privilege matrix** (any connection):

```sql
SELECT
  has_schema_privilege('<runtime role>', 'sessions', 'USAGE')                              AS sessions_usage,
  has_table_privilege('<runtime role>', 'sessions.__96bulls_migrations', 'SELECT')         AS ledger_select,
  has_table_privilege('<runtime role>', 'sessions.__96bulls_role_validation', 'SELECT')    AS marker_select,
  has_table_privilege('<runtime role>', 'sessions.__96bulls_role_validation', 'UPDATE')    AS marker_update_must_be_f,
  has_table_privilege('<runtime role>', 'pg_catalog.pg_roles', 'SELECT')                   AS pg_roles,
  has_table_privilege('<runtime role>', 'pg_catalog.pg_auth_members', 'SELECT')            AS pg_auth_members,
  has_table_privilege('<runtime role>', 'pg_catalog.pg_db_role_setting', 'SELECT')         AS pg_db_role_setting,
  has_table_privilege('<runtime role>', 'pg_catalog.pg_shdepend', 'SELECT')                AS pg_shdepend,
  has_table_privilege('<runtime role>', 'pg_catalog.pg_namespace', 'SELECT')               AS pg_namespace,
  has_table_privilege('<runtime role>', 'pg_catalog.pg_proc', 'SELECT')                    AS pg_proc,
  has_table_privilege('<runtime role>', 'pg_catalog.pg_database', 'SELECT')                AS pg_database,
  has_function_privilege('<runtime role>', 'pg_catalog.aclexplode(aclitem[])', 'EXECUTE')  AS aclexplode,
  has_function_privilege('<runtime role>', 'pg_catalog.pg_describe_object(oid, oid, integer)', 'EXECUTE') AS describe_object,
  has_function_privilege('<runtime role>', 'pg_catalog.to_regprocedure(text)', 'EXECUTE')  AS to_regprocedure,
  has_function_privilege('<runtime role>', 'pg_catalog.to_regnamespace(text)', 'EXECUTE')  AS to_regnamespace;
```

Expected: every column `t`, except `marker_update_must_be_f`, which is `f`. A `f` on
`ledger_select` or `marker_select` is blocker B6. The two `sessions.__96bulls_*` checks
error with `42P01` if P6 found the tables absent; record that.

**RT4: run the validator as the runtime identity, exactly as `schemaVerdict` does.** It
needs the runtime credential, injected at exec time by the approved mechanism and never
written or recorded. It writes nothing: the transaction is `READ ONLY` and ends in
`ROLLBACK`. Run from the repository root at `28a8f0d`:

```sh
psql -v ON_ERROR_STOP=1 -X -d "<runtime connection, injected at exec time>" <<'SQL'
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;
SET LOCAL statement_timeout = 2000;
SELECT current_user, session_user, current_setting('transaction_read_only') AS read_only;
SELECT count(*) AS ledger_rows FROM sessions.__96bulls_migrations;
SELECT generation, status FROM sessions.__96bulls_role_validation
 WHERE name = 'session_directory_search_worker';
\i supabase/bootstrap/session_directory_search_worker_verify.sql
SELECT generation, status FROM sessions.__96bulls_role_validation
 WHERE name = 'session_directory_search_worker';
ROLLBACK;
SQL
```

Expected: `current_user` is the RT1 or R2 role and `read_only` is `on`. Both `SELECT`s
return. The `DO $verify$` block completes, after the migrations. `ROLLBACK` follows.
**Only a complete RT4 after the full apply is proof.** An early `55000` before or during
the apply ("the role does not exist", "approved RPC missing") is **not** proof. The DO
block raises as soon as it reaches that check, so later catalog paths never run for this
identity: the `pg_proc` signature and ACL checks, the `pg_namespace` ACL check and the
`pg_shdepend` / `pg_database` / `pg_describe_object` loop. RT4 counts as evidence only
when every statement completes against the fully migrated database and the verifier
raises nothing. A `42501` anywhere means blocker B6.

**RT5: in-cluster probe, no credential handling.** The `/readyz` body is the
`schemaVerdict` JSON. On failure it carries `reason`, plus `error_code` for
`role_validation_failed`. Ingest listens on `INGEST_PORT` (default 8080). Workers serve
health on `WORKER_HEALTH_PORT` (default 8081). Production namespace, deployment names and
cluster context are NOT OBSERVED, and so is whether the image contains `wget` or `curl`.

```sh
kubectl --context <prod context> -n <namespace> exec deploy/<ingest deployment> -- \
  wget -qO- http://127.0.0.1:8080/readyz
kubectl --context <prod context> -n <namespace> exec deploy/<worker deployment> -- \
  wget -qO- http://127.0.0.1:8081/readyz
```

#### Runtime secret target (R1, R2)

**Runtime secret target: NOT OBSERVED.** What the repo says, with no values:

- The services read `HOOPDB_URL` (`openHoopDb`, `src/shared/db.ts`).
- AWS: `stacks/aws/eks.ts` maps it to `{ existingSecret: i.runtimeSecretName, key:
"HOOPDB_URL" }`. `runtimeSecretName` is `96bulls-runtime` in
  `stacks/aws/examples/aws-proof.inputs.example.json`,
  `docs/plans/aws-sessions-dev.inputs.sanitized.json` and
  `docs/plans/aws-proof.inputs.sanitized.json`.
- GCP: `stacks/gcp/config.ts` maps it to `{ existingSecret: i.existingSecretName, key:
"HOOPDB_URL" }`, and `docs/gcp-stage.md` shows `"existingSecretName":
"96bulls-runtime"`.
- `stacks/aws/eks.ts`: the Secret is "created by the secrets sync, not by IaC".
  `docs/aws-stage.md` creates the Secrets "from 1Password".
  `docs/relayhistory-t0-runbook.md` (A1) puts the Supabase migration and runtime roles in
  the 1Password vault `96Bulls-prod`. The repo names no production stage inputs file and
  no 1Password item for the 96Bulls services' `HOOPDB_URL`, so both are unknown.

**How the Kubernetes Secret is fed from 1Password: the repo defines no sync resource.**
I searched the whole repo for `OnePasswordItem`, `ExternalSecret`, `SecretStore`,
`itemPath`, `onepassword` and `1password-connect`. None of them appears in any stack,
chart value, manifest or doc. The only mentions are prose: `stacks/aws/eks.ts` ("existing
Kubernetes Secrets synced from 1Password, never through chart values"; "created by the
secrets sync, not by IaC") and `stacks/aws/relayauth.ts` ("by hand or by the secrets
sync"). The procedures the repo does document are **manual operator pipes**:

- `docs/aws-stage.md` builds the `relayauth` Secret with `op item get "sessions-dev
relayauth" … | jq '{…kind:"Secret"…}' | kubectl apply -f -`.
- It patches `SUMMARY_MASTER_KEYS` into `96bulls-runtime` with `op read
"op://96Bulls-dev/sessions-dev hoopdb/summary-master-keys"` and `kubectl -n 96bulls
patch secret 96bulls-runtime`.

So the sync resource kind for production is **unknown**. It may be none (a manual
`kubectl apply` or `patch`), or a controller installed outside this repo. R1 discovers
which. Every R1 command prints names, kinds, keys and `itemPath` only.

**Hazard:** a Secret created with `kubectl apply` carries a
`kubectl.kubernetes.io/last-applied-configuration` annotation that contains the
plaintext values. Never print a Secret's annotation values or `-o yaml`/`-o json`. R1c
prints annotation **keys** only.

**R1** (needs Khaliq's OK; names only, never values):

```sh
# R1a: which Secret and key each production workload reads HOOPDB_URL from (env and envFrom)
kubectl --context <prod context> -n <namespace> get deploy,statefulset,job -o jsonpath='{range .items[*]}{.kind}/{.metadata.name}{"\tenv="}{range .spec.template.spec.containers[*].env[?(@.name=="HOOPDB_URL")]}{.valueFrom.secretKeyRef.name}/{.valueFrom.secretKeyRef.key}{end}{"\tenvFrom="}{range .spec.template.spec.containers[*].envFrom[*]}{.secretRef.name}{" "}{end}{"\n"}{end}'

# R1b: which keys that Secret holds (key names only)
kubectl --context <prod context> -n <namespace> get secret <secret name from R1a> \
  -o go-template='{{range $k, $v := .data}}{{$k}}{{"\n"}}{{end}}'

# R1c: what owns or manages that Secret (owner kinds and names, managedFields managers, annotation and label keys only)
kubectl --context <prod context> -n <namespace> get secret <secret name from R1a> -o go-template='owners:{{range .metadata.ownerReferences}} {{.kind}}/{{.name}}{{end}}{{"\n"}}managers:{{range .metadata.managedFields}} {{.manager}}({{.operation}}){{end}}{{"\n"}}annotation-keys:{{range $k, $v := .metadata.annotations}} {{$k}}{{end}}{{"\n"}}label-keys:{{range $k, $v := .metadata.labels}} {{$k}}{{end}}{{"\n"}}'

# R1d: is any 1Password or External Secrets controller installed? (CRD names only)
kubectl --context <prod context> get crd -o name | grep -i -E 'onepassword|externalsecret|secretstore|sealedsecret|secretproviderclass|vault|sops' || echo "no matching secret CRDs"

# R1e: only if R1d lists onepassworditems.onepassword.com: the item path each one maps to its Secret
kubectl --context <prod context> -n <namespace> get onepassworditems.onepassword.com -o jsonpath='{range .items[*]}{.metadata.name}{"\titemPath="}{.spec.itemPath}{"\n"}{end}'

# R1f: only if R1d lists externalsecrets.external-secrets.io: target Secret, store and remote keys (names only)
kubectl --context <prod context> -n <namespace> get externalsecrets.external-secrets.io -o jsonpath='{range .items[*]}{.metadata.name}{"\ttarget="}{.spec.target.name}{"\tstore="}{.spec.secretStoreRef.kind}/{.spec.secretStoreRef.name}{"\tkeys="}{range .spec.data[*]}{.secretKey}<-{.remoteRef.key}/{.remoteRef.property}{" "}{end}{range .spec.dataFrom[*]}{.extract.key}{" "}{end}{"\n"}{end}'

# R1g: required before reading any R1f remote key as a 1Password item. For the store each
# ExternalSecret references, print only the store name and its provider type (the key under
# .spec.provider). Never print the provider's fields: they hold hosts, vault maps and auth refs.
kubectl --context <prod context> -n <namespace> get secretstores.external-secrets.io <store name from R1f> \
  -o go-template='{{.metadata.name}}{{"\t"}}{{range $k, $v := .spec.provider}}{{$k}}{{end}}{{"\n"}}'
kubectl --context <prod context> get clustersecretstores.external-secrets.io <store name from R1f> \
  -o go-template='{{.metadata.name}}{{"\t"}}{{range $k, $v := .spec.provider}}{{$k}}{{end}}{{"\n"}}'
```

Use the `SecretStore` form when R1f shows `store=SecretStore/<name>`, and the
`ClusterSecretStore` form when it shows `store=ClusterSecretStore/<name>`.

Expected:

- R1a: every workload that uses HoopDb shows the same Secret, key `HOOPDB_URL`
  (`96bulls-runtime` in the repo's example inputs).
- R1b: the key list includes `HOOPDB_URL`.
- R1c shows how the Secret got there. An `onepassword` owner means R1e; an
  `ExternalSecret` owner means R1f. `kubectl-client-side-apply`, `kubectl-patch` or
  `kubectl-create` managers with no owner mean the manual pipe, and then there is no
  in-cluster mapping to query.
- R1e: an `itemPath` (`vaults/<vault>/items/<item>`) names the 1Password vault and
  item.
- R1f: a remote key names a 1Password item **only if** R1g shows a 1Password provider type
  (`onepassword`, or `onepasswordSDK`) for that exact store. Even then, the vault comes
  from the store's configuration, which is not printed, so the vault is taken from
  Khaliq's record.
- With a manual pipe, the item comes only from the operator's record, and is unknown
  until Khaliq states it.
- **Supported paths.** R1 recognizes exactly three paths:
  1. **1Password Operator:** R1c shows an owner `OnePasswordItem/<name>`, and R1e gives
     its `itemPath`.
  2. **External Secrets plus a proved 1Password provider:** R1c shows an owner
     `ExternalSecret/<name>`, R1f names its store, and R1g shows provider type
     `onepassword` or `onepasswordSDK` for that exact store.
  3. **Documented manual kubectl pipe:** R1c shows no owner, and the only managers are
     `kubectl-client-side-apply`, `kubectl-create`, `kubectl-patch`, `kubectl-edit` or
     `kubectl`. The item then comes from Khaliq's record.
- **STOP / UNKNOWN outcomes.** Record `UNKNOWN`, stop R1 and R2, and take it to Khaliq
  without guessing an item when:
  - R1c shows an owner kind other than `OnePasswordItem` or `ExternalSecret`;
  - R1c shows **no owner, but a clearly attributed manager or controller that is not one
    of the three supported paths**, for example `sealed-secrets-controller`, a Secrets
    Store CSI driver, a Vault agent or injector, SOPS or a GitOps controller, or any
    manager other than the kubectl managers listed above. This applies even when the
    controller is well known;
  - R1d lists a secret-sync CRD other than `onepassworditems.onepassword.com` or
    `externalsecrets.external-secrets.io`, and R1c attributes the Secret to it;
  - R1g shows a provider type other than `onepassword` or `onepasswordSDK`, or the
    referenced store does not exist;
  - the managers cannot be attributed.
- In every case R1 prints names, kinds, manager names, keys and `itemPath` only. Never
  print a Secret's values, its annotation values, or the Secret as JSON or YAML.

**R2** (needs Khaliq's OK; titles and field labels only, never values), run only after
R1 has named the item:

```sh
# R2a: confirm the item exists in the vault R1 named (titles only)
op item list --vault "<vault from R1e, or Khaliq's record>" --format json | jq -r '.[].title'
# R2b: field labels of that item, never values (the value fields stay inside the pipe)
op item get "<item from R1e, from R1f when R1g shows a 1Password provider, or Khaliq's record>" --vault "<vault>" --format json | jq -r '.fields[] | .label'
```

Then Khaliq states the database role that item's `HOOPDB_URL` connects as, or RT1
confirms it from live connections. Record the vault, the item title, the field label, the
sync kind and provider type (R1c to R1g) and the role name. Never record a field value.

## 4. Preflight queries (read-only; each needs Khaliq's explicit OK before it runs)

Run these as the connection that will run the migrator, unless the row says otherwise.
None of them writes. The "expected" column is what a safe go looks like; anything else
stops the activation and goes to Khaliq.

| id  | purpose                                                                           | query                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | expected for go                                                                                                                                                                                                                                                                                                                                                                                                                  |
| --- | --------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P1  | server version                                                                    | `SHOW server_version_num; SELECT version();`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | a number. Then choose the 1.3 row.                                                                                                                                                                                                                                                                                                                                                                                               |
| P2  | live identity and attributes of this connection                                   | `SELECT current_user, session_user, r.rolsuper, r.rolcreaterole, r.rolbypassrls, r.rolinherit FROM pg_roles r WHERE r.rolname = current_user;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Bootstrap connection: `rolsuper = t`, or `rolcreaterole = t` with P1 `< 160000`. Migrator: unknown spec; record it.                                                                                                                                                                                                                                                                                                              |
| P3  | does the worker role exist, and is it to spec                                     | `SELECT oid, rolcanlogin, rolinherit, rolbypassrls, rolsuper, rolcreaterole, rolcreatedb, rolreplication FROM pg_roles WHERE rolname = 'session_directory_search_worker';`                                                                                                                                                                                                                                                                                                                                                                                                                                           | 0 rows (phase 1 will create it), or 1 row with every attribute `f`                                                                                                                                                                                                                                                                                                                                                               |
| P3b | its settings and memberships (only if P3 found a row)                             | the runbook "Checks" block: `SELECT setdatabase, setconfig FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole WHERE r.rolname = 'session_directory_search_worker';` and `SELECT g.rolname AS role, u.rolname AS member, m.admin_option FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles u ON u.oid = m.member WHERE 'session_directory_search_worker' IN (g.rolname, u.rolname);`                                                                                                                                                                                                | exactly `0 \| {statement_timeout=2s}`; no membership rows                                                                                                                                                                                                                                                                                                                                                                        |
| P4  | existing references to the role (only if P3 found a row)                          | `SELECT d.deptype, d.dbid, db.datname, d.classid::regclass AS catalog, CASE WHEN d.dbid IN (0, (SELECT oid FROM pg_database WHERE datname = current_database())) THEN pg_describe_object(d.classid, d.objid, d.objsubid) END AS object FROM pg_shdepend d LEFT JOIN pg_database db ON db.oid = d.dbid WHERE d.refclassid = 'pg_authid'::regclass AND d.refobjid = 'session_directory_search_worker'::regrole ORDER BY 1, 3, 5;` (the A7 guarded form: objects in another database are named by database only, because `pg_describe_object` cannot resolve them from here)                                            | before `20261030020000`: 0 rows. After the full set: only `a` rows for schema `sessions` and the three approved functions.                                                                                                                                                                                                                                                                                                       |
| P5  | existing grants, ACLs, default ACLs, RLS and dependencies for every role involved | the exhaustive catalog audit A0 to A10 in 4.1 (not `information_schema`, which shows only what the current role can see)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | see each query in 4.1                                                                                                                                                                                                                                                                                                                                                                                                            |
| P6  | do the ledger and marker tables exist                                             | `SELECT to_regclass('sessions.__96bulls_migrations') AS ledger, to_regclass('sessions.__96bulls_role_validation') AS marker;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | record. NULL means `migrate.ts` has never run here.                                                                                                                                                                                                                                                                                                                                                                              |
| P7  | ledger contents (only if P6 found the ledger)                                     | `SELECT name, sha256, applied_at FROM sessions.__96bulls_migrations ORDER BY name;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | every applied row's sha256 equals the 2.2 table; no `vendor:` names. Any mismatch makes the run fail by design: stop.                                                                                                                                                                                                                                                                                                            |
| P8  | marker (only if P6 found the marker)                                              | `SELECT name, generation, status, updated_at FROM sessions.__96bulls_role_validation;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | record. A `'pass'` for another generation is expected to flip to `'pending'` on the next migrate (see 6.4).                                                                                                                                                                                                                                                                                                                      |
| P9  | other migration ledgers on the target                                             | `SELECT to_regclass('supabase_migrations.schema_migrations');` and, if not NULL, `SELECT version, name FROM supabase_migrations.schema_migrations ORDER BY version;`                                                                                                                                                                                                                                                                                                                                                                                                                                                 | record. Non-empty means `scripts/sb db push` was used: see blocker B3.                                                                                                                                                                                                                                                                                                                                                           |
| P10 | which schema objects already exist                                                | `SELECT n, to_regclass(n) FROM unnest(ARRAY['sessions.session_index', 'sessions.machines', 'sessions.pipeline_events', 'sessions.pipeline_jobs', 'sessions.session_summaries', 'sessions.session_directory', 'sessions.session_directory_rollout', 'sessions.session_directory_search_token_policy', 'sessions.session_directory_resurrection_fences']) AS n;`                                                                                                                                                                                                                                                       | consistent with P7. Objects present without ledger rows means blocker B3.                                                                                                                                                                                                                                                                                                                                                        |
| P11 | extensions                                                                        | `SELECT e.extname, e.extversion, n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname IN ('pg_trgm', 'btree_gin');` and `SELECT name, default_version, installed_version FROM pg_available_extensions WHERE name IN ('pg_trgm', 'btree_gin');`                                                                                                                                                                                                                                                                                                                                | both available. If `pg_trgm` is already installed, it must be in schema `public`: the migrations reference `public.gin_trgm_ops` explicitly, and `CREATE EXTENSION IF NOT EXISTS … WITH SCHEMA public` does not move an existing install. `btree_gin` is used through default opclasses, so its schema is not referenced. The repo does not say whether the migrator may create extensions on this project: unknown.             |
| P12 | leakproof flags (G6's RLS design depends on them)                                 | `SELECT proname, proleakproof FROM pg_proc WHERE pronamespace = 'pg_catalog'::regnamespace AND proname IN ('texteq', 'textlike', 'texticlike', 'text_pattern_lt', 'text_pattern_ge') ORDER BY 1;`                                                                                                                                                                                                                                                                                                                                                                                                                    | as in `docs/evidence/session-directory-plans.md`: `texteq`, `text_pattern_lt` and `text_pattern_ge` true; `textlike` and `texticlike` false                                                                                                                                                                                                                                                                                      |
| P13 | runtime role access for the live gate                                             | RT2 and RT3 in 3.4, then RT4 as the runtime identity                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | as stated for RT2 to RT4. Otherwise see blocker B6.                                                                                                                                                                                                                                                                                                                                                                              |
| P14 | table sizes for the lock analysis                                                 | `SELECT c.oid::regclass, c.reltuples::bigint AS est_rows, pg_total_relation_size(c.oid) AS bytes FROM pg_class c WHERE c.oid IN (SELECT to_regclass(x) FROM unnest(ARRAY['sessions.session_index', 'sessions.session_index_ext', 'sessions.machines', 'sessions.pipeline_events', 'sessions.pipeline_events_default', 'sessions.pipeline_dead_letters', 'sessions.pipeline_jobs', 'sessions.session_summaries', 'sessions.migration_ingest_audit', 'sessions.session_directory']) AS x WHERE to_regclass(x) IS NOT NULL UNION SELECT t.relid FROM pg_partition_tree(to_regclass('sessions.pipeline_events')) AS t);` | record. The `pg_partition_tree` branch adds every partition of `sessions.pipeline_events` (each monthly `pipeline_events_YYYY_MM`, the default partition and the parent). It returns no rows when the parent does not exist. These are every existing table a 6.3 file locks (`ACCESS EXCLUSIVE` or `SHARE ROW EXCLUSIVE`) or scans on a backlog run. `reltuples` is a planner estimate, not a count; `-1` means never analyzed. |
| P15 | current activity on the database (no query text)                                  | `SELECT pid, usename, application_name, backend_type, state, wait_event_type, xact_start FROM pg_stat_activity WHERE datname = current_database() AND state <> 'idle';`                                                                                                                                                                                                                                                                                                                                                                                                                                              | record. Long-running transactions must be resolved before any `ACCESS EXCLUSIVE` step.                                                                                                                                                                                                                                                                                                                                           |
| P16 | locks held on the affected tables                                                 | `SELECT l.pid, l.relation::regclass, l.mode, l.granted FROM pg_locks l WHERE l.relation IN (SELECT to_regclass(x) FROM unnest(ARRAY['sessions.session_index', 'sessions.session_index_ext', 'sessions.machines', 'sessions.pipeline_events', 'sessions.pipeline_events_default', 'sessions.pipeline_dead_letters', 'sessions.pipeline_jobs', 'sessions.session_summaries', 'sessions.migration_ingest_audit', 'sessions.session_directory']) AS x WHERE to_regclass(x) IS NOT NULL UNION SELECT t.relid FROM pg_partition_tree(to_regclass('sessions.pipeline_events')) AS t);`                                      | record                                                                                                                                                                                                                                                                                                                                                                                                                           |
| P17 | is a migrator already running (the advisory lock 961996, 1)                       | `SELECT pid, granted FROM pg_locks WHERE locktype = 'advisory' AND classid = 961996 AND objid = 1 AND objsubid = 2;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | 0 rows                                                                                                                                                                                                                                                                                                                                                                                                                           |
| P18 | timeouts that apply to the migrator session                                       | `SHOW statement_timeout; SHOW lock_timeout; SHOW idle_in_transaction_session_timeout;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | record. `migrate.ts` sets none of them.                                                                                                                                                                                                                                                                                                                                                                                          |

### 4.1 Grants and ownership audit (read-only; replaces a grants-view summary)

`information_schema` views show only privileges visible to the current role, and they
omit default ACLs, policies and dependencies. So the audit reads the catalogs directly.

**Roles involved.** Use this list in every query below:

- `session_directory_search_worker`;
- `authenticator`, `authenticated`, `anon` and `service_role`;
- `relayhistory_app` (named in the `20261030020000` revoke);
- `postgres`;
- `<migrator role>` (P2) and `<runtime role>` (RT1 or R2).

Record every result in the section 0 format. "Expected" below gives the state before #38
and after a full apply. Rows not covered by it go to Khaliq. They are not fixed during
the audit.

**A0: role inventory**

```sql
SELECT rolname, oid, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin,
       rolreplication, rolbypassrls
  FROM pg_roles
 WHERE rolname IN ('session_directory_search_worker', 'authenticator', 'authenticated', 'anon',
                   'service_role', 'relayhistory_app', 'postgres', '<migrator role>', '<runtime role>')
 ORDER BY rolname;
```

**A1: relation ACLs and owners (`pg_class.relacl`)** in `sessions`. A NULL `relacl` means
the default: owner only. The query shows both the raw and the effective entries.

```sql
SELECT c.oid::regclass AS relation, c.relkind, pg_get_userbyid(c.relowner) AS owner,
       c.relacl IS NULL AS default_acl,
       CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee,
       pg_get_userbyid(a.grantor) AS grantor, a.privilege_type, a.is_grantable
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,
         acldefault(CASE WHEN c.relkind = 'S' THEN 's'::"char" ELSE 'r'::"char" END, c.relowner))) a
 WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
 ORDER BY 1, 5, 7;
```

Expected after the apply, for the G6 tables, from the migrations:

- `session_directory`: `authenticated` has no table-level `SELECT` (column grants only;
  see A2). `service_role` has all privileges. Nothing for `anon` or `PUBLIC`.
- `session_directory_users` and `session_directory_machines`: `SELECT` for
  `authenticated`; all privileges for `service_role`.
- `session_directory_rollout` and `session_directory_resurrection_fences`: all privileges
  for `service_role` only.
- `session_directory_search_token_policy`: owner only.
- `__96bulls_role_validation`: no `PUBLIC` (with #99: `SELECT` for the runtime role).
- `session_directory_search_worker` appears on **no** relation.
- `service_role`'s broad `ALL` on the `sessions` tables comes from `20261025000000` lines
  527 and 528, plus later explicit grants. It is the recorded trusted-admin decision in
  4.2, not a violation. `authenticated` and `anon` must have nothing beyond the grants
  listed here.

**A2: column ACLs (`pg_attribute.attacl`)**

```sql
SELECT c.oid::regclass AS relation, att.attname,
       CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee,
       pg_get_userbyid(a.grantor) AS grantor, a.privilege_type, a.is_grantable
  FROM pg_attribute att
  JOIN pg_class c ON c.oid = att.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(att.attacl) a
 WHERE n.nspname = 'sessions' AND att.attnum > 0 AND NOT att.attisdropped AND att.attacl IS NOT NULL
 ORDER BY 1, 3, 2;
```

Expected after the apply:

- `authenticated` has `SELECT` on exactly the 24 `session_directory` columns listed in
  `20261030020000`. None of `search_text`, `short_summary`,
  `short_summary_scrub_ruleset`, `short_summary_scrub_version`, `short_summary_source` or
  `short_summary_generated_at` is among them.
- `authenticated` has `SELECT` on the 8 `machines` columns listed in `20261030000000`.
- No column ACL names `session_directory_search_worker`.

**A3: schema ACLs and owners (`pg_namespace.nspacl`)**

```sql
SELECT n.nspname, pg_get_userbyid(n.nspowner) AS owner, n.nspacl IS NULL AS default_acl,
       CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee,
       pg_get_userbyid(a.grantor) AS grantor, a.privilege_type, a.is_grantable
  FROM pg_namespace n
  CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) a
 WHERE n.nspname IN ('sessions', 'public', 'extensions')
 ORDER BY 1, 4, 6;
```

Expected after the apply: `session_directory_search_worker` has exactly `USAGE` on
`sessions`, not grantable. With #99, the runtime role also has `USAGE` on `sessions`.

**A4: function ACLs, owners and security (`pg_proc.proacl`)**

```sql
SELECT p.oid::regprocedure AS function, pg_get_userbyid(p.proowner) AS owner,
       p.prosecdef AS security_definer, p.proconfig, p.proacl IS NULL AS default_acl,
       CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee,
       a.privilege_type, a.is_grantable
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
 WHERE n.nspname = 'sessions'
 ORDER BY 1, 6;
```

Expected after the apply. The three approved RPCs are `search_session_directory(...)`,
`session_directory_pointers(text, text, text)` and `session_directory_readiness()`:

- On each approved RPC, `EXECUTE` goes to the owner and `session_directory_search_worker`
  only. `security_definer = t`, and `proconfig` pins `search_path`.
- `session_directory_worker_scope(text)` has `EXECUTE` for the owner only.
- `session_directory_record_fences()` has the owner only.
- `session_directory_search_text(...)` has the owner plus `service_role`.
- `session_directory_search_query(text)` and `session_directory_tombstone_clears_summary()`
  have the owner only (PUBLIC revoked).
- No other function names `session_directory_search_worker`.

**A5: default privileges (`pg_default_acl`)**

```sql
SELECT pg_get_userbyid(d.defaclrole) AS for_objects_created_by, n.nspname AS in_schema,
       d.defaclobjtype,
       CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee,
       a.privilege_type, a.is_grantable
  FROM pg_default_acl d
  LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
  CROSS JOIN LATERAL aclexplode(d.defaclacl) a
 ORDER BY 1, 2, 3, 4, 5;
```

Expected: no row where `for_objects_created_by` or `grantee` is
`session_directory_search_worker`, since phase 1, the guard and phase 2 all refuse that.
Record every row whose `for_objects_created_by` is `<migrator role>`. Those grants land on
every object the migrations create. The `DO $revoke$` blocks strip them from the RPCs but
not from tables.

**A6: RLS flags and policies (`pg_class.relrowsecurity`, `relforcerowsecurity`,
`pg_policy`)**

```sql
SELECT c.oid::regclass AS relation, c.relrowsecurity, c.relforcerowsecurity
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p')
 ORDER BY 1;

SELECT pol.polrelid::regclass AS relation, pol.polname, pol.polcmd, pol.polpermissive,
       ARRAY(SELECT CASE WHEN r = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(r) END
               FROM unnest(pol.polroles) AS r) AS roles,
       pg_get_expr(pol.polqual, pol.polrelid) AS using_expr,
       pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check_expr
  FROM pg_policy pol
  JOIN pg_class c ON c.oid = pol.polrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'sessions'
 ORDER BY 1, 2;
```

Expected after the apply:

- `relrowsecurity = t` on all six `session_directory*` tables.
- `relforcerowsecurity = f`. The plan evidence records "FORCE=false. Without FORCE the
  owner (the RPCs' definer) is not subject to RLS", and the RPCs depend on that.
- Exactly three G6 policies: `session_directory_member_read`,
  `session_directory_users_member_read` and `session_directory_machines_member_read`.
  Each is `SELECT` (`polcmd = 'r'`), permissive, `TO authenticated`.
- No policy names `session_directory_search_worker`.

**A7: shared dependencies and ownership, per role (`pg_shdepend`)**

```sql
SELECT r.rolname, d.deptype, d.dbid, db.datname, d.classid::regclass AS catalog,
       CASE WHEN d.dbid IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
            THEN pg_describe_object(d.classid, d.objid, d.objsubid) END AS object
  FROM pg_shdepend d
  JOIN pg_roles r ON r.oid = d.refobjid
  LEFT JOIN pg_database db ON db.oid = d.dbid
 WHERE d.refclassid = 'pg_authid'::regclass
   AND r.rolname IN ('session_directory_search_worker', 'authenticator', 'authenticated', 'anon',
                     'service_role', 'relayhistory_app', 'postgres', '<migrator role>', '<runtime role>')
 ORDER BY 1, 2, 6;
```

`deptype`: `o` owner, `a` ACL, `r` policy, `i` initial privilege. PostgreSQL records no
`pg_shdepend` rows for objects owned by the bootstrap superuser, so ownership by that
role shows only in A1 and A4.

Expected for `session_directory_search_worker`:

- before #38: no rows;
- after the apply: only `a` rows, in this database, for schema `sessions` and the three
  approved functions. That is what phase 2 enforces.

Rows in another `datname` fail phase 2 ("keep one migrated database per cluster").

**A8: object dependencies (`pg_depend`).** What depends on the G6 tables (foreign keys,
triggers, policies, views, rules), and the extension members the migrations reference:

```sql
SELECT pg_describe_object(d.classid, d.objid, d.objsubid) AS dependent,
       pg_describe_object(d.refclassid, d.refobjid, d.refobjsubid) AS referenced, d.deptype
  FROM pg_depend d
 WHERE d.refclassid = 'pg_class'::regclass
   AND d.refobjid IN (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                       WHERE n.nspname = 'sessions'
                         AND c.relname IN ('session_directory', 'session_directory_users',
                                           'session_directory_machines', 'session_directory_rollout',
                                           'session_directory_resurrection_fences', 'session_index'))
   AND d.classid <> 'pg_class'::regclass
 ORDER BY 2, 1;

SELECT e.extname, n.nspname AS ext_schema, oc.opcname, ocn.nspname AS opclass_schema
  FROM pg_depend d
  JOIN pg_extension e ON e.oid = d.refobjid
  JOIN pg_namespace n ON n.oid = e.extnamespace
  JOIN pg_opclass oc ON oc.oid = d.objid
  JOIN pg_namespace ocn ON ocn.oid = oc.opcnamespace
 WHERE d.refclassid = 'pg_extension'::regclass AND d.classid = 'pg_opclass'::regclass
   AND d.deptype = 'e' AND e.extname IN ('pg_trgm', 'btree_gin') AND oc.opcmethod = (SELECT oid FROM pg_am WHERE amname = 'gin')
 ORDER BY 1, 3;
```

Expected:

- Before #38, the first query lists only `session_index`'s dependents. Record them: a
  dependent that is not in the repo (a view, say) blocks a later contract revoke and a
  down.
- If `pg_trgm` is installed, `gin_trgm_ops` is in schema `public`.

**A9: role memberships (`pg_auth_members`)**, PostgreSQL 16+ form; on 15, drop the two
`*_option` columns other than `admin_option`:

```sql
SELECT g.rolname AS role, u.rolname AS member, gr.rolname AS grantor,
       m.admin_option, m.inherit_option, m.set_option
  FROM pg_auth_members m
  JOIN pg_roles g ON g.oid = m.roleid
  JOIN pg_roles u ON u.oid = m.member
  LEFT JOIN pg_roles gr ON gr.oid = m.grantor
 WHERE g.rolname IN ('session_directory_search_worker', 'authenticator', 'authenticated', 'anon',
                     'service_role', 'relayhistory_app', 'postgres', '<migrator role>', '<runtime role>')
    OR u.rolname IN ('session_directory_search_worker', 'authenticator', 'authenticated', 'anon',
                     'service_role', 'relayhistory_app', 'postgres', '<migrator role>', '<runtime role>')
 ORDER BY 1, 2;
```

Expected:

- `session_directory_search_worker` is a member of nothing.
- Its only member, and only after the separate binding step, is `authenticator`, with
  `admin_option = f` and `inherit_option = f`.
- `<runtime role>` is not a member of `session_directory_search_worker`, `service_role`
  or any `BYPASSRLS` role. With #99 (`960ed64`, pending) the migrator refuses any membership
  of the runtime role, and any member except `HOOPDB_RUNTIME_WORKLOAD_ROLE` without ADMIN
  OPTION.

**A10: effective privilege matrix** (what each role can actually do, memberships
included):

```sql
SELECT r.rolname, o.obj,
       has_table_privilege(r.oid, o.obj, 'SELECT') AS sel,
       has_table_privilege(r.oid, o.obj, 'INSERT') AS ins,
       has_table_privilege(r.oid, o.obj, 'UPDATE') AS upd,
       has_table_privilege(r.oid, o.obj, 'DELETE') AS del
  FROM pg_roles r
  CROSS JOIN unnest(ARRAY['sessions.session_directory', 'sessions.session_directory_users',
                          'sessions.session_directory_machines', 'sessions.session_directory_rollout',
                          'sessions.session_directory_resurrection_fences',
                          'sessions.session_directory_search_token_policy', 'sessions.session_index',
                          'sessions.__96bulls_migrations', 'sessions.__96bulls_role_validation']) AS o(obj)
 WHERE r.rolname IN ('session_directory_search_worker', 'authenticator', 'authenticated', 'anon',
                     'service_role', 'relayhistory_app', '<runtime role>')
   AND to_regclass(o.obj) IS NOT NULL
 ORDER BY 1, 2;
```

Expected after the apply:

- `session_directory_search_worker` is `f` everywhere.
- `anon` is `f` everywhere.
- `authenticated` has `sel = f` on `session_directory` at table level. `has_table_privilege`
  is false when only column grants exist; A2 shows those.
- The runtime role has `sel = t` on both `__96bulls_*` tables and `upd = f`.

### 4.2 Authorization prerequisite: PUBLIC EXECUTE on future functions (blocker B15)

**The gap.** Exposure is not the problem; PUBLIC `EXECUTE` on future functions is.
PostgreSQL grants `EXECUTE` to `PUBLIC` on every new function by default. A function
created in `sessions` after the last PASS therefore stays PUBLIC-executable until someone
revokes it. Any role with `USAGE` on `sessions` can call it directly, and so can
PostgREST's `/rpc/<name>` for any role `authenticator` can switch to, while `sessions` is
an exposed schema.

- The migrations' `DO $revoke$` blocks strip non-owner `EXECUTE` only from the functions
  they create, at creation time.
- Phase 2 checks only grants that name `session_directory_search_worker`, and it ignores
  `PUBLIC`.
- **The live gate on `main` does not see it.** On `main` (at `28a8f0d`, and unchanged in `src/shared/db.ts` and `src/hoopdb/migrate.ts` through `44615f2`), `schemaVerdict` (`src/shared/db.ts`) runs exactly the ledger check,
  the marker check, phase 2 (the Worker role validation, which ignores `PUBLIC`) and the
  marker again. It never inspects other `sessions` functions, so a PUBLIC-executable
  function created after PASS does not close the gate, and readers stay ready.
- **No migrate on `main` fixes it either.** `migrate.ts` revokes PUBLIC `EXECUTE` only
  through each file's own statements, when that file is first applied. A ledgered file is
  never rerun, and no step revokes PUBLIC on functions created outside the migration
  files.
- **Pending, unmerged: draft PR #99** (code at `960ed64`, under review). It adds a live
  `SESSIONS_FUNCTION_GATE_SQL` check to readiness: "no sessions function may be
  EXECUTE-able through PUBLIC, nor by the connected (runtime) role unless it owns the
  function … or is a superuser". Its migrator also "revokes PUBLIC EXECUTE on every
  migrate". Even if merged, that gate fails readiness; it does not stop a direct call
  made before it runs. Its migrator gate also refuses before any write unless the
  owner's global function default is exactly `{<self>:EXECUTE}` and every `sessions`
  creator is covered (see 3.4). Until #99 merges, none of this applies to `main`.

**The required end state.** Every intended RPC has explicit `EXECUTE` grants to the exact
API roles that need it, and `PUBLIC` is revoked. Functions created later do not become
PUBLIC-executable.

Production activation stays blocked until **one** of these is proved and approved by
Khaliq:

- **(A) Owner-scoped global default revoke.** Run
  `ALTER DEFAULT PRIVILEGES FOR ROLE <creator role> REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;`
  for every role that creates functions in `sessions`. It comes with an ACL manifest:
  Y4 and Y6 before and after, listing every function whose effective `EXECUTE` changes,
  and every caller that relies on PUBLIC `EXECUTE` today. Khaliq approves the blast
  radius.
  - **Or an equivalent durable control**, for example a reviewed schema-specific event
    trigger that revokes `PUBLIC` `EXECUTE` on every function created in `sessions`.
  - The repo contains neither, nor a manifest: unknown. Either one is a privilege change
    and needs its own OK.
- **(B) No untrusted path to `sessions`.** Prove two things. First, PostgREST does not
  expose `sessions` (`db_schemas`). Second, no untrusted role (`anon`, `authenticated`
  or similar) has `USAGE` on `sessions`, directly or through membership, or a direct
  database path. (B) is not weakened here: if any one of these holds, (B) is unsatisfied.

**What the repo says (B) faces.** The `sessions` RPCs are intentionally reachable through
PostgREST and `authenticator`:

- `20261025000000_sessions_index.sql` line 522 runs `GRANT USAGE ON SCHEMA sessions TO
authenticated, service_role;`.
- The search RPC is called as PostgREST `POST /rpc/search_session_directory` with
  `Content-Profile: sessions` (`src/hoopdb/search-query.ts`,
  `src/hoopdb/search-index-contract.ts`, `docs/aws-e2e-proof.md`).
- The Worker reaches it through the separately approved `GRANT
session_directory_search_worker TO authenticator` binding.

On that design (B) is unsatisfied by repo design, and activation requires (A) or an
equivalent reviewed durable control. The queries below record the live state. They do not change the requirement.

**`service_role`: a broad, trusted-admin decision (not the PUBLIC-boundary blocker).**

- `20261025000000_sessions_index.sql` lines 527 and 528 run `GRANT ALL ON ALL TABLES IN
SCHEMA sessions TO service_role;` and `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions
TO service_role;`.
- `20261025000100_repo_identity.sql` line 689, `20261025000200_scrub_quarantine.sql`
  line 197 and `20261025000300_upsert_keep_on_null.sql` line 170 each repeat `GRANT
EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions TO service_role;`. Line 170 grants
  `EXECUTE` on every function that exists in `sessions` when it runs, to `service_role`,
  and nothing else. Lines 166 to 169 of the same file revoke all function privileges
  from `PUBLIC`, `anon` and `authenticated`, then re-grant `authenticated` `EXECUTE` on
  `jwt_org_id()`, `jwt_workspace_ids()`, `member_of(text, text[])`,
  `repo_group(text, uuid)`, `repo_live(text, uuid)` and
  `repo_visibility_ids(text, uuid)`.
- `ON ALL …` covers only the objects that exist when each statement runs. Later files
  grant `service_role` explicitly (for example `GRANT ALL ON sessions.session_directory…
TO service_role`).
- The G6 files revoke `service_role` explicitly from the Worker RPCs:
  `search_session_directory`, `session_directory_pointers`,
  `session_directory_readiness` and `session_directory_worker_scope`, plus
  `session_directory_record_fences` and `session_directory_search_token_policy`.

`service_role` is the trusted, privileged backend role, so this breadth is recorded as an
explicit trusted-admin decision for Khaliq. It is not part of B15. It belongs in the ACL
manifest as expected:

- `service_role`: `USAGE` on `sessions`; `ALL` on the tables those files cover;
  `EXECUTE` on the functions those files cover; **not** `EXECUTE` on the four Worker RPCs
  above; no rights on the token policy table.
- `authenticated`, `anon` and `session_directory_search_worker` **must not be broadened**:
  - `authenticated` keeps exactly the migrations' explicit grants (Y3 and Y4; A1 and A2);
  - `anon` keeps none;
  - the Worker keeps exactly phase 2's allowlist.

  An `ON ALL …` or default-privilege grant to any of the three is a violation.

**Evidence queries.** Read-only, names and booleans only, each needs Khaliq's OK. **All
NOT OBSERVED.** Record each in the section 0 format.

| id  | maps                                                                                                                                                      | query                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | expected under (A) or (B)                                                                                                                                                                                                                      | observed     |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| Y1  | every role `authenticator` can become, directly or through nested membership (PostgREST can `SET ROLE` to any of them)                                    | `WITH RECURSIVE reach(oid, via) AS (SELECT m.roleid, 'direct'::text FROM pg_auth_members m JOIN pg_roles a ON a.oid = m.member WHERE a.rolname = 'authenticator' UNION SELECT m.roleid, 'nested'::text FROM pg_auth_members m JOIN reach ON m.member = reach.oid) SELECT g.rolname AS can_become, min(reach.via) AS via FROM reach JOIN pg_roles g ON g.oid = reach.oid GROUP BY 1 ORDER BY 1;`                                                                                                                                                                                                                                                                                                                                                                                                                               | recorded. Every `can_become` role must appear in Y3, Y4 and Y5.                                                                                                                                                                                | NOT OBSERVED |
| Y2  | exposed schemas, from in-database PostgREST config. Prints only the `pgrst.db_schemas` entry; every other setting, including any secret, is filtered out. | `SELECT r.rolname, s.setdatabase, x AS db_schemas_setting FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole CROSS JOIN LATERAL unnest(s.setconfig) AS x WHERE r.rolname = 'authenticator' AND x LIKE 'pgrst.db\_schemas=%';`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | (B): no row listing `sessions`. (A): recorded.                                                                                                                                                                                                 | NOT OBSERVED |
| Y2b | exposed schemas, if not set in the database                                                                                                               | the project's API settings ("exposed schemas"), read by Khaliq. Where Supabase keeps this is not stated in the repo: unknown.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | as Y2                                                                                                                                                                                                                                          | NOT OBSERVED |
| Y3  | `USAGE` on `sessions`, effective (membership-aware) and direct-or-PUBLIC, for the fixed API roles **plus every Y1 role**                                  | `WITH RECURSIVE reach(oid) AS (SELECT m.roleid FROM pg_auth_members m JOIN pg_roles a ON a.oid = m.member WHERE a.rolname = 'authenticator' UNION SELECT m.roleid FROM pg_auth_members m JOIN reach ON m.member = reach.oid), r AS (SELECT oid, rolname FROM pg_roles WHERE rolname IN ('anon', 'authenticated', 'authenticator', 'service_role', 'session_directory_search_worker') OR oid IN (SELECT oid FROM reach)) SELECT r.rolname, has_schema_privilege(r.oid, 'sessions', 'USAGE') AS usage_effective, EXISTS (SELECT 1 FROM pg_namespace n, aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) a WHERE n.nspname = 'sessions' AND a.grantee IN (r.oid, 0) AND a.privilege_type = 'USAGE') AS usage_direct_or_public FROM r ORDER BY 1;`                                                                     | (B): `anon`, `authenticated` and every untrusted Y1 role `f`. As designed: `authenticated`, `service_role` and the Worker `t`.                                                                                                                 | NOT OBSERVED |
| Y4  | effective `EXECUTE` per `sessions` function, for the fixed API roles **plus every Y1 role** (one row per function and role)                               | `WITH RECURSIVE reach(oid) AS (SELECT m.roleid FROM pg_auth_members m JOIN pg_roles a ON a.oid = m.member WHERE a.rolname = 'authenticator' UNION SELECT m.roleid FROM pg_auth_members m JOIN reach ON m.member = reach.oid), r AS (SELECT oid, rolname FROM pg_roles WHERE rolname IN ('anon', 'authenticated', 'authenticator', 'service_role', 'session_directory_search_worker') OR oid IN (SELECT oid FROM reach)) SELECT p.oid::regprocedure AS function, r.rolname, has_function_privilege(r.oid, p.oid, 'EXECUTE') AS can_execute, EXISTS (SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE') AS public_execute FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace CROSS JOIN r WHERE n.nspname = 'sessions' ORDER BY 1, 2;` | `public_execute = f` on every function. Each `can_execute = t` is an intended, explicit grant listed in the ACL manifest. The `service_role` rows match the 4.2 ruling. After the apply, the Worker is `t` exactly on the three approved RPCs. | NOT OBSERVED |
| Y5  | direct login path and elevated attributes, for the fixed API roles **plus every Y1 role**                                                                 | `WITH RECURSIVE reach(oid) AS (SELECT m.roleid FROM pg_auth_members m JOIN pg_roles a ON a.oid = m.member WHERE a.rolname = 'authenticator' UNION SELECT m.roleid FROM pg_auth_members m JOIN reach ON m.member = reach.oid), r AS (SELECT oid, rolname FROM pg_roles WHERE rolname IN ('anon', 'authenticated', 'authenticator', 'service_role', 'session_directory_search_worker') OR oid IN (SELECT oid FROM reach)) SELECT r.rolname, g.rolcanlogin, g.rolsuper, g.rolbypassrls, g.rolinherit FROM r JOIN pg_roles g ON g.oid = r.oid ORDER BY 1;`                                                                                                                                                                                                                                                                        | `anon`, `authenticated` and every untrusted Y1 role: `rolcanlogin = f`, `rolsuper = f`, `rolbypassrls = f`.                                                                                                                                    | NOT OBSERVED |
| Y6  | default privileges that grant `EXECUTE` to PUBLIC on new functions                                                                                        | `SELECT pg_get_userbyid(d.defaclrole) AS creator, n.nspname AS in_schema, a.privilege_type, a.grantee = 0 AS to_public FROM pg_default_acl d LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace CROSS JOIN LATERAL aclexplode(d.defaclacl) a WHERE d.defaclobjtype = 'f' ORDER BY 1, 2;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Under (A), after the revoke: a row per function creator showing the revoke is in effect, and no `to_public = t`. Before it: recorded. With no `pg_default_acl` row for a creator, the built-in default (PUBLIC `EXECUTE`) applies.             | NOT OBSERVED |
| Y7  | event triggers, for the "equivalent durable control" option                                                                                               | `SELECT evtname, evtevent, evtenabled, evtfoid::regprocedure AS function, evttags FROM pg_event_trigger ORDER BY 1;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | under the event-trigger option: the reviewed trigger, enabled, on `ddl_command_end` for `CREATE FUNCTION`. Otherwise: recorded.                                                                                                                | NOT OBSERVED |

**Role coverage and STOP rule.** Y3, Y4 and Y5 take their role set from the same
recursive `authenticator` membership as Y1, plus the fixed API roles. They resolve roles
by OID, so a role that does not exist (for example the Worker before phase 1) simply has
no row; record its absence. **STOP** and take it to Khaliq, without deciding (A) or (B),
when any of these holds:

- a role Y1 returns has no row in Y3, Y4 or Y5 (for example a query was edited, errored
  or timed out);
- a Y1 role is not one of `anon`, `authenticated`, `service_role` or
  `session_directory_search_worker`, and the ACL manifest does not already name it as an
  approved API role with its exact expected grants.

## 5. Apply sequence (placeholders only; every step needs Khaliq's explicit OK)

No connection string appears here, by design. `<…>` marks a value supplied at execution
time by the approved mechanism (for example `op run` injecting an env var into the child
process only, as `scripts/sb` does). Never write such a value to disk or into this
document.

0. **Pin the code.** Check out `28a8f0d`, or build the exact image the readers will run,
   and compute the generation locally (no connection):

   ```sh
   git -C <checkout> rev-parse HEAD            # expect 28a8f0d1243494a8e6bf6f9dd11a9380c5b86285
   GEN=$(bun src/hoopdb/migrate.ts --generation)
   echo "$GEN"                                 # offline expectation: d48ab0224b0e…b5ef86
   ```

   Run it from the same image as the readers, because the readers compare this exact
   value.

1. **Preflight** (section 4). Record every result in the section 0 format. Stop on any
   unexpected result.

2. **Phase 1 (privileged).** Only with the 1.3 decision for the observed version, as the
   approved identity:

   ```sh
   psql -v ON_ERROR_STOP=1 -X -d "<bootstrap connection, injected at exec time>" \
     -f supabase/bootstrap/session_directory_search_worker.sql
   ```

   Exit 0: the role exists to spec. `42501`: this caller may not create it, so stop
   (section 1). `55000`: an existing role differs; nothing was changed, so stop. An
   operator decides the fix.

3. **Phase 1 checks** (P3, P3b, P4): every attribute `f`; exactly
   `0 | {statement_timeout=2s}`; no members; no `pg_shdepend` rows.

4. **Migrate**, as the approved schema-owner identity:

   ```sh
   HOOPDB_SUPABASE_SHIM=false \
   HOOPDB_ROLE_BOOTSTRAP=false \
   HOOPDB_ROLE_VERIFY=true \
   HOOPDB_URL="<migrator connection, injected at exec time>" \
   bun src/hoopdb/migrate.ts
   ```

   - `HOOPDB_SUPABASE_SHIM=false` is mandatory. The default (`true`) applies
     `supabase/tests/supabase_shim.sql` ("Never apply this to a Supabase project") and
     ledgers it.
   - `HOOPDB_ROLE_BOOTSTRAP=false`: phase 1 already ran as its own approved step. Setting
     it to `true` would rerun phase 1 as the migrator. That only verifies when the role
     exists, but it would create the role if the role were absent and the migrator is a
     CREATEROLE role below 160000, which bypasses the separately approved step 2.
   - `HOOPDB_ROLE_VERIFY=true` (the default): phase 2 and `'pass'` happen in-process.
   - The equivalent container or Job (`96b-hoopdb-migrate`) needs the same three flags.
     Whether production runs the binary or a Job is unknown.
   - **If draft PR #99 merges first** (code at `960ed64`, pending, not merged): the
     profile is resolved before any connection. `HOOPDB_DB_MODE=supabase` becomes
     required, `HOOPDB_SUPABASE_SHIM` must be unset or `false`, `HOOPDB_ROLE_BOOTSTRAP`
     must not be forced on, and `HOOPDB_RUNTIME_ROLE=<runtime role>` (the RT1 or R2 name)
     and `HOOPDB_RUNTIME_ROLE_LOGIN=<true|false>` become required. On Supabase the
     operator must create that dedicated role first (its own privileged step and OK);
     the migrator only verifies it. That role is
     then granted `USAGE` on `sessions` and `SELECT` on the ledger and the marker. The run
     also refuses `mixed_migration_authority`, `unledgered_schema` and `ledger_gap`. Rerun
     RT3 and RT4 after the migrate.
   - Expected log lines: `role validation pending` → `migration applied` for each
     unledgered file → `role validation passed` → `hoopdb migrations complete`.

5. **External verifier path.** Only if step 4 ran with `HOOPDB_ROLE_VERIFY=false`. The
   log then says `role validation left pending: in-process verifier disabled; an
external verifier must pass it`. From the repository root (psql 12+, `\ir` resolves
   the verifier next to the script):

   ```sh
   GEN=$(bun src/hoopdb/migrate.ts --generation)      # same image as the readers
   psql -v ON_ERROR_STOP=1 -X -v generation="$GEN" \
     -d "<verifier connection, injected at exec time>" \
     -f supabase/bootstrap/session_directory_search_worker_mark_pass.sql
   ```

   In one transaction it runs `pg_advisory_xact_lock(961996, 1)`, then phase 2, then sets
   `'pass'` only where `status = 'pending'` for exactly `$GEN`. Otherwise it fails with
   `55000` and changes nothing.

6. **Post-apply evidence** (section 8).

**Out of scope for this activation. Each is its own reviewed step with its own OK, and
the repo gives no values for it:**

- seeding `sessions.session_directory_search_token_policy` (issuer, audience). Until it
  is seeded, every RPC call is refused with `42501`;
- `GRANT session_directory_search_worker TO authenticator` (NOINHERIT, no ADMIN, no
  INHERIT grant on 16+) and issuing the worker's credential;
- setting `session_directory_rollout.completed_at` (the backfill lane);
- the contract revoke (2.4);
- any image deploy.

## 6. Expected locks and timing

Lock levels are PostgreSQL's documented levels for each statement type. **None of them
was observed on the target.** Under `migrate.ts`, each file is one transaction, so every
lock taken inside a file is held until that file's `COMMIT`. A transaction holding
`ACCESS EXCLUSIVE` blocks all reads and writes of that table until then.
`CREATE INDEX CONCURRENTLY` cannot run inside a transaction block, so no file can use it
under this runner. **Every index build in the set is non-concurrent.** `migrate.ts` sets
no `lock_timeout`. A queued `ACCESS EXCLUSIVE` request therefore waits indefinitely
behind any open transaction on that table, and every later query on the table queues
behind it. The repo does not provide a way to set `lock_timeout` for the migrator
session (unknown; see blocker B7).

### 6.1 G6 files

| file             | statement                                                                                                                            | lock on existing objects                                                                                     | blocks                                                                     | timing                                                                                                                                           |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `20261030010000` | `CREATE EXTENSION IF NOT EXISTS pg_trgm`                                                                                             | none on sessions tables                                                                                      | —                                                                          | unknown                                                                                                                                          |
|                  | `CREATE TABLE` ×4 (`session_directory*`, `_rollout`)                                                                                 | new tables. The `_users` and `_machines` foreign keys reference a table created in the same transaction.     | nothing outside the transaction                                            | unknown; expected small (new objects)                                                                                                            |
|                  | 13 × `CREATE INDEX` (non-concurrent; two GIN trigram)                                                                                | `SHARE` on the new tables only                                                                               | nothing outside the transaction (the tables are empty and not yet visible) | unknown; expected small (empty tables)                                                                                                           |
|                  | `ENABLE ROW LEVEL SECURITY`, `CREATE POLICY`, `GRANT` / `REVOKE`                                                                     | new tables only                                                                                              | —                                                                          | unknown                                                                                                                                          |
| `20261030020000` | `DO $role_guard$`                                                                                                                    | catalog reads                                                                                                | —                                                                          | unknown                                                                                                                                          |
|                  | `ALTER TABLE sessions.session_directory ADD COLUMN …` with CHECKs, then `ADD CONSTRAINT … CHECK` ×2 (validated, not `NOT VALID`)     | **`ACCESS EXCLUSIVE` on `sessions.session_directory`**, held to COMMIT. The validated CHECKs scan the table. | all reads and writes of the directory                                      | **unknown**: depends on row count (P14, NOT OBSERVED). If `20261030010000` applies in the same run, the table is empty.                          |
|                  | `CREATE TRIGGER session_directory_tombstone_clears_summary`                                                                          | `SHARE ROW EXCLUSIVE` (already covered by the `ACCESS EXCLUSIVE` above)                                      | —                                                                          | unknown                                                                                                                                          |
|                  | `DROP INDEX sessions.session_directory_metadata_trgm_idx`, `DROP INDEX sessions.session_directory_project_trgm_idx` (non-concurrent) | `ACCESS EXCLUSIVE` on the directory                                                                          | all access                                                                 | unknown                                                                                                                                          |
|                  | `CREATE INDEX session_directory_tenant_search_trgm_idx`, `…_tenant_project_trgm_idx` (GIN, **non-concurrent**)                       | `SHARE` (already inside the `ACCESS EXCLUSIVE`)                                                              | all access, until COMMIT                                                   | **unknown**: proportional to directory rows and `search_text` size. GIN trigram builds over a populated table are the largest timing risk in G6. |
|                  | `REVOKE SELECT … FROM authenticated` / column `GRANT SELECT (...)`                                                                   | catalog update; lock level not stated in the repo                                                            | unknown                                                                    | unknown                                                                                                                                          |
|                  | functions, the token-policy table, grants on functions                                                                               | new objects                                                                                                  | —                                                                          | unknown                                                                                                                                          |
| `20261031001000` | `CREATE TABLE sessions.session_directory_resurrection_fences` with a foreign key to the directory                                    | `SHARE ROW EXCLUSIVE` on `sessions.session_directory`                                                        | writes to the directory                                                    | unknown; expected short (no scan)                                                                                                                |
|                  | `CREATE TRIGGER session_directory_record_fences`                                                                                     | `SHARE ROW EXCLUSIVE` on the directory                                                                       | writes                                                                     | unknown                                                                                                                                          |
|                  | functions, the `DO $revoke$` grant loop                                                                                              | new objects                                                                                                  | —                                                                          | unknown                                                                                                                                          |

### 6.2 When the G6 lock risk is low

If P7 shows `20261030010000` unapplied, `sessions.session_directory` is created in the
same run as `20261030020000`. It holds no rows, because nothing writes it before the
projector runs in a deployed image. In that case the G6 `ACCESS EXCLUSIVE` and
non-concurrent GIN builds are over an empty table and contend with nothing.

If P7 shows `20261030010000` already applied and P14 shows a populated directory, the
`20261030020000` transaction blocks every directory read and write for an unknown
duration. That needs a maintenance-window decision (blocker B7).

### 6.3 Per-file impact, all 17 migrations

Read this table with these definitions:

- **Existing tables touched**: tables the file changes that an earlier file created.
  These are the only places a backlog run can contend with live traffic.
- **Strongest lock**: the strongest documented lock any statement in the file takes on an
  existing table. It is held until the file's `COMMIT`.
- **Rows touched**: DML the file runs at migrate time.
- **Destructive**: the file drops or deletes data, or drops an object that is not
  recreated.
- **Fresh run**: every table the file touches was created earlier in the same run. Rows
  are then 0 and nothing contends.
- **Backlog run**: the file applies to a database where earlier files were applied
  earlier and their tables hold production rows. Row counts are NOT OBSERVED (P14).

Timing is **unknown** for every file on a backlog run: no row counts are observed, and
the repo has no timing evidence against production-shaped data.

| #   | file                                                               | creates (new)                                                                                                                                                                                                                                                                                                                                                                            | existing tables touched (statements)                                                                                                                                                                                                                                                                                                                          | strongest lock on existing tables                                                                                            | rows touched at migrate time                                                                                                                                                                                                                                                                                                                         | destructive?                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | backlog effect if earlier files hold production rows                                                                                                                                                                           |
| --- | ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | `20261025000000_sessions_index.sql`                                | schema `sessions` (`IF NOT EXISTS`); `session_index` + 9 indexes; `machines` (`IF NOT EXISTS`); `pipeline_events` (partitioned by `at`) + default partition + 2 indexes; `pipeline_dead_letters` + 2 indexes; functions (`pipeline_events_maintain`, `upsert_session_index`, `set_session_state`, `upsert_machine`, `jwt_org_id`, `jwt_workspace_ids`, `member_of`, …); RLS + 2 policies | `sessions.machines` only if it already exists (`ALTER TABLE … ADD COLUMN IF NOT EXISTS machine_name, hostname_aliases text[] NOT NULL DEFAULT '{}', client_version`)                                                                                                                                                                                          | `ACCESS EXCLUSIVE` on `machines` (only if it exists)                                                                         | `DO $$ BEGIN PERFORM sessions.pipeline_events_maintain(); END $$;` runs at migrate time. It creates monthly partitions, `DROP TABLE`s expired partitions, `DELETE FROM sessions.pipeline_events_default WHERE at < v_cutoff`, and `DELETE FROM sessions.pipeline_dead_letters WHERE state = 'resolved' AND resolved_at < now() - interval '30 days'` | **no at migrate time.** On any run where this file gets that far, `pipeline_events` and `pipeline_dead_letters` were created earlier in the same transaction (no `IF NOT EXISTS`), so they are empty and the maintain call only creates the current and next 2 monthly partitions. The function is also the daily retention job ("run daily from the Worker's cron over REST as service_role"). That job drops expired partitions and deletes rows, but it is operational, not part of the migrate. | Cannot apply on a backlog: `CREATE TABLE sessions.session_index` has no `IF NOT EXISTS`, so it fails with `42P07` and rolls back. That is the B3 case.                                                                         |
| 2   | `20261025000100_repo_identity.sql`                                 | `repos`, `repo_names` + indexes; repo functions; replaces `upsert_session_index`; RLS + 2 policies                                                                                                                                                                                                                                                                                       | `session_index`: `ADD COLUMN git_remote_url`, `git_root_commit … CHECK (…)`, `repo_id`; `CREATE INDEX si_remote`, `CREATE INDEX si_repo` (non-concurrent)                                                                                                                                                                                                     | `ACCESS EXCLUSIVE` on `session_index` (`ADD COLUMN` with `CHECK`), then `SHARE` for the two index builds, all held to COMMIT | none (the backfill functions are created, not run)                                                                                                                                                                                                                                                                                                   | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | **High risk:** two non-concurrent index builds plus a validated `CHECK` on `session_index`, the physical manifest table, all under `ACCESS EXCLUSIVE`. Blocks every read and write of `session_index` for an unknown duration. |
| 3   | `20261025000200_scrub_quarantine.sql`                              | trigger function `session_index_scrub_sticky`; replaces `upsert_session_index`                                                                                                                                                                                                                                                                                                           | `session_index`: `ADD COLUMN scrubbed boolean NOT NULL DEFAULT false`, `ADD COLUMN scrub_quarantined boolean NOT NULL DEFAULT false`, `COMMENT ON COLUMN` ×2, `CREATE TRIGGER session_index_scrub_sticky`                                                                                                                                                     | `ACCESS EXCLUSIVE` on `session_index`                                                                                        | none                                                                                                                                                                                                                                                                                                                                                 | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `ACCESS EXCLUSIVE` on `session_index`. Constant defaults; whether this rewrites the table is not stated in the repo, so the cost is unknown.                                                                                   |
| 4   | `20261025000300_upsert_keep_on_null.sql`                           | —                                                                                                                                                                                                                                                                                                                                                                                        | none (`CREATE OR REPLACE FUNCTION sessions.upsert_session_index`)                                                                                                                                                                                                                                                                                             | none on tables (function replace only)                                                                                       | none                                                                                                                                                                                                                                                                                                                                                 | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Changes upsert semantics for live writers immediately at COMMIT.                                                                                                                                                               |
| 5   | `20261025000600_pipeline_queue.sql`                                | `pipeline_jobs` + 3 indexes                                                                                                                                                                                                                                                                                                                                                              | `pipeline_events`: `DROP CONSTRAINT` / `ADD CONSTRAINT pipeline_events_stage_check` (validated), and `UPDATE … SET stage = …`. `pipeline_dead_letters`: the same constraint swap, `UPDATE … SET stage, id, message`, `ADD COLUMN session_id, machine_id`, and `CREATE INDEX pipeline_dead_letters_session_idx` (non-concurrent)                               | `ACCESS EXCLUSIVE` on `pipeline_events` (every partition) and `pipeline_dead_letters`                                        | `UPDATE` of every `pipeline_events` row with stage `storage.event`, `index.upsert` or `transform`; `UPDATE` of the matching `pipeline_dead_letters` rows, **rewriting their primary key `id`**                                                                                                                                                       | data rewrite (stage renames, dead-letter ids rewritten), no deletes                                                                                                                                                                                                                                                                                                                                                                                                                                 | Full validation scans of both tables, plus an index build, under `ACCESS EXCLUSIVE`. Blocks pipeline writes.                                                                                                                   |
| 6   | `20261030000000_relayhistory_machines_compat.sql`                  | —                                                                                                                                                                                                                                                                                                                                                                                        | `machines`: `ADD COLUMN IF NOT EXISTS workspace_id text NOT NULL DEFAULT 'default'`, `hostname`, `label`, `relayhistory_version`, `cursors_json jsonb NOT NULL DEFAULT '{}'::jsonb`; `ALTER COLUMN workspace_id SET DEFAULT 'default'`; `REVOKE SELECT … FROM authenticated` plus a column `GRANT SELECT`                                                     | `ACCESS EXCLUSIVE` on `machines`                                                                                             | none                                                                                                                                                                                                                                                                                                                                                 | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | **Narrows `authenticated`** to 8 columns of `machines`. Any tenant reader that selects other columns starts getting `42501`.                                                                                                   |
| 7   | `20261030010000_session_directory.sql` (#38)                       | `session_directory`, `_users`, `_machines`, `_rollout`; **13** indexes; RLS + 3 policies; grants                                                                                                                                                                                                                                                                                         | none                                                                                                                                                                                                                                                                                                                                                          | none on existing tables                                                                                                      | `INSERT INTO sessions.session_directory_rollout (singleton) VALUES (true)`                                                                                                                                                                                                                                                                           | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | none (all new objects)                                                                                                                                                                                                         |
| 8   | `20261030020000_session_directory_short_summary.sql` (#38/#72/#96) | 3 functions + the search RPC; `session_directory_search_token_policy` (empty); 2 GIN indexes; grants                                                                                                                                                                                                                                                                                     | `session_directory`: `ADD COLUMN` ×5 with CHECKs, `ADD CONSTRAINT` ×2, `CREATE TRIGGER`, `DROP INDEX` ×2, `CREATE INDEX` ×2 (GIN, non-concurrent), `REVOKE` / column `GRANT`                                                                                                                                                                                  | `ACCESS EXCLUSIVE` on `session_directory`                                                                                    | none                                                                                                                                                                                                                                                                                                                                                 | drops `session_directory_metadata_trgm_idx` and `session_directory_project_trgm_idx`, replaced in the same transaction                                                                                                                                                                                                                                                                                                                                                                              | If `session_directory` is populated: full scans and two GIN builds under `ACCESS EXCLUSIVE`. `authenticated` loses `search_text` and `short_summary` (`42501` for any reader of those columns).                                |
| 9   | `20261031000000_migration_assertion_ledger.sql`                    | `migration_assertion_uses`, `migration_assertion_chunks`, `migration_ingest_audit` + indexes; an append-only trigger; grants                                                                                                                                                                                                                                                             | none                                                                                                                                                                                                                                                                                                                                                          | none                                                                                                                         | none                                                                                                                                                                                                                                                                                                                                                 | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | none (new objects)                                                                                                                                                                                                             |
| 10  | `20261031000100_manifest_readiness.sql`                            | `manifest_readiness`, `backfill_activation_ready`, `export_deferred_sessions`, `claimless_uses` (`IF NOT EXISTS`); grants                                                                                                                                                                                                                                                                | `pipeline_events`, `pipeline_dead_letters`, `pipeline_jobs`: `DROP CONSTRAINT …_stage_check` then `ADD CONSTRAINT …_stage_check CHECK (…)` (validated)                                                                                                                                                                                                        | `ACCESS EXCLUSIVE` on all three pipeline tables                                                                              | none                                                                                                                                                                                                                                                                                                                                                 | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | **High risk:** three full validation scans under `ACCESS EXCLUSIVE` on the queue and event tables. Blocks ingest writes and worker claims.                                                                                     |
| 11  | `20261031001000_session_directory_reads_expand.sql` (#38/#72)      | `session_directory_resurrection_fences`; 4 functions; grants                                                                                                                                                                                                                                                                                                                             | `session_directory`: a foreign key from the new table; `CREATE TRIGGER session_directory_record_fences`                                                                                                                                                                                                                                                       | `SHARE ROW EXCLUSIVE` on `session_directory`                                                                                 | none                                                                                                                                                                                                                                                                                                                                                 | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Blocks directory writes briefly; no scan.                                                                                                                                                                                      |
| 12  | `20261101000000_migration_ingest_audit_base_read.sql`              | `migration_base_read_uses` + index; grants                                                                                                                                                                                                                                                                                                                                               | `migration_ingest_audit`: `DROP CONSTRAINT` / `ADD CONSTRAINT migration_ingest_audit_result_check` (validated)                                                                                                                                                                                                                                                | `ACCESS EXCLUSIVE` on `migration_ingest_audit`                                                                               | none                                                                                                                                                                                                                                                                                                                                                 | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Validation scan of the audit table.                                                                                                                                                                                            |
| 13  | `20261101000200_claimless_window_activations.sql`                  | `claimless_window_activations`; an append-only function and 2 triggers; grants                                                                                                                                                                                                                                                                                                           | none                                                                                                                                                                                                                                                                                                                                                          | none                                                                                                                         | none                                                                                                                                                                                                                                                                                                                                                 | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | none (new objects)                                                                                                                                                                                                             |
| 14  | `0001_96bulls_observability.sql`                                   | 2 trigger functions; `pipeline_jobs_session_uuid_idx`                                                                                                                                                                                                                                                                                                                                    | `pipeline_jobs`: `ADD COLUMN IF NOT EXISTS session_uuid, tenant_id, traceparent … CHECK (…)`, `DROP TRIGGER IF EXISTS` / `CREATE TRIGGER pipeline_jobs_observability`, `CREATE INDEX IF NOT EXISTS pipeline_jobs_session_uuid_idx` (non-concurrent). `pipeline_dead_letters`: `ADD COLUMN IF NOT EXISTS session_uuid, traceparent`, and the trigger replaced. | `ACCESS EXCLUSIVE` on `pipeline_jobs` and `pipeline_dead_letters`                                                            | none                                                                                                                                                                                                                                                                                                                                                 | drops and recreates its own triggers                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Blocks claims on `pipeline_jobs` for a `CHECK` scan plus an index build.                                                                                                                                                       |
| 15  | `0002_session_ext_and_summaries.sql`                               | `session_index_ext`, `session_summaries`, `session_verdicts` (`IF NOT EXISTS`); RLS; grants                                                                                                                                                                                                                                                                                              | none, unless the tables pre-exist (then the `CREATE … IF NOT EXISTS` statements are no-ops)                                                                                                                                                                                                                                                                   | none                                                                                                                         | none                                                                                                                                                                                                                                                                                                                                                 | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | none                                                                                                                                                                                                                           |
| 16  | `0003_fanout_ledger.sql`                                           | —                                                                                                                                                                                                                                                                                                                                                                                        | `session_index_ext`: `ADD COLUMN IF NOT EXISTS fanout_revision bigint NOT NULL DEFAULT 0`                                                                                                                                                                                                                                                                     | `ACCESS EXCLUSIVE` on `session_index_ext`                                                                                    | none                                                                                                                                                                                                                                                                                                                                                 | no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Constant default; rewrite cost unknown.                                                                                                                                                                                        |
| 17  | `0004_summary_pointers.sql`                                        | —                                                                                                                                                                                                                                                                                                                                                                                        | `session_summaries`: **`DELETE FROM sessions.session_summaries;`**, `DROP COLUMN IF EXISTS summary, model_id`, `ADD COLUMN IF NOT EXISTS status text NOT NULL CHECK (…)`, `object_key`, `object_sha256 … CHECK`, `key_id`, `DROP` / `ADD CONSTRAINT session_summaries_pointer`                                                                                | `ACCESS EXCLUSIVE` on `session_summaries`                                                                                    | **deletes every row**                                                                                                                                                                                                                                                                                                                                | **yes**: all rows deleted, columns `summary` and `model_id` dropped. The file says the rows are derived data that Bob Costas's sweep rebuilds.                                                                                                                                                                                                                                                                                                                                                      | On a backlog run, every stored summary is gone until the sweep rebuilds it. That needs its own explicit OK (B8).                                                                                                               |

The G6-only statement detail is in 6.1. Lock levels are PostgreSQL's documented levels
for each statement type, not observations.

### 6.4 Readiness and availability risk: zero downtime cannot be guaranteed

**Zero downtime cannot be guaranteed for this activation.** Whether readers go unready,
and how many, depends on which images are deployed now. The deployed images and their
generation are **NOT OBSERVED** (D1 to D3 below). What happens to traffic then depends on
the orchestrator's readiness behavior, which is also NOT OBSERVED (O1).

What the code says, by deployed reader version:

- **Readers on #96 or later** (images containing `28a8f0d`'s `schemaVerdict`):
  - `MARKER_PENDING_SQL` upserts the single row `name = 'session_directory_search_worker'`
    to `('pending', <new GEN>)` before any migration step.
  - Every such reader (ingest `/readyz`, worker `/readyz`, worker claim gate) requires
    `status = 'pass' AND generation = <its own GEN>`.
  - So from the pending write until phase 2 passes, **all** of them go unready at once.
    Every ingest replica returns `/readyz` 503 (`validation_pending`). Every worker returns
    503 and claims nothing ("worker paused: live role validation gate closed; claiming
    nothing").
  - After the pass, a #96+ reader built for a different generation stays 503 until an
    image of the new generation replaces it.
- **Readers older than #96** do not read the marker. Their `waitForSchema` and ingest
  `/readyz` (for example at `8ec4b09`) only check that their own expected names are in
  `sessions.__96bulls_migrations`. A migrate that only adds ledger rows does not make them
  unready. They are still exposed to the locks below, and to the grant changes in 6.3
  (`20261030000000` narrows `machines` for `authenticated`; `20261030020000` narrows
  `session_directory`).
- **Any reader:** the `ACCESS EXCLUSIVE` locks in 6.1 and 6.3 block reads and writes on
  the affected tables for the duration of each file, whatever `/readyz` says.

**D1 to D3: deployed image and generation observation** (D1 and D2 need Khaliq's OK and
are read-only against the cluster and the registry. D3 is local and read-only: it reads
git objects and writes nothing. All three print image references, digests, revisions and
hashes only.)

```sh
# D1: image and resolved digest of every running container that uses HoopDb
kubectl --context <prod context> -n <namespace> get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .status.containerStatuses[*]}{.name}={.image}@{.imageID}{" "}{end}{"\n"}{end}'
# D2: the source revision of each distinct digest. The publish workflows set the
# org.opencontainers.image.revision label to github.sha. crane is one way to read it;
# whether it is installed is unknown.
crane config <repository>@<digest from D1> | jq -r '.config.Labels["org.opencontainers.image.revision"]'
# D3 (local, read-only, no connection; run inside a clone that has the revision): is it
# #96 or later, and what generation does it expect? Reads git objects only; creates no
# worktree, checkout, ref or file.
git merge-base --is-ancestor 28a8f0d1243494a8e6bf6f9dd11a9380c5b86285 <revision from D2> && echo "#96+" || echo "pre-#96"
python3 -I - <revision from D2> <<'PY'
# The generation() algorithm of src/hoopdb/migrate.ts at #96+, computed from git objects.
import hashlib, json, subprocess, sys
rev = sys.argv[1]
def sql(d):
    out = subprocess.check_output(["git", "ls-tree", "--name-only", f"{rev}:{d}"], text=True)
    return sorted(n for n in out.split("\n") if n.endswith(".sql"))
def sha(path):
    return hashlib.sha256(subprocess.check_output(["git", "cat-file", "blob", f"{rev}:{path}"])).hexdigest()
parts = [[f"upstream:{f}", sha(f"supabase/migrations/{f}")] for f in sql("supabase/migrations")]
parts += [[f"96bulls:{f}", sha(f"src/hoopdb/migrations/{f}")] for f in sql("src/hoopdb/migrations")]
parts.append(["verify:session_directory_search_worker", sha("supabase/bootstrap/session_directory_search_worker_verify.sql")])
print(hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest())
PY
```

The D3 generation applies only to `#96+` revisions. Before relying on it, confirm that
`git show <revision>:src/hoopdb/migrate.ts` defines the same `generation()`. For a
`pre-#96` revision, record "no marker check".

Record, per deployment: the image, the digest, the revision, `#96+` or `pre-#96`, and the
generation. Compare with P8 (the marker's current generation and status). If every reader
is pre-#96, the marker flip makes none of them unready. What remains is an availability
and latency risk from the locks: requests may block or time out while a lock is held. It
is not a guaranteed outage. If any reader is #96+, all #96+ readers go unready during the window.

**#96+ reader non-ready window** ≈ the migrate duration (unknown) + the time to roll every
#96+ reader to the new generation (unknown). This is the window in which #96+ readers
report not ready. It is not the same as a user-visible outage. That depends on the
deployed reader mix (D1 to D3: how many readers are #96+ and on which generation) and on
the orchestrator behavior (O1), and on the lock durations in 6.3 for every reader.

What happens to traffic during that window is decided by the orchestrator, which the
repo does not configure for the 96Bulls services. The chart is external, and no
`readinessProbe` for ingest or workers appears in this repo. All of the following is
**unknown**:

| orchestrator behavior                                                    | effect during the window                                                                                                                                                                                                                                                                                                                                                       |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Kubernetes `readinessProbe` on `/readyz` (port 8080 ingest, 8081 worker) | All ingest pods are removed from Service endpoints at the same time. Ingest is **unavailable**: clients see connection errors or load-balancer 5xx. Workers stay running but idle, and the queue grows. A newly started ingest pod also blocks in `waitForSchema` (`src/alleyoop/ingest.ts`, timeout 300 s, then exits), so it may restart repeatedly until the marker passes. |
| probe on a TCP port or `/healthz`, or no probe                           | Pods keep receiving traffic. Request handling during `validation_pending` is not gated by `/readyz` in the code reviewed here, but writes block on the `ACCESS EXCLUSIVE` locks. Workers still pause, because the claim gate is in-process.                                                                                                                                    |
| rolling update gated on readiness                                        | A new pod that is not ready stalls the rollout. With `maxUnavailable` > 0, old pods may already be gone. The rollout parameters are unknown.                                                                                                                                                                                                                                   |

**O1** (needs Khaliq's OK; reads the spec only):

```sh
kubectl --context <prod context> -n <namespace> get deploy -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .spec.template.spec.containers[*]}{.readinessProbe}{"\t"}{.livenessProbe}{end}{"\t"}{.spec.strategy}{"\n"}{end}'
```

Record the probe path, port and timing, and the rollout strategy, for each deployment.
Plan the window on the result (blocker B13).

## 7. Rollback and forward recovery

| step                   | rollback in the repo                                                                              | what it does                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | what it does not do                                                                                                                                          | forward recovery                                                                                                                                                                                                                                                    |
| ---------------------- | ------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| phase 1                | none (manual)                                                                                     | —                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | —                                                                                                                                                            | The runbook's removal step, as a superuser, after every down that references the role: `DROP OWNED BY session_directory_search_worker; DROP ROLE session_directory_search_worker;` "in each database". Needs its own OK.                                            |
| `20261030010000`       | **none in `supabase/rollbacks/`**                                                                 | —                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | —                                                                                                                                                            | forward-only or snapshot recovery (7.1). No down path exists; dropping the four tables by hand would be an unreviewed operation.                                                                                                                                    |
| `20261030020000`       | `supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql` (`BEGIN` … `COMMIT`) | restores full-table `GRANT SELECT` to `authenticated`; drops the search RPC, the token-policy table and `session_directory_search_query`; revokes the worker's `sessions` access; drops both tenant GIN indexes and the tombstone trigger and function; rebuilds `search_text` without the summary; drops the `short_summary*` columns and constraints; recreates `session_directory_project_trgm_idx` and `session_directory_metadata_trgm_idx` (non-concurrent, inside the transaction) | **does not drop the role** ("It does not drop the role: the migration never created it"); does not drop `btree_gin`; does not touch the ledger or the marker | Run it only after the `20261031001000` down ("Run later downs first, in reverse migration order"). Its `UPDATE` and index builds take `ACCESS EXCLUSIVE` on a possibly populated directory; timing unknown.                                                         |
| `20261031001000`       | `supabase/rollbacks/20261031001000_session_directory_reads_expand.down.sql`                       | drops `session_directory_readiness()`, `session_directory_pointers(...)`, `session_directory_worker_scope(text)`, the fences trigger and function, and the fences table                                                                                                                                                                                                                                                                                                                   | does not touch the role, the ledger or the marker                                                                                                            | —                                                                                                                                                                                                                                                                   |
| ledger after a down    | **no repo procedure**                                                                             | —                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | The down files leave the `upstream:…` rows in `sessions.__96bulls_migrations`.                                                                               | A later `migrate.ts` run skips the reverted files as already applied. Phase 2 then fails (`approved RPC missing`) and every reader stays closed. Re-applying needs ledger rows removed by hand, an operation the repo does not describe: unknown, needs a decision. |
| marker after a down    | —                                                                                                 | —                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | —                                                                                                                                                            | Phase 2 fails on the next live check, so `/readyz` is 503 and claims stop (fail closed). That is the intended safe state.                                                                                                                                           |
| failed migrate mid-run | automatic                                                                                         | The failing file's transaction rolls back; earlier files stay committed and ledgered; the marker stays `'pending'`.                                                                                                                                                                                                                                                                                                                                                                       | —                                                                                                                                                            | Fix the cause, then rerun `migrate.ts`. It skips the ledgered files and restarts at `'pending'`.                                                                                                                                                                    |
| failed phase 2         | automatic                                                                                         | rolls back; the marker stays `'pending'`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | —                                                                                                                                                            | "Revoke or restore by hand (an operator decision), then rerun."                                                                                                                                                                                                     |

Erasure caveat (from the contract): "PostgreSQL WAL, replicas and backups keep their own
copies until retention ages them out." A rollback does not purge `short_summary` text
from those.

### 7.1 Forward-only and snapshot recovery for the 15 files with no down

Only `20261030020000` and `20261031001000` have down files (`supabase/rollbacks/`). These
15 have **no down file**. Each one can be undone only by a new forward migration or by
restoring a snapshot:

1. `20261025000000_sessions_index.sql`
2. `20261025000100_repo_identity.sql`
3. `20261025000200_scrub_quarantine.sql`
4. `20261025000300_upsert_keep_on_null.sql`
5. `20261025000600_pipeline_queue.sql`
6. `20261030000000_relayhistory_machines_compat.sql`
7. `20261030010000_session_directory.sql`
8. `20261031000000_migration_assertion_ledger.sql`
9. `20261031000100_manifest_readiness.sql`
10. `20261101000000_migration_ingest_audit_base_read.sql`
11. `20261101000200_claimless_window_activations.sql`
12. `0001_96bulls_observability.sql`
13. `0002_session_ext_and_summaries.sql`
14. `0003_fanout_ledger.sql`
15. `0004_summary_pointers.sql`

**Forward-only recovery (the default).** The ledger forbids editing an applied file
("changed after it was applied; add a new migration instead"). A defect in any of the 15
is fixed by a new, reviewed migration with a later name, applied by `migrate.ts` under its
own OK. It changes the generation, so readers need an image built with it. What forward-only
recovery cannot undo:

- data that a file deleted or rewrote: `0004` (`DELETE FROM sessions.session_summaries`,
  dropped `summary` and `model_id`); the daily `pipeline_events_maintain()` job that
  `20261025000000` installs (dropped partitions, retention deletes). That is operational,
  not the migrate-time call, which only touches tables created in the same run; `20261025000600` (stage renames and rewritten
  dead-letter ids);
- grants that a file narrowed: `20261030000000` (`machines` column grants) and
  `20261030020000` (`session_directory` column grants). A forward migration can re-grant
  these, but anything denied in the meantime stays denied until it runs.

**Snapshot recovery (PITR or a pre-apply backup).** This is the only way to get back data
destroyed by the three files above, or to undo a set that cannot be fixed going forward.
It is a whole-database operation. It also discards every write made after the restore
point, in every schema, including writes from RelayHistory's Worker and the T0 hold/replay
path (`docs/relayhistory-supabase-migration.md`). It therefore needs its own decision from
Khaliq, separate from the apply. **Availability is NOT OBSERVED:**

| id  | evidence                                                                                                          | how to fill it (needs Khaliq's OK)                                                                                                                                                                                                                                                                                                                           | expected for go                                                                                                                      |
| --- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| S1  | Is PITR enabled on `pyamkrhoyszgzirqqffq`, what is the retention window, and when was the latest physical backup? | the Supabase dashboard (Database, then Backups) for the project, read by Khaliq. The repo has no CLI or API command for this: unknown.                                                                                                                                                                                                                       | PITR enabled, with a window that covers the apply plus the verification period, or a fresh backup taken immediately before the apply |
| S2  | A restore point, recorded immediately before step 4 of section 5                                                  | `SELECT now() AS restore_point, pg_current_wal_lsn() AS lsn;` on the migrator connection, just before the migrator starts. Whether `pg_current_wal_lsn()` is executable by that role is NOT OBSERVED.                                                                                                                                                        | a timestamp and an LSN in the evidence record                                                                                        |
| S3  | Optional logical pre-apply backup of the tables the destructive files touch                                       | `pg_dump --format=custom --data-only --table='sessions.session_summaries' --table='sessions.pipeline_events*' --table='sessions.pipeline_dead_letters' -d "<connection, injected at exec time>" -f <secure path>`. It reads tenant data, so the output is sensitive. Where it may be stored is unknown; the T0 runbook uses `/secure/t0/` for its artifacts. | a dump file, its sha256 and its row counts recorded. Never commit it.                                                                |

A restore also returns `sessions.__96bulls_migrations` and the marker to their pre-apply
state. Readers built for the new generation then stay non-ready (`schema_pending` or
`validation_pending`), and the previous image must be redeployed. The role
`session_directory_search_worker` is cluster-wide. Whether a project restore keeps or
removes it on Supabase is unknown, so run phase 1 again (verify only) after any restore.

## 8. Post-apply readiness evidence

Record each item in the section 0 format.

| id  | check                                       | command or query                                                                                                                                                                                                                                                      | expected                                                                                                                                                |
| --- | ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| E1  | the ledger is complete                      | `SELECT name, sha256 FROM sessions.__96bulls_migrations ORDER BY name;`                                                                                                                                                                                               | all 17 schema names in 2.2, with the listed sha256 values (plus `shim:supabase_roles` only if it was already present; it must not be added by this run) |
| E2  | the marker is pass for this generation      | `SELECT generation, status, updated_at FROM sessions.__96bulls_role_validation WHERE name = 'session_directory_search_worker';`                                                                                                                                       | `status = 'pass'`, `generation = $GEN` from step 0                                                                                                      |
| E3  | phase 2 passes on its own                   | `psql -v ON_ERROR_STOP=1 -X -d "<…>" -f supabase/bootstrap/session_directory_search_worker_verify.sql`                                                                                                                                                                | exit 0                                                                                                                                                  |
| E4  | role references                             | P4                                                                                                                                                                                                                                                                    | only `a` rows: schema `sessions` plus the three approved functions                                                                                      |
| E5  | the live gate is ok                         | `schemaVerdict` result, as logged by a reader on this generation (the log line `live role validation gate closed` must be absent)                                                                                                                                     | `{ ok: true }`                                                                                                                                          |
| E6  | worker `/readyz`                            | `curl -fsS -o /dev/null -w '%{http_code}' <worker health endpoint>/readyz`                                                                                                                                                                                            | `200`. Needs a deployed image on this generation, so it requires its own deploy OK.                                                                     |
| E7  | ingest `/readyz`                            | `curl -sS <ingest endpoint>/readyz`                                                                                                                                                                                                                                   | `200` with `{"ok":true}`. Same deploy dependency.                                                                                                       |
| E8  | `authenticated` cannot read summary columns | as an `authenticated` JWT in a **non-prod** rehearsal: select `short_summary` from the directory                                                                                                                                                                      | `42501`                                                                                                                                                 |
| E9  | negative drift check, **non-prod only**     | in a disposable rehearsal database on the same generation: `GRANT SELECT ON sessions.session_directory TO session_directory_search_worker;` → probe `/readyz` → `REVOKE SELECT ON sessions.session_directory FROM session_directory_search_worker;` → probe `/readyz` | First probe 503, `role_validation_failed`, `error_code` `55000`. Second probe 200. **Never run this against `pyamkrhoyszgzirqqffq`.**                   |

The repo has no automated "post-apply evidence" script that produces E1 to E9 against a
hosted target: unknown.

## 9. Open decisions and blockers for Khaliq

| #   | decision or blocker                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | source                                                                                                        |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| B1  | **The PG16+ path.** The server version is NOT OBSERVED. On 160000 or later, phase 1 refuses Supabase `postgres` (`42501`). Options are in 1.3: a superuser run through Supabase support, a reviewed spec change, or deferral. Each needs an explicit choice; the runbook says it is "not a step to work around."                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | phase 1 SQL; runbook                                                                                          |
| B2  | **#226 cutover before the contract revoke.** The expand migration revokes nothing. The contract migration (revoking direct SELECT on `session_directory`, `_users`, `_machines`, `session_index` and `session_directory_rollout` from `authenticated` and from the static reader's role) does not exist yet. It may be written and applied only after #226 cuts over behind a flag with shadow parity, and logs prove zero direct reads over an agreed window.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | contract doc; `20261031001000` header                                                                         |
| B3  | **Which migration runner and ledger the target uses.** `docs/relayhistory-t0-runbook.md` (A3) applies `supabase/migrations` with `scripts/sb db push` (Supabase CLI). That path writes no `sessions.__96bulls_migrations` ledger, runs neither phase 1 nor phase 2, and writes no marker, so no #96+ reader ever becomes ready. `migrate.ts` on a target built that way would try to reapply `20261025000000_sessions_index.sql` (`CREATE TABLE sessions.session_index` with no `IF NOT EXISTS`), and that fails. Separately, `migrate.ts` applies every unledgered file (rows 1 to 17), not just G6. A decision is needed on the runner and on how existing state is adopted into the ledger. The repo provides no adoption procedure: unknown.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | T0 runbook; `migrate.ts`; `supabase/README.md` ("Nothing in this directory has been applied to any database") |
| B4  | **Images built from `main` need these migrations applied before they are deployed.** The #38 merge says: "Required before any image built from this main is deployed: the migrations applied, with Khaliq's OK." With #96, a reader on generation `$GEN` is not ready until the ledger holds every schema step and the marker is `'pass'` for `$GEN`. Deploying first leaves `/readyz` at 503 and the worker claiming nothing. Migrating first flips any running #96+ reader to non-ready until it is replaced (6.4). Order: migrate (with OK), then deploy (with a separate OK), in one planned window.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | #38 merge body; `src/shared/db.ts`; `src/shared/worker.ts`; `src/alleyoop/ingest.ts`                          |
| B5  | Other activation blockers listed in the #38 merge: the #89 write-path issues; #74's `probes.ts:68` old 2-arg RPC check; #72's `worker_scope` token pins; the pointer and readiness consumer migration; Relay Desktop (UNOWNED). This dossier does not check their current status: unknown.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | #38 merge body                                                                                                |
| B6  | **Runtime role access to the ledger and marker.** No repo file grants `SELECT` on `sessions.__96bulls_migrations` or `sessions.__96bulls_role_validation` to the service role, and `migrate.ts` revokes PUBLIC on the marker. Unless the runtime role is the migrator, or otherwise has access (P13), the live gate fails with `42501`. Draft PR #99 (code at `960ed64`, pending, not merged) adds that grant through `HOOPDB_RUNTIME_ROLE` and requires a dedicated runtime role in `supabase` mode. Until it merges, decide on the provisioning, which is a privilege change and needs its own OK. Either way, prove it with RT1 to RT5.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `src/hoopdb/migrate.ts`; `src/shared/db.ts`                                                                   |
| B7  | **Locks (all 17 files, 6.3).** Under `migrate.ts` every file is one transaction, so `CREATE INDEX CONCURRENTLY` is impossible, and no `lock_timeout` is set. `20261025000000` cannot run as a backlog: `CREATE TABLE sessions.session_index` fails with `42P07` first, and the file rolls back whole. When it does run, its `pipeline_events_maintain()` call (`LOCK TABLE sessions.pipeline_events_default IN SHARE ROW EXCLUSIVE MODE`, `CREATE TABLE … LIKE`, `ATTACH PARTITION`, `DROP TABLE` of expired partitions, `DELETE`s) touches only tables created in the same transaction. Its only lock on a pre-existing table is `ACCESS EXCLUSIVE` on `machines`, if that table already exists. The same function, run daily by the Worker's cron, takes those locks on the live tables, and the migrate must not overlap it (P15 and P16 show it). On a backlog run against populated tables (P14), these hold `ACCESS EXCLUSIVE` for unknown durations: `20261025000100` (`session_index`: `ADD COLUMN … CHECK`, plus non-concurrent `si_remote` and `si_repo` builds); `20261025000200` (`session_index`: two `ADD COLUMN … NOT NULL DEFAULT false` and a trigger); `20261025000600` (`pipeline_events` and every partition, plus `pipeline_dead_letters`: validated CHECK swaps, row `UPDATE`s, a non-concurrent `pipeline_dead_letters_session_idx`); `20261030000000` (`machines`); `20261030020000` (`session_directory`: CHECKs and two non-concurrent GIN builds); `20261031000100` (`pipeline_events`, `pipeline_dead_letters`, `pipeline_jobs`: validated CHECK swaps); `20261101000000` (`migration_ingest_audit`: a validated CHECK swap); `0001` (`pipeline_jobs`: `ADD COLUMN … CHECK`, a trigger, a non-concurrent `pipeline_jobs_session_uuid_idx`; `pipeline_dead_letters`: columns and a trigger); `0003` (`session_index_ext`: `ADD COLUMN … NOT NULL DEFAULT 0`); `0004` (`session_summaries`: `DELETE` and `DROP COLUMN`). `20261031001000` takes `SHARE ROW EXCLUSIVE` on `session_directory`. Decide on a maintenance window or write hold, and on a session `lock_timeout` mechanism. The repo has none: unknown. | 6.1, 6.3                                                                                                      |
| B8  | **Destructive non-G6 step.** `0004_summary_pointers.sql` runs `DELETE FROM sessions.session_summaries;` if unledgered. It needs explicit confirmation before any run that would include it.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | 6.3                                                                                                           |
| B9  | **Post-apply deployment steps that the migration does not do:** seeding `session_directory_search_token_policy` (the RPCs refuse every call until then), the `authenticator` binding and worker credential, and the rollout marker. Values and owners: unknown.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | `20261030020000`; contract doc                                                                                |
| B10 | **Rollback limits.** No down for `20261030010000`. The downs leave ledger rows behind, and the repo has no procedure for re-applying after a down. The role is removed only by the separate superuser step.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | 7                                                                                                             |
| B11 | **Stale documentation to fix before activation** (docs only, not schema): (a) `docs/session-search-index-contract.md`, the "Caller" row of the search RPC table, still says "the migration's first statement creates the role … fails closed with `42710` … The down path drops the role it owns". Its "Rollback" bullet also says the down drops the "Worker role". Both contradict #96, where the migration never creates the role, failures are `42704` and `55000`, and the down never drops it. (b) The runbook ("Rehearsal databases") gives the new sha256 of `20261030020000` as `f86e5dc9...0d66`. The file at `28a8f0d` hashes to `c223b77e…5918de4`. Draft PR #99 (pending) corrects the runbook value and pins it with a test.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | contract doc; runbook                                                                                         |
| B12 | **Authorization for every step above, in Khaliq's own words:** the read-only audit (section 4, 4.1, RT1 to RT5, R1, R2, O1, S1 to S3), phase 1, migrate, any external verifier run, any deploy, and the B6/B9 privilege and credential steps.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `AGENTS.md`                                                                                                   |
| B13 | **Zero downtime cannot be guaranteed; a user-visible outage is not asserted.** If the deployed readers include #96+ images, those readers are non-ready from the pending marker write until phase 2 passes, and until each runs the new generation (the #96+ reader non-ready window, 6.4). Pre-#96 readers ignore the marker. Whether any of this is user-visible, and for how long, is conditional on three things: the deployed reader mix (D1 to D3), the orchestrator's traffic behavior on `/readyz` 503 (O1), and the lock durations in 6.3. Those durations depend on the existing-data state (P7, P14), and the locks create availability and latency risk, not a guaranteed outage. All of these inputs are NOT OBSERVED. Once they are observed, Khaliq decides whether a maintenance window, a write hold or neither is needed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | 6.4                                                                                                           |
| B14 | **Snapshot recovery decision.** 15 of the 17 files have no down file (7.1). Three of them destroy or rewrite data on a backlog run. PITR and backup availability are NOT OBSERVED (S1). Decide whether a fresh backup or a recorded PITR restore point (S2) is a precondition for the apply.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | 7.1                                                                                                           |
| B15 | **Authorization prerequisite (helm-chart-chief ruling): activation is blocked.** The problem is PUBLIC `EXECUTE` on future functions, not exposure. A `sessions` function created after PASS is PUBLIC-executable by PostgreSQL's default and stays so. On `main` no migrate revokes it, and the live gate (ledger, marker and phase 2 only) does not detect it. It can be called directly or through PostgREST RPC. The live `sessions`-function gate, the per-migrate PUBLIC revoke and the migrator's exact function-default and creator-coverage gate exist only in draft PR #99 (code at `960ed64`), which is pending and unmerged; on Supabase that gate stays refused until (A) or an equivalent is decided. Clearing B15 needs (A) an owner-scoped global default revoke with an ACL manifest and Khaliq's blast-radius approval, or an equivalent durable control (for example a reviewed schema-specific event trigger). The alternative, (B), is proof that `sessions` is not exposed (`db_schemas`) and that no untrusted role has `USAGE` or a direct path. The repo intends `sessions` RPCs to be reachable through PostgREST and `authenticator` (`GRANT USAGE ON SCHEMA sessions TO authenticated`; `Content-Profile: sessions`), so (B) is unsatisfied on that design and (A) or its equivalent is required. Intended RPCs need explicit grants to the exact API roles, with PUBLIC revoked. Evidence: Y1 to Y7, all NOT OBSERVED.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | 4.2                                                                                                           |

## Review and validation of this document

- Built only from the repository at `28a8f0d`: `git fetch`, then
  `git show --stat 9688d63e 28a8f0d`, and `git log --first-parent` per file. No database connection, no
  `op`, no credential read, no deploy.
- The sha256 values and the generation were computed offline from the repo files with
  the `generation()` algorithm. They must be reconfirmed from the deploy image with
  `bun src/hoopdb/migrate.ts --generation`.
- PostgreSQL lock levels are the documented levels for each statement type. They were
  not observed on the target.
- Revision 2, after helm-chart-chief's HOLD on `d5b0d9d`:
  - per-file impact for all 17 migrations (6.3);
  - a runtime-identity proof for the live validator (RT1 to RT5), with draft PR #99
    referenced as pending;
  - the readiness outage stated as orchestrator-dependent (6.4, B13);
  - forward-only and snapshot recovery for the 15 files with no down (7.1, B14);
  - the exhaustive catalog grants audit (4.1);
  - the index count corrected to 13;
  - exact R1 and R2.
- Revision 3, after helm-chart-chief's re-review HOLD on `8c20306`:
  - P14, P16 and B7 now cover every table and lock in the 17-row table, including
    `session_index_ext` and `migration_ingest_audit`;
  - 6.1 states 13 `CREATE INDEX`;
  - `pg_default_acl` is removed from the validator's needs, and only a complete
    post-apply RT4 counts as proof;
  - R1 and R2 discover the real sync resource (the repo defines none) and print names
    and `itemPath` only;
  - the outage is qualified (zero downtime cannot be guaranteed; #96+ readers go unready,
    pre-#96 readers ignore the marker), with deployed-image observation D1 to D3;
  - P4 uses the guarded A7 form;
  - #99 is referenced at its pending amended head `613627a`.
- Revision 4, after the HOLD on `949f332`:
  - P14 and P16 include every `pipeline_events` partition through `pg_partition_tree`;
  - B7 and 6.3 state `20261025000000`'s `pipeline_events_maintain()` impact;
  - R1g inspects the `SecretStore` or `ClusterSecretStore` provider type before a remote
    key is read as a 1Password item, with explicit STOP/UNKNOWN outcomes;
  - the outage is qualified as the #96+ reader non-ready window;
  - D3 is read-only (`git ls-tree` and `git cat-file`). Its script reproduces
    `d48ab022…b5ef86` at `28a8f0d` and at `origin/main` `7ea53b8`.
- Revision 5, after the verdict on `4ffd4da`:
  - section 4.2, blocker B15 and evidence Y1 to Y7 (PUBLIC `EXECUTE` on future
    functions; (B) is unsatisfied by repo design, so (A) or an equivalent reviewed
    durable control is required);
  - the `service_role` breadth recorded as a trusted-admin decision in the ACL manifest;
  - B13 and 6.4 made conditional (no user-visible outage is asserted; locks are an
    availability and latency risk);
  - R1 STOP/UNKNOWN extended to attributed but unsupported controllers and managers.
- Revision 6, after the final HOLD on `02eb16e`:
  - removed the claims that the live gate catches PUBLIC drift and that the next migrate
    fixes it. On `main` the gate runs only the ledger, the marker and phase 2; the
    `sessions`-function gate and per-migrate revoke are in pending, unmerged #99
    (`0abfa27`);
  - added `20261025000300` line 170 to the `service_role` inventory, quoted exactly;
  - Y1 is now recursive, Y3 to Y5 evaluate every Y1 role, and any unevaluated or
    unapproved Y1 role is an explicit STOP;
  - #99 at `0abfa27` edits the phase 2 verifier, so merging it changes the generation;
  - `main` is now `44615f2`, with the migration set, runner and gate unchanged.
- Revision 7, with #99 round 6 (code at `960ed64`):
  - replaced the pending `0abfa27` references (3.4, A9, 4.2, 5, B6, B15) with #99's
    code at `960ed64`; its exact-head CI run and artifact IDs are in the PR #99 body;
  - added #99's function-boundary gate: exact owner function default
    `{<self>:EXECUTE}`, pre-creation creator coverage, no `sessions`-scoped grant;
  - #99's generation, recomputed offline at `960ed64`: `dd2ca0d4…ba7d07` (`main`
    `b00d4f3` stays `d48ab022…b5ef86`);
  - `main` is now `b00d4f3`, with the migration set, runner and gate unchanged since
    `28a8f0d`.
- Formatted with the locked Prettier 3.9.10 (`--no-config`).
