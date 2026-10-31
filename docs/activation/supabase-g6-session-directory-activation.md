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

| id  | query or command (verbatim) | expected | observed | timestamp (UTC) | operator | connected as (role name only) |
| --- | --------------------------- | -------- | -------- | --------------- | -------- | ----------------------------- |
| P1  | `SHOW server_version_num;`  | see 1.2  | NOT OBSERVED | —           | —        | —                             |

Rules for the record:

- Record role names, object names, counts, booleans and hashes. Never record a connection
  string, host password, token, JWT, secret value or 1Password field value.
- Paste result rows verbatim. If a query errors, record the SQLSTATE and message.
- One authorization covers only the queries it names. Do not add queries during an audit.

### Fields that are NOT OBSERVED

| field | status | how to fill it once Khaliq authorizes it (read-only) | section |
| ----- | ------ | --------------------------------------------------- | ------- |
| Server version | **NOT OBSERVED** | P1 | 1, 4 |
| Live role identity of the bootstrap and migrator connections | **NOT OBSERVED** | P2 | 3, 4 |
| Effective privileges (role attributes, worker role, grants, runtime role SELECT on the ledger and marker) | **NOT OBSERVED** | P2, P3, P4, P5, P13 | 3, 4 |
| Installed migration state: the `sessions.__96bulls_migrations` ledger | **NOT OBSERVED** | P6, P7 | 2, 4 |
| Installed migration state: the `sessions.__96bulls_role_validation` marker | **NOT OBSERVED** | P8 | 4, 8 |
| Installed migration state: any Supabase CLI ledger | **NOT OBSERVED** | P9 | 2, 9 |
| Lock observations (live sessions, held locks, migrator advisory lock, table sizes) | **NOT OBSERVED** | P14, P15, P16, P17 | 6 |
| Runtime secret target (which 1Password item and which env var the services use) | **NOT OBSERVED** | R1, R2 | 3.4 |

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

| observed `server_version_num` | bootstrap caller | what the repo says happens | options for Khaliq |
| ----------------------------- | ---------------- | -------------------------- | ------------------ |
| `< 160000` (PostgreSQL 15) | `postgres` with CREATEROLE (P2 must show `rolcreaterole = t`) | Creating the role grants `postgres` nothing; phase 1 creates it to spec. | (a) Approve running phase 1 as `postgres`. (b) Defer G6 activation. |
| `>= 160000` (PostgreSQL 16, 17 or later) | `postgres` (not a superuser) | Phase 1 refuses with `42501`. The runbook calls this "an open blocker, not a step to work around." | (a) Have a superuser run phase 1 through Supabase support. Who does it, and how, is unknown. (b) Make an explicit, reviewed change to the role spec, and to the guard and verifier, so they accept the creator's ADMIN grant. The repo has no such change, and its safety is unknown. (c) Defer G6 activation. Do not grant ADMIN by hand, and do not edit the guard in place. |
| any | a real superuser | Phase 1 creates the role with no membership. | Applies only if a superuser connection is available and approved. Availability on this project is unknown. |

## 2. Exact migration inventory and order

### 2.1 What #38 and #96 changed (from `git show --stat 9688d63e 28a8f0d`)

| file | added or changed by | role |
| ---- | ------------------- | ---- |
| `supabase/migrations/20261030010000_session_directory.sql` | #38 (added) | migration, expand |
| `supabase/migrations/20261030020000_session_directory_short_summary.sql` | #38 (added); also changed by #72 (`0904999`) and #96 (`28a8f0d`) | migration, expand |
| `supabase/migrations/20261031001000_session_directory_reads_expand.sql` | #38 (added); also changed by #72 | migration, expand only |
| `supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql` | #38; changed by #72 and #96 | rollback (not run by the migrator) |
| `supabase/rollbacks/20261031001000_session_directory_reads_expand.down.sql` | #38 | rollback (not run by the migrator) |
| `supabase/bootstrap/session_directory_search_worker.sql` | #96 | phase 1 (not ledgered) |
| `supabase/bootstrap/session_directory_search_worker_verify.sql` | #96 | phase 2 and the live gate (not ledgered) |
| `supabase/bootstrap/session_directory_search_worker_mark_pass.sql` | #96 | external verifier (not ledgered) |
| `src/hoopdb/migrate.ts`, `src/shared/db.ts`, `src/shared/worker.ts`, `src/alleyoop/ingest.ts` | #96 | runner, marker, live gate, `/readyz` |

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

| # | ledger name | sha256 | origin | expand or contract | applied on target? |
| - | ----------- | ------ | ------ | ------------------ | ------------------ |
| 1 | `upstream:20261025000000_sessions_index.sql` | `6bfdb4c3…8874a3` | relayhistory-cloud #171, #28 | base schema | NOT OBSERVED |
| 2 | `upstream:20261025000100_repo_identity.sql` | `3b1d74f6…28c13a` | relayhistory-cloud #177 | base | NOT OBSERVED |
| 3 | `upstream:20261025000200_scrub_quarantine.sql` | `97ec2f03…0c7f4398` | relayhistory-cloud #185 | base | NOT OBSERVED |
| 4 | `upstream:20261025000300_upsert_keep_on_null.sql` | `17539224…5a649910` | relayhistory-cloud #185 | base | NOT OBSERVED |
| 5 | `upstream:20261025000600_pipeline_queue.sql` | `41f16006…44177042` | relayhistory-cloud #210 | base | NOT OBSERVED |
| 6 | `upstream:20261030000000_relayhistory_machines_compat.sql` | `22fe48a3…29f227` | #28 | expand | NOT OBSERVED |
| 7 | `upstream:20261030010000_session_directory.sql` | `2427948e…48fa2f8` | **#38** | expand | NOT OBSERVED |
| 8 | `upstream:20261030020000_session_directory_short_summary.sql` | `c223b77e…5918de4` | **#38 / #72 / #96** | expand (it also replaces two of #38's indexes and narrows `authenticated` to column grants) | NOT OBSERVED |
| 9 | `upstream:20261031000000_migration_assertion_ledger.sql` | `0683aca3…20d40689` | #43 | not G6 | NOT OBSERVED |
| 10 | `upstream:20261031000100_manifest_readiness.sql` | `1ff02258…242242d6` | #51 / #52 | not G6 | NOT OBSERVED |
| 11 | `upstream:20261031001000_session_directory_reads_expand.sql` | `6957cd17…20ec15` | **#38 / #72** | **expand only** | NOT OBSERVED |
| 12 | `upstream:20261101000000_migration_ingest_audit_base_read.sql` | `44813425…98acea` | #79 | not G6 | NOT OBSERVED |
| 13 | `upstream:20261101000200_claimless_window_activations.sql` | `6dde3342…bd5d1a` | #52 | not G6 | NOT OBSERVED |
| 14 | `96bulls:0001_96bulls_observability.sql` | `b9d059c4…a430a8` | 96bulls | not G6 | NOT OBSERVED |
| 15 | `96bulls:0002_session_ext_and_summaries.sql` | `2fee02ab…25568` | 96bulls | not G6 | NOT OBSERVED |
| 16 | `96bulls:0003_fanout_ledger.sql` | `cccdd28c…18c7` | 96bulls | not G6 | NOT OBSERVED |
| 17 | `96bulls:0004_summary_pointers.sql` | `fe351f30…94b2e94` | 96bulls | not G6; **destructive** (see 6.3) | NOT OBSERVED |
| — | `verify:session_directory_search_worker` (not ledgered) | `b974b099…c3a968f` | #96 | phase 2 | — |

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
(P6, P7), so which files a production run would apply is unknown. See blocker B3.

### 2.3 What each G6 file does

**`20261030010000_session_directory.sql` (#38, expand).**
`CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;`. Creates
`sessions.session_directory` (primary key `(org_id, workspace_id, harness, session_id)`),
`sessions.session_directory_users` and `sessions.session_directory_machines` (foreign keys
to the directory, `ON DELETE CASCADE`), and 14 non-concurrent `CREATE INDEX` statements,
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

| item | required value (from the SQL) |
| ---- | ----------------------------- |
| name | `session_directory_search_worker` |
| attributes | `NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION` (`rolcanlogin`, `rolinherit`, `rolbypassrls`, `rolsuper`, `rolcreaterole`, `rolcreatedb` and `rolreplication` all `f`) |
| `pg_db_role_setting` | exactly one row: `setdatabase = 0`, `setconfig = ARRAY['statement_timeout=2s']`. `2000ms`, `2000`, any extra setting and any per-database row are refused. |
| membership (as member) | none |
| members (of it) | none until the separately approved binding. The only allowed member is `authenticator`, with `rolinherit = f`, `admin_option = f`, and on 16+ `inherit_option = f`. |
| owned objects | none (`pg_class`, `pg_proc`, `pg_namespace`, `pg_type`, `pg_database`) |
| explicit privileges before `20261030020000` | none, except `USAGE` on `sessions`, which the migration grants |

Phase 1 creates the role with:

```sql
CREATE ROLE session_directory_search_worker
  NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER ROLE session_directory_search_worker SET statement_timeout = '2s';
```

### 3.2 Who must run each step

| step | must run as | live identity on target |
| ---- | ----------- | ----------------------- |
| phase 1 (create) | a superuser; or, only below 160000, a CREATEROLE role | **NOT OBSERVED** (P2) |
| phase 1 (role already exists) | any role that can read `pg_roles`, `pg_db_role_setting` and `pg_auth_members` (verify only) | **NOT OBSERVED** |
| migrations (`migrate.ts`) | the schema owner. The T0 runbook (A1) describes "a schema-owner role for migrations". It must be able to create in `sessions`, create extensions and grant to `authenticated`, `service_role` and the worker. | **NOT OBSERVED** (P2, P5) |
| phase 2 and `mark_pass.sql` | any role that can read the catalogs ("the migration owner (`hoop`, or Supabase `postgres`) is enough"). `mark_pass.sql` also needs UPDATE on `sessions.__96bulls_role_validation`. | **NOT OBSERVED** |
| live gate (`schemaVerdict`) | the service's own `HOOPDB_URL` role | **NOT OBSERVED** (R1, P13) |

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

### 3.4 Runtime role needs, and the runtime secret target

The live gate (`schemaVerdict`, `src/shared/db.ts`) runs as the role in `HOOPDB_URL`, in
one `READ ONLY`, `READ COMMITTED` transaction with `SET LOCAL statement_timeout` from
`HOOPDB_ROLE_GATE_TIMEOUT_MS` (default 2000, allowed 1..10000). That role needs:

- `SELECT` on `sessions.__96bulls_migrations`;
- `SELECT` on `sessions.__96bulls_role_validation`;
- read access to `pg_roles`, `pg_auth_members`, `pg_db_role_setting`, `pg_shdepend`,
  `pg_namespace`, `pg_proc`, `pg_database` and `pg_default_acl` (the runbook calls these
  "PUBLIC-readable catalogs"; that they are readable on this project is NOT OBSERVED);
- `USAGE` on schema `sessions`.

**Gap found in the repo:** `migrate.ts` creates both tables as the migrator and runs
`REVOKE ALL ON sessions.__96bulls_role_validation FROM PUBLIC`. No file in the repository
grants `SELECT` on either table to any runtime role. If the runtime role is not the
migrator, and has no other path to those tables (ownership, membership or default
privileges; `BYPASSRLS` does not grant table privileges), the gate fails with `42501`.
It then reports `role_validation_failed`, and `/readyz` returns 503. Whether the runtime
role has that access is NOT OBSERVED (P13). How the grant should be provisioned is
unknown; see blocker B6.

**Runtime secret target: NOT OBSERVED.** What the repo says, with no values:

- The services read `HOOPDB_URL` (`openHoopDb` in `src/shared/db.ts`).
- On AWS, `stacks/aws/eks.ts` takes it from the Kubernetes Secret named by
  `runtimeSecretName`, key `HOOPDB_URL` ("`external` (default): HOOPDB_URL from
  runtimeSecretName (Supabase)"). On GCP, `stacks/gcp/config.ts` takes it from
  `existingSecretName`, key `HOOPDB_URL`.
- `docs/relayhistory-t0-runbook.md` (A1) says the Supabase migration and runtime
  credentials live in the 1Password vault `96Bulls-prod`. Which item, and which role it
  names, is not stated for the 96Bulls services.

To fill it (each needs Khaliq's OK; record names only, never values):

| id | command | record |
| -- | ------- | ------ |
| R1 | the reviewed deploy inputs for the production stage: the `runtimeSecretName` or `existingSecretName` value, plus the secret-sync mapping from that Kubernetes Secret key `HOOPDB_URL` to its 1Password item | Secret name, key name, 1Password vault and item title |
| R2 | Khaliq confirms which database role that item's URL connects as. Only the role name is recorded. | role name |

## 4. Preflight queries (read-only; each needs Khaliq's explicit OK before it runs)

Run these as the connection that will run the migrator, unless the row says otherwise.
None of them writes. The "expected" column is what a safe go looks like; anything else
stops the activation and goes to Khaliq.

| id | purpose | query | expected for go |
| -- | ------- | ----- | --------------- |
| P1 | server version | `SHOW server_version_num; SELECT version();` | a number. Then choose the 1.3 row. |
| P2 | live identity and attributes of this connection | `SELECT current_user, session_user, r.rolsuper, r.rolcreaterole, r.rolbypassrls, r.rolinherit FROM pg_roles r WHERE r.rolname = current_user;` | Bootstrap connection: `rolsuper = t`, or `rolcreaterole = t` with P1 `< 160000`. Migrator: unknown spec; record it. |
| P3 | does the worker role exist, and is it to spec | `SELECT oid, rolcanlogin, rolinherit, rolbypassrls, rolsuper, rolcreaterole, rolcreatedb, rolreplication FROM pg_roles WHERE rolname = 'session_directory_search_worker';` | 0 rows (phase 1 will create it), or 1 row with every attribute `f` |
| P3b | its settings and memberships (only if P3 found a row) | the runbook "Checks" block: `SELECT setdatabase, setconfig FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole WHERE r.rolname = 'session_directory_search_worker';` and `SELECT g.rolname AS role, u.rolname AS member, m.admin_option FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles u ON u.oid = m.member WHERE 'session_directory_search_worker' IN (g.rolname, u.rolname);` | exactly `0 \| {statement_timeout=2s}`; no membership rows |
| P4 | existing references to the role (only if P3 found a row) | `SELECT d.deptype, d.dbid, pg_describe_object(d.classid, d.objid, d.objsubid) FROM pg_shdepend d WHERE d.refclassid = 'pg_authid'::regclass AND d.refobjid = 'session_directory_search_worker'::regrole;` | before `20261030020000`: 0 rows. After the full set: only `a` rows for schema `sessions` and the three approved functions. |
| P5 | existing grants on G6 objects | `SELECT table_name, grantee, privilege_type FROM information_schema.role_table_grants WHERE table_schema = 'sessions' AND table_name LIKE 'session_directory%' ORDER BY 1, 2, 3;` and `SELECT rolname FROM pg_roles WHERE rolname IN ('authenticator', 'authenticated', 'anon', 'service_role', 'relayhistory_app');` | before #38: 0 grant rows. The role list is recorded as found. |
| P6 | do the ledger and marker tables exist | `SELECT to_regclass('sessions.__96bulls_migrations') AS ledger, to_regclass('sessions.__96bulls_role_validation') AS marker;` | record. NULL means `migrate.ts` has never run here. |
| P7 | ledger contents (only if P6 found the ledger) | `SELECT name, sha256, applied_at FROM sessions.__96bulls_migrations ORDER BY name;` | every applied row's sha256 equals the 2.2 table; no `vendor:` names. Any mismatch makes the run fail by design: stop. |
| P8 | marker (only if P6 found the marker) | `SELECT name, generation, status, updated_at FROM sessions.__96bulls_role_validation;` | record. A `'pass'` for another generation is expected to flip to `'pending'` on the next migrate (see 6.4). |
| P9 | other migration ledgers on the target | `SELECT to_regclass('supabase_migrations.schema_migrations');` and, if not NULL, `SELECT version, name FROM supabase_migrations.schema_migrations ORDER BY version;` | record. Non-empty means `scripts/sb db push` was used: see blocker B3. |
| P10 | which schema objects already exist | `SELECT n, to_regclass(n) FROM unnest(ARRAY['sessions.session_index', 'sessions.machines', 'sessions.pipeline_events', 'sessions.pipeline_jobs', 'sessions.session_summaries', 'sessions.session_directory', 'sessions.session_directory_rollout', 'sessions.session_directory_search_token_policy', 'sessions.session_directory_resurrection_fences']) AS n;` | consistent with P7. Objects present without ledger rows means blocker B3. |
| P11 | extensions | `SELECT e.extname, e.extversion, n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname IN ('pg_trgm', 'btree_gin');` and `SELECT name, default_version, installed_version FROM pg_available_extensions WHERE name IN ('pg_trgm', 'btree_gin');` | both available. If `pg_trgm` is already installed, it must be in schema `public`: the migrations reference `public.gin_trgm_ops` explicitly, and `CREATE EXTENSION IF NOT EXISTS … WITH SCHEMA public` does not move an existing install. `btree_gin` is used through default opclasses, so its schema is not referenced. The repo does not say whether the migrator may create extensions on this project: unknown. |
| P12 | leakproof flags (G6's RLS design depends on them) | `SELECT proname, proleakproof FROM pg_proc WHERE pronamespace = 'pg_catalog'::regnamespace AND proname IN ('texteq', 'textlike', 'texticlike', 'text_pattern_lt', 'text_pattern_ge') ORDER BY 1;` | as in `docs/evidence/session-directory-plans.md`: `texteq`, `text_pattern_lt` and `text_pattern_ge` true; `textlike` and `texticlike` false |
| P13 | runtime role access for the live gate (replace `<runtime role>` with the R2 name) | `SELECT has_schema_privilege('<runtime role>', 'sessions', 'USAGE'), has_table_privilege('<runtime role>', 'sessions.__96bulls_migrations', 'SELECT'), has_table_privilege('<runtime role>', 'sessions.__96bulls_role_validation', 'SELECT');` (only if P6 found both tables) | all `t`. Otherwise see blocker B6. |
| P14 | table sizes for the lock analysis | `SELECT c.oid::regclass, c.reltuples::bigint AS est_rows, pg_total_relation_size(c.oid) AS bytes FROM pg_class c WHERE c.oid IN (SELECT to_regclass(x) FROM unnest(ARRAY['sessions.session_directory', 'sessions.session_index', 'sessions.machines', 'sessions.pipeline_events', 'sessions.pipeline_dead_letters', 'sessions.pipeline_jobs', 'sessions.session_summaries', 'sessions.migration_ingest_audit']) AS x WHERE to_regclass(x) IS NOT NULL);` | record. `reltuples` is a planner estimate, not a count. |
| P15 | current activity on the database (no query text) | `SELECT pid, usename, application_name, backend_type, state, wait_event_type, xact_start FROM pg_stat_activity WHERE datname = current_database() AND state <> 'idle';` | record. Long-running transactions must be resolved before any `ACCESS EXCLUSIVE` step. |
| P16 | locks held on the affected tables | `SELECT l.pid, l.relation::regclass, l.mode, l.granted FROM pg_locks l WHERE l.relation IN (SELECT to_regclass(x) FROM unnest(ARRAY['sessions.session_directory', 'sessions.session_index', 'sessions.machines', 'sessions.pipeline_events', 'sessions.pipeline_dead_letters', 'sessions.pipeline_jobs', 'sessions.session_summaries']) AS x WHERE to_regclass(x) IS NOT NULL);` | record |
| P17 | is a migrator already running (the advisory lock 961996, 1) | `SELECT pid, granted FROM pg_locks WHERE locktype = 'advisory' AND classid = 961996 AND objid = 1 AND objsubid = 2;` | 0 rows |
| P18 | timeouts that apply to the migrator session | `SHOW statement_timeout; SHOW lock_timeout; SHOW idle_in_transaction_session_timeout;` | record. `migrate.ts` sets none of them. |

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

| file | statement | lock on existing objects | blocks | timing |
| ---- | --------- | ------------------------ | ------ | ------ |
| `20261030010000` | `CREATE EXTENSION IF NOT EXISTS pg_trgm` | none on sessions tables | — | unknown |
| | `CREATE TABLE` ×4 (`session_directory*`, `_rollout`) | new tables. The `_users` and `_machines` foreign keys reference a table created in the same transaction. | nothing outside the transaction | unknown; expected small (new objects) |
| | 14 × `CREATE INDEX` (non-concurrent; two GIN trigram) | `SHARE` on the new tables only | nothing outside the transaction (the tables are empty and not yet visible) | unknown; expected small (empty tables) |
| | `ENABLE ROW LEVEL SECURITY`, `CREATE POLICY`, `GRANT` / `REVOKE` | new tables only | — | unknown |
| `20261030020000` | `DO $role_guard$` | catalog reads | — | unknown |
| | `ALTER TABLE sessions.session_directory ADD COLUMN …` with CHECKs, then `ADD CONSTRAINT … CHECK` ×2 (validated, not `NOT VALID`) | **`ACCESS EXCLUSIVE` on `sessions.session_directory`**, held to COMMIT. The validated CHECKs scan the table. | all reads and writes of the directory | **unknown**: depends on row count (P14, NOT OBSERVED). If `20261030010000` applies in the same run, the table is empty. |
| | `CREATE TRIGGER session_directory_tombstone_clears_summary` | `SHARE ROW EXCLUSIVE` (already covered by the `ACCESS EXCLUSIVE` above) | — | unknown |
| | `DROP INDEX sessions.session_directory_metadata_trgm_idx`, `DROP INDEX sessions.session_directory_project_trgm_idx` (non-concurrent) | `ACCESS EXCLUSIVE` on the directory | all access | unknown |
| | `CREATE INDEX session_directory_tenant_search_trgm_idx`, `…_tenant_project_trgm_idx` (GIN, **non-concurrent**) | `SHARE` (already inside the `ACCESS EXCLUSIVE`) | all access, until COMMIT | **unknown**: proportional to directory rows and `search_text` size. GIN trigram builds over a populated table are the largest timing risk in G6. |
| | `REVOKE SELECT … FROM authenticated` / column `GRANT SELECT (...)` | catalog update; lock level not stated in the repo | unknown | unknown |
| | functions, the token-policy table, grants on functions | new objects | — | unknown |
| `20261031001000` | `CREATE TABLE sessions.session_directory_resurrection_fences` with a foreign key to the directory | `SHARE ROW EXCLUSIVE` on `sessions.session_directory` | writes to the directory | unknown; expected short (no scan) |
| | `CREATE TRIGGER session_directory_record_fences` | `SHARE ROW EXCLUSIVE` on the directory | writes | unknown |
| | functions, the `DO $revoke$` grant loop | new objects | — | unknown |

### 6.2 When the G6 lock risk is low

If P7 shows `20261030010000` unapplied, `sessions.session_directory` is created in the
same run as `20261030020000`. It holds no rows, because nothing writes it before the
projector runs in a deployed image. In that case the G6 `ACCESS EXCLUSIVE` and
non-concurrent GIN builds are over an empty table and contend with nothing.

If P7 shows `20261030010000` already applied and P14 shows a populated directory, the
`20261030020000` transaction blocks every directory read and write for an unknown
duration. That needs a maintenance-window decision (blocker B7).

### 6.3 Non-G6 files that the same run may apply (only if unledgered; NOT OBSERVED)

| file | risky statement | lock or effect | note |
| ---- | --------------- | -------------- | ---- |
| `20261030000000_relayhistory_machines_compat.sql` | `ALTER TABLE sessions.machines ADD COLUMN IF NOT EXISTS workspace_id text NOT NULL DEFAULT 'default'`, … | `ACCESS EXCLUSIVE` on `sessions.machines` | constant defaults; scan or rewrite cost not stated in the repo: unknown |
| `20261031000100_manifest_readiness.sql` | `ALTER TABLE sessions.pipeline_events DROP CONSTRAINT …; ADD CONSTRAINT pipeline_events_stage_check CHECK (…)`, and the same for `pipeline_dead_letters` and `pipeline_jobs` | `ACCESS EXCLUSIVE` on each table, plus a full validation scan (not `NOT VALID`) | **risky on populated pipeline tables**; sizes NOT OBSERVED (P14) |
| `20261101000000_migration_ingest_audit_base_read.sql` | `ALTER TABLE sessions.migration_ingest_audit DROP CONSTRAINT …; ADD CONSTRAINT … CHECK (result IN ('committed', 'base_read'))` | `ACCESS EXCLUSIVE` plus a validation scan | size NOT OBSERVED |
| `0004_summary_pointers.sql` | `DELETE FROM sessions.session_summaries;` then `ALTER TABLE sessions.session_summaries DROP COLUMN IF EXISTS summary, …` | **deletes every row**, plus `ACCESS EXCLUSIVE` | intended ("derived data … re-queues"), but destructive on a populated table. It needs its own explicit OK. |

### 6.4 Readiness flip during migrate

`MARKER_PENDING_SQL` upserts the single row `name = 'session_directory_search_worker'` to
`('pending', <new GEN>)` before any step. Any reader already running an image that
carries the marker check (#96 or later) requires `status = 'pass' AND generation = <its
own GEN>`. From that moment until the run's phase 2 passes, every such reader reports
`/readyz` 503 and its worker claims nothing. After the pass, readers on an older
generation stay non-ready until they are replaced. Whether any #96+ reader is running
against this target is NOT OBSERVED.

## 7. Rollback and forward recovery

| step | rollback in the repo | what it does | what it does not do | forward recovery |
| ---- | -------------------- | ------------ | ------------------- | ---------------- |
| phase 1 | none (manual) | — | — | The runbook's removal step, as a superuser, after every down that references the role: `DROP OWNED BY session_directory_search_worker; DROP ROLE session_directory_search_worker;` "in each database". Needs its own OK. |
| `20261030010000` | **none in `supabase/rollbacks/`** | — | — | unknown. No down path exists; dropping the four tables by hand would be an unreviewed operation. |
| `20261030020000` | `supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql` (`BEGIN` … `COMMIT`) | restores full-table `GRANT SELECT` to `authenticated`; drops the search RPC, the token-policy table and `session_directory_search_query`; revokes the worker's `sessions` access; drops both tenant GIN indexes and the tombstone trigger and function; rebuilds `search_text` without the summary; drops the `short_summary*` columns and constraints; recreates `session_directory_project_trgm_idx` and `session_directory_metadata_trgm_idx` (non-concurrent, inside the transaction) | **does not drop the role** ("It does not drop the role: the migration never created it"); does not drop `btree_gin`; does not touch the ledger or the marker | Run it only after the `20261031001000` down ("Run later downs first, in reverse migration order"). Its `UPDATE` and index builds take `ACCESS EXCLUSIVE` on a possibly populated directory; timing unknown. |
| `20261031001000` | `supabase/rollbacks/20261031001000_session_directory_reads_expand.down.sql` | drops `session_directory_readiness()`, `session_directory_pointers(...)`, `session_directory_worker_scope(text)`, the fences trigger and function, and the fences table | does not touch the role, the ledger or the marker | — |
| ledger after a down | **no repo procedure** | — | The down files leave the `upstream:…` rows in `sessions.__96bulls_migrations`. | A later `migrate.ts` run skips the reverted files as already applied. Phase 2 then fails (`approved RPC missing`) and every reader stays closed. Re-applying needs ledger rows removed by hand, an operation the repo does not describe: unknown, needs a decision. |
| marker after a down | — | — | — | Phase 2 fails on the next live check, so `/readyz` is 503 and claims stop (fail closed). That is the intended safe state. |
| failed migrate mid-run | automatic | The failing file's transaction rolls back; earlier files stay committed and ledgered; the marker stays `'pending'`. | — | Fix the cause, then rerun `migrate.ts`. It skips the ledgered files and restarts at `'pending'`. |
| failed phase 2 | automatic | rolls back; the marker stays `'pending'` | — | "Revoke or restore by hand (an operator decision), then rerun." |

Erasure caveat (from the contract): "PostgreSQL WAL, replicas and backups keep their own
copies until retention ages them out." A rollback does not purge `short_summary` text
from those.

## 8. Post-apply readiness evidence

Record each item in the section 0 format.

| id | check | command or query | expected |
| -- | ----- | ---------------- | -------- |
| E1 | the ledger is complete | `SELECT name, sha256 FROM sessions.__96bulls_migrations ORDER BY name;` | all 17 schema names in 2.2, with the listed sha256 values (plus `shim:supabase_roles` only if it was already present; it must not be added by this run) |
| E2 | the marker is pass for this generation | `SELECT generation, status, updated_at FROM sessions.__96bulls_role_validation WHERE name = 'session_directory_search_worker';` | `status = 'pass'`, `generation = $GEN` from step 0 |
| E3 | phase 2 passes on its own | `psql -v ON_ERROR_STOP=1 -X -d "<…>" -f supabase/bootstrap/session_directory_search_worker_verify.sql` | exit 0 |
| E4 | role references | P4 | only `a` rows: schema `sessions` plus the three approved functions |
| E5 | the live gate is ok | `schemaVerdict` result, as logged by a reader on this generation (the log line `live role validation gate closed` must be absent) | `{ ok: true }` |
| E6 | worker `/readyz` | `curl -fsS -o /dev/null -w '%{http_code}' <worker health endpoint>/readyz` | `200`. Needs a deployed image on this generation, so it requires its own deploy OK. |
| E7 | ingest `/readyz` | `curl -sS <ingest endpoint>/readyz` | `200` with `{"ok":true}`. Same deploy dependency. |
| E8 | `authenticated` cannot read summary columns | as an `authenticated` JWT in a **non-prod** rehearsal: select `short_summary` from the directory | `42501` |
| E9 | negative drift check, **non-prod only** | in a disposable rehearsal database on the same generation: `GRANT SELECT ON sessions.session_directory TO session_directory_search_worker;` → probe `/readyz` → `REVOKE SELECT ON sessions.session_directory FROM session_directory_search_worker;` → probe `/readyz` | First probe 503, `role_validation_failed`, `error_code` `55000`. Second probe 200. **Never run this against `pyamkrhoyszgzirqqffq`.** |

The repo has no automated "post-apply evidence" script that produces E1 to E9 against a
hosted target: unknown.

## 9. Open decisions and blockers for Khaliq

| # | decision or blocker | source |
| - | ------------------- | ------ |
| B1 | **The PG16+ path.** The server version is NOT OBSERVED. On 160000 or later, phase 1 refuses Supabase `postgres` (`42501`). Options are in 1.3: a superuser run through Supabase support, a reviewed spec change, or deferral. Each needs an explicit choice; the runbook says it is "not a step to work around." | phase 1 SQL; runbook |
| B2 | **#226 cutover before the contract revoke.** The expand migration revokes nothing. The contract migration (revoking direct SELECT on `session_directory`, `_users`, `_machines`, `session_index` and `session_directory_rollout` from `authenticated` and from the static reader's role) does not exist yet. It may be written and applied only after #226 cuts over behind a flag with shadow parity, and logs prove zero direct reads over an agreed window. | contract doc; `20261031001000` header |
| B3 | **Which migration runner and ledger the target uses.** `docs/relayhistory-t0-runbook.md` (A3) applies `supabase/migrations` with `scripts/sb db push` (Supabase CLI). That path writes no `sessions.__96bulls_migrations` ledger, runs neither phase 1 nor phase 2, and writes no marker, so no #96+ reader ever becomes ready. `migrate.ts` on a target built that way would try to reapply `20261025000000_sessions_index.sql` (`CREATE TABLE sessions.session_index` with no `IF NOT EXISTS`), and that fails. Separately, `migrate.ts` applies every unledgered file (rows 1 to 17), not just G6. A decision is needed on the runner and on how existing state is adopted into the ledger. The repo provides no adoption procedure: unknown. | T0 runbook; `migrate.ts`; `supabase/README.md` ("Nothing in this directory has been applied to any database") |
| B4 | **Images built from `main` need these migrations applied before they are deployed.** The #38 merge says: "Required before any image built from this main is deployed: the migrations applied, with Khaliq's OK." With #96, a reader on generation `$GEN` is not ready until the ledger holds every schema step and the marker is `'pass'` for `$GEN`. Deploying first leaves `/readyz` at 503 and the worker claiming nothing. Migrating first flips any running #96+ reader to non-ready until it is replaced (6.4). Order: migrate (with OK), then deploy (with a separate OK), in one planned window. | #38 merge body; `src/shared/db.ts`; `src/shared/worker.ts`; `src/alleyoop/ingest.ts` |
| B5 | Other activation blockers listed in the #38 merge: the #89 write-path issues; #74's `probes.ts:68` old 2-arg RPC check; #72's `worker_scope` token pins; the pointer and readiness consumer migration; Relay Desktop (UNOWNED). This dossier does not check their current status: unknown. | #38 merge body |
| B6 | **Runtime role access to the ledger and marker.** No repo file grants `SELECT` on `sessions.__96bulls_migrations` or `sessions.__96bulls_role_validation` to the service role, and `migrate.ts` revokes PUBLIC on the marker. Unless the runtime role is the migrator, or otherwise has access (P13), the live gate fails with `42501`. Decide on the provisioning, which is a privilege change and needs its own OK. | `src/hoopdb/migrate.ts`; `src/shared/db.ts` |
| B7 | **Locks.** Every index build is non-concurrent and inside a transaction, and there is no `lock_timeout`. If the directory or pipeline tables are populated (P14), `20261030020000`, `20261031000100` and `20261101000000` hold `ACCESS EXCLUSIVE` for unknown durations. Decide on a maintenance window or write fence, and on a session `lock_timeout` mechanism. The repo has none: unknown. | 6.1, 6.3 |
| B8 | **Destructive non-G6 step.** `0004_summary_pointers.sql` runs `DELETE FROM sessions.session_summaries;` if unledgered. It needs explicit confirmation before any run that would include it. | 6.3 |
| B9 | **Post-apply deployment steps that the migration does not do:** seeding `session_directory_search_token_policy` (the RPCs refuse every call until then), the `authenticator` binding and worker credential, and the rollout marker. Values and owners: unknown. | `20261030020000`; contract doc |
| B10 | **Rollback limits.** No down for `20261030010000`. The downs leave ledger rows behind, and the repo has no procedure for re-applying after a down. The role is removed only by the separate superuser step. | 7 |
| B11 | **Stale documentation to fix before activation** (docs only, not schema): (a) `docs/session-search-index-contract.md`, the "Caller" row of the search RPC table, still says "the migration's first statement creates the role … fails closed with `42710` … The down path drops the role it owns". Its "Rollback" bullet also says the down drops the "Worker role". Both contradict #96, where the migration never creates the role, failures are `42704` and `55000`, and the down never drops it. (b) The runbook ("Rehearsal databases") gives the new sha256 of `20261030020000` as `f86e5dc9...0d66`. The file at `28a8f0d` hashes to `c223b77e…5918de4`. | contract doc; runbook |
| B12 | **Authorization for every step above, in Khaliq's own words:** the read-only audit (section 4 and R1/R2), phase 1, migrate, any external verifier run, any deploy, and the B6/B9 privilege and credential steps. | `AGENTS.md` |

## Review and validation of this document

- Built only from the repository at `28a8f0d` (`git fetch`, then `git show --stat
  9688d63e 28a8f0d` and `git log --first-parent` per file). No database connection, no
  `op`, no credential read, no deploy.
- The sha256 values and the generation were computed offline from the repo files with
  the `generation()` algorithm. They must be reconfirmed with `bun src/hoopdb/migrate.ts
  --generation` from the deploy image.
- PostgreSQL lock levels are the documented levels for each statement type. They were
  not observed on the target.
