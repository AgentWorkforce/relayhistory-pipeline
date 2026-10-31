# Bootstrapping and validating the session directory search Worker role

`supabase/migrations/20261030020000_session_directory_short_summary.sql` needs the role
`session_directory_search_worker` to exist before it runs, and it neither creates nor
alters that role. Creating a role needs CREATEROLE, and on PostgreSQL 16+ setting a role
parameter also needs ADMIN OPTION on the role. A least-privilege migration owner (CNPG
`hoop`) has neither. So a privileged step provisions the role, and the migration only
verifies it. If the role is absent, the migration fails with `42704`. If it differs from
the spec, the migration fails with `55000`, naming every violation. Either way it fails
before any DDL, so nothing is applied.

The migration's guard runs only once, because the ledger then skips the file. So every
deploy runs two separate checks:

| Phase        | File                                                            | When                                                 | Run as                                                 | Checks                                                                                            |
| ------------ | --------------------------------------------------------------- | ---------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| 1. bootstrap | `supabase/bootstrap/session_directory_search_worker.sql`        | before every migrate                                 | a superuser                                            | provisions the bare role if absent; otherwise verifies the role spec. It does not look at grants. |
| 2. verify    | `supabase/bootstrap/session_directory_search_worker_verify.sql` | after every migrate, once all migrations are applied | any role that can read the catalogs (`hoop` is enough) | the role spec plus the exact direct-grant allowlist. Read-only.                                   |

Nothing here runs automatically against a real database. Every apply needs Khaliq's
explicit OK.

## Interface (stable; #75 wires the deploy-time provisioning against this)

**Deploy order:** phase 1 (superuser), then `hoopdb-migrate` (which runs phase 2 and records PASS; see the readiness marker below). Each step uses
`psql -v ON_ERROR_STOP=1 -X -f <file>` against the HoopDb or Supabase database, and the
deploy stops at the first non-zero exit. Both files are single atomic `DO` blocks.

Role spec (both phases):

| Item     | Value                                                                                                                                                                                                                                                                            |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Role     | `session_directory_search_worker`                                                                                                                                                                                                                                                |
| Attrs    | `NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION`                                                                                                                                                                                                |
| Settings | exactly one row in `pg_db_role_setting`: `setdatabase = 0`, `setconfig = {statement_timeout=2s}`. Other spellings (`2000ms`, `2000`), other values, extra settings and per-database settings are all refused.                                                                    |
| Members  | it is a member of no role. Its only allowed member is the separately approved authenticator binding: `authenticator`, NOINHERIT, no ADMIN OPTION, and no INHERIT grant on PostgreSQL 16+. The migration owner and every other role hold no membership and no ADMIN OPTION on it. |

Phase 1, `session_directory_search_worker.sql`:

| Item    | Value                                                                                                          |
| ------- | -------------------------------------------------------------------------------------------------------------- |
| Does    | creates the role to spec if it is absent; otherwise verifies it. It never alters an existing role.             |
| Run as  | a superuser. A non-superuser with CREATEROLE is accepted only on PostgreSQL 15 and older (see Supabase below). |
| Success | exit 0: the role exists to spec.                                                                               |
| Failure | `42501`: this caller may not create the role. `55000`: an existing role differs. Nothing is changed.           |

Phase 2, `session_directory_search_worker_verify.sql`:

| Item                               | Value                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Required direct grants             | exactly: `USAGE` on schema `sessions`, and `EXECUTE` on each approved RPC, none `WITH GRANT OPTION`.                                                                                                                                                                                                                                                                                                                        |
| Approved RPCs (this migration set) | `sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer)` (from `20261030020000`); `sessions.session_directory_pointers(text, text, text)` and `sessions.session_directory_readiness()` (from `20261031001000`). A migration that adds, removes or re-signs one updates the list in the same change. |
| Missing or redefined               | an approved RPC that is missing, or a function of the same name with another signature, fails. The allowlist never shrinks to match the database.                                                                                                                                                                                                                                                                           |
| Everything else                    | nothing else may reference the role in any database of the cluster (`pg_shdepend`): no owned objects; no table, column, sequence, function, schema, database, type, language, large object, parameter, foreign server or tablespace privileges; no default privileges; no RLS policies.                                                                                                                                     |
| One database per cluster           | roles are cluster-wide, and phase 2 can't see other databases' catalogs. So any reference to the role from another database of the same cluster (for example a second migrated copy) fails closed. Keep the migration set in one database per cluster, as the CNPG HoopDb cluster and a Supabase project do.                                                                                                                |
| PUBLIC                             | only aclitems that name this role count. Privileges it gets through PUBLIC are not its own grants and are ignored.                                                                                                                                                                                                                                                                                                          |
| Success                            | exit 0. It changes nothing.                                                                                                                                                                                                                                                                                                                                                                                                 |
| Failure                            | `55000`, naming every violation. Nothing is changed; the deploy must stop and an operator decides the fix.                                                                                                                                                                                                                                                                                                                  |

Phase 1 creates no membership when a superuser runs it. On PostgreSQL 16+, a non-superuser
CREATEROLE creator is automatically granted ADMIN OPTION on the new role. The migration
refuses that grant, so phase 1 refuses such a caller up front (`42501`).

## Readiness (what makes a reader ready)

A reader is ready only when all three hold:

1. the ledger is complete;
2. the marker in `sessions.__96bulls_role_validation` is `'pass'` for the reader's own
   migration-set generation;
3. phase 2 passes **live**, on this check.

The marker is generation, coordination and audit evidence, never the sole authority. A
grant added after a PASS fails the next check with no migrator involved. The same rule
gates `waitForSchema` (startup), worker startup, every worker `/readyz` probe, every
ingest `/readyz` probe, and the worker's claim loop before it takes new work.

The check (`schemaVerdict` in `src/shared/db.ts`) is one transaction, `READ ONLY`
and pinned to `READ COMMITTED`, in this order:

1. the ledger (`schema_pending` if incomplete);
2. the marker (`validation_pending` unless `'pass'` for this generation; phase 2 is
   then skipped, so nothing becomes ready without it);
3. phase 2 (`role_validation_failed` with its SQLSTATE on any error);
4. the marker again, as the **final read** (`validation_pending` unless still `'pass'`
   for this generation).

`READ COMMITTED` is deliberate. Every statement takes a fresh snapshot, so the final read
sees a migrator that set the marker to `'pending'` while phase 2 was running. Under
`REPEATABLE READ` it would reuse the first snapshot and miss that change.

- **One source:** it executes `supabase/bootstrap/session_directory_search_worker_verify.sql`,
  the same file the migrator runs and `mark_pass.sql` includes with `\ir`.
- **Bounded:** the transaction has its own `statement_timeout`
  (`HOOPDB_ROLE_GATE_TIMEOUT_MS`, default 2000). Values outside 1..10000, or non-integers,
  close the gate (`gate_timeout_invalid`) rather than running unbounded.
- **No lock, no cache:** it never takes the migrator's advisory lock, so a long migrate
  never blocks a probe, and it runs on every probe and before every claim.
- **Its own role:** it runs as the role the service connects as (`HOOPDB_URL`). That
  role needs SELECT on the ledger and the marker table. Phase 2 reads only PUBLIC-readable
  catalogs: `pg_roles`, `pg_auth_members`, `pg_db_role_setting`, `pg_shdepend`,
  `pg_namespace`, `pg_proc`, `pg_database` and `pg_default_acl`.
- **Fails closed:** any error closes the gate, is logged with its SQLSTATE and stage
  (`live role validation gate closed`) and makes the reader not ready. That covers a
  violation (`55000`), an unreadable catalog (`42501`), a timeout (`57014`) and a lost
  connection.

**Current-tick semantics.** A verdict holds as of its final read and only for the one
action it gates: one probe response, or one claim. The worker checks the gate again
before every stage's claim and before its side tick, so a gate that closes mid-tick stops
the next claim.

The most a closing gate can leave running is the batch claimed just before it closed:
at most `WORKER_BATCH_SIZE` messages (default 10) of one stage. That batch runs to
completion inside its poll call (acked, retried or dead-lettered). If the worker
crashes, the batch is redelivered when its lease (`WORKER_LEASE_SECONDS`, default 300)
expires. So the window is one batch's processing time. While the gate is closed the
worker claims nothing, and it resumes on the first check that passes.

Record format, the contract for #75's external verifier:

| Column       | Type and value                                                                                                                                                                                                                                            |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`       | `text PRIMARY KEY`, always `'session_directory_search_worker'`                                                                                                                                                                                            |
| `generation` | `text NOT NULL`: sha256 hex of the migration set (every `supabase/migrations` and `src/hoopdb/migrations` file by name and sha256, plus the phase 2 verifier). Print it with `bun src/hoopdb/migrate.ts --generation` from the same image as the readers. |
| `status`     | `text NOT NULL`, `'pending'` or `'pass'`                                                                                                                                                                                                                  |
| `updated_at` | `timestamptz NOT NULL`                                                                                                                                                                                                                                    |

Transitions:

1. The migrator takes `pg_advisory_lock(961996, 1)`, so concurrent migrators run one at a
   time. Before any migration step, it writes `('pending', <its generation>)` and
   commits. From that moment, every reader is non-ready.
2. Phase 2 and the change to `'pass'` happen in one transaction, still under the lock, and
   only for the generation that is pending. Nothing else writes `'pass'`.
3. Anything else leaves the row `'pending'`: a crash mid-migration, a migration failure, a
   phase 2 failure, or a failure between the last migration commit and phase 2. The
   next migrator for that image starts again at step 1.
4. A `'pass'` for a different generation (a stale earlier PASS, or another image's)
   is not ready for these readers.

**With the in-process verifier off** (`HOOPDB_ROLE_VERIFY=false`), the migrator still
writes `'pending'` but never `'pass'`. Only an external verifier can then make the
readers ready, by running, after migrate:

```sh
GEN=$(bun src/hoopdb/migrate.ts --generation)
psql -v ON_ERROR_STOP=1 -X -v generation="$GEN" \
  -f supabase/bootstrap/session_directory_search_worker_mark_pass.sql
```

In one transaction, that script:

- takes the same advisory lock (`pg_advisory_xact_lock(961996, 1)`);
- runs phase 2;
- sets `'pass'` only if the row is `'pending'` for exactly `$GEN`.

Otherwise it fails with `55000` and changes nothing.

Kubernetes Job and pod ordering is defense in depth only. Correctness rests on the
marker: a reader started early, or a migrator that died, simply stays non-ready.

## Database mode, runtime role and one migration authority

`src/hoopdb/migrate.ts` resolves an explicit profile **before opening any connection**. It
never infers anything from the hostname. An invalid profile writes nothing anywhere.

| Variable                       | Contract                                                                                                                                                                                                                                   |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `HOOPDB_DB_MODE`               | required, no default: `local` (Docker stack), `selfhosted` (CNPG, Cloud SQL) or `supabase`. Anything else: `db_mode_invalid`.                                                                                                              |
| `HOOPDB_SUPABASE_SHIM`         | `local` and `selfhosted`: must be set explicitly to `true` or `false` (`shim_unset` otherwise). `supabase`: only unset or `false`. `true` is refused (`shim_forbidden`), because `supabase_shim.sql` must never run on a Supabase project. |
| `HOOPDB_ROLE_BOOTSTRAP`        | defaults to the shim choice. In `supabase` mode it is refused if forced on (`bootstrap_forbidden`); the bootstrap is an operator step there.                                                                                               |
| `HOOPDB_RUNTIME_ROLE`          | required (`runtime_role_unset`): the role the services connect as.                                                                                                                                                                         |
| `HOOPDB_RUNTIME_ROLE_LOGIN`    | required in `selfhosted` and `supabase` (`runtime_role_login_unset`): `true` if the runtime role logs in itself, `false` if the services log in as a workload role that is a member of it.                                                 |
| `HOOPDB_RUNTIME_WORKLOAD_ROLE` | optional: the one role allowed to be a member of the runtime role (without ADMIN OPTION).                                                                                                                                                  |

**Runtime role: dedicated, exact least privilege.** It is checked under the migrator's
advisory lock before any write (before the ledger and marker tables are created, before
the pending marker upsert, before any step), and again after the last migration, before
phase 2 can mark PASS.

- **Missing role:** `runtime_role_missing`.
- **The migrator itself (owner-as-runtime):** accepted only in `local` (the Docker
  stack, where one superuser runs everything). Refused in `selfhosted` and `supabase`
  (`runtime_role_unsafe`).
- **Who creates the dedicated role:**
  - `selfhosted` (CNPG, Cloud SQL): the privileged
    `supabase/bootstrap/hoopdb_runtime_role.sql`, run as a superuser (CNPG `postgres`)
    before migrate on every deploy, like the Worker bootstrap. It is idempotent, creates
    the role with no privileges and never sets a password; the credential is provisioned
    separately. #75 wires it for AWS CNPG; GCP needs the same step.
  - `supabase`: pre-provisioned by an operator and verified by the migrator. On
    PostgreSQL 16+ the creator-ADMIN caveat of the Worker applies here too: a role
    created by Supabase's non-superuser `postgres` would carry ADMIN for `postgres` and
    be refused. This is a decision point in #98.

**The allowlist** (`runtimeRoleViolations` in `src/hoopdb/migrate-guards.ts`). Every
violation is named:

- LOGIN or NOLOGIN exactly as `HOOPDB_RUNTIME_ROLE_LOGIN` says. NOSUPERUSER, NOCREATEDB,
  NOCREATEROLE, NOREPLICATION, NOBYPASSRLS.
- A member of no role. Its only member may be `HOOPDB_RUNTIME_WORKLOAD_ROLE`, without
  ADMIN OPTION.
- Owns nothing, in any database. No CREATE on any schema or on the database, however it
  is obtained (PUBLIC or memberships included).
- **Approved, and nothing else:**
  - `USAGE` on schema `sessions`;
  - `SELECT` on `sessions.__96bulls_migrations` and `sessions.__96bulls_role_validation`,
    with no GRANT OPTION;
  - `CONNECT` / `TEMPORARY` on the database;
  - EXECUTE only on `sessions` trigger functions, which cannot be called directly.

  Any other direct reference in any database (`pg_shdepend`) is refused: table, column,
  sequence or function privileges, default ACLs, policies. So is any effective table,
  column or sequence privilege, or EXECUTE on any callable `sessions` function, because
  the role now has schema USAGE.

- **Out of scope here:** the services' own runtime DML grants (ingest, workers) belong to
  #98. A runtime role holding them is refused until this allowlist is extended in a
  reviewed change.

The migrator grants exactly `USAGE` on `sessions` and `SELECT` on the two tables,
idempotently.

**One migration authority and the full ordered sequence (decision point).** Also under the
lock and before any write, the migrator refuses:

- `mixed_migration_authority`: `supabase_migrations.schema_migrations` (the
  `supabase db push` history) records any of these migrations, or exists but can't be
  read.
- `ledger_unknown_step`: the ledger names a schema step this image doesn't have.
- `ledger_gap`: the ledger is not one contiguous prefix of the ordered steps (upstream,
  then 96bulls). A first 96bulls row with no upstream rows is refused, never backfilled.
- `migration_changed`: a recorded step's file changed.
- `unledgered_schema`: an object that an unrecorded step creates already exists. The
  inventory is every table, view, sequence, index, function, trigger, policy and role in
  the migration files' CREATE statements (`inventoryOf`; a test proves it covers
  everything a full migration leaves in `sessions`). So a database where only
  `sessions.session_directory` exists is refused before any write, not after seven
  ledger rows.

It never imports a backlog into the ledger and never marks a step applied. Resolving any of
these is an explicit operator decision, recorded in the activation dossier (#98):

1. choose one authority for this database (`hoopdb-migrate`, or the Supabase CLI, never
   both);
2. reconcile by hand and review the result: for example, recreate a disposable database;
   or verify each object against the file and have the owner write ledger rows in a
   reviewed change;
3. only then rerun the migrator.

**Evidence.** The `pg-proof` workflow (`.github/workflows/pg-proof.yml`) runs
`scripts/pg-proof/role-proof.ts` on the exact head against real PostgreSQL 15 and 17
service containers. It uploads `pg-proof-pg15` and `pg-proof-pg17` artifacts, each a JSON
file (head, server version, each case's pass/fail) plus its log.

## CNPG (self-hosted HoopDb)

- **Phase 1, every deploy (required for existing clusters):** a Job ordered before
  `hoopdb-migrate`, for example a pre-migrate step in the same release. It runs phase 1 as
  the cluster superuser `postgres` (the CNPG superuser secret, `enableSuperuserAccess`),
  never as `hoop`, against the HoopDb database. It must succeed before migrate starts.
- **Phase 1, new clusters (optional extra):** `bootstrap.initdb.postInitApplicationSQLRefs`
  pointing at a ConfigMap built from the same file. CNPG runs it once, as `postgres`, at
  init. It never reruns, so the Job above is still the source of truth.
- **Phase 2, every deploy:** `hoopdb-migrate` itself runs it after the last migration (the
  `verify:session_directory_search_worker` step, `HOOPDB_ROLE_VERIFY`, on by default). With `HOOPDB_ROLE_VERIFY=false`, a
  separate post-migrate Job must run `session_directory_search_worker_mark_pass.sql` (not
  the bare verifier, which records nothing), or no reader becomes ready.

A CNPG managed role (`spec.managed.roles`) can express the attributes but not the
role-level `statement_timeout`, so it is not enough on its own.

`hoopdb-migrate` (`src/hoopdb/migrate.ts`) runs both checks on **every** migrate, and never
records either in the ledger:

- **Phase 1** is the `bootstrap:session_directory_search_worker` step, which runs before
  the migrations when the profile enables it (`HOOPDB_ROLE_BOOTSTRAP`, default the
  explicit `HOOPDB_SUPABASE_SHIM` choice; never in `supabase` mode, see below). As non-superuser `hoop`, it only verifies an
  already-provisioned role and fails closed with `42501` if the role is absent, so the
  deploy Job above must run first. On the local Docker stack, the migrator is the database
  superuser and the step creates the role.
- **Phase 2** is the `verify:` step, which runs after the migrations in every stage.

## Supabase (hosted)

On Supabase, `postgres` is not a superuser. Its behavior depends on the server version:

1. Read the version: `SHOW server_version_num;`.
2. **Below 160000 (PostgreSQL 15):** an operator runs the command above as `postgres`
   (CREATEROLE). Creating the role grants `postgres` nothing.
3. **160000 or above:** do not run it as `postgres`. The bootstrap refuses it with
   `42501`, because `postgres` would receive ADMIN OPTION on the role and the migration
   would then refuse it. This needs a decision before any apply: either a superuser run
   through Supabase support, or an explicit, reviewed change to the spec. It is an open
   blocker, not a step to work around.
4. Check the role with the queries below, then apply the migrations with `hoopdb-migrate`.
   It writes the pending marker, runs phase 2 and records PASS. Migrations applied any
   other way write no marker, so no reader becomes ready (fail closed).
5. With `HOOPDB_ROLE_VERIFY=false`, an operator runs `mark_pass.sql` (above) as `postgres`
   after the migrations. It must exit 0 before the deploy counts as done.

## Checks

```sql
SELECT rolcanlogin, rolinherit, rolbypassrls, rolsuper, rolcreaterole, rolcreatedb, rolreplication
  FROM pg_roles WHERE rolname = 'session_directory_search_worker';
SELECT setdatabase, setconfig FROM pg_db_role_setting s
  JOIN pg_roles r ON r.oid = s.setrole WHERE r.rolname = 'session_directory_search_worker';
SELECT g.rolname AS role, u.rolname AS member, m.admin_option FROM pg_auth_members m
  JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles u ON u.oid = m.member
 WHERE 'session_directory_search_worker' IN (g.rolname, u.rolname);
SELECT d.deptype, pg_describe_object(d.classid, d.objid, d.objsubid) FROM pg_shdepend d
 WHERE d.refclassid = 'pg_authid'::regclass
   AND d.refobjid = 'session_directory_search_worker'::regrole;
```

Expected results:

- every attribute is `f`;
- exactly one settings row, `0 | {statement_timeout=2s}`;
- no membership rows until the separate authenticator binding.
- after the migrations, `pg_shdepend` lists only `a` rows: schema `sessions` and the three
  approved functions. Before `20261030020000`, it lists none.

## Rollback and removal

The migration's down path
(`supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql`) revokes
what the migration granted and leaves the role at its spec. Removing the role is a
separate operator step, done as a superuser after every down that references it:
`DROP OWNED BY session_directory_search_worker; DROP ROLE session_directory_search_worker;`
in each database.

## Rehearsal databases

The HoopDb ledger records each migration's sha256. This hotfix changes
`20261030020000`, so a rehearsal database that already applied the old file fails with
"changed after it was applied". Recreate any such disposable database. This includes a
local Docker stack: run `96bulls down <stage> --purge`, then `up`. The old file was never
applied to a real one. The sha256 of `20261030020000` as merged (#96, after #72's token pins) is
`c223b77e4c875cbc36499e6ca1c8e521b564ebf25facd87313b3e58335918de4`. The earlier draft value
`f86e5dc9...0d66` was superseded before merge. A test pins the checksum.
