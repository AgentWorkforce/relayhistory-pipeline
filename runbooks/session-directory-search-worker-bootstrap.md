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

**Deploy order:** phase 1 (superuser), then `hoopdb-migrate`, then phase 2. Each step uses
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

## CNPG (self-hosted HoopDb)

- **Phase 1, every deploy (required for existing clusters):** a Job ordered before
  `hoopdb-migrate`, for example a pre-migrate step in the same release. It runs phase 1 as
  the cluster superuser `postgres` (the CNPG superuser secret, `enableSuperuserAccess`),
  never as `hoop`, against the HoopDb database. It must succeed before migrate starts.
- **Phase 1, new clusters (optional extra):** `bootstrap.initdb.postInitApplicationSQLRefs`
  pointing at a ConfigMap built from the same file. CNPG runs it once, as `postgres`, at
  init. It never reruns, so the Job above is still the source of truth.
- **Phase 2, every deploy:** `hoopdb-migrate` itself runs it after the last migration (the
  `verify:session_directory_search_worker` step, `HOOPDB_ROLE_VERIFY`, on by default). A
  separate post-migrate Job running phase 2 as `hoop` is equivalent.

A CNPG managed role (`spec.managed.roles`) can express the attributes but not the
role-level `statement_timeout`, so it is not enough on its own.

`hoopdb-migrate` (`src/hoopdb/migrate.ts`) runs both checks on **every** migrate, and never
records either in the ledger:

- **Phase 1** is the `bootstrap:session_directory_search_worker` step, which runs before
  the migrations whenever the Supabase role shim is enabled (`HOOPDB_ROLE_BOOTSTRAP`,
  default `HOOPDB_SUPABASE_SHIM`). As non-superuser `hoop`, it only verifies an
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
4. Check the role with the queries below, then apply the migrations.
5. After the migrations, an operator runs phase 2 as `postgres` (it only reads the
   catalogs). It must exit 0 before the deploy counts as done.

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
applied to a real one. The sha256 of `20261030020000` changes from `ef41a4aa...679f` to
`f86e5dc9...0d66`.
