# Bootstrapping the session directory search Worker role

`supabase/migrations/20261030020000_session_directory_short_summary.sql` needs the role
`session_directory_search_worker` to exist before it runs, and it neither creates nor
alters that role. Creating a role needs CREATEROLE, and on PostgreSQL 16+ setting a role
parameter also needs ADMIN OPTION on the role. A least-privilege migration owner (CNPG
`hoop`) has neither. So a privileged step provisions the role once, and the migration
only verifies it. If the role is absent, the migration fails with `42704`. If it differs
from the spec, the migration fails with `55000`, naming every violation. Either way it
fails before any DDL, so nothing is applied.

Nothing here runs automatically against a real database. Every apply needs Khaliq's
explicit OK.

## Interface (stable; #75 wires the deploy-time provisioning against this)

| Item     | Value                                                                                                                                                                                                                                   |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Role     | `session_directory_search_worker`                                                                                                                                                                                                       |
| Attrs    | `NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION`                                                                                                                                                       |
| Settings | exactly one row in `pg_db_role_setting`: `setdatabase = 0`, `setconfig = {statement_timeout=2s}`. Other spellings (`2000ms`, `2000`), other values, extra settings and per-database settings are all refused.                           |
| Members  | none, and it is a member of none. The migration owner and every other role hold no membership and no ADMIN OPTION on it. The later authenticator binding (`GRANT ... TO authenticator`, NOINHERIT, no ADMIN OPTION) is a separate step. |
| File     | `supabase/bootstrap/session_directory_search_worker.sql`: one atomic `DO` block. It is idempotent and never alters an existing role.                                                                                                    |
| Run as   | a superuser. A non-superuser with CREATEROLE is accepted only on PostgreSQL 15 and older (see Supabase below).                                                                                                                          |
| When     | before the migrate run that first applies `20261030020000`. It is safe to run before every migrate.                                                                                                                                     |
| Command  | `psql -v ON_ERROR_STOP=1 -X -f supabase/bootstrap/session_directory_search_worker.sql` against the target database                                                                                                                      |
| Success  | exit 0: the role exists exactly as specified (created now, or already present).                                                                                                                                                         |
| Failure  | `42501`: this caller may not create the role. `55000`: an existing role differs. Nothing is changed in either case, and the deploy must stop.                                                                                           |

The bootstrap creates no membership when a superuser runs it. On PostgreSQL 16+, a
non-superuser CREATEROLE creator is automatically granted ADMIN OPTION on the new role.
The migration refuses that grant, so the bootstrap refuses such a caller up front
(`42501`).

## CNPG (self-hosted HoopDb)

The deploy runs the file as the cluster superuser `postgres`, using the CNPG superuser
secret (`enableSuperuserAccess`), never as `hoop`, and only then starts the
`hoopdb-migrate` Job. There are two ways to wire it:

- **Every deploy (required for existing clusters):** a Job ordered before
  `hoopdb-migrate`, for example a pre-migrate step in the same release. It runs the
  command above as `postgres` against the HoopDb database and must succeed before
  migrate starts.
- **New clusters (optional extra):** `bootstrap.initdb.postInitApplicationSQLRefs`
  pointing at a ConfigMap built from the same file. CNPG runs it once, as `postgres`, in
  the application database at init. It never reruns, so the Job above is still the
  source of truth.

A CNPG managed role (`spec.managed.roles`) can express the attributes but not the
role-level `statement_timeout`, so it is not enough on its own.

`hoopdb-migrate` (`src/hoopdb/migrate.ts`) also runs the same file as its
`bootstrap:session_directory_search_worker` step whenever the Supabase role shim is
enabled (`HOOPDB_ROLE_BOOTSTRAP`, which defaults to `HOOPDB_SUPABASE_SHIM`). As
non-superuser `hoop`, that step only verifies an already-provisioned role. It fails
closed with `42501` if the role is absent, so the deploy Job above must run first. On the
local Docker stack, the migrator is the database superuser and the step creates the role.

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

## Checks

```sql
SELECT rolcanlogin, rolinherit, rolbypassrls, rolsuper, rolcreaterole, rolcreatedb, rolreplication
  FROM pg_roles WHERE rolname = 'session_directory_search_worker';
SELECT setdatabase, setconfig FROM pg_db_role_setting s
  JOIN pg_roles r ON r.oid = s.setrole WHERE r.rolname = 'session_directory_search_worker';
SELECT g.rolname AS role, u.rolname AS member, m.admin_option FROM pg_auth_members m
  JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles u ON u.oid = m.member
 WHERE 'session_directory_search_worker' IN (g.rolname, u.rolname);
```

Expected results:

- every attribute is `f`;
- exactly one settings row, `0 | {statement_timeout=2s}`;
- no membership rows until the separate authenticator binding.

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
