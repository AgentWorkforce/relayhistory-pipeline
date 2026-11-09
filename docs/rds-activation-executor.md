# RDS activation executor (separate-database layout)

Contract id: **`rds-activation/v1`**. Nothing here authorizes running it against a real
database. A live run needs Khaliq's explicit OK in his own words.

Target, fixed by the infra postgres-layout contract (infra #107, separate service
databases on one RDS PostgreSQL instance):

| Item | Value (fixed in v1) |
|---|---|
| Database | `hoopdb` |
| Schema | `sessions` |
| Migration owner (LOGIN) | `hoop` |
| Runtime role (LOGIN) | `sessions_pipeline_runtime` |
| Named executor (NOLOGIN, transaction-scoped) | `hoopdb_activation_executor` |
| Fixed service roles | generated from the repository's role SQL (below) |

The names are constants of `rds-activation/v1`, not inputs. A different set is a
different contract id. PostgreSQL 16 or later is required. PostgreSQL 15 is refused
before any change, because its CREATEROLE semantics differ: it records no creator
ADMIN edge, and any CREATEROLE role there can alter any non-superuser role.

## Entrypoint

```
bun src/hoopdb/rds-activation.ts <command> [flags]
node --experimental-strip-types src/hoopdb/rds-activation.ts <command> [flags]   # Node 22.6+
```

The driver uses only erasable TypeScript syntax: no parameter properties, enums or
namespaces. A unit test runs it under `node --experimental-strip-types`, so this cannot
regress.

| Command | What it does | Connects as | Database | Transaction |
|---|---|---|---|---|
| `artifact` | Writes the artifact identity to the result channel. No connection. | none | none | none |
| `create-database` | Step 1. `CREATE DATABASE hoopdb TEMPLATE template0` if absent, then verifies it (UTF8, not a template, owner = provisioner or `hoop`). Idempotent; a concurrent creator (42P04) is tolerated and verified. | provisioning login (`--provisioner`, the RDS master) | `--maintenance-db` (default `postgres`) | none. PostgreSQL forbids `CREATE DATABASE` in a transaction block, because it copies the template on disk and cannot be rolled back. |
| `activate` | Step 2. All role, grantor-graph, setting, LOGIN, default-privilege, ownership and cleanup work, then the final graph validation. Skipped (status `already_activated`) when the shared catalogs show the activated state. | provisioning login | `hoopdb` | exactly one (`BEGIN` ... `COMMIT`); any failure rolls back everything |
| `verify` | Step 3. Read-only. Proves `hoop` and `sessions_pipeline_runtime` log in with their passwords, and re-derives the final graph as `hoop`. | `hoop`, then `sessions_pipeline_runtime` | `hoopdb` | `READ ONLY` |
| `run` | `create-database`, then `activate`, then `verify`. | as above | as above | as above |
| `rotate-self` | One LOGIN role changes its own password, then proves that the new one logs in and the old one is refused (28P01). | the role itself (`--role`) | `hoopdb` | one (the verifier is bound and used through `EXECUTE`, as in `activate`) |

### Transaction boundary and who does what inside `activate`

The steps run in this order inside one transaction, with `lock_timeout = 10s` and a
session advisory lock, so two executors never interleave:

| Fragment | Runs as | Does |
|---|---|---|
| `00_preconditions` | provisioner | Read-only. Checks PG16+, session_user = current_user = the named provisioner, non-superuser with CREATEROLE and CREATEDB that inherits `rds_superuser`, `rdsadmin` SUPERUSER, `hoopdb` still owned by the provisioner and pristine (no schema besides `public`, no extension besides plpgsql, empty `public`, no foreign default ACL), every spec role and the executor absent, and the verifiers well-formed and distinct. |
| `10_provisioner_function_defaults` | provisioner | `ALTER DEFAULT PRIVILEGES FOR ROLE rds_superuser` and `FOR ROLE <provisioner>` `REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`, **in `hoopdb` only**. On RDS both can CREATE in `sessions` (#389), and only an `rds_superuser` inheritor can set that role's default. |
| `20_executor` | provisioner | `CREATE ROLE hoopdb_activation_executor NOLOGIN NOINHERIT CREATEROLE`, then a SET-only self grant, then `SET LOCAL ROLE`. Asserts the executor's exact two edges. |
| `30_roles` | executor | Creates every spec role NOLOGIN with its exact settings, then asserts the grantor graph (below). |
| `40_login` | executor | `ALTER ROLE ... LOGIN PASSWORD <verifier>` for every LOGIN role, clears the bound verifiers, then grants itself SET on `hoop`. |
| `50_owner_handoff` | provisioner, then `hoop` | `ALTER DATABASE hoopdb OWNER TO hoop` (through the SET chain). Then, as `hoop`: `REVOKE ALL ON DATABASE hoopdb FROM PUBLIC`, `GRANT CONNECT` to each LOGIN role, `CREATE SCHEMA sessions`, and hoop's own global function default. |
| `60_cleanup` | provisioner | `DROP ROLE hoopdb_activation_executor`. |
| `70_validate` | provisioner | Final validation (below). A mismatch raises, and the whole transaction rolls back. |

### Grantor graph

```
during the transaction (PostgreSQL 16+; "a" ADMIN, "i" INHERIT, "s" SET)

  provisioner ──(a, grantor rdsadmin: automatic creator edge)──────────▶ executor
  provisioner ──(s, grantor provisioner: self grant, no INHERIT)───────▶ executor
  executor    ──(a, grantor rdsadmin: automatic creator edge)──────────▶ hoop
  executor    ──(a, grantor rdsadmin)──────────────────────────────────▶ sessions_pipeline_runtime
  executor    ──(a, grantor rdsadmin)──────────────────────────────────▶ each fixed service role
  executor    ──(s, grantor executor: SET chain for ALTER DATABASE OWNER)▶ hoop

after DROP ROLE hoopdb_activation_executor (fragment 60)

  (no edges) No pg_auth_members row has a HoopDb role or the executor as role,
  member or grantor. hoop is a member of nothing (not rds_superuser) and has no
  members, and the provisioner holds no edge on any HoopDb role.
```

`DROP ROLE` removes every row in which the dropped role is the role, the member or
the grantor. This was measured on PostgreSQL 16.15 and 17.11: 0 edges and 0 dangling
grantors remain. The automatic creator edges cannot be revoked by a non-superuser
beforehand, and nothing is re-granted under another grantor, because the target graph
is empty. Fragment 70 asserts the empty graph, the absence of the executor and of any
dangling grantor, the exact attributes and settings of every spec role, that `hoop`
owns `hoopdb` and `sessions`, an exact `hoopdb` ACL (`hoop` as owner, CONNECT only for
each LOGIN role, nothing for PUBLIC or the provisioner), the exact three global function
defaults, no other default-ACL row, and that no role other than `hoop` holds CREATE or
TEMPORARY.

After commit, the provisioner cannot CONNECT to `hoopdb` (42501) or ALTER, SET ROLE
to, GRANT or DROP any HoopDb role, and `hoop` cannot CREATE ROLE.

## Role set (generated, never hand-maintained)

The driver builds the role spec at runtime from these files, all of which are part of
the artifact identity. At v1 it holds 17 roles, of which 9 are LOGIN (`hoop`,
`sessions_pipeline_runtime` and 7 service logins). `bulls96_babysitter_retention` is
deferred to NOLOGIN (below), and 7 more roles are NOLOGIN by design.

- `rds_migration_roles.sql`: the fixed NOLOGIN inventory.
- `session_consumer_roles.sql`: the bare consumer role.
- `session_directory_search_worker.sql` and every operator `*_role.sql` (except the
  legacy runtime-role files): LOGIN and every `ALTER ROLE ... SET k = v`.

A unit test fails when any bootstrap file creates or alters, by name, a role that the
spec does not cover. A second unit test fails when a file that arms LOGIN or sets a role
setting is not a spec source. After activation, `hoop` holds no ADMIN or CREATEROLE.
So on this layout every LOGIN and every setting must be created by activation, and the
operator `*_role.sql` files must not still need to create or alter a role.

One deliberate exception: `bulls96_babysitter_retention` is created NOLOGIN, with its
settings. The landed (frozen) migration `20261105042000_babysitter_evidence_retention.sql`
refuses a role that can already log in when it first applies. Nobody can arm LOGIN
later on this layout, so that login is unavailable until a reviewed path exists. A unit
test pins the exception to the migration text.

## Inputs

Non-secret inputs are a flag or an environment variable. When both are set and differ,
the input is refused.

| Flag | Env | Required | Meaning |
|---|---|---|---|
| `--host` | `HOOPDB_ACTIVATION_HOST` | yes (not `artifact`) | RDS writer endpoint host name |
| `--port` | `HOOPDB_ACTIVATION_PORT` | no (5432) | |
| `--provisioner` | `HOOPDB_ACTIVATION_PROVISIONER` | all except `artifact`, `rotate-self` | Name of the provisioning login (the RDS master). Must equal `session_user`. It may not be a HoopDb, executor or RDS platform role. |
| `--maintenance-db` | `HOOPDB_ACTIVATION_MAINTENANCE_DB` | no (`postgres`) | Database for step 1 and the activated-state probe |
| `--tls` | `HOOPDB_ACTIVATION_TLS` | no (`verify-full`) | `verify-full` or `disable`. `disable` is refused unless the host is a loopback address (emulation only). |
| `--ca-file` | `HOOPDB_ACTIVATION_CA_FILE` | with `verify-full` | Path to the pinned RDS CA bundle (public data). Chain and host name are both verified; an IP host is refused. |
| `--role` | `HOOPDB_ACTIVATION_ROLE` | `rotate-self` only | A LOGIN role of the spec |
| `--secrets-stdin` | none | no | Read secrets from stdin instead of the environment |
| `--result-file` | `HOOPDB_ACTIVATION_RESULT_FILE` | one of the two | Path for the JSON result. It is created with mode 0600 and refused if it exists. |
| `--result-fd` | `HOOPDB_ACTIVATION_RESULT_FD` | one of the two | Already-open file descriptor, 3 or above |

Secrets come only from the environment or stdin at runtime, never from argv, a file or a
connection URL. Arguments that look like a secret or a URL are refused.

| Env | stdin JSON | Needed by |
|---|---|---|
| `HOOPDB_ACTIVATION_PROVISIONER_PASSWORD` | `provisioner_password` | `create-database`, `activate`, `run` |
| `HOOPDB_ACTIVATION_PASSWORD_<ROLE>` (upper-case role name, for example `HOOPDB_ACTIVATION_PASSWORD_HOOP`) | `role_passwords: {"<role>": "..."}` | every LOGIN role for `activate` and `run`; `hoop` and `sessions_pipeline_runtime` for `verify` |
| `HOOPDB_ACTIVATION_CURRENT_PASSWORD`, `HOOPDB_ACTIVATION_NEW_PASSWORD` | `current_password`, `new_password` | `rotate-self` |

With `--secrets-stdin`, stdin must be one JSON object with only these keys, and none of
the secret variables may be set. A password variable for a role outside the spec is
refused. Role passwords must be printable ASCII, at least 32 characters, all distinct,
and different from the provisioner password. The driver sends only a client-computed
SCRAM-SHA-256 verifier (4096 iterations, random salt) to the server, never the
plaintext. The verifiers are bound as bind parameters and used through `EXECUTE`, so
they appear in no top-level statement text, and they are cleared after use. Nothing is
written to disk except the result, which never contains a secret or a verifier.

## Outputs

The result target is validated and opened **before any database work**. A file is
created exclusively (0600, refused if it exists); a descriptor must be open. A bad
channel is exit 2, with no server contact. The write loops over short writes and
retries `EAGAIN`, and a result file is fsynced before close.

stdout and stderr carry human-readable progress only. They are **not** an interface and
must never be parsed. The machine-readable result is one JSON document written once, at
exit, to `--result-file` or `--result-fd`, and nowhere else. Invalid input (exit 2)
also writes a result (`status: "invalid_input"`, `exit_code: 2`, the reason in
`steps[0].message`, never a value) whenever the raw arguments or environment name one
unambiguous, usable result target. Only when the result channel is itself the invalid
or ambiguous part is exit 2 the sole signal.

| Code | `status` | Meaning |
|---|---|---|
| 0 | `activated`, `already_activated`, `verified`, `database_ready`, `rotated`, `artifact` | success |
| 2 | `invalid_input` | bad flags, environment or secrets; no connection was made |
| 3 | `refused` | the server reported a policy or precondition refusal (SQLSTATE class 0A, 22, 28, 42 or 55) in step 1, before or in fragment 00, or at the `rotate-self` login; nothing was changed |
| 4 | `rolled_back` | the `activate` transaction failed after fragment 00 and was rolled back; an empty `hoopdb` from step 1 may remain, and a re-run is safe |
| 5 | `verify_failed` | post-commit verification (or the `rotate-self` re-login check) returned a verdict: fragment 70 or a login was refused by the server, or the runtime probe mismatched. `activate` did commit. A transport failure during verify is `internal_error` instead |
| 1 | `internal_error` | anything else. This includes a lost connection, a socket error or an admin shutdown before or in fragment 00 (no policy verdict; nothing was changed), and a lost `COMMIT` reply whose outcome could not be decided. On a lost `COMMIT` reply the driver re-checks the shared catalogs on a fresh connection: if activated, it carries on and verifies; if still fresh, the result is `rolled_back`; otherwise it is `internal_error` (indeterminate; run `verify`). A server error reply to `COMMIT` is `rolled_back` |

Result document (v1):

```json
{
  "contract": "rds-activation/v1",
  "artifact_sha256": "<64 hex>",
  "artifact_files": [["supabase/bootstrap/rds_activation/00_preconditions.sql", "<sha256>"], ["...", "..."]],
  "migration_generation": "<64 hex: migrate.ts generation() of the same tree, informational>",
  "command": "run",
  "status": "activated",
  "exit_code": 0,
  "server_version_num": 170004,
  "steps": [{ "name": "create-database", "status": "ok", "sqlstate": null, "message": "created" }],
  "database": { "name": "hoopdb", "created": true, "owner": "hoop" },
  "graph": { "edges": [], "helper_present": false, "roles": ["anon", "..."] }
}
```

`steps[].message` is a server or driver message, scrubbed of every secret and of any
SCRAM verifier.

## Artifact identity

`artifact_sha256` is the SHA-256 of the UTF-8 JSON array `artifact_files`. That array
is the ordered list of `[repo-relative path, sha256(file bytes)]`: every SQL fragment in
execution order, then the role-spec source files (sorted), then the driver
`src/hoopdb/rds-activation.ts`. Any byte change to any of them changes the identity.
`bun src/hoopdb/rds-activation.ts artifact --result-fd 3 3>artifact.json` produces it
without connecting. `migration_generation` is reported for pairing with the migrator,
but it is not part of the artifact hash.

## Migrator contract after activation

Run the phase-1 bootstrap chain (`SESSION_BOOTSTRAP_FILES`), then the migrator, as
`hoop`, with:

```
HOOPDB_DB_MODE=rds
HOOPDB_RDS_LAYOUT=separate-database
HOOPDB_RUNTIME_ROLE=sessions_pipeline_runtime
HOOPDB_RUNTIME_ROLE_LOGIN=true
HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke
```

Why the existing preflight refuses a plain `hoop`: in rds mode `rdsOwnerPreflight`
requires the migration owner to be a CREATEROLE member of `rds_superuser`. That
membership is its proof that the server is RDS-shaped, and the legacy `rds_*`
bootstrap files run as that identity. A plain `hoop` therefore fails with
`rds_profile_unproven` on both `owner_rds_superuser` and `owner_createrole`.
`HOOPDB_RDS_LAYOUT=separate-database` selects `rdsSeparateOwnerPreflight` instead. It
requires:

- `hoop` is not a superuser, and has no CREATEROLE, CREATEDB, REPLICATION or BYPASSRLS;
- `hoop` is a member of no role (so not `rds_superuser`), and has no members;
- `hoop` owns the current database and `sessions`;
- the fixed `rdsadmin` SUPERUSER exists, and so does a NOLOGIN, non-superuser `rds_superuser`;
- the activation executor is gone, and the session uses no SET ROLE.

In this layout the runtime role must have no member at all: the master-owner creator
edge is refused. With the variable unset or set to `master-owner`, today's rds contract
is unchanged. The function-boundary gate is the same rds gate: `rdsadmin` is the
excluded trust root, and `rds_superuser` and the provisioner are covered by fragment
10.

Emulated on PG16 and PG17: the phase-1 chain, a full migrate, runtime readiness
(`schemaVerdict` as `sessions_pipeline_runtime`), a second migrate (0 applied) and
`verify` all pass after activation.

## Known limitations (v1, accepted)

- **Babysitter retention is not supported on this layout in v1.**
  `bulls96_babysitter_retention` is created NOLOGIN. The frozen migration
  `20261105042000` refuses it if it can already log in, and nobody can arm LOGIN after
  activation. A unit test pins the exception to the migration text. Follow-up: "arm
  babysitter retention on separate-database layout" (a forward migration in its own slot).
- **Activation is the only role-shaping path.** The operator `*_role.sql` files are
  unsupported on this layout. They issue `ALTER ROLE`, which `hoop` cannot run.
  Activation and the migrator together cover the full role set. Separate-database
  variants of their grant and verify parts, which never `ALTER ROLE` and are selected
  by `HOOPDB_RDS_LAYOUT`, are a follow-up PR.
- **No new service role in place.** Roles are instance-wide, and nobody holds ADMIN
  after activation. A future role needs a reviewed activation v2 or a new instance.
- **Fixed names.** `hoopdb`, `hoop`, `sessions_pipeline_runtime` and the executor name
  are v1 constants.

## Live-run verification items (infra owner's sandbox run)

- Real `rds_superuser` behaviour:
  - effective CREATE on `sessions` (#389);
  - any hidden ADMIN path over the HoopDb roles (confirm that the master is refused ALTER, GRANT and DROP);
  - whether the master or `rds_superuser` can CONNECT to `hoopdb` despite the revoked PUBLIC grant.
- `log_statement` and parameter logging in the parameter group. The bound SCRAM verifiers (never plaintext) could be logged.
- Verified TLS against the pinned RDS CA bundle. A wrong CA and a wrong host name must both be refused.

## Security model and trade-offs

- **Catalog lookups cannot be spoofed by the database owner.** After activation `hoop`
  owns `hoopdb`. So it could create shadow `pg_roles` or `pg_auth_members` tables and
  point a search_path at them (`ALTER DATABASE hoopdb SET search_path`, or its own role
  default). Every connection therefore starts with
  `-c search_path=pg_catalog,pg_temp` (a startup option, which beats database and role
  defaults). Every transaction also runs `SET LOCAL search_path = pg_catalog, pg_temp`.
  Every catalog reference in the fragments, the driver and `rdsSeparateOwnerPreflight`
  is `pg_catalog`-qualified, and a unit test enforces that. Fragments 00 and 70 also
  refuse any database-level role setting in `hoopdb`. Emulated on PG16 and PG17: a
  shadow `pg_auth_members` plus `ALTER DATABASE hoopdb SET search_path` makes `verify`
  fail closed (exit 5) and still report the real extra edge.

- **No standing capability.** After `activate`, only `rdsadmin` (AWS) holds ADMIN on any
  HoopDb role. The provisioner holds nothing on `hoopdb`, and `hoop` cannot create or
  alter roles.
- **Rotation is self-rotation only** (`rotate-self`). Each LOGIN role changes its own
  password while logged in with the current one. A master-driven rotation cannot work:
  after activation the master can neither grant a helper ADMIN on a role ("permission
  denied to grant role") nor alter it ("permission denied to alter role"). Measured on
  PG16 and PG17. Keeping a master creator edge instead would let the master grant
  itself SET and become the role, which breaks "the provisioning login holds nothing".
  1Password is the system of record, with each secret versioned.
- **A lost service-role password has no customer-held recovery.** On the same instance
  the master is refused `DROP ROLE` on the HoopDb roles, so the existing roles block
  re-activation. A snapshot restore brings back the same roles and verifiers. The
  break-glass path is therefore one of:
  - a new RDS instance, with a logical dump/restore of `hoopdb`, re-activated there; or
  - AWS support.
- **Fresh instance only.** Roles are instance-wide, so a second activation on the same
  instance is refused by fragment 00 (each existing role is listed).
- **Database ACL is ours.** PUBLIC loses CONNECT, TEMPORARY and CREATE on `hoopdb`.
  Denying our roles CONNECT to *other* databases (for example `postgres` or
  `relaycontrol`, which keep the PostgreSQL default PUBLIC CONNECT) is each database
  owner's job under infra #107, not this executor's.
- **Not covered by the emulation.** Real `rds_superuser` behaviour (#389 CREATE, any
  hidden ADMIN path), IAM authentication, parameter groups and `log_statement` settings
  that might log bind parameters (the verifiers, never plaintext). These are live gates
  for the approved live run.
