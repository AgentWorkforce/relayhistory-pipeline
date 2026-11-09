# RDS activation executor (separate-database layout) — interface

Contract id: **`rds-activation/v1`**. Status: interface, then a reviewed implementation in
the same draft PR. Nothing here authorizes running it against a real database. A live
run needs Khaliq's explicit OK in his own words.

Target, fixed by the infra postgres-layout contract (infra #107, separate service
databases on one RDS PostgreSQL instance):

| Item | Value (fixed in v1) |
|---|---|
| Database | `hoopdb` |
| Schema | `sessions` |
| Migration owner (LOGIN) | `hoop` |
| Runtime role (LOGIN) | `sessions_pipeline_runtime` |
| Named executor (NOLOGIN, transaction-scoped) | `hoopdb_activation_executor` |

The names are constants of `rds-activation/v1`, not inputs. A different set is a
different contract id.

## Entrypoint

```
bun src/hoopdb/rds-activation.ts <command> [flags]
```

| Command | What it does | Connects as | Database | Transaction |
|---|---|---|---|---|
| `artifact` | Writes the artifact identity to the result channel. No connection. | none | none | none |
| `create-database` | Step 1. `CREATE DATABASE hoopdb` if absent, then verifies it. Idempotent. | provisioning login (`--provisioner`, the RDS master) | `--maintenance-db` (default `postgres`) | none (autocommit; PostgreSQL forbids `CREATE DATABASE` in a transaction block) |
| `activate` | Step 2. All role, grantor-graph, LOGIN, default-privilege, ownership and cleanup work, then the final graph validation. | provisioning login | `hoopdb` | exactly one (`BEGIN` ... `COMMIT`); any failure rolls back everything |
| `verify` | Step 3. Read-only. Proves `hoop` and `sessions_pipeline_runtime` log in with their passwords and re-derives the final graph. | `hoop`, then `sessions_pipeline_runtime` | `hoopdb` | read-only |
| `run` | `create-database`, then `activate` (skipped if the shared catalogs already show the activated state), then `verify`. | as above | as above | as above |

Who does what inside `activate` (one transaction, in this order):

1. Provisioning login: read-only preconditions; sets the function defaults of
   `rds_superuser` and of itself **in `hoopdb` only** (`pg_default_acl` is per database).
2. Provisioning login: `CREATE ROLE hoopdb_activation_executor NOLOGIN NOINHERIT CREATEROLE`,
   then grants itself `SET` (not `INHERIT`) on it, then `SET LOCAL ROLE` to it.
3. `hoopdb_activation_executor`: creates `hoop`, `sessions_pipeline_runtime` and the fixed
   NOLOGIN roles, all NOLOGIN first; checks the grantor graph; arms LOGIN with the
   password verifiers; grants itself `SET` on `hoop`.
4. Provisioning login: `ALTER DATABASE hoopdb OWNER TO hoop` (through the SET chain).
5. `hoop` (`SET LOCAL ROLE`): database ACL (`REVOKE ALL ... FROM PUBLIC`,
   `GRANT CONNECT ... TO sessions_pipeline_runtime`), `CREATE SCHEMA sessions`, its own
   global function default.
6. `RESET ROLE`; the provisioning login drops `hoopdb_activation_executor`, which removes
   every membership edge the bootstrap created.
7. Final validation of attributes, the membership graph, ownership, the database ACL
   and the default ACLs. A mismatch raises and the whole transaction rolls back.

After `activate` commits: no helper role, no membership edge touching any HoopDb role,
`hoop` is not a member of `rds_superuser` and has no CREATEROLE/CREATEDB, and the
provisioning login holds nothing on `hoopdb` (it cannot even CONNECT).

## Inputs

Non-secret inputs: a flag or an environment variable. The flag wins only when the
environment variable is unset. When both are set and differ, the input is refused.

| Flag | Env | Required | Meaning |
|---|---|---|---|
| `--host` | `HOOPDB_ACTIVATION_HOST` | yes (not `artifact`) | RDS writer endpoint |
| `--port` | `HOOPDB_ACTIVATION_PORT` | no (5432) | |
| `--provisioner` | `HOOPDB_ACTIVATION_PROVISIONER` | `create-database`, `activate`, `run` | Name of the provisioning login (the RDS master). Must equal `session_user`. |
| `--maintenance-db` | `HOOPDB_ACTIVATION_MAINTENANCE_DB` | no (`postgres`) | Database for step 1 |
| `--tls` | `HOOPDB_ACTIVATION_TLS` | no (`verify-full`) | `verify-full` or `disable`. `disable` is refused unless the host is a loopback address (emulation only). |
| `--ca-file` | `HOOPDB_ACTIVATION_CA_FILE` | with `verify-full` | Path to the pinned RDS CA bundle (public data, not a credential). Chain and hostname are both verified. |
| `--secrets-stdin` | none | no | Read secrets from stdin (below) instead of the environment |
| `--result-file` | `HOOPDB_ACTIVATION_RESULT_FILE` | one of the two | Path for the JSON result (created with mode 0600; refused if it exists) |
| `--result-fd` | `HOOPDB_ACTIVATION_RESULT_FD` | one of the two | Already-open file descriptor (3 or above) for the JSON result |

Secrets come only from the environment or stdin at runtime, never from argv, a file or a
connection URL:

| Env | stdin JSON key | Needed by |
|---|---|---|
| `HOOPDB_ACTIVATION_PROVISIONER_PASSWORD` | `provisioner_password` | `create-database`, `activate`, `run` |
| `HOOPDB_ACTIVATION_HOOP_PASSWORD` | `hoop_password` | `activate` (fresh), `verify`, `run` |
| `HOOPDB_ACTIVATION_RUNTIME_PASSWORD` | `runtime_password` | `activate` (fresh), `verify`, `run` |

With `--secrets-stdin`, stdin must be one JSON object with only these keys, and none of
the three variables may be set. The two role passwords must be distinct and at least 32
characters long, and neither may equal the provisioner password. The driver sends only
a client-computed SCRAM-SHA-256 verifier to the server, never the plaintext. Nothing is
written to disk except the result, which never contains a secret or a verifier.

## Outputs

stdout and stderr carry human-readable progress only. They are **not** an interface and
must never be parsed. The machine-readable result is one JSON document written once, at
exit, to `--result-file` or `--result-fd`, and nowhere else.

Exit codes:

| Code | `status` | Meaning |
|---|---|---|
| 0 | `activated`, `already_activated`, `verified`, `database_ready`, `artifact` | success |
| 2 | `invalid_input` | bad flags, environment or secrets; no connection was made |
| 3 | `refused` | a precondition failed; nothing was changed (step 1 may report `database.created=false`) |
| 4 | `rolled_back` | the `activate` transaction failed and was rolled back; an empty `hoopdb` from step 1 may remain, and re-running is safe |
| 5 | `verify_failed` | post-commit verification failed; `activate` did commit |
| 1 | `internal_error` | anything else |

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
  "steps": [{ "name": "create-database", "status": "ok", "sqlstate": null, "message": null }],
  "database": { "name": "hoopdb", "created": true, "owner": "hoop" },
  "graph": { "edges": [], "helper_present": false, "roles": [] }
}
```

`steps[].message` is a server or driver message with any secret material removed. The
result never contains passwords, SCRAM verifiers or connection strings.

## Artifact identity

`artifact_sha256` is the SHA-256 of the UTF-8 JSON array `artifact_files`: the ordered
list of `[repo-relative path, sha256(file bytes)]` for every SQL fragment in execution
order, followed by the driver `src/hoopdb/rds-activation.ts`. Any byte change to a
fragment or the driver changes the identity. `bun src/hoopdb/rds-activation.ts artifact
--result-fd 3` prints it without connecting. `migration_generation` is reported for
pairing with the migrator, but it is not part of the artifact hash.

## Migrator contract after activation

Run the phase-1 bootstrap chain and then the migrator as `hoop`, with:

```
HOOPDB_DB_MODE=rds
HOOPDB_RDS_LAYOUT=separate-database
HOOPDB_RUNTIME_ROLE=sessions_pipeline_runtime
HOOPDB_RUNTIME_ROLE_LOGIN=true
HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke
```

`HOOPDB_RDS_LAYOUT=separate-database` selects the separate-database owner preflight.
It requires `hoop` to be a non-superuser with no CREATEROLE, CREATEDB, REPLICATION or
BYPASSRLS, to be a member of no role (so not `rds_superuser`), and to have no members.
`hoop` must own the current database and `sessions`. The fixed `rdsadmin` SUPERUSER and
a NOLOGIN, non-superuser `rds_superuser` must exist, and the activation executor must be
gone. Unset, or `master-owner`, keeps today's rds contract unchanged.
