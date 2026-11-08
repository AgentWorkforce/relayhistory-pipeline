# Shared-database isolation for HoopDb (`sessions`) — design

Status: design only. This document changes no SQL, TypeScript, migration, bootstrap
or proof behaviour, and authorizes no deploy, role creation or database change. Every
behaviour change it lists is a separate reviewed PR; any migration it needs takes a new
slot assigned by the migration registry (none is assigned here).

Input: the infra shared-store contract (AgentWorkforce/infra PR80,
`docs/shared-postgres-storage-contract.md`, and the `terraform/modules/postgres-layout`
README). That contract fixes one RDS instance and one application database, with
per-service schemas and per-service migration-owner and runtime roles. It also says the
current HoopDb function-boundary bootstrap "assumes a dedicated database and changes
defaults for multiple owners. Do not run it unchanged against this shared database."
This document explains how to make the HoopDb side shared-database-safe.

Citations are `path:line` on `origin/main` at `fc10bb03`.

## 1. Target and invariants

Target: the HoopDb pipeline state lives in schema `sessions` inside the shared
application database. Relaycontrol lives in `relaycontrol`, and later services
(Relaycast, History, Relayfile) live in their own schemas.

Invariants that must hold and be checked, not assumed:

- I1 `sessions` is owned by the HoopDb migration owner. Every relation, function, type
  and sequence in `sessions` is owned by that role.
- I2 No HoopDb role (owner, runtime or any fixed NOLOGIN role) holds any privilege on
  another service's schema or objects. No other service's role holds any privilege on
  `sessions` or its objects. PUBLIC holds nothing on `sessions` (USAGE, CREATE or
  EXECUTE).
- I3 No object owned by a HoopDb role exists outside `sessions`, except the exceptions
  listed below.
- I4 HoopDb bootstrap and migrations never change database-level ACLs, other roles'
  default privileges, or objects they do not own.
- I5 A foreign object anywhere in the database cannot make HoopDb readiness fail or
  pass. Readiness may fail only on a real exposure of `sessions` or a real excess
  privilege of a HoopDb role.

Objects we own or depend on outside `sessions` today:

| Object | Where | Shared-DB disposition |
|---|---|---|
| Extensions `pg_trgm`, `btree_gin` in `public` | `supabase/migrations/20261030010000_session_directory.sql:9`, `supabase/migrations/20261030020000_session_directory_short_summary.sql:177`; opclass use `public.gin_trgm_ops` at `20261030010000_session_directory.sql:134,138`, `20261030020000_session_directory_short_summary.sql:255,263`, `20261101050000_session_directory_list.sql:246` | Landed bytes are frozen. The platform admin pre-creates both extensions `WITH SCHEMA public`, owned by the admin, under the contract's "administrator-owned" extension namespace. Our `CREATE EXTENSION IF NOT EXISTS` then is a no-op. A proof must show that it is a no-op for a non-owner without CREATE on the database (open question Q4). |
| Database ACL: `REVOKE TEMPORARY ON DATABASE ... FROM PUBLIC`, `GRANT CONNECT ON DATABASE ... TO <runtime>` | `supabase/bootstrap/hoopdb_runtime_role.sql:175-179`, `supabase/bootstrap/hoopdb_runtime_role_rds.sql:134-141` (requires our owner to own the database) | Removed from our bootstrap in shared mode. Database ACLs belong to the platform admin. We only verify them read-only (section 5). |
| Global function defaults for several roles | `supabase/bootstrap/hoopdb_function_default_revoke.sql:94-97` (owner and every superuser), `rds_owner_function_default_revoke.sql:100-103`, `cloudsql_owner_function_default_revoke.sql:82-85` (current_user = master/owner) | Restricted to our owner role only (section 3). |
| Event trigger `hoopdb_sessions_function_boundary` and schema `hoopdb_boundary` | `supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql:466-475`; `src/hoopdb/migrate-guards.ts:1100-1103` | Supabase profile only (`src/hoopdb/migrate.ts:228`). This mode is not allowed in the shared RDS profile. An event trigger is database-wide and would fire for relaycontrol DDL. |
| Schema `history` (History-feature track) | `supabase/history-migrations/0001_history_schema.sql:6`; `src/history-db/migrate.ts:21-23` | A second schema we own. This conflicts with the contract's History service (`relayhistory` / `history_owner`). Open question Q5. Until it is resolved, it is excluded from the shared install. |
| Cluster-global role names | section 2 | Roles are cluster-wide, not per schema. See the collision analysis below. |

No publications exist (`CREATE PUBLICATION` appears nowhere). No `ALTER DATABASE` exists.
Function `search_path` is pinned to `pg_catalog, pg_temp` or
`pg_catalog, sessions, pg_temp` across the migrations. History pins
`pg_catalog, history` (`supabase/history-migrations/0002_probe_connections.sql:20,30,39`).
That is consistent with the contract rule that search_path is not authorization.

## 2. Roles

Names fixed by the postgres-layout contract for this service: schema `sessions`,
migration owner `hoop`, runtime role `sessions_pipeline_runtime`. The contract assigns
History to `relayhistory` / `history_owner` / `history_runtime` and Control to
`relaycontrol` / `control_owner` / `control_api_runtime`. `hoop` already is our
documented migration owner (`runbooks/session-directory-search-worker-bootstrap.md:7,469`).
The runtime role is configurable (`HOOPDB_RUNTIME_ROLE`, runbook line 181). Today's
examples use `bulls96_runtime` (`supabase/bootstrap/hoopdb_runtime_role_rds.sql:34`).
For shared mode it must be set to `sessions_pipeline_runtime`.

Roles this repo creates today (fixed NOLOGIN set):
`supabase/bootstrap/rds_migration_roles.sql:122-174` and
`cloudsql_migration_roles.sql:100-152` create `anon`, `authenticated`, `service_role`,
`session_directory_search_worker`, `session_token_verifier`, `bulls96_session_ingest`,
`bulls96_session_ingest_seeder`, `bulls96_canonical_feature_producer`, `bulls96_read_api`,
`bulls96_project_events`, `bulls96_privacy_eraser` and `bulls96_babysitter_retention`.
They also create a disposable trampoline (`bulls96_rds_role_seed` / `bulls96_cloudsql_role_seed`).
The runtime LOGIN role is created in `hoopdb_runtime_role{,_rds}.sql:86`.

Collision risk: `anon`, `authenticated` and `service_role` are generic Supabase names that
another Supabase-derived service, possibly relaycontrol, may also create or expect.
Because roles are cluster-global, a shared `authenticated` would let one service's grants
reach the other (`src/hoopdb/sessions-function-acl.json` grants `authenticated` 6 and
`service_role` 54 EXECUTEs). Rule: in shared mode these three are not created by us. Either
(a) a later forward migration (new slot) replaces them with `bulls96_`-prefixed roles, or
(b) the platform asserts that no other service uses those names. (a) is preferred. The
`bulls96_` and `session_*` names are low-risk, but readiness must still prove them
membership-free and unused by foreign schemas (section 5).

Creation rule: the platform (shared admin identity, Q1) creates `hoop` and
`sessions_pipeline_runtime`, then `CREATE SCHEMA sessions AUTHORIZATION hoop`. Our
bootstrap creates only roles nested under our service. Each of those roles is
membership-free, holds no privilege outside `sessions`, and is granted only by `hoop`. The
platform grants `hoop` exactly the CREATEROLE (or equivalent) needed for that. `hoop` is
**not** a member of `rds_superuser` and is not the RDS master. That reverses today's RDS
contract (runbook lines 717-720; `src/hoopdb/migrate.ts:805-845` requires `rds_superuser`
membership and refuses otherwise).

Who runs each step (shared mode):

| Step | Identity |
|---|---|
| Create `hoop`, `sessions_pipeline_runtime`, schema `sessions`, extensions, database CONNECT/TEMPORARY ACL | platform admin (Q1) |
| Owner function default (section 3) | `hoop` |
| Fixed NOLOGIN roles (trampoline pattern) | `hoop` (needs CREATEROLE, Q2) |
| Phase 1 verifiers | `hoop` (read-only) |
| Migrator, ACL manifest application | `hoop` |
| Runtime readiness | `sessions_pipeline_runtime` |
| Password set/rotate | approved operator path, from 1Password, never on disk |

## 3. Default privileges

PostgreSQL constraint: a per-schema `ALTER DEFAULT PRIVILEGES ... IN SCHEMA` can only
*add* to the global default. It cannot revoke the built-in PUBLIC EXECUTE on new
functions. That is already documented at `supabase/bootstrap/hoopdb_function_default_revoke.sql:4-5`
and `src/hoopdb/migrate-guards.ts:795`. So the brief's literal target,
"`ALTER DEFAULT PRIVILEGES FOR ROLE <owner> IN SCHEMA sessions REVOKE EXECUTE ... FROM PUBLIC`",
would be a silent no-op and would leave every new function PUBLIC-executable. The safe form
is scoped **per owner, not per schema**. It becomes effectively per-schema through
invariant D3 below.

Before (database-wide, multiple owners):

```
-- selfhosted, as superuser: for hoop AND every superuser (hoopdb_function_default_revoke.sql:95-97)
ALTER DEFAULT PRIVILEGES FOR ROLE hoop     REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;  -- every rolsuper
-- rds, as the RDS master (rds_owner_function_default_revoke.sql:100-103)
ALTER DEFAULT PRIVILEGES FOR ROLE <rds master> REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
-- cloudsql, as the owner (cloudsql_owner_function_default_revoke.sql:82-85): same shape
```

The superuser and master lines change the defaults of identities that relaycontrol (or the
admin) may use to create objects. That is the contract's "changes defaults for multiple
owners".

After (shared mode, new bootstrap file, run as `hoop`):

```
ALTER DEFAULT PRIVILEGES FOR ROLE hoop REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
-- nothing else; no IN SCHEMA GRANT of any kind (grants come only from the ACL manifest)
```

Supporting invariants, verified before and after, fail closed, nothing changed on refusal:

- D1 The caller is `hoop`, is not SUPERUSER, and is not a member of `rds_superuser`.
  The statement names only `current_user`.
- D2 `pg_default_acl` rows with `defaclrole = hoop` are exactly `{hoop:EXECUTE}` for
  object type `f` with namespace 0, and there are no others.
- D3 `hoop` has CREATE on `sessions` and on no other schema
  (`has_schema_privilege(hoop, n.oid, 'CREATE')` across all non-system namespaces), and it
  does not have CREATE on the database. So every function `hoop` can create lands in
  `sessions`. That makes the owner-global default per-schema in effect.
- D4 No other role can CREATE in `sessions`. That reuses the existing check at
  `hoopdb_function_default_revoke.sql:64-76` and drops the superuser branch.
- D5 There are no `pg_default_acl` rows with `defaclnamespace = sessions` that grant
  anyone but the row's own role. That is the existing check at
  `hoopdb_function_default_revoke.sql:77-88` and `src/hoopdb/migrate-guards.ts:1433-1442`.

Proof that relaycontrol's defaults are untouched: the new file issues one statement whose
`FOR ROLE` is `current_user`, and D1 proves that this is `hoop`. A pg-proof case must
snapshot all `pg_default_acl` rows where `defaclrole <> hoop` before and after, and require
byte equality. It must include a seeded `control_owner` with a deliberately PUBLIC-granting
global default, which must survive unchanged. The per-run `REVOKE EXECUTE ... FROM PUBLIC`
on existing `sessions` functions in the migrator, and the live gate
(`src/shared/db.ts:136-150`, namespace-filtered at line 139), close any residual window.

## 4. Migrator

- **Schema-qualified only.** Every created object is `sessions.`-qualified. Index,
  trigger and policy names are syntactically unqualified, but they are bound to a
  qualified table. All 51 sentinel probes are qualified (`src/hoopdb/migration-sentinels.ts:18-23`),
  and all 305 ACL-manifest keys are `sessions.`-qualified. The ledger and readiness tables
  are created in `sessions` (`src/hoopdb/migrate.ts:489-500`). `CREATE SCHEMA IF NOT EXISTS sessions`
  (`migrate.ts:489`) becomes a no-op once the platform has created the schema. In shared mode
  it must be preceded by an ownership assertion (schema exists and `nspowner = hoop`)
  rather than creating it.
- **Footprint checks scoped to `sessions`.** Findings:
  - `src/hoopdb/migrate-guards.ts:945` checks the existence of an unqualified index by
    `pg_class.relname` across **all** schemas. A relaycontrol index with the same name (for
    example `si_session`) would count as ours. That poisons the unledgered-footprint
    check (violates I5). Fix: join `pg_namespace` and filter `nspname = 'sessions'`.
  - `migrate-guards.ts:1380`, when `sessions` is absent, enumerates `rolsuper OR current_user`.
    In shared mode, superusers are platform identities and not our creators. Fix: when
    `sessions` is absent in shared mode, refuse (`sessions_schema_missing`). The platform
    creates the schema.
  - `migrate-guards.ts:1444-1448` lists all non-HoopDb event triggers in the database. If
    they are used as a refusal input in shared mode, relaycontrol's event triggers would
    poison our gate. Fix: report them only, and refuse only triggers owned by a HoopDb role.
  - `migrate-guards.ts:629,641` (runtime CREATE/USAGE allowlist over all non-system
    schemas) is correct as is. A runtime role with USAGE on `relaycontrol` is a real
    I2 breach and must fail closed.
  - Lines 558-564, 800, 1360, 1433-1440 and 1758-1759 are already namespace-filtered or
    role-filtered.
- **No manifest-union bypass.** Today the single source of truth for function ACLs is
  `src/hoopdb/sessions-function-acl.json`, loaded at `migrate-guards.ts:1611-1620`. It is
  composed from lanes with exactly one owner per function
  (`scripts/sessions-function-boundary/generate.ts:27-49`). The set union at
  `migrate-guards.ts:1701` only enumerates manifest and actual keys for comparison. Every
  actual entry not in the manifest is reported as `extra`. *Bypass*, as named in the
  contract ("never ... union foreign functions into an allowlist"), means any of these:
  (a) widening the ACL query beyond `nspname = 'sessions'`; (b) building an expected set
  from observed catalog state, such as foreign schemas or functions created outside a
  migration; (c) adding foreign or non-`sessions.` keys to the manifest or the verifier
  allowlists (`scripts/sessions-function-boundary/verifiers.ts:9-30`); or (d) downgrading
  `extra`/`missing_refused` to warnings in shared mode. Guards to add: the manifest loader
  refuses any key that does not start with `sessions.`; a unit test asserts that the
  manifest key set equals the lane-composed set; and a pg-proof case seeds a `relaycontrol`
  function with PUBLIC EXECUTE and requires our gate to be unchanged. Our ACL comparison
  must not see that function. A seeded `sessions.` function that is not in the manifest
  must still refuse.
- **Frozen bytes.** Landed migration files and their checksums
  (`migrate-guards.ts:44-60`) do not change. Any shared-mode change to objects is a new
  migration in a registry-assigned slot. Schema names are never renamed in place, as the
  contract also requires.

## 5. Readiness

The readiness check (`schemaVerdict`, `src/shared/db.ts:192-243`) today runs the marker read, the four
phase-2 verifier SQLs (`db.ts:218-222`) and the live function gate. Shared-mode checks,
all read-only:

1. `sessions.nspowner = hoop`; schema ACL is exactly the manifest (no PUBLIC, no foreign role).
2. No object in `sessions` owned by a non-HoopDb role (`pg_class`, `pg_proc`, `pg_type`
   filtered on the `sessions` namespace).
3. No object owned by `hoop` or by any fixed HoopDb role outside `sessions`
   (`pg_shdepend` owner deps for those roles in the current DB). Allowed exceptions: none
   while the History track is excluded (Q5).
4. Default privileges shape: D2 and D5 from section 3. Rows for other `defaclrole`s are
   ignored and never compared.
5. Role membership: `sessions_pipeline_runtime` is a member of no role, has exactly the
   one permitted creator edge, and `hoop` is not a member of the runtime role (contract:
   "no migration-owner membership in runtime roles"). Each fixed role's membership must be
   exactly the existing per-role spec. "Exactly one armed" means: exactly one runtime role
   is LOGIN-armed for this deployment, matching `HOOPDB_RUNTIME_ROLE`.
6. Cross-service denial is a proof, not a readiness check. Readiness runs as runtime and
   cannot log in as relaycontrol's roles.

Fifth verifier asset: **yes, recommended.** Items 1-4 are owner/database-shape checks
that the four role verifiers (`searchWorker`, `tokenVerifier`, `ingestSeeder`, `ingest`;
`src/shared/db.ts:119`) do not cover. This must be a single coordinated change, not
implemented here:
- new `supabase/bootstrap/sessions_shared_isolation_verify.sql` plus its `*_SQL_PATH` export in `src/shared/db.ts:113-118`;
- `SchemaVerifierSql` gains a fifth key (`db.ts:119`) and `schemaVerdict` runs it (`db.ts:218-222`);
- `src/session-pipeline/derived-schema.ts:5-17` reads it; the Lambda embed plugin
  (`scripts/build-lambda-artifact.ts:69-70`, `stacks/aws/derived-schema-build.ts`) and the
  buffered-GC build (`scripts/build-buffered-gc.ts:9-10`) recompile it;
- both count guards change 4 to 5 together: `src/session-ingest/publication-runtime.ts:58`
  and `src/session-ingest/buffered-gc-job.ts:77`;
- test doubles with four-key `verifiers` objects are updated
  (`src/session-ingest/__tests__/buffered-gc-job.test.ts:53,61`, `publication-runtime.test.ts:35`, the
  postgres tests that `Object.values(assets.verifiers)`).
The verifier must branch on the bound `hoopdb.db_mode` so that non-shared modes keep
today's semantics. Otherwise it would be a readiness regression for selfhosted, cloudsql
and supabase.

## 6. Managed-RDS denial and restore

Today: the RDS profile assumes that our migration owner *is* the master. It is a
CREATEROLE `rds_superuser` member that owns the database (runbook lines 717-720,
`src/hoopdb/migrate.ts:814-844`). `rdsadmin` is excluded from creator coverage as the
provider trust root (`src/hoopdb/migrate-guards.ts:1244-1246`). The emulation
(`scripts/pg-proof/rds-profile-proof.ts:1-15`) creates a master-owned database, and it
checks that `ALTER DEFAULT PRIVILEGES FOR ROLE rdsadmin` is denied (line 199). No proof
covers a second service in the same database or snapshot/PITR restore. "Restore" in that
proof (lines 361-423) means re-running the bootstrap.

Changes:
- A new `HOOPDB_DB_MODE` value (proposed `rds-shared`, default off) with its own preflight.
  Owner is `hoop`: not master, not `rds_superuser`, not a superuser, does not own the
  database, owns `sessions`. `rdsadmin` stays the excluded trust root. The master is
  treated like any other foreign role and must hold no grant on `sessions` objects.
  (Whether `rds_superuser` members hold predefined roles such as `pg_read_all_data`
  on the target engine version is unverified here; Q6.)
- `rds_migration_roles.sql` and `hoopdb_runtime_role_rds.sql` get shared-mode
  counterparts. These run as `hoop`, contain no database ACL statements, and do not create
  `anon`, `authenticated` or `service_role` (section 2).
- New proof `scripts/pg-proof/shared-db-proof.ts` (CI matrix PG16 and PG17, bootstrap
  superuser `rdsadmin`). It seeds `relaycontrol` with `control_owner` and
  `control_api_runtime`. With real non-owner LOGIN sessions, each side must fail SELECT,
  INSERT, DDL and EXECUTE on the other side. It also covers the foreign-object poisoning
  cases from section 4, the default-privilege snapshot from section 3, and an
  install-both / upgrade-one / restart sequence. The restore case runs `pg_dump`/`pg_restore`
  into a fresh database as the emulated stand-in, then repeats the denial matrix. A real
  RDS snapshot/PITR restore proof remains a live-environment task under Khaliq's explicit
  OK, as the contract requires.

## 7. Cutover ordering and change inventory

Order (each step is its own PR; nothing touches a real database without separate approval):
1. Docs (this PR). 2. Guard fixes in section 4. These are safe in every mode, because
the namespace filters only narrow. 3. Shared-mode bootstrap files plus the `rds-shared`
preflight, default off. 4. Fifth verifier asset (one coordinated change). 5. Forward
migration that replaces `anon`, `authenticated` and `service_role` grants, if option (a)
is chosen. 6. `shared-db-proof` in CI. 7. Runbook section. 8. Live install, restore and
denial proof under explicit approval. Existing dedicated-database installs keep their
current mode. Moving one into the shared database is a data migration with its own
reviewed cutover, not a mode flip.

| File | Change | Risk | Registry slot |
|---|---|---|---|
| `src/hoopdb/migrate-guards.ts:945` | index existence filtered to `sessions` | low (narrows) | none |
| `src/hoopdb/migrate-guards.ts:1380` | shared mode: refuse when `sessions` is absent instead of enumerating superusers | low | none |
| `src/hoopdb/migrate-guards.ts:1444-1448` | foreign event triggers reported only; refuse HoopDb-owned ones | medium (gate semantics) | none |
| `src/hoopdb/migrate-guards.ts:1611-1620` | manifest loader refuses non-`sessions.` keys | low | none |
| `src/hoopdb/migrate.ts:228-237,805-845` | new `rds-shared` mode and preflight (owner `hoop`, not `rds_superuser`) | high (trust root) | none |
| `src/hoopdb/migrate.ts:489` | shared mode: assert `sessions` exists and is owned by `hoop`; never create it | low | none |
| new `supabase/bootstrap/shared_owner_function_default_revoke.sql` | `FOR ROLE hoop` only, D1-D5 | medium | none (bootstrap) |
| new `supabase/bootstrap/shared_migration_roles.sql` | fixed roles without generic names; no DB ACL | medium | none (bootstrap) |
| new `supabase/bootstrap/hoopdb_runtime_role_shared.sql` | runtime role verify/create; no `ON DATABASE` statements | medium | none (bootstrap) |
| `supabase/bootstrap/hoopdb_function_default_revoke.sql`, `rds_*`, `cloudsql_*`, `hoopdb_runtime_role*.sql` | unchanged; header note "not for shared DB" in the bootstrap PR | low | none |
| `supabase/migrations/*` (landed) | unchanged (frozen) | none | none |
| forward migration: `anon`/`authenticated`/`service_role` replaced by prefixed roles | regrants plus manifest/lane update | high (ACL churn) | new slot, registry-assigned; none in this PR |
| new `supabase/bootstrap/sessions_shared_isolation_verify.sql` plus `src/shared/db.ts`, `derived-schema.ts`, build plugins, `publication-runtime.ts:58`, `buffered-gc-job.ts:77`, tests | fifth verifier asset, count 4 to 5 together | medium (compiled assets) | none |
| new `scripts/pg-proof/shared-db-proof.ts` plus `.github/workflows/pg-proof.yml` job | two-service denial, poisoning, restore | low | none |
| `runbooks/session-directory-search-worker-bootstrap.md` | "Shared RDS database" section | low | none |
| `supabase/history-migrations/*`, `src/history-db/migrate.ts` | excluded from shared install pending Q5 | n/a | none |
| `scripts/private-state-bootstrap.ts`, `docs/private-state-bootstrap.md` | none. This is the Alchemy/Cloudflare control-state store, not Postgres | none | none |

## 8. Open questions for infra

- Q1 Which named identity runs the platform bootstrap (role and schema creation,
  extensions, database ACL)? The RDS master, or a dedicated admin role? HoopDb will not use
  the master for anything.
- Q2 Will `hoop` get CREATEROLE so it can create its nested fixed roles (PG16+ ADMIN
  semantics)? Or will the platform create those roles from a list we publish?
- Q3 Does the database name stay `relaycontrol` for existing installs (the contract's
  default)? Our code uses `current_database()` everywhere, so either is fine, but runbooks
  must say so.
- Q4 Is there a shared extension schema, or `public` owned by the admin? Is `public`
  CREATE revoked from PUBLIC (PG15+ default)? Our landed migrations reference
  `public.gin_trgm_ops`.
- Q5 Should the `history` schema track belong to the contract's History service
  (`relayhistory`, `history_owner`)? Or does it stay a HoopDb-owned second schema? (The
  brief text said `sessions` is owned by "the History owner role"; the contract assigns
  `sessions` to `hoop`. This design follows the contract.)
- Q6 Search_path policy: do roles get `ALTER ROLE ... SET search_path` pinned per service?
  Do we treat `rds_superuser` as a trust root equal to `rdsadmin` for denial purposes?
- Q7 Does relaycontrol create or depend on `anon`, `authenticated` or `service_role`?
