# Non-superuser consumer owner: credential-presence prerequisite

PostgreSQL's `pg_roles.rolpassword` is the literal mask `********` even when
the actual password is NULL. It cannot establish that a bare consumer has no
credential. The consumer bootstrap reads `pg_authid` where already authorized;
otherwise it requires the exact operator-installed probe described here. It
never treats a masked or unavailable value as absence.

This prerequisite is separate from the migration-history adoption and the
temporary CREATEROLE authority needed by the official consumer owner bootstrap.
It does not adopt ledger rows, grant catalog SELECT, reset a password, change
schema ownership, provision a runtime credential or enable a service.

## Explicit operator ceremony

Use the selected database and the unchanged, common owner of both `sessions`
and `search_index`. That non-superuser owner needs CREATEROLE and the exact
ADMIN-only creator edge to the bare `search_consumer_runtime` role. Provision
the bare role with `session_consumer_roles.sql` as that owner. If necessary,
that owner may create an empty `search_index` schema before the operator step;
do not transfer ownership or change an existing schema to make admission pass.

A separate PostgreSQL superuser operator then runs, once:

```sh
psql -X -v ON_ERROR_STOP=1 -f supabase/bootstrap/session_consumer_credential_probe.sql
```

Connection selection and credentials come from the operator's existing secret
store; no URL or password belongs in this command, source or evidence. The
installer requires a direct superuser session, refuses an existing private
schema, and creates no role. It installs one no-argument SECURITY DEFINER
function in a superuser-owned private schema. Only the common schema owner
receives schema USAGE and function EXECUTE. The function returns one boolean
about the fixed consumer role; it never returns a password or hash. It checks
the genuine session login, no SET ROLE, both schema owners, CREATEROLE and the
creator ADMIN edge. It refuses a missing or already-LOGIN consumer.

Run the official `SERVICE=session-pipeline-consumer-bootstrap` as the owner.
Before using the helper, it pins the body byte for byte, definer and private
schema ownership, argument/result/language/volatility properties, search path,
and narrow ACLs. Missing or modified code, extra grants and an unknown or
pre-existing credential refuse before promotion. An existing LOGIN must
already have the exact global `statement_timeout=60s` operator claim, with no
other settings. Successful claimed-role reruns need no credential probe.

After successful bootstrap/verification, the operator may provision the
dedicated runtime password through the existing supported secret ceremony and
remove temporary CREATEROLE from the owner. Normal migrations accept the
claimed role without CREATEROLE; rerunning the owner bootstrap still requires
its documented owner authority. Do not reset or erase an existing credential
to get past a refusal. Stop and investigate the unexpected role state.

## Scope and proof

The disposable PostgreSQL 17 test exercises the real non-superuser owner, full
consumer bootstrap and idempotent rerun. It causally refuses missing or changed
helpers, broadened ACLs, a wrong caller, SET ROLE impersonation, a preseeded
password, validity and an unclaimed LOGIN. Catalog SELECT remains denied.
Tests use an isolated synthetic cluster and never apply to an installed stack.

This installer intentionally requires a real PostgreSQL superuser. RDS,
Cloud SQL, Supabase and other constrained operator models are **unproven and
unsupported by this helper** until their source owner supplies and qualifies
an equally narrow supported mechanism. A provider's superuser-like role name
does not satisfy `rolsuper`. No provider privilege bypass or blanket catalog
grant is an alternative.
