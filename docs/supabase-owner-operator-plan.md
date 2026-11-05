# Read-only existing-owner operator plan

This entry uses the operator's existing Supabase CLI2.122.0 login on sf-frame to obtain its normal temporary cli_login_postgres connection in memory. It parses the CLI dry-run output as data and never evaluates or prints it. The script permits only project pyamkrhoyszgzirqqffq, the expected CLI principal and postgres database, verified TLS over the session pooler, and one READ ONLY transaction with SET LOCAL ROLE postgres. Existing sessions schema ownership must already be postgres. It never changes ownership, defaults, passwords, roles, migration history, schema or app routing.

The managed CLI login may initialize or renew Supabase's standard temporary login role as part of authentication. It does not rotate the postgres password or create a bespoke privileged fallback role. Source: https://supabase.com/docs/guides/local-development/local-and-hosted-projects . Credentials remain in subprocess memory; no secret file, shell evaluation or argument contains them.

Run from the exact clean reviewed checkout on sf-frame:

```
OPERATOR_MODE=plan OPERATOR_SOURCE_SHA=<exact40hexHEAD> OPERATOR_GENERATION=<exact64hexsourcegeneration> bun scripts/schema-release-owner-operator-plan.ts
```

The invocation accepts no extra argument and has no apply branch. It calls the canonical migrator's shared inspectMigrations under its advisory transaction lock; this is not a new migration authority. The output is safe migration inventory, hashes, ACL preview and whether the installed event-trigger body matches this source. A successful read-only plan is not apply approval: final combined upload source, installed trigger refresh, role/erasure/replay gates and any manual deployment exception remain separate requirements. In this live project the stale postgres-owned trigger must be refreshed from the final combined source before new grants/functions are applied; do not change ownership or pretend the global-default gate proves that body current.

Validation:17 focused input/credential boundary controls and root typecheck. Removing the target pin makes4 controls fail; removing plan-only mode/argv enforcement makes3 controls fail. An actual prior same-client read-only Supabase connection verified effective_owner postgres/login_role cli_login_postgres/read_only on and the existing global-default-revoke boundary true, with no credential output. Final script's immutable real read-only run and independent review remain to be recorded.
