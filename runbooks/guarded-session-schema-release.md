# Guarded production session-schema release (draft)

This is a manual GitHub Actions route, not an activation or a deployment. It uses the authoritative pipeline migrator. It never invokes Supabase CLI migration apply, a role/bootstrap script, Alchemy, or a Worker deployment.

## Prerequisites and current blockers

An existing GitHub administrator must create **session-schema-production** before this workflow can run. Configure directly named human User reviewers (Khaliq: User ID 1724137), and one custom deployment branch policy: type branch, name main. Team-only reviewers are deliberately insufficient for the current approval verifier. Mirror the existing approved owner URL into the environment secret **SUPABASE_MIGRATION_OWNER_URL** using an approved RAM/stdin path. Do not put credentials in a repository, file, artifact, command argument, GITHUB_ENV, or log. The workflow does not create the environment or configure any secret. Its GitHub token only reads Actions, deployments and repository contents; any inaccessible verification API fails closed.

The environment gate is checked before its name is emitted as the dependent job's environment output. The protected job then requires an actual approved review-history entry for that environment ID by a presently configured human reviewer. Admin bypass without approval, bot approval, missing environment, wrong branch policy, and workflow reruns all refuse. A rerun requires a new manual dispatch and approval.

The only accepted connection is the pinned Supabase pooler **aws-0-us-east-1.pooler.supabase.com:5432/postgres**, project **pyamkrhoyszgzirqqffq**, connection username **relayhistory_owner.pyamkrhoyszgzirqqffq**, with actual session_user/current_user **relayhistory_owner**. The client uses the committed Supabase Root 2021 CA with certificate and hostname verification. URL SSL parameters cannot replace this TLS configuration. The owner must be LOGIN and must have no SUPERUSER, CREATEROLE, CREATEDB, BYPASSRLS or REPLICATION. There is no owner/admin fallback.

As of the operator's 2026-11-04 03:51 UTC readonly inventory, the existing owner is NOSUPERUSER/NOCREATEROLE and cannot bootstrap missing roles; ingest/seeder roles and ingest ledger/auth tables were absent. A separate reviewed administrator bootstrap path is required before schema apply: search-worker role, token-verifier role, writer roles when their migrations are included, their dedicated login/JWT bindings, and the selected catalog-proven function boundary. Use the exact reviewed bootstrap runbooks/scripts appropriate to the installed PG version and Supabase creator edge. This CI must never run those scripts or try a broader credential. The read-only plan checks bare search/token role existence/attributes; authoritative migration and phase-2 SQL still enforce complete memberships, settings and effective privileges. Passing a plan is not a phase-2 verdict.

Review the exact migration inventory on the eventual merged main SHA. The base used to author this route (1247da78064c41879327196e05bdf4ca6d754c63) includes auth173 but does not itself supply the pending writer-ledger schema. Merge/re-CI prerequisite source changes in dependency order; this route must not manufacture a successful ingest cutover from an incomplete migration set.

Separately provision/review the private ingest Worker, ingestion Hyperdrive/login, signing/forwarding bindings and token secrets, namespace permissions and operator-controlled canary. The 03:46 UTC inventory reported no ingest Worker, only the old read-role Hyperdrive, and no RHC forwarding bindings. These are upload-release dependencies owned by the operator, not actions performed by this workflow. Full CH/read backfill is a separate lane.

## Plan, review, then apply

1. Merge the reviewed release route and required schema source onto main through the authorized integration process. Wait for the **current main SHA's push** ci and pg-proof workflows to complete successfully. Every job must succeed, including checks, session-read-worker / worker, and real PostgreSQL 15/16/17. Failed, skipped, incomplete, missing, truncated or different-SHA evidence refuses.
2. Compute the migration generation from that exact checkout: `bun src/hoopdb/migrate.ts --generation`. The workflow recomputes it before requesting protected approval and before connecting.
3. Dispatch **guarded session schema release** on main with mode=plan, exact sha, exact generation, and the already installed function-boundary choice (event-trigger or global-default-revoke). Leave plan_run and approval empty. Review/approve the protected environment job.
4. Download **schema-release-plan** from the successful plan run. Review project, source SHA, generation, boundary, ledger digest, each migration name/SHA/pending bit, and the exact ACL preview. The database work runs in REPEATABLE READ READ ONLY, with the existing advisory lock and shared migration preflight. No bootstrap, migration, grant or marker SQL runs. The artifact contains names, hashes and ACL evidence only, no credential or source content.
5. After reviewing that concrete plan, dispatch mode=apply on the **same still-current main SHA** within 24 hours, supplying the plan run ID and exactly:
   `APPLY pyamkrhoyszgzirqqffq <sha> <generation> PLAN <plan_run>`.
   Approve this new protected environment job. The prior plan must be a successful first-attempt manual run of this same workflow/SHA; its immutable artifact must bind project/generation/boundary/run. The typed string alone never authorizes an unreviewed or foreign plan.
6. Apply repeats the read-only inspection, then takes the migrator's session advisory lock and repeats its normal authority/role/sequence preflight. It compares the current ledger digest to the plan **under that lock**, rechecks live GitHub source/CI/human approval immediately before the first write, then calls the existing runMigrations with Supabase mode, shim=false, bootstrap=false, verify=true and bulls96_runtime LOGIN=true. The existing phase-2 role verifiers and exact ACL guards must all finish before the existing PASS marker. A final SELECT requires that PASS marker for the exact approved generation.

No database URL is exposed to install/check-out/download/upload steps. Only the database invocation receives it. The connection has a ten-second connect bound, five-second lock timeout, five-minute statement timeout and one-minute idle-transaction timeout. Errors print fixed/sanitized codes, never driver message/detail/hint or connection URLs. Plan artifacts expire after one day. Concurrent runs serialize; an apply is not canceled by a second dispatch.

## Failure and rollback

A missing bootstrap, unsafe role, unproven boundary, mixed Supabase CLI history, ledger gap/checksum drift, changed ledger since planning, stale source/CI, or missing human approval stops the route. Do not edit a ledger, suppress a verifier, disable TLS or switch credentials to continue. Obtain reviewed prerequisite work, re-run a new read-only plan, and approve the new exact generation.

Migrations commit individually. A late failure can leave earlier committed steps plus a PENDING readiness marker; it is not an automatic schema rollback. Keep ingestion/activation held, record the failing safe code and exact run, and use the reviewed forward repair/retry procedure. Never auto-drop tables, erase migration history, reset roles or mark PASS manually. This workflow does not flip routing or claim full read/search activation.

## Source validation

Synthetic checks cover exact GitHub main/run/workflow/CI/job provenance, protected branch/reviewer/actual approval, rerun refusal, typed plan bindings, connection/TLS pins, and step-scoped secret wiring. PGlite runs the actual migrator and readonly inspection against a populated ledger; the ledger/marker remain identical. Wrong planned ledger and revoked final release gate refuse before the first persistent write. Reverting those controls must make their corresponding checks fail.

The protected production environment, approval-history API permissions and real pooler connection have not been exercised by an author dispatch. Operator setup plus independent source review, current-head CI and a real **plan-only** run are required before claiming the route executable. No production migration, credential/configuration change or deployment was performed while authoring this draft.

GitHub primary references: [environment configuration](https://docs.github.com/en/rest/deployments/environments), [deployment branch policies](https://docs.github.com/en/rest/deployments/branch-policies), [workflow approval history](https://docs.github.com/en/rest/actions/workflow-runs#get-the-review-history-for-a-workflow-run).


## Operator read-only plan while reviewer billing is blocked

The 2026-11-04 GitHub administrator setup returned HTTP 422: the repository plan
does not support required environment reviewers. An empty unprotected
session-schema-production exists; no secret or dispatch is configured. This is
an explicit blocker for the CI apply route. Do not replace the missing human
gate with a typed string or admin bypass.

For a reviewed SSH session, scripts/schema-release-operator-plan.ts is a separate
PLAN-ONLY entry. Set RELEASE_MODE=plan, RELEASE_SHA to its exact immutable reviewed
checkout, RELEASE_GENERATION to that checkout's generation, and RELEASE_BOUNDARY
to the already verified boundary. Supply SUPABASE_MIGRATION_OWNER_URL only in the
process environment using the approved credential system; leave RELEASE_PLAN_RUN
and RELEASE_APPROVAL unset. Invoke:
  bun scripts/schema-release-operator-plan.ts

It enforces the same connection/owner pins and REPEATABLE READ READ ONLY inspection,
emits a safe JSON plan (or fixed refusal code), and executes no apply branch.
There is no CLI apply option, GitHub approval simulation, credential escalation,
or automatic exception. The operator must separately authorize an exact concrete
manual-apply exception to the standing release contract before any alternative
write route is implemented or used.

Boundary ownership is material: the existing event-trigger wrapper binds its
trigger/helper/schema to its executor. Running it as postgres does NOT establish
an event-trigger boundary for relayhistory_owner; the existing guard will refuse
the owner mismatch. Audit actual schema creators/owners/default privileges before
choosing an operator-reviewed dedicated-owner global-default-revoke plan. Do not
transfer owners or install the postgres wrapper to guess past that check.
