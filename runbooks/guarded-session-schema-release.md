# Guarded production session-schema release (draft)

This is a manual GitHub Actions route, not an activation or a deployment. It uses the authoritative pipeline migrator. It never invokes Supabase CLI migration apply, a role/bootstrap script, Alchemy, or a Worker deployment. Apart from the migrator's own writes, its only SQL write is the CI event-trigger function refresh described under "Function boundary", built from the committed reviewed function body.

## Prerequisites and current blockers

An existing GitHub administrator must create **session-schema-production** before this workflow can run. Configure exactly one custom deployment branch policy (type branch, name main) with protected-branches deployment disabled. Where the repository plan supports required reviewers, configure directly named human User reviewers (Khaliq: User ID 1724137); Team-only reviewers are deliberately insufficient for the current approval verifier and refuse. Mirror the approved **postgres** owner session-pooler URL (connection username postgres.<supabase-project>, see the connection pins below) into the environment secret **SUPABASE_MIGRATION_OWNER_URL** using an approved RAM/stdin path; doing so is a credential change that needs Khaliq's separate explicit OK. Do not put credentials in a repository, file, artifact, command argument, GITHUB_ENV, or log. The workflow does not create the environment or configure any secret. Its GitHub token only reads Actions, deployments and repository contents; any inaccessible verification API fails closed.

The environment gate is checked before its name is emitted as the dependent job's environment output. The protected job then rechecks the gate and its human authority. When the environment has a required-reviewers rule, it requires an actual approved review-history entry for that environment ID by a presently configured human User reviewer, and any rejection recorded for that environment refuses; a reviewer rule never falls back to dispatcher identity. When the repository plan cannot create a reviewer rule (no required_reviewers protection rule exists), the human authority is the dispatch itself: both the run's actor and its triggering actor must be the pinned human User account (Khaliq, User ID 1724137). Admin bypass without approval, bot approval or bot dispatch, a foreign approver or dispatcher, missing environment or protection metadata, wrong branch policy, and workflow reruns all refuse. A rerun requires a new manual dispatch (and approval, where a reviewer rule exists).

The only accepted connection is the pinned Supabase pooler **aws-0-us-east-1.pooler.supabase.com:5432/postgres**, project **<supabase-project>**, connection username **postgres.<supabase-project>**, with actual session_user/current_user **postgres** in database **postgres**. Supabase's project owner role postgres is the production migration authority for this route; there is no dedicated relayhistory_owner (or other) owner variant, and any other username, host, port, database or project refuses. The only URL query parameters accepted are sslmode=require|verify-full and channel_binding=require; they cannot replace the client's TLS configuration, which uses the committed Supabase Root 2021 CA with certificate and hostname verification. The owner must match Supabase's exact observed postgres attribute shape: LOGIN, NOSUPERUSER, and CREATEROLE, CREATEDB, BYPASSRLS and REPLICATION all present, and it must own the sessions schema. Any other shape (including a superuser) refuses with release_owner. The workflow never creates, grants or alters a role to make that inspection pass, and there is no owner/admin fallback.

As of the operator's 2026-11-04 03:51 UTC readonly inventory (taken under the earlier dedicated-owner design), ingest/seeder roles and ingest ledger/auth tables were absent. Although the pinned postgres owner has CREATEROLE, this route runs the migrator with bootstrap=false and never bootstraps missing roles. A separate reviewed administrator bootstrap path is required before schema apply: search-worker role, token-verifier role, writer roles when their migrations are included, their dedicated login/JWT bindings, the global-default-revoke function-default control, and the existing reviewed sessions event trigger owned by postgres (see "Function boundary" below; this route refreshes that trigger's function body but never creates the trigger). Use the exact reviewed bootstrap runbooks/scripts appropriate to the installed PG version and Supabase creator edge. This CI must never run those scripts or try a broader credential. The read-only plan checks bare search/token role existence/attributes; authoritative migration and phase-2 SQL still enforce complete memberships, settings and effective privileges. Passing a plan is not a phase-2 verdict.

Review the exact migration inventory on the eventual merged main SHA. The base used to author this route (1247da78064c41879327196e05bdf4ca6d754c63) includes auth173 but does not itself supply the pending writer-ledger schema. Merge/re-CI prerequisite source changes in dependency order; this route must not manufacture a successful ingest cutover from an incomplete migration set.

Separately provision/review the private ingest Worker, ingestion Hyperdrive/login, signing/forwarding bindings and token secrets, namespace permissions and operator-controlled canary. The 03:46 UTC inventory reported no ingest Worker, only the old read-role Hyperdrive, and no RHC forwarding bindings. These are upload-release dependencies owned by the operator, not actions performed by this workflow. Full CH/read backfill is a separate lane.

## Plan, review, then apply

1. Merge the reviewed release route and required schema source onto main through the authorized integration process. Wait for the **current main SHA's push** ci and pg-proof workflows to complete successfully. Every job must succeed, including checks, session-read-worker / worker, and real PostgreSQL 15/16/17. Failed, skipped, incomplete, missing, truncated or different-SHA evidence refuses.
2. Compute the migration generation from that exact checkout: `bun src/hoopdb/migrate.ts --generation`. The workflow recomputes it before requesting protected approval and before connecting.
3. Dispatch **guarded session schema release** on main with mode=plan, exact sha, exact generation, and boundary=global-default-revoke. That is the workflow's only boundary option and the only value the release contract accepts; it is not a choice between boundaries. Any other value, event-trigger included, refuses with release_inputs before any GitHub API call or database connection. Leave plan_run and approval empty. Approve the protected environment job (with no reviewer rule, the pinned account must have made the dispatch itself).
4. Download **schema-release-plan** from the successful plan run. Review project, source SHA, generation, boundary, ledger digest, each migration name/SHA/pending bit, the exact ACL preview, and existingBoundary (the existing event trigger's identity evidence and its current function-body digest, prosrc_sha256). The database work runs in REPEATABLE READ READ ONLY: the pinned owner identity check, then the existing advisory lock and shared migration preflight, then the boundary inspection. The plan refuses unless the global-default-revoke control is catalog-proven (release_boundary_control) and the existing event trigger already has the exact safe identity (release_boundary_identity); only its function body may still be the preceding release's. No bootstrap, migration, grant, boundary or marker SQL runs. The artifact contains names, hashes, role names, flags and ACL evidence only, no credential or source content.
5. After reviewing that concrete plan, dispatch mode=apply on the **same still-current main SHA** within 24 hours, supplying the plan run ID and exactly:
   `APPLY <supabase-project> <sha> <generation> PLAN <plan_run>`.
   Approve this new protected environment job (with no reviewer rule, the pinned account must make this dispatch itself). The prior plan must be a successful first-attempt manual run of this same workflow/SHA created within the last 24 hours; its immutable artifact must bind project/SHA/generation/boundary/run and carry a safe existingBoundary. The typed string alone never authorizes an unreviewed or foreign plan.
6. Apply repeats the read-only inspection (owner identity, global control, event-trigger identity) and refuses if the ledger digest (release_ledger_changed) or the existingBoundary digest (release_boundary_changed) differs from the plan. It then calls the existing runMigrations with Supabase mode, shim=false, bootstrap=false, verify=true, function boundary global-default-revoke and bulls96_runtime LOGIN=true. That takes the migrator's session advisory lock, repeats its normal authority/role/sequence preflight and compares the current ledger digest to the plan **under that lock**. Immediately before the first write it rechecks live GitHub source/CI/human authority and then performs the CI boundary refresh described below. The existing phase-2 role verifiers and exact ACL guards must all finish before the existing PASS marker. A final SELECT requires that PASS marker for the exact approved generation, and the exact event-trigger boundary is verified once more after PASS.

### Function boundary: required control and CI refresh

There is no operator choice between function-boundary modes on this route. global-default-revoke is the required **pre-refresh control**: the catalog must already prove it at plan time and again under the migrator lock, and it is what the migration profile runs under. The reviewed sessions event trigger is refreshed and verified by CI itself, never selected by the operator:

- It must already exist with the exact safe identity recorded in the approved plan: ddl_command_end with the exact tag set, enabled ALWAYS; trigger, function and its private schema owned by the postgres migration owner; the reviewed plpgsql VOLATILE SECURITY INVOKER function returning event_trigger with the exact function configuration; EXECUTE for its owner only and none for PUBLIC; matching runtime-role setting.
- Under the migrator lock, CI refuses unless the committed wrapper source (supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql) matches its pinned digest (release_boundary_wrapper) and the live trigger evidence still equals the plan's (release_boundary_changed). It then replaces that one function's definition (CREATE OR REPLACE with the committed body) in its own transaction and commits only if the full event-trigger evidence is then satisfied and the body digest is exact; otherwise it rolls back and refuses (for example release_boundary_refresh_unproven).
- The refresh never creates the trigger, transfers owners, changes ACLs or connects as another role. A missing trigger, a trigger owned by any role other than postgres, or any other identity drift refuses before the refresh.

No database URL is exposed to install/check-out/download/upload steps. Only the database invocation receives it. The connection has a ten-second connect bound, five-second lock timeout, five-minute statement timeout and one-minute idle-transaction timeout. Errors print fixed/sanitized codes, never driver message/detail/hint or connection URLs. Plan artifacts expire after one day. Concurrent runs serialize; an apply is not canceled by a second dispatch.

## Failure and rollback

A missing bootstrap, unsafe or wrong owner, unproven global control, missing or drifted event trigger, mixed Supabase CLI history, ledger gap/checksum drift, changed ledger since planning, stale source/CI, or missing human approval stops the route. Do not edit a ledger, suppress a verifier, disable TLS or switch credentials to continue. Obtain reviewed prerequisite work, re-run a new read-only plan, and approve the new exact generation.

The boundary refresh commits on its own before the PENDING readiness marker and the first migration; a later failure does not revert it (the refreshed body is the exact committed source). Migrations commit individually. A late failure can leave earlier committed steps plus a PENDING readiness marker; it is not an automatic schema rollback. Keep ingestion/activation held, record the failing safe code and exact run, and use the reviewed forward repair/retry procedure. Never auto-drop tables, erase migration history, reset roles or mark PASS manually. This workflow does not flip routing or claim full read/search activation.

## Source validation

Synthetic checks cover exact GitHub main/run/workflow/CI/job provenance, protected branch/reviewer/actual approval and the pinned-dispatcher fallback, rerun refusal, typed plan bindings, refusal of every boundary input except global-default-revoke (plan and apply) and the workflow's single boundary option, the exact postgres owner shape, connection/TLS pins, the exact safe event-trigger plan binding, refusal of a non-committed wrapper before any database call, and step-scoped secret wiring. PGlite runs the actual migrator and readonly inspection against a populated ledger; the ledger/marker remain identical. Wrong planned ledger and revoked final release gate refuse before the first persistent write. Reverting those controls must make their corresponding checks fail.

The protected production environment, approval-history API permissions and real pooler connection have not been exercised by an author dispatch. Operator setup plus independent source review, current-head CI and a real **plan-only** run are required before claiming the route executable. No production migration, credential/configuration change or deployment was performed while authoring this draft.

GitHub primary references: [environment configuration](https://docs.github.com/en/rest/deployments/environments), [deployment branch policies](https://docs.github.com/en/rest/deployments/branch-policies), [workflow approval history](https://docs.github.com/en/rest/actions/workflow-runs#get-the-review-history-for-a-workflow-run).


## Operator read-only plan while reviewer billing is blocked

The 2026-11-04 GitHub administrator setup returned HTTP 422: the repository plan
does not support required environment reviewers. At that time an empty
unprotected session-schema-production existed; no secret or dispatch was
configured. The CI gate now handles a missing reviewer rule only through the
pinned-dispatcher fallback above (first-attempt dispatch by Khaliq's own User
account); the environment's branch policy and every other gate still apply. Do
not replace the human gate with a typed string or admin bypass.

For a reviewed SSH session, scripts/schema-release-operator-plan.ts is a separate
PLAN-ONLY entry. Set RELEASE_MODE=plan, RELEASE_SHA to its exact immutable reviewed
checkout, RELEASE_GENERATION to that checkout's generation, and
RELEASE_BOUNDARY=global-default-revoke (the only accepted value; event-trigger or
anything else refuses with release_inputs before connecting). Supply the postgres
owner URL as SUPABASE_MIGRATION_OWNER_URL only in the
process environment using the approved credential system; leave RELEASE_PLAN_RUN
and RELEASE_APPROVAL unset. Invoke:
  bun scripts/schema-release-operator-plan.ts

It enforces the same input contract, connection/postgres owner pins and REPEATABLE
READ READ ONLY inspection (including the global control and event-trigger identity
checks), emits a safe JSON plan (or fixed refusal code), and executes no apply branch.
There is no CLI apply option, GitHub approval simulation, credential escalation,
or automatic exception. The operator must separately authorize an exact concrete
manual-apply exception to the standing release contract before any alternative
write route is implemented or used.

Boundary ownership is material: the event trigger, its function and its private
schema must be owned by the pinned postgres migration owner, and the sessions
schema must be owned by postgres. There is no dedicated-owner variant of this
route; an object owned by any other role (for example a former relayhistory_owner
design) refuses (release_owner, release_boundary_control or
release_boundary_identity). Audit actual
schema creators/owners/default privileges, and confirm the global-default-revoke
control, in the read-only plan. Do not transfer owners, re-run the bootstrap
wrapper as a different role, or switch boundary modes to guess past those checks.
