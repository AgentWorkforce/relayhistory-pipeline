# Guarded production session-schema release (draft)

This is a manual GitHub Actions route, not an activation or a deployment. It uses the authoritative pipeline migrator. It never invokes Supabase CLI migration apply, Alchemy, or a Worker deployment. Apart from the migrator's own writes, its SQL writes are the CI event-trigger function refresh described under "Function boundary", built from the committed reviewed function body, and the separately approved **bootstrap** mode described under "Bootstrap stage", which creates only absent session roles from the committed reviewed bootstrap chain.

## Prerequisites and current blockers

An existing GitHub administrator must create **session-schema-production** before this workflow can run. Configure exactly one custom deployment branch policy (type branch, name main) with protected-branches deployment disabled. Where the repository plan supports required reviewers, configure directly named human User reviewers (Khaliq: User ID 1724137); Team-only reviewers are deliberately insufficient for the current approval verifier and refuse. Mirror the approved **postgres** owner session-pooler URL (connection username postgres.<supabase-project>, see the connection pins below) into the environment secret **SUPABASE_MIGRATION_OWNER_URL** using an approved RAM/stdin path; doing so is a credential change that needs Khaliq's separate explicit OK. Do not put credentials in a repository, file, artifact, command argument, GITHUB_ENV, or log. The workflow does not create the environment or configure any secret. Its GitHub token only reads Actions, deployments and repository contents; any inaccessible verification API fails closed.

The environment gate is checked before its name is emitted as the dependent job's environment output. The protected job then rechecks the gate and its human authority. When the environment has a required-reviewers rule, it requires an actual approved review-history entry for that environment ID by a presently configured human User reviewer, and any rejection recorded for that environment refuses; a reviewer rule never falls back to dispatcher identity. When the repository plan cannot create a reviewer rule (no required_reviewers protection rule exists), the human authority is the dispatch itself: both the run's actor and its triggering actor must be the pinned human User account (Khaliq, User ID 1724137). Admin bypass without approval, bot approval or bot dispatch, a foreign approver or dispatcher, missing environment or protection metadata, wrong branch policy, and workflow reruns all refuse. A rerun requires a new manual dispatch (and approval, where a reviewer rule exists).

The only accepted connection is the pinned Supabase pooler **aws-0-us-east-1.pooler.supabase.com:5432/postgres**, project **<supabase-project>**, connection username **postgres.<supabase-project>**, with actual session_user/current_user **postgres** in database **postgres**. Supabase's project owner role postgres is the production migration authority for this route; there is no dedicated relayhistory_owner (or other) owner variant, and any other username, host, port, database or project refuses. The only URL query parameters accepted are sslmode=require|verify-full and channel_binding=require; they cannot replace the client's TLS configuration, which uses the committed Supabase Root 2021 CA with certificate and hostname verification. The owner must match Supabase's exact observed postgres attribute shape: LOGIN, NOSUPERUSER, and CREATEROLE, CREATEDB, BYPASSRLS and REPLICATION all present, and it must own the sessions schema. Any other shape (including a superuser) refuses with release_owner. The workflow never creates, grants or alters a role to make that inspection pass, and there is no owner/admin fallback.

As of the operator's 2026-11-04 03:51 UTC readonly inventory (taken under the earlier dedicated-owner design), ingest/seeder roles and ingest ledger/auth tables were absent. The migrator itself still runs with bootstrap=false. Absent session roles listed in SESSION_BOOTSTRAP_ROLES (src/hoopdb/session-bootstrap-chain.ts) are created only by this workflow's separately approved **bootstrap** mode (see "Bootstrap stage"), and apply refuses while any is absent (release_bootstrap_required). Everything else remains a separate reviewed administrator path before schema apply: dedicated login passwords/JWT bindings, the global-default-revoke function-default control, and the existing reviewed sessions event trigger owned by postgres (see "Function boundary" below; this route refreshes that trigger's function body but never creates the trigger). Apart from the bootstrap chain files run by the bootstrap mode, this CI never runs administrator scripts or tries a broader credential. The read-only plan checks bare search/token role existence/attributes; authoritative migration and phase-2 SQL still enforce complete memberships, settings and effective privileges. Passing a plan is not a phase-2 verdict.

Review the exact migration inventory on the eventual merged main SHA. The base used to author this route (1247da78064c41879327196e05bdf4ca6d754c63) includes auth173 but does not itself supply the pending writer-ledger schema. Merge/re-CI prerequisite source changes in dependency order; this route must not manufacture a successful ingest cutover from an incomplete migration set.

Separately provision/review the private ingest Worker, ingestion Hyperdrive/login, signing/forwarding bindings and token secrets, namespace permissions and operator-controlled canary. The 03:46 UTC inventory reported no ingest Worker, only the old read-role Hyperdrive, and no RHC forwarding bindings. These are upload-release dependencies owned by the operator, not actions performed by this workflow. Full CH/read backfill is a separate lane.

## Plan, review, then apply

1. Merge the reviewed release route and required schema source onto main through the authorized integration process. Wait for the **current main SHA's push** ci and pg-proof workflows to complete successfully. Every job must succeed, including checks, session-read-worker / worker, and real PostgreSQL 15/16/17. Failed, skipped, incomplete, missing, truncated or different-SHA evidence refuses.
2. Compute the migration generation from that exact checkout: `bun src/hoopdb/migrate.ts --generation`. The workflow recomputes it before requesting protected approval and before connecting.
3. Dispatch **guarded session schema release** on main with mode=plan, exact sha, exact generation, boundary=global-default-revoke, the required **target** and the required **target_sha**. global-default-revoke is the workflow's only boundary option and the only value the release contract accepts; it is not a choice between boundaries. Any other value, event-trigger included, refuses with release_inputs before any GitHub API call or database connection. **target** is the last schema step this release may apply, exactly as ledgered (for example `upstream:20261105047000_coordination_fact_readers.sql`). **target_sha** is the full 40-hex commit the release is anchored to. It must be sha itself or on sha's first-parent (main) history; a commit reachable only through a merged side branch is refused, because the runtime image is published at target_sha and only first-parent main commits carry the release's CI provenance. When target_sha differs from sha, target_sha's own **push** ci and pg-proof workflows on main must also be green exactly as in step 1 (latest run completed and successful; every job, including checks, session-read-worker / worker and real PostgreSQL 15/16/17, completed and successful at target_sha), checked at plan and again under the lock. Its tree must contain the target with no later step of the target's own lane (release_target_tail). Already-applied steps of the other lane may follow the target (for example 96bulls lane steps after an upstream target): they are part of the manifest and bound by name and sha256 like every other step. Every held step (any pending step after the target, in either lane) must be absent from target_sha's tree: the tree must hold exactly the release tree's schema steps without the held ones (same names, same file sha256, nothing else; release_target_tree_mismatch). For example 113dc6576d014ede149c0f1cdc484c5e980dd4e4 is on the first-parent history of later main and its upstream tail is 20261105047000. When nothing will be held, use target_sha = sha. **Images for a prefix release are published at target_sha**: its `--generation` is the only generation the release may mark PASS. Leave target_generation, plan_run and approval empty. Approve the protected environment job (with no reviewer rule, the pinned account must have made the dispatch itself). The database job checks out full history (fetch-depth: 0) and reads target_sha only from the local Git object database (no fetch). These refuse:
   - an abbreviated or unknown target_sha, or one that is not sha and not on sha's first-parent history, including a side commit merged into sha's history (release_target_sha, release_target_sha_unknown, release_target_sha_not_ancestor);
   - a target_sha other than sha whose own push ci or pg-proof run on main is missing, incomplete or failed (release_target_ci), or whose jobs are failed, incomplete, missing, truncated or tested another SHA (release_target_ci_jobs);
   - a target_sha tree with a later step of the target's own lane after the target (release_target_tail);
   - a target_sha tree whose steps differ from the release prefix (release_target_tree_mismatch);
   - a target_sha tree that lacks a phase 2 verifier file (release_target_verifier_missing).
4. Download **schema-release-plan** from the successful plan run. Review:
   - project, source SHA, generation, boundary, ledger digest;
   - each migration name/SHA/pending bit and the exact ACL preview;
   - existingBoundary: the existing event trigger's identity evidence and its current function-body digest, prosrc_sha256;
   - **target**;
   - **expectedPending**: the pending steps up to and including the target, the only steps the apply may run;
   - **held**: the pending steps after the target, never applied by this release;
   - **targetSha**;
   - **targetManifest**: the target commit's schema steps as [name, sha256] and its 7 phase 2 verifier digests;
   - **manifestDigest**: the one canonical digest of that manifest (src/hoopdb/migrate.ts releaseManifestDigest);
   - **targetGeneration**: the generation algorithm over the target commit's own steps and verifier files, i.e. exactly what `bun src/hoopdb/migrate.ts --generation` prints on a checkout of target_sha. It equals generation when held is empty (the plan refuses otherwise), and it is the only generation this release may mark PASS.

   The database work runs in REPEATABLE READ READ ONLY: the pinned owner identity check, then the existing advisory lock and shared migration preflight, then the boundary inspection. The plan refuses unless the global-default-revoke control is catalog-proven (release_boundary_control) and the existing event trigger already has the exact safe identity (release_boundary_identity); only its function body may still be the preceding release's. The plan also refuses an unknown target (release_target_unknown), an already applied target (release_target_not_pending) and a target below its lane's ledgered tail (release_target_passed); ledger_gap is unchanged. No bootstrap, migration, grant, boundary or marker SQL runs. The artifact contains names, hashes, role names, flags and ACL evidence only, no credential or source content.
   **Bootstrap stage.** The plan artifact has a **bootstrap** section: the bootstrap chain files from SESSION_BOOTSTRAP_ROLES with their sha256 at sha (sha256_release) and at target_sha (sha256_target), each listed role with present / exact / will_create, required, and chain_digest (sha256 over the ordered `file` newline `sha256_release` newline lines). The plan refuses release_bootstrap_role_drift when a present role is not the exact shape its file provisions: no SUPERUSER, CREATEDB, CREATEROLE, REPLICATION or BYPASSRLS, connection limit -1, no outbound membership, LOGIN, inbound membership and role settings as its contract in SESSION_BOOTSTRAP_ROLES states (for example only the reviewed authenticator edge for the worker/verifier roles), and otherwise no inbound membership except, on PostgreSQL 16+ in Supabase mode, the creator edge (member postgres, grantor superuser supabase_admin, ADMIN without INHERIT or SET). When required is true the plan prints `bootstrap stage required before apply`, and apply refuses release_bootstrap_required with the absent role names until the stage has run. When it is required:
   1. Review the bootstrap section, then dispatch mode=bootstrap with the same sha, generation, target, target_sha, target_generation and plan run, and exactly:
      `BOOTSTRAP <supabase-project> <sha> TARGET <target> <target_sha> CHAIN <chain_digest> ROLES <will_create> PLAN <plan_run>`
      where will_create is the plan's sorted, comma-joined will_create names, or `none`. Any other string refuses release_bootstrap_approval. It runs in the same protected environment with the same gate as apply.
   2. **Before connecting**, the stage digests the chain files at sha (from the Git object database) and refuses release_bootstrap_plan_changed unless that digest equals both the plan's chain_digest and the approval's CHAIN. Then, under the migrator advisory lock, it rechecks GitHub authority and the anchor, recomputes the bootstrap section, and refuses release_bootstrap_plan_changed on any difference from the plan. If nothing is required it records skipped evidence and exits successfully.
   3. Otherwise, in one transaction with db_mode supabase, it runs each chain file that provisions an absent role (psql meta lines stripped). Before sending any of it, it refuses release_bootstrap_chain_unsafe when a file contains transaction control: COMMIT, ROLLBACK, ABORT, SAVEPOINT, RELEASE, START TRANSACTION or PREPARE TRANSACTION anywhere in the text (inside a function body or comment too), or, outside dollar-quoted bodies, quotes and comments, a statement beginning with BEGIN or END (so a top-level `END;` cannot commit early). It requires the same transaction id before and after the chain SQL. It then reads the whole cluster back and commits only if: exactly the approved roles were created, each in its fresh bare shape with no VALID UNTIL; no other role was added, removed or had its name, attribute flags, connection limit or password validity (rolvaliduntil) changed; every membership edge of the cluster (role, member, grantor, ADMIN/INHERIT/SET) is unchanged, except, on PostgreSQL 16+ in Supabase mode, exactly the creator edge of each created role; every per-database and per-role setting row (pg_db_role_setting, compared by a server-side digest that is never recorded) is unchanged apart from a created role's own contract settings; and every chain role matches its contract. Object privileges (grants on schemas, tables, functions) and passwords are out of scope of this readback. Otherwise it rolls back and refuses release_bootstrap_readback.
   4. The non-secret evidence (role names, flags, validity, edges, file digests; no passwords, setting values or setting digests, URLs or hosts) is uploaded as **schema-release-bootstrap**. Login passwords are still set by the operator outside CI. Then continue with apply using the same plan run.

5. After reviewing that concrete plan, dispatch mode=apply on the **same still-current main SHA** within 24 hours. Supply the same target and target_sha, the plan run ID, target_generation copied from the plan's **targetGeneration**, and exactly:
   `APPLY <supabase-project> <sha> <generation> TARGET <target> <target_sha> <target_generation> MANIFEST <manifest_digest> PLAN <plan_run>`
   with manifest_digest copied from the plan's **manifestDigest**.
   Approve this new protected environment job (with no reviewer rule, the pinned account must make this dispatch itself). The prior plan must be a successful first-attempt manual run of this same workflow/SHA created within the last 24 hours. Its immutable artifact must bind project/SHA/generation/boundary/target/targetSha/targetGeneration/manifestDigest/run and carry a safe existingBoundary, and its manifest must recompute to manifestDigest and targetGeneration (release_plan_artifact). The typed string alone never authorizes an unreviewed or foreign plan.
6. Apply works in this order:
   1. **Before connecting**, it recomputes the target_sha anchor from the local object database and refuses any difference from the plan (release_target_manifest_changed). The anchor covers ancestry, tree equality with this checkout's prefix, verifier files, manifest, manifestDigest and targetGeneration.
   2. It repeats the read-only inspection (owner identity, global control, event-trigger identity). It refuses if the ledger digest (release_ledger_changed) or the existingBoundary digest (release_boundary_changed) differs from the plan, if the live expectedPending and held lists differ from the plan's (release_target_pending_changed), or if the anchor recomputed there differs (release_target_generation_changed, release_target_manifest_changed).
   3. It then calls the existing runMigrations with Supabase mode, shim=false, bootstrap=false, verify=true, function boundary global-default-revoke, bulls96_runtime LOGIN=true and the approved target binding. That takes the migrator's session advisory lock and repeats its normal authority/role/sequence preflight.
   4. **Under that lock and before any write**, the migrator does four things:
      - compares the current ledger digest to the plan;
      - recomputes the pending/held split and refuses the same way;
      - recomputes the manifest from the on-disk prefix files plus the target commit's verifier digests, and refuses a digest other than the approved one (release_target_manifest_changed);
      - refuses held steps that come without that binding (release_target_binding_required).
   5. **Immediately before the first write**, it rechecks live GitHub source/CI/human authority, recomputes the target_sha anchor once more, and then performs the CI boundary refresh described below.
   6. The pending marker is written for targetGeneration and the steps up to the target apply. Held steps are never applied or ledgered.
   7. Phase 2 runs on that prefix. First come this release tree's role verifiers, hardening and exact ACL guards. The ACL manifest is scoped to the prefix: a manifest function is excused only when it does not exist and a held step creates it, and every existing function, including ones a held step replaces, must still match exactly. Then come the target commit's own 7 verifiers, read from the object database with their digests checked.
   8. Only after both verifier sets pass is the existing PASS marker written, for targetGeneration, never for the full-tree generation. Any failure leaves the marker pending for targetGeneration.
   9. A final SELECT requires PASS for the exact approved targetGeneration (release_marker). The apply also reads back that every expectedPending step is ledgered and no held step is (release_target_readback), and the exact event-trigger boundary is verified once more after PASS. It prints `release_target_held <n>`.

   **Images after a prefix release:**
   - An image built at target_sha (whose `--generation` is targetGeneration) is then ready, and its 043700 owner-buffered parity check matches.
   - An image of the full tree stays schema_pending until a later release applies the held steps. That release's PASS moves to its own generation, and the target image then reports validation_pending.

   **A prefix the verifiers cannot validate fails closed.** An example is a target that holds steps whose objects a verifier requires, such as the 96bulls lane on an empty database. Phase 2 rolls back, the marker stays pending for targetGeneration, and the release needs a later target.

### Function boundary: required control and CI refresh

There is no operator choice between function-boundary modes on this route. global-default-revoke is the required **pre-refresh control**: the catalog must already prove it at plan time and again under the migrator lock, and it is what the migration profile runs under. The reviewed sessions event trigger is refreshed and verified by CI itself, never selected by the operator:

- It must already exist with the exact safe identity recorded in the approved plan: ddl_command_end with the exact tag set, enabled ALWAYS; trigger, function and its private schema owned by the postgres migration owner; the reviewed plpgsql VOLATILE SECURITY INVOKER function returning event_trigger with the exact function configuration; EXECUTE for its owner only and none for PUBLIC; matching runtime-role setting.
- Under the migrator lock, CI refuses unless the committed wrapper source (supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql) matches its pinned digest (release_boundary_wrapper) and the live trigger evidence still equals the plan's (release_boundary_changed). It then replaces that one function's definition (CREATE OR REPLACE with the committed body) in its own transaction and commits only if the full event-trigger evidence is then satisfied and the body digest is exact; otherwise it rolls back and refuses (for example release_boundary_refresh_unproven).
- The refresh never creates the trigger, transfers owners, changes ACLs or connects as another role. A missing trigger, a trigger owned by any role other than postgres, or any other identity drift refuses before the refresh.

No database URL is exposed to install/check-out/download/upload steps. Only the database invocation receives it. The connection has a ten-second connect bound, five-second lock timeout, five-minute statement timeout and one-minute idle-transaction timeout. Errors print fixed/sanitized codes, never driver message/detail/hint or connection URLs. Plan artifacts expire after one day. Concurrent runs serialize; an apply is not canceled by a second dispatch.

## Failure and rollback

A missing bootstrap (release_bootstrap_required: run the bootstrap stage), drifted bootstrap role (release_bootstrap_role_drift), unsafe or wrong owner, unproven global control, missing or drifted event trigger, mixed Supabase CLI history, ledger gap/checksum drift, changed ledger since planning, stale source/CI, or missing human approval stops the route. Do not edit a ledger, suppress a verifier, disable TLS or switch credentials to continue. Obtain reviewed prerequisite work, re-run a new read-only plan, and approve the new exact generation.

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
checkout, RELEASE_GENERATION to that checkout's generation,
RELEASE_BOUNDARY=global-default-revoke (the only accepted value; event-trigger or
anything else refuses with release_inputs before connecting), RELEASE_TARGET to the
target step and RELEASE_TARGET_SHA to the full target commit (the checkout must hold
full history; the JSON includes target, expectedPending, held, targetSha,
targetManifest, manifestDigest and targetGeneration, the generation an apply of that
split marks PASS). Supply the postgres owner URL as SUPABASE_MIGRATION_OWNER_URL only
in the process environment using the approved credential system; leave
RELEASE_TARGET_GENERATION, RELEASE_PLAN_RUN and RELEASE_APPROVAL unset. Invoke:
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
