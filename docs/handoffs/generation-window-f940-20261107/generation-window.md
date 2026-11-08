# Generation window at f9409ccb — source-only draft

> Current update: see [approved-window-current-status.md](approved-window-current-status.md). The user directly approved the four-role bootstrap and schema window; the four roles were created/verified at 21:59Z and plan/capture refreshed at 22:02–22:04Z. Runtime coordination remains open. The original draft and 18:47 bindings below are historical.

Prepared for pipeline-lead@linuyc-244866483991928832. Runtime release-owner coordination is codex-relay-desktop-find-prior@khaliqs-macbook-pro-244648072446341120; SQL transport uses the existing khaliqgant CLI session on sf-frame. No apply, runtime deployment, role bootstrap, approval record, rebind, feature activation or Cloud merge has been performed by this preparation.

Status: schema plan and capture are concrete; the COMPLETE generation window is NOT execution-ready. Required live prerequisites and artifact/capture slots below are not established. Do not present the whole-window approval until execution-targets.json has no unresolved slots and its final digest is fixed. The read-only plan/capture do not establish those prerequisites.

## 1. Immutable source, schema generation and captured evidence

- Schema/operator source: f9409ccb9e46c3f100cf3017847d8ed62bc716c7.
- The documentation handoff commit/branch is NOT the execution source. Commands must run from the pinned f9409ccb checkout; do not replace SOURCE with this docs-only commit.
- Target schema generation G2: e778dbb6cb68bc0e8939797691565db0b1dd8d357d7d790b375036e0070923ce.
- Last retained ingest G1: 78c7b28e5ebce7f5f534a1f3f41a55845251e950b6c4c49f676f2bda358c91d6.
- G1 evidence is retained Worker inputs and the earlier window receipt, NOT a fresh database policy/admission or all-runtime observation. Confirm the actual pre-window readiness marker, singleton policy, all admission generations and live runtime settings before opening the window. An unexpected generation is a stop; never substitute the capture hash.
- Captured ledger: 3173f2df7f749fd79c340286f3284d91d8f5a8b988b7f1b1a11e01c3a3307641.
- Plan SHA256: 9f18fc767ca002522060e53012e861411dc52c7b90d77172254d30cafc4d3100.
- Plan at: 2026-11-07T18:47:10.129Z. First expiry: 2026-11-07T19:47:10.129Z.
- Capture SHA256: f561d85d2b422c5fc0bcc07ff4bede94e071d086d89dcb8c71752c045dcf33e0.
- Capture at: 2026-11-07T18:47:44.645Z; expires 19:47:44.645Z.
- CaptureGeneration: 242ea83ea9d5115aa99fbda73b0e7590f44180c7b0bd74a89065cabcc575e984.
- ACL preview SHA256: d953e10c3e9c519e559d2728c0665de946ba223c952da1dd9c1ba3e28a3b7aad.
- Wrapper SQL SHA256: 09b1d7804612f38fc121106004752348d56d8781930b66e840e1ef71391c4808.
- Expected wrapper prosrc SHA256: 8061b2c51d99613e3fd606bc4b2d639107638171e77122e0abb59a628f90fda0.
- Existing installed wrapper prosrc was 94d0aa84f59f014b682608e9b651ce0fd0864c3f8dfaa30e5fc55d98944501fc, reviewed identity/shape intact but NOT equal to the new source. The owner adapter deliberately refreshes that same-owner function inside the authorized apply route. Successful capture is not a claim that the old wrapper already matches.
- CaptureGeneration hashes capture metadata; it is NOT the generation for any runtime or rebind. All G2 runtime generation settings and the rebind --to use e778dbb6... above.

The actual capture-compatible plan route is schema-release-owner-operator-plan.ts (OPERATOR_MODE=plan), using the existing sf-frame Supabase CLI temporary login cli_login_postgres with SET ROLE postgres. The generic schema-release-operator-plan.ts requires a different dedicated-owner URL and returns a different report shape; it was not run or rewritten. No owner URL/password/Management API/principal fallback was used.

Execution checkout retained on sf-frame:
 /home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/root-readonly-release-f9409ccb-20261107

Exact raw files accompany this draft: readonly-plan.json and capture.json. In that checkout they are .96bulls/owner-apply/readonly-plan.json and docs/plans/supabase-owner-apply.capture.json. No approval.json exists there.

## 2. The complete pending prefix

The attached plan is authoritative for names and SQL checksums. There are 21 pending upstream migrations:

020000 session_replay_pages; 021000 session_large_read_jobs; 022000 session_desktop_catalog; 023000 session_analysis_publications; 024000 session_brief_documents; 025000 canonical_feature_journal; 026000 canonical_history_lineage; 030000 canonical_feature_consumer_acks; 031000 desktop_catalog_forget_reproject; 032000 session_large_read_routing_fixes; 033000 canonical_project_events; 034000 session_directory_read_api; 035000 semantic_copy_state; 036000 canonical_consumer_lease; 037000 session_directory_document_pool; 038000 claim_attempt_records_retirement; 039000 buffered_gc_single_candidate; 040000 ingest_lazy_source_tables; 041000 ingest_privacy_authority; 042000 babysitter_evidence_retention; 043000 brief_titles.

Each full name is upstream:20261105<slot>_<suffix>.sql. Do not apply only 040000-043000 or use the older 35-row ledger as proof the preceding 17 are installed. History's separate migration track is not included in this owner-apply command.

## 3. Pre-window requirements: finish before any live write

1. Verify exact source, clean tracked tree apart from the capture and permitted version-only CLI cache; exact generation; source SQL audit; fresh matching plan/capture; unchanged canonical ledger/catalog; valid existing boundary identity.
2. Obtain read-only role attributes/memberships/settings evidence for the additional roles in section 7. The plan explicitly checks bare search/token roles only. If any prerequisite is missing/unsafe, do not start owner-apply and do not attempt to repair it through this adapter.
3. Any needed privileged bootstrap must be a separately reviewed and explicitly authorized prerequisite before the window. Supabase mode refuses migrator role bootstrap. The schema quote below does not silently grant a new credential, role administrator or IAM route.
4. Prepare a qualified G2 Worker plan/build, G2 GC ZIP/template/immutable version, publisher launcher/artifact decision and all active generation-bound consumer decisions. Populate execution-targets.json with actual current preimages and reviewed outputs, hashes and owners. This draft has no such runtime capture.
5. Record G1 marker/PASS, policy, all admissions and live generation values. Check policy validity/expiry and other admission attestations; rebind preserves them, it does not renew or invent privacy/source proof.
6. Rehearse the exact full 21-step source union and the selected old/new runtime compatibility against an owned fixture. Earlier two-step window evidence does not measure or prove this union.
7. Record a bounded pre-window traffic baseline and GC health/retention backlog. The current nonzero 503 baseline must remain separate from intentional generation refusals; do not treat absence of pool errors as proof all uploads work.
8. Have all operators, existing credentials, transport, rollback/forward-repair owners and deploy packages ready before closing uploads. Root/lead roles are not a reason to skip these checks.

The earliest captured evidence expires at 19:47:10.129Z. Refresh plan and capture if needed; that changes their hashes and the approval bindings. Never change timestamps in an old report.

## 4. One proposed approval and exact owner-apply fields

There is no magic server-enforced English passphrase; the source validates a nonempty verbatim quote plus all bindings. The operator must retain Khaliq's actual direct authorization, not synthesize it from a peer saying GO.

For ONE complete-window approval, first finish execution-targets.json, fix its final SHA256 and replace the one placeholder in the proposed quote below. The target manifest does not contain this quote or its own digest, avoiding a self-referential hash. As delivered its readiness is HOLD; the placeholder is NOT approval-ready.

Proposed exact quote for Khaliq to say himself in the executing lead/operator session:

> I approve the manual Supabase owner-schema apply and generation window for project pyamkrhoyszgzirqqffq, schema source f9409ccb9e46c3f100cf3017847d8ed62bc716c7, target generation e778dbb6cb68bc0e8939797691565db0b1dd8d357d7d790b375036e0070923ce, starting ledger 3173f2df7f749fd79c340286f3284d91d8f5a8b988b7f1b1a11e01c3a3307641, read-only plan 9f18fc767ca002522060e53012e861411dc52c7b90d77172254d30cafc4d3100 and capture f561d85d2b422c5fc0bcc07ff4bede94e071d086d89dcb8c71752c045dcf33e0. Apply exactly its 21 pending migrations and reviewed existing boundary refresh, then execute only the reviewed runtime replacements and generation rebind in execution-targets.json SHA256 <FINAL_EXECUTION_TARGETS_SHA256>. I accept the measured upload-refusal window recorded in that packet. This does not authorize new role/IAM/credential provisioning, publisher activation, backfills, source deletion, Cloud PR landing or feature-flag activation.

If the plan/capture/source changes, replace the bound values before obtaining the direct quote; never retain this stale quote with different hashes.

The following is a DOCUMENT TEMPLATE ONLY, not a created approval record. After direct approval the executing operator places the actual fresh JSON in the ignored .96bulls/owner-apply/approval.json; at must be a fresh real ISO timestamp after capture. The exact actual quote, not the placeholder text, goes in quote:

~~~json
{
  "version": 1,
  "decision": "approved",
  "by": "Khaliq Gant",
  "at": "<ACTUAL_APPROVAL_TIMESTAMP_AFTER_CAPTURE>",
  "quote": "<ACTUAL_DIRECT_VERBATIM_APPROVAL>",
  "execution": "ssh-sf-frame-owner-schema-apply",
  "project": "pyamkrhoyszgzirqqffq",
  "sourceSha": "f9409ccb9e46c3f100cf3017847d8ed62bc716c7",
  "generation": "e778dbb6cb68bc0e8939797691565db0b1dd8d357d7d790b375036e0070923ce",
  "ledgerSha256": "3173f2df7f749fd79c340286f3284d91d8f5a8b988b7f1b1a11e01c3a3307641",
  "planSha256": "9f18fc767ca002522060e53012e861411dc52c7b90d77172254d30cafc4d3100",
  "captureSha256": "f561d85d2b422c5fc0bcc07ff4bede94e071d086d89dcb8c71752c045dcf33e0",
  "captureGeneration": "242ea83ea9d5115aa99fbda73b0e7590f44180c7b0bd74a89065cabcc575e984",
  "wrapperSha256": "09b1d7804612f38fc121106004752348d56d8781930b66e840e1ef71391c4808"
}
~~~

The application deploy tools have their OWN capture/approval formats. This schema approval.json is not passed to the Worker, CloudFormation, AWS stage or publisher launcher. The same actual human quote may support specifically bound tool records only for actions it really authorizes. Do not fabricate those records before approval.

## 5. Exact schema apply and generation rebind commands

On sf-frame, in the exact clean checkout above with Bun 1.4.2 and existing Supabase CLI 2.122.0. These commands are for the later approved execution; they were NOT run during this preparation.

~~~sh
cd /home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/root-readonly-release-f9409ccb-20261107
export PATH=/home/khaliqgant/.local/share/mise/installs/bun/1.4.2/bin:$PATH
SOURCE=f9409ccb9e46c3f100cf3017847d8ed62bc716c7
G1=78c7b28e5ebce7f5f534a1f3f41a55845251e950b6c4c49f676f2bda358c91d6
G2=e778dbb6cb68bc0e8939797691565db0b1dd8d357d7d790b375036e0070923ce

# Only after all pre-window checks and actual direct approval.
# Compute this from the actual approved file; never substitute captureGeneration.
APPROVAL_SHA=$(sha256sum .96bulls/owner-apply/approval.json | cut -d' ' -f1)
bun scripts/schema-release-owner-apply.ts apply \
  --source-sha "$SOURCE" --generation "$G2" \
  --capture-sha f561d85d2b422c5fc0bcc07ff4bede94e071d086d89dcb8c71752c045dcf33e0 \
  --approval-sha "$APPROVAL_SHA"
~~~

Require owner_apply_verified, generation G2, applied 21 on the unchanged starting ledger and no remaining pending inventory. The apply rechecks plan/catalog, canonical advisory lock and owner/backend identity, refreshes the existing boundary from source, runs the canonical migrations and phase-2 checks, and finally establishes PASS. There is no skip-verifier or force route.

After G2 PASS and the reviewed runtime replacements, use the real explicit project input (the old runbook omitted this requirement):

~~~sh
SUPABASE_PROJECT_REF=pyamkrhoyszgzirqqffq \
  bun scripts/ingest-generation-rebind.ts --from "$G1" --to "$G2"

SUPABASE_PROJECT_REF=pyamkrhoyszgzirqqffq \
  bun scripts/ingest-generation-rebind.ts --from "$G1" --to "$G2" --execute
~~~

The first command is a ROLLBACK dry run: it still executes tentative updates inside a transaction. Do not run it before the approved window as a no-mutation inventory query. It requires target PASS, existing policy at G1 or G2 and ALL admissions at G1/G2, preserves policy identity/attestation/expiry, updates policy/admissions atomically and reports counts. After execute, an optional reviewed idempotency check must report policy already/admissionsRebound 0. Do not run --execute during draft preparation.

## 6. Runtime replacement matrix and exact source entrypoints

None of the following deployment packages was captured/built by this source-only draft. Do not invent a missing live source SHA, ZIP version, service unit, profile or credential. Proposed G2 candidate source for new builds is main f9409ccb; current production is NOT claimed to run that source.

| Runtime | Generation and code requirement | Build/deploy path and owner | Missing live pins |
|---|---|---|---|
| 96bulls-session-ingest | INGEST_EXPECT_GENERATION=G2; retain buffered mode, inline0, replica off and all other approved settings. Proposed candidate main f9409ccb, not a claim the deployed 6440 base was upgraded. Worker generation is supplied from config (unlike compiled GC/publisher); using a retained 6440/diagnostic/compatibility child instead is a distinct compatibility choice requiring its own G2 proof/capture. | sf-frame clean installed candidate checkout; scripts/private-ingest-operator.ts plan/deploy below. Existing release owner acquires approved Cloudflare/state credentials through the qualified kjg-lap path, RAM/stdin only. Old Mini-only a6e driver/broker are fixed to G1 inputs/SHA/host; they CANNOT be reused unchanged for this G2 candidate. | actual latest deployed source/version/module+binding hashes, preservation of any newer upload repair/compatibility change, G2 semantic input hash, native plans/provenance/control-state pins, actual G2 artifact, reviewed memory-only host broker |
| sessions-buffered-gc, stack SessionsBufferedGc | INGEST_GC_EXPECT_GENERATION=G2 AND embedded compiled generation G2. Rebuild main f9409ccb; old 6440/462/G1 ZIP cannot be relabeled with an env change. | Build on sf-frame with scripts/build-buffered-gc.ts. Existing AWS operator/release owner on kjg-lap or Mini uses the approved account941433935742/us-east-1 profile/transport after actual identity verification. Current profile readiness is not established here. Preserve/qualify permanent GC enabled state and retention policy. | actual live template/params/state/enabled/health, ZIP hash and immutable S3 ObjectVersion, account/role authorization, template change-set identity and exact no-replacement delta |
| Existing canonical publisher and publication GC | INGEST_PUBLISH_EXPECT_GENERATION=G2 AND compiled generation G2. Rebuild main f9409ccb; preserve each existing enablement switch independently; do NOT activate dormant publisher work. | scripts/build-canonical-publication.ts on sf-frame; Node24 host entry publication-main.ts. Lead disposition244908112388333568: documented INACTIVE, no redeploy planned unless a running publisher is discovered. Retained migration-host candidate i-070f91abb67aa60de is NOT a publisher deployment or fresh host/process/role proof. Runtime credentials must be the pinned host IMDSv2 role, not an operator SSO key. The deployment credential owner is the lead's existing AWS release owner; exact current installation/service launcher is UNKNOWN here. | current active host/service unit/process, current artifact and flags, pinned role/profile/secret/bucket/KMS/instance identity, actual G2 artifact and reviewed replacement/restart command; keep publication GC alive independently |
| Active managed consumers: HoopDb indexer and session-pipeline Lambda | They carry compiled final-write generation authority. Audit their current activity/generation; source has no single env-only generation override. Update qualified G2 binaries before any G2 managed publication is enabled/allowed. If deliberately inactive, retain off and record that decision. | AWS source path is bin/96bulls plan aws-sessions-prod --capture then the approved, exact captured bin/96bulls up aws-sessions-prod route; deployment is owned by AWS release operator. Candidate main f9409ccb, independent of old private-ingest6440. | live inventory, immutable artifacts, captured allowed plan shape and source/inputs/state binding; no whole-stack deployment inferred from the three-runtime request |

Worker source commands, after creating new nonsecret sessionIngest inputs with expectedGeneration G2 and preserving reviewed other settings:

~~~sh
CI=true bun scripts/private-ingest-operator.ts plan \
  --expected-sha "$SOURCE" --inputs-sha256 "<REVIEWED_G2_INPUTS_SHA256>"

# Only with fresh bound application approval and reviewed retained snapshot.
CI=true bun scripts/private-ingest-operator.ts deploy \
  --expected-sha "$SOURCE" --inputs-sha256 "<REVIEWED_G2_INPUTS_SHA256>" \
  --provenance-sha256 "<REVIEWED_G2_PROVENANCE_FILE_SHA256>"
~~~

Credential values are never command arguments or files. Use only the separately qualified memory-only broker. No stock Alchemy deployment, no ambient account fallback, no queue re-creation/adoption or binding loss. The four-resource/private-surface contract and full current binding descriptors must survive. The recent Hyperdrive limit30 is an external config preimage to preserve, not a schema generation.

GC source build, from a separate CLEAN installed f9409ccb build checkout (the operator checkout now has a tracked capture difference; do not use --allow-wip):

~~~sh
bun scripts/build-buffered-gc.ts /home/khaliqgant/relay-briefs/gc-g2-f940-<UNIQUE>
~~~

Verify manifest head=f9409ccb, dirty=false, generation=G2, verifier hashes, template hash, bundle and ZIP. Review upload/immutable object readback before use. Existing artifact bucket candidate is 96bulls-sessions-prod-gc-artifacts-941433935742; do not reuse the old 80a271d5 ZIP or its version.

IMPORTANT template correction: main f9409ccb requires AwsAccountId and SupabaseProjectRef parameters and wires INGEST_GC_AWS_ACCOUNT_ID / INGEST_GC_SUPABASE_PROJECT_REF. The old params-on.json has neither. Do not blindly use --use-previous-template. Review the current template against the main template and use the captured exact new template/params when required. Required explicit values are 941433935742 and pyamkrhoyszgzirqqffq. Preserve all other live parameters from a fresh read; old params-on.json is historical evidence only.

Historical credential-wrapper locator (not a current qualification): /Users/khaliqgant/relay-briefs/migration-fast-20261103/gc-apply-20261105/awsx.sh on the Mini, using the existing sessions-prod profile. A fresh kjg-lap transport/profile pin must be supplied by the release owner; do not copy credentials from that path or assume it names a current session.

The proposed CloudFormation source route is:

~~~sh
# AWSX denotes the already verified approved profile wrapper, NOT a new credential.
"$AWSX" cloudformation create-change-set \
  --stack-name SessionsBufferedGc --change-set-name "<REVIEWED_UNIQUE_NAME>" \
  --change-set-type UPDATE \
  --template-body file://stacks/aws/buffered-gc.template.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters file://"<REVIEWED_G2_GC_PARAMETERS_JSON>"

"$AWSX" cloudformation describe-change-set \
  --stack-name SessionsBufferedGc --change-set-name "<EXACT_CHANGESET_ID>"

# Only execute after the actual diff is reviewed and bound in the final targets.
"$AWSX" cloudformation execute-change-set \
  --stack-name SessionsBufferedGc --change-set-name "<EXACT_CHANGESET_ID>"
~~~

IAM/template deltas beyond the already approved exact replacement are not covered automatically; stop if the change set introduces new resources/role policies/replacement or any unreviewed field. The actual full template, not a declared ArtifactSha256 alone, determines the deployed function. Wait for terminal stack success and verify function code/env, schedule/alarm/role/state and actual GC success. An active buffered backlog with GC off/refusing is a pre-window readiness gap, not solved by preserving a disabled flag or claiming the source is default-off.

Publisher source build/entry commands:

~~~sh
bun scripts/build-canonical-publication.ts /home/khaliqgant/relay-briefs/publication-g2-f940-<UNIQUE>
# On its VERIFIED existing Node24 runtime host, through its reviewed launcher:
node index.mjs --continuous
node index.mjs --gc --continuous
~~~

These are source entrypoints, NOT an invented systemd/SSM deployment command. No actual launcher/service identity was found in the reviewed capture; execution-targets.json must name it before a complete-window approval. The runtime requires AWS_REGION=us-east-1, HOOPDB_DB_MODE=supabase, explicit INGEST_PUBLISH_AWS_ACCOUNT_ID, INGEST_PUBLISH_SUPABASE_PROJECT_REF and INGEST_PUBLISH_INSTANCE_ID, plus bucket, generation, secret ARN, KMS key ARN, instance profile ARN and role name. Preserve existing scoped inputs; do not broaden IAM/KMS/bucket policies or create source/frontier attestations here. INGEST_PUBLISH_ENABLED and INGEST_PUBLICATION_GC_ENABLED are independent switches.

## 7. Migration/phase-2 prerequisites the plan did not establish

The source of inspectMigrations explicitly checks only session_directory_search_worker and session_token_verifier as bare, non-elevated roles. It previews function ACLs and ledger, but does not run every pending migration or every phase-2 verifier. ownerCapture records boundary shape and plan/catalog consistency; it does not add a complete fixed-role bootstrap proof.

| Pending migration(s) | Required role/prerequisite | Source and operational implication |
|---|---|---|
| 020000/022000/023000/024000 | session_directory_search_worker; built-in anon/authenticated/service_role are referenced | Search/token bare-role prerequisite was checked by the plan; full memberships/effective privileges and phase-2 contracts still run during apply. |
| 025000/026000/030000/036000/037000 | bulls96_canonical_feature_producer; existing ingest/seeder principals | 025000 grants schema/journal access unconditionally to producer. 036000 does NOT create a new role named canonical consumer; it extends the existing producer's lease/checkpoint/ack path. Complete read-only phase-2 privilege/member proof and existing-role safety are needed. |
| 032000/038000/039000 | existing bulls96_session_ingest and seeder contracts; revised RPC surface | No automatic role provisioning. 039000 changes GC claim behavior and the source generation; qualify matching runtime behavior and healthy GC. |
| 033000 | bulls96_project_events | Referenced immediately by REVOKE/GRANT. canonical_project_events_verify.sql is a phase-2 step: attributes, membership, approved LOGIN settings, exact three read RPCs, no table/column/sequence rights. Missing role can fail after earlier migrations have committed. |
| 034000 and 043000 | bulls96_read_api | 034000 refuses absent role 42704 and unsafe attributes/memberships 55000. It allows an approved existing LOGIN and only the narrow Supabase creator edge. 043000 grants its title RPC to that existing role. Bare bootstrap is session_read_api_roles.sql; do not reset an existing approved login or memberships silently. |
| 041000 | bulls96_privacy_eraser | Migration explicitly refuses absent role42704 or elevated attributes55000. Provisioned bare by session_ingest_roles.sql; later LOGIN/credentials are a separate operator phase, not this window. Installing the schema does not activate privacy authority or settle erasure obligations. |
| 042000 | bulls96_babysitter_retention, plus read-api/ingest/seeder references | Must exist NOLOGIN, NOINHERIT, not elevated and not a member at install; absent42704/unsafe55000. Do NOT run babysitter_retention_role.sql (LOGIN phase) before this migration. |
| 021000/031000/035000/040000 | existing owner/schema/built-in service roles and the preceding migration prefix | No new dedicated fixed role identified in these migration sources. This does not waive their DDL/ownership/ACL/extension/lock/prerequisite checks. |

The privileged role source is supabase/bootstrap/session_ingest_roles.sql (ingest, seeder, canonical_feature_producer, project_events, privacy_eraser, babysitter_retention) plus session_read_api_roles.sql. Supabase bootstrap accepts only its reviewed privileged postgres context; the temp owner adapter is not an unreviewed grant/CREATEROLE fallback. Never set HOOPDB_ROLE_BOOTSTRAP=true on this Supabase migrator: it is refused by design.

The canonical main verifier list includes search Worker, token verifier, ingest seeder, ingest, and canonical project events. Unified function ACL and phase-2 checks also remain authoritative. Extra READ API/privacy/retention operator verifier/LOGIN stages must be reviewed for their own later activation; do not confuse installing their grants with enabling a service.

Obtain full role/catalog checks BEFORE owner-apply. If a new role is missing, repair under the separately reviewed bootstrap authorization BEFORE marking the generation pending. Do not discover this after the first 17 schema commits.

## 8. Window order, outage estimate, verification and forward repair

W0 (outside outage): finish all holds above, exact full-prefix rehearsal, fresh runtime preimages/captures and uploads/GC baseline; refresh schema plan/capture if expired; obtain the ONE fully bound direct approval.

W1: canonical owner-apply. The first writes can include boundary refresh and readiness pending before individual schema transactions. Require final G2 PASS and no pending inventory. Do not proceed through a refused apply as though PASS were established.

W2: execute the reviewed runtime replacements in the final targets packet: ingest Worker, permanent buffered GC, and generation-bound managed consumers as qualified. Publisher is documented INACTIVE with no redeploy planned under lead disposition244908112388333568. If the release owner discovers a running canonical publisher, require its exact authorized G2 replacement or stop before W3; do not carry a live G1 publisher across rebind. All required G2 artifacts/configs must agree. No first-time publisher activation, feature flips or Cloud landing belongs to this step.

W3: rebind dry run, review exact counts, then execute once using actual G1 and schema G2. No admission grant/expiry renewal or source attestation is manufactured.

W4: bounded post-window verification: G2 PASS, no pending schema, exact wrapper, policy atG2 and zero admissions outsideG2; provider/code/env/immutable artifact readbacks; real existing-scope upload+ACK and canonical readback where qualified; delivery and ingest 2xx back to the measured pre-window level, no generation_not_ready/readiness-generation errors; healthy GC invocations/backlog and publisher/consumer health where previously active. Continue comparing generic 503s separately; the prior limit30 observation was14/199 503, not an all-green baseline. Sanitized natural-traffic diagnostics only; no tenant bodies or invented successful synthetic production ACK.

Duration: the old two-migration runbook estimated10–20minutes, dominated by deploys. That is historical, not a measured bound for21 migrations. No honest exact outage duration is established for this source union. Upload refusals can start when the marker becomes pending and persist until G2 code/config/PASS/policy/admissions align; a failed runtime or verifier can extend the outage indefinitely. Set and obtain acceptance of a measured window budget after the exact rehearsal and dry deploy timing; do not promise the historical estimate. Capture expiry is NOT an outage deadline.

Abort/repair:
- Before owner-apply's first live write: stop if any hold, drift, expired capture, missing role, unready artifact or changed generation appears. No schema rollback is needed if nothing was written.
- Once apply is invoked, even a failure before any schema ledger entry can follow an already committed boundary refresh. Reinspect catalog/ledger, do not assume atomic rollback of the whole operation.
- Individual migrations commit. If a later migration/phase2 fails, retain all receipts and actual ledger/marker state. No manual ledger edits, checksum substitutions, skipped verifier, old readiness PASS restoration, down migrations or broad transaction retry.
- If ledger changed, original OWNER_APPLY.ledger/source evidence refuses reuse. Prepare/review a normal new source pin for the exact inspected partial ledger (schema generation may remain G2 if only operator pin changes), then obtain fresh plan/capture and new exact approval. If only boundary/catalog changed, fresh plan/capture is still required. Resume only the canonical remaining prefix; never rerun the old approval against a partial state.
- Built-in 40P01 retry is limited to bootstrap/verification transactions after full rollback (three attempts with1s/2s backoff); schema migrations and an entire apply are NOT automatically retried. Other errors are not converted to retry permission.
- Once G2 PASS exists, redeploying G1 code or rebinding toG1 cannot restore G1 PASS. Fix forward to the approved G2 cohort. Rebind's transaction rolls back on refusal; a committed G2 rebind is idempotent, not a license to force policy back.
- Preserve permanent cleanup/erasure obligations and owner records. Do not disable GC first, delete stages/receipts, mint frontiers or unpause canonical publication as a workaround.

Cloud first remains the feature cutover path: after this generation window, each feature separately follows the lead's migration/verification, default-off deployment, backfill verification, read-API canary, explicitly approved Cloud companion landing and Cloud flag activation. This schema/runtime window does not authorize that later cutover, the full Neon copy, deleting Neon or changing reader routing.

## 9. Source evidence and handoff

Commands and fields were checked at f9409ccb in:
- src/hoopdb/owner-operator-apply.ts and scripts/schema-release-owner-apply.ts (approval fields, ledger/freshness, capture hash versus schema generation, canonical apply/forward repair).
- scripts/schema-release-owner-operator-plan.ts and src/hoopdb/migrate.ts (existing temporary CLI route, exact source, two-role-only prerequisite inspection, complete migration/phase-2 plan).
- scripts/ingest-generation-rebind.ts and src/hoopdb/ingest-generation-rebind.ts (explicit project input, dry-run transaction, preserved policy/atomic admissions and target PASS).
- docs/private-ingest-operator.md and stacks/cloudflare/session-ingest.ts (retained-plan deploy path, credential isolation, supplied Worker generation).
- scripts/build-buffered-gc.ts, stacks/aws/buffered-gc.template.json and src/session-ingest/buffered-gc-job.ts (compiled assets/G2, new explicit target parameters).
- scripts/build-canonical-publication.ts, publication-main.ts and publication-runtime.ts (Node24 entry, distinct flags, IMDS-only runtime identity, explicit target inputs).
- runbooks/buffered-canonical-drain.md, src/hoopdb/indexer.ts, src/session-pipeline/lambda.ts (additional generation-bound consumers, activation gates).
- Exact pending migration sources and bootstrap files listed in section7.

All artifact and deployment HOLDs are explicit in execution-targets.json. No source/SQL was modified for this draft, and no production write or approval file was created. The attached plan/capture bytes preserve their original SHA256.


## Supporting evidence update (19:23Z)

See [supporting-readiness.md](supporting-readiness.md) and the raw receipt hashes in MANIFEST.json. The fresh census confirms G1 marker/policy/all 1,028 admissions. Four required roles are missing and need a separately authorized exact bare bootstrap before apply; no production bootstrap ran. The full owned PostgreSQL 17.11 fixture passed all 21 schema files and five phase-two verifiers, actual generation rebind and custody ACK checks; apply cost was 2.893 seconds on synthetic data. This does not resolve production data/lock cost or runtime replacement time, and is not a live outage budget. execution-targets.json retains HOLD with explicit missing runtime/health pins and the bootstrap authorization gap. The initial source commands/approval template above remain a draft until those gaps are resolved and a fresh plan/capture is bound.


## Retained runtime history and Worker delta review

See [retained-aws-targets.md](retained-aws-targets.md), retained-aws-targets.json and [worker-compatibility-review.md](worker-compatibility-review.md). Historical sessions-prod wrapper/profile and last retained GC code identity are concrete, but are not fresh provider qualification. The latest retained Nov6 GC direct ZIP code update is newer than its older CFN S3 version; do not conflate them. The retained i-070f91abb67aa60de record is a migration-host provisioning proof, not a publisher deployment; actual publisher launcher/host/deploy receipt remain missing.

The broad Worker delta review cannot certify that every new path is default-off: f940's /v1/turns/batches is registered unconditionally, while C3 authority and Babysitter routing mutations are guarded and inactive under the retained missing flags. Diagnostic and shared transport changes also run without a flag. The detailed report records 106 passing focused tests. Pipeline-lead has accepted the additive endpoint exception as recorded below; execution-targets.json remains HOLD for the other prerequisites. No new production query, IAM/config change, credential acquisition or deployment was performed for this source/history review.


## Lead disposition update — 244908112388333568

Worker compatibility is PASS WITH ONE EXPLICIT EXCEPTION: turns is an accepted tested additive endpoint, with current relayhistory-api TURNS_WRITE_PATH UNKNOWN, accepted risk. Diagnostic logging and S3 version metadata are accepted active-compatible changes; no server gate/source/artifact change is requested. The retained config's alternative custody/privacy/retention/Neon-exit selections remain inactive; default relayhistory custody remains active. This does not convert the original all-new-paths-default-off assertion into a true statement or verify fresh live bindings.

Publisher plan: documented INACTIVE because no live publisher deployment is evidenced; no publisher redeploy in this window. If the release owner finds a running canonical publisher, it must be redeployed with G2 or stopped before rebind, using an exact qualified action and applicable authorization. This is a conditional planning disposition, not a fresh process inventory, proof of actual absence, or authorization to stop a service. Historical instance identity/launcher/IAM remain unverified and must not be invented. Other readiness/approval requirements remain in force.
