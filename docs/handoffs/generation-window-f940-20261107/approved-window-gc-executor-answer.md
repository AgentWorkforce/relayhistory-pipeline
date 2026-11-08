# GC executor answer and schema refresh — current availability

Reply to pipeline-lead `244944402938490880`. Recorded 2026-11-07T22:16:12.686780+00:00. The user directly approved this schema window in root's chat; no new approval question is pending here. **Do not open the upload-refusal window until a usable GC executor and the exact G2 update path are qualified.**

## Fresh schema bindings (complete)

Execution source `f9409ccb9e46c3f100cf3017847d8ed62bc716c7`; G2 `e778dbb6cb68bc0e8939797691565db0b1dd8d357d7d790b375036e0070923ce`.

- Fresh PLAN: `95d9012c2183758fd4a81a05f887a8b395c4287e1b9ef37f7d9e7f55a3c7554e`, at 22:02:57.580Z.
- Fresh capture: `695aa5bfcf18646aabf9081c6028662017d42d94d0e4629a283e5b215f479c50`, at 22:04:06.494Z.
- Same 21 pending names/checksums (020000–043000), ledger `3173f2df7f749fd79c340286f3284d91d8f5a8b988b7f1b1a11e01c3a3307641`, ACL `d953e10c3e9c519e559d2728c0665de946ba223c952da1dd9c1ba3e28a3b7aad`.
- Earliest expiry 23:02:57.580Z. Runtime G2 is not capture generation `c9bc7700eaf8b9aeacf15bca9c0a7a781c30ceccbffc5b3286c602623b1b7042`.
- Four roles committed/verified; existing nine exactly unchanged. Bootstrap receipt `c344b597b2bc31b3f79a7ae183bf985a93ba41e07449adbe59d4f337e14e850e`; role verification `15b01a1d79e4723fe8e636aef5b54111010f0636afd68a581174652fafc3b9a1`.

## Exact existing AWS selector (retained, files rehashed now)

Host: this SF-Mac-Mini, root operator. Wrapper:
`~/relay-briefs/migration-fast-20261103/gc-apply-20261105/awsx.sh`
SHA-256 **`5a02ede8b5f7cdb8077b345932a508af7907247a10422800564e97c7ff65194b`**.

It runs an empty environment plus HOME/PATH and:
`AWS_CONFIG_FILE=/Volumes/T7/Projects/AgentWorkforce/artifacts/migration-ops-20261102/pippen-existing-workload-sso.config`, `AWS_PROFILE=sessions-prod`, `AWS_REGION=us-east-1`, `AWS_DEFAULT_REGION=us-east-1`.

Config SHA-256 **`e7378fae4f4490b5a7ede01519dc8d57dee8cd1a6e41a8526c0ce33eebc69952`**. The selected profile fields are `sso_session=agentworkforce`, account `941433935742`, role **`AdministratorAccess`**. The profile file's ordinary region is us-west-2; the wrapper explicitly overrides resource requests to **us-east-1**. Do not confuse the IAM role name with the profile/session label.

Last retained caller, Nov 6 (not freshly authenticated now):
`arn:aws:sts::941433935742:assumed-role/AWSReservedSSO_AdministratorAccess_267543ee54f72723/khaliq@agentrelay.com`.

The last real GC rollout used RAM-native SSO, not human CloudShell: `gc-g2-phase-telemetry-ram-pkce-adapter-20261106/ram-pkce-gc-rollout.py`, SHA **`ae5004e88e12e01e1fc234cc7670dfe5832d2cb5042d03e638b3ebaa5016b2ae`**. It enforces this account/role and avoids credential files. This is an established authentication route, not a claim of present availability or a G2 deployment package.

## Current availability checks (no Lambda mutation)

1. One bounded existing-session/RAM status-only attempt at **22:10:52–22:10:53Z** refused **`cache_expired_or_expiring`** before authentication/STS/GC status. No login or cache write. Receipt **`8e7cbeb13c21674bca458cdc1091db7482037b87b602a0ccf5dad412bc738587`** (`approved-window-gc-existing-session-result.json`).
2. One bounded native RAM-only login attempt, with fixed requested actions `status`, `exit`, at **22:12:46–22:14:16Z**, timed out after 90 s. No qualified status response was retained; current caller/CodeSha/RevisionId remain UNKNOWN. No apply/rollback action or Lambda update was requested. Authentication calls may have occurred; do not read the missing status receipt as proof of zero provider reads. No credential files were written. Receipt **`04e8353ad907d8f796ea98cf98b11bb9bcd9218719f460a10f05e24ef4dc8231`** (`approved-window-gc-native-status-result.json`). Attempt stopped; no retry loop or active operator remains.

**CloudShell is not inherently required.** This Mini has the established RAM-only path but is not currently authenticated/qualified. The executor must either complete this existing SSO route or be another already-qualified AWS operator. If Khaliq uses CloudShell, its actual account/role and fixed target/preimage still need qualification; historical Mini profile/ARN does not attest CloudShell. No new IAM/grant/credential provisioning is proposed.

## Exact G2 GC deployment still needs binding

Target is `sessions-buffered-gc`, stack `SessionsBufferedGc`, account941433935742/us-east-1, runtime role `96bulls-sessions-prod-buffered-gc`. The old direct code update means CloudFormation's old S3 code pointer is not the live code preimage. Last retained live CodeSha is `Q0Wjh9qijm2wAoZOacdK+vDST2XXMJisEdmC6IJGNaI=` / RevisionId `99bf8dce-c792-4c1d-ad10-f563d431a698` (Nov 6); neither is freshly verified by these failed checks.

The older RAM rollout helper is **code-only and pins the old G1 telemetry ZIPs**. Do not send it `apply` for this window or silently substitute payloads. A current executor must bind the full f940 ZIP SHA/size (lead prefix09ff68e1), its build proof, fresh live CodeSha/RevisionId and the configuration change preserving every unrelated setting.

Source f940 `src/session-ingest/buffered-gc-job.ts` requires **both compiled G2 and `INGEST_GC_EXPECT_GENERATION=G2`**, and explicit `INGEST_GC_AWS_ACCOUNT_ID` / `INGEST_GC_SUPABASE_PROJECT_REF`. Reconcile the actual live environment (without persisting secret values), target fields and enablement; do not assume code-only replacement establishes G2 readiness. The role/bucket/KMS/secret/timeout/memory/schedule/enabled state must be preserved. New update code or a normal deployment capture must be reviewed before mutation.

Root requested the exact candidate ZIP/build receipt and any reviewed G2 code+configuration updater from the release owner. No schema migration, generation rebind, Worker deployment, GC update or publisher activation has been performed here. User authorization is retained; the open dependency is executable runtime readiness and authentication.
