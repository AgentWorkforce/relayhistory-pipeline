# Retained AWS targets for the generation window

Reply to pipeline-lead message 244903836907462656; also provided to release owner codex-relay-desktop-find-prior. Source/history only: no new AWS call, credential acquisition or provider write was made. These facts are last-retained observations, not current deployment qualification. Machine-readable facts and all hashes are in retained-aws-targets.json.

## Operator path and identity

Historical Mini wrapper:
~~~text
/Users/khaliqgant/relay-briefs/migration-fast-20261103/gc-apply-20261105/awsx.sh
SHA256 5a02ede8b5f7cdb8077b345932a508af7907247a10422800564e97c7ff65194b
~~~
It invokes aws through env -i, sets AWS_CONFIG_FILE to /Volumes/T7/Projects/AgentWorkforce/artifacts/migration-ops-20261102/pippen-existing-workload-sso.config (SHA256 e7378fae4f4490b5a7ede01519dc8d57dee8cd1a6e41a8526c0ce33eebc69952), AWS_PROFILE=sessions-prod and AWS_REGION=AWS_DEFAULT_REGION=us-east-1. The profile selects SSO account 941433935742 and permission set AdministratorAccess. Its stored profile region is us-west-2, so the wrapper's explicit us-east-1 override matters. It uses an existing SSO cache, not a token embedded in the wrapper.

Root historically invoked this from the Mac mini. I have no retained evidence that Khaliq runs this workflow himself in CloudShell; do not invent a CloudShell requirement. This wrapper is not the latest credential transport: the Nov6 telemetry rollout used fresh same-Mac native RAM PKCE, verified the exact reserved AdministratorAccess identity, and wrote no credential/cache files. Its adapter path is artifacts/phase2-custody-20261105/gc-g2-phase-telemetry-ram-pkce-adapter-20261106/ram-pkce-gc-rollout.py, SHA256 ae5004e88e12e01e1fc234cc7670dfe5832d2cb5042d03e638b3ebaa5016b2ae. That old adapter is scoped to its old code-only tuple, not a generic G2 deployment authorization. No old wrapper/helper has been requalified for today's session, source, role, live preimage or operation.

Last retained caller ARN:
~~~text
arn:aws:sts::941433935742:assumed-role/AWSReservedSSO_AdministratorAccess_267543ee54f72723/khaliq@agentrelay.com
~~~
This is an operator SSO identity, not the buffered-GC or publisher runtime identity.

## Buffered GC: latest retained code differs from the old stack artifact

Stack SessionsBufferedGc, stack ID arn:aws:cloudformation:us-east-1:941433935742:stack/SessionsBufferedGc/41960230-d8bb-11f1-a5d3-0effd0cd1d67; function sessions-buffered-gc; role arn:aws:iam::941433935742:role/96bulls-sessions-prod-buffered-gc; region us-east-1.

Latest retained observed code: 2026-11-06T19:36:52.819980Z, successful direct conditional UpdateFunctionCode (Publish:false), source d41e5d4bd605cef506c48f6b288cf239701c59bd, then-current generation 78c7b28e5ebce7f5f534a1f3f41a55845251e950b6c4c49f676f2bda358c91d6. It observed Active / LastUpdateStatus Successful; it did not establish GC health or fix SQL timeout57014.

~~~text
CodeSha256 Q0Wjh9qijm2wAoZOacdK+vDST2XXMJisEdmC6IJGNaI=
ZIP SHA256 4345a387daa28e6db002864e69c74afaf0d24f65d73098ac11d982e8824635a2
RevisionId 99bf8dce-c792-4c1d-ad10-f563d431a698
Receipt SHA256 a6e39b73c324f4d49ec56d941ba0cdddea0a2118fe3e0f49beec1be027c66b33
~~~

The earlier CloudFormation artifact, observed before the Nov6 code experiments and restored by the 06:27 rollback, was:
~~~text
CodeBucket 96bulls-sessions-prod-gc-artifacts-941433935742
CodeKey buffered-gc/sha256/e104cbf0b7700b33a988b59dd18f4d121aad915947a553c00291d1602578859e/buffered-gc.zip
CodeVersion 9Sjyb1PXOZcVFpO2W99s5cGF.pBYBJMy
ZIP SHA256 e104cbf0b7700b33a988b59dd18f4d121aad915947a553c00291d1602578859e
CodeSha256 4QTL8LdwCzOpiLWd0Y9NEhqtkVlHpVPAApHRYCV4hZ4=
~~~

Do not bind the later Q0Wjh9 code to that earlier S3 version: the last update was a direct ZIP update and its receipt records no corresponding S3 ObjectVersion. Fresh actual function configuration/code identity, CFN template/parameters, enabled state, schedule/role and health remain required. No fresh current CodeSha256 or current S3 version is claimed here. The new f940 template also requires explicit AwsAccountId/SupabaseProjectRef; do not blindly reuse the old template/parameters.

## Publisher host and last deployment: missing evidence

Actual canonical publisher/GC host, launcher/systemd unit/script hash and last deploy receipt are UNKNOWN/MISSING. The retained candidate i-070f91abb67aa60de is from Nov4 EC2 migration-runner provisioning/test-size receipts, not a publisher deployment receipt. Those receipts explicitly say app_enabled=false / application_access_policy=false, and record an expiry at 2026-11-04T07:47:43Z. They do not establish whether that instance is still present or has since been repurposed. The copied ec2-retained-migration-host-proof.json makes this limitation inspectable.

The Nov7 05:57 publication packet likewise says publisherAWSDeployment NOT_VERIFIED and actualArtifactUnitConfigRolePolicyHealth UNKNOWN. Its expectedHostFromSource label is not independent evidence of a live publisher. No literal instance pin is embedded in f940's tracked source; the source runtime requires an explicitly supplied, qualified instance identity. Do not assume this migration host is the service host, invent a systemd unit, or grant it publication permissions from these receipts.

The release owner should qualify the actual runtime inventory before declaring a replacement target, or record a reviewed inactive decision if no publisher exists. Source build success at e778 does not answer that question.


## Lead disposition update — 244908112388333568

Worker compatibility is PASS WITH ONE EXPLICIT EXCEPTION: turns is an accepted tested additive endpoint, with current relayhistory-api TURNS_WRITE_PATH UNKNOWN, accepted risk. Diagnostic logging and S3 version metadata are accepted active-compatible changes; no server gate/source/artifact change is requested. The retained config's alternative custody/privacy/retention/Neon-exit selections remain inactive; default relayhistory custody remains active. This does not convert the original all-new-paths-default-off assertion into a true statement or verify fresh live bindings.

Publisher plan: documented INACTIVE because no live publisher deployment is evidenced; no publisher redeploy in this window. If the release owner finds a running canonical publisher, it must be redeployed with G2 or stopped before rebind, using an exact qualified action and applicable authorization. This is a conditional planning disposition, not a fresh process inventory, proof of actual absence, or authorization to stop a service. Historical instance identity/launcher/IAM remain unverified and must not be invented. Other readiness/approval requirements remain in force.
