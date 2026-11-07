# Approved schema window: committed and verified G2

The direct user approvals were “create the 4 missing NOLOGIN roles” and “schema window approved”. The one role bootstrap committed and verified all 13 prerequisite roles, including the four new NOLOGIN roles, with the nine existing role records unchanged.

The canonical owner apply ran once from frozen source `f9409ccb9e46c3f100cf3017847d8ed62bc716c7`, from 22:23:13Z to 22:31:04Z on 2026-11-07. It returned `owner_apply_verified`, applying all 21 pending migrations and skipping 35 stored migrations. G2 is `e778dbb6cb68bc0e8939797691565db0b1dd8d357d7d790b375036e0070923ce`; the final ledger digest is `502349fe27871cb12de3fb93d8b02c3a1ece691ad500c1d7a6b2176dbed66502`. The canonical verification checked PASS at G2, no pending inventory, and the verified function boundary. Receipt: `approved-window-schema-apply-result.json`, SHA-256 `d690c2b2eaf95f8612bd4b052a54ca87de3c7db7ada53ec00b7aa5926aea25d7`.

The refreshed PLAN/capture and approval metadata are preserved. The actual direct quote is retained; the approval-record timestamp is explicitly the operator's record time, not a fabricated user utterance time. Preapply G1 marker, policy and 1031 admissions were verified before apply; these are historical preapply evidence.

Pipeline-lead and the release owner were notified of committed/PASS G2. The Worker deployment now proceeds through its qualified private-ingest operator, with a fresh metadata bracket. Root must await its G2 deployment/readback before the canonical generation-rebind dry run and execute. Root has not deployed a runtime, rebound admissions, activated a publisher or flipped a feature flag. Upload recovery is not yet verified.

Buffered GC is deferred by pipeline-lead message 244948358983348224. Its generation-checked claim refuses the old generation after G2; no backlog or health assertion is inferred. Publisher remains under the documented inactive disposition.

At the user's explicit “trigger aws access again”, native AWS authentication and fixed GC status succeeded at 22:19:56Z. The session was closed and RAM credentials discarded. No Lambda update was issued; future operator use requires a fresh login. Receipt: `approved-window-aws-user-retry-result.json` (SHA-256 `9816ec74e6ee3a494ea073182364b2c94d4e8410b96064c8138a60eef9998508`).

Earlier plan documents, expired captures and missing-role/runtime records remain historical evidence; this supplement and execution-targets.json describe the current window state. This schema result does not establish complete Neon exit, a healthy upload path, canonical publication, or Brian's readback.

Latest lead report (message 244953681824702464): Worker apply returned `UNKNOWN_HOLD_NO_RETRY`, receipt prefix `ee6f0c6e`. Read-only reconciliation belongs to the release owner. Root has not run either rebind command and holds for qualified readback; no retry or down-migration.

Reconciliation receipt `98b3e9bf` establishes B_NOT_APPLIED_THIS_ATTEMPT: the active Worker remains G1 and this attempt made no Worker upload. Offline replay `4fc9bd57` reproduces a source-map pin refusal before network; the actual map differs only in relative source path prefixes, with identical source contents and mappings. Replacement preparation receipt `6e9c4719` passed at 22:42Z without writes. A coordinated Worker apply/readback remains required before rebind; preparation is not deployment proof.

Current terminal generation state: G2 Worker code/bindings verified by readback `8f046043`; canonical rebind dry run `c59068e1` passed, then one execute `83716c6b` committed at 22:53:23Z. Policy and all 1031 admissions moved. Read-only census `ce0fccaa` at 22:54:00Z confirms marker PASS G2, policy G2/unexpired and 1031/1031 admissions G2 with zero unexpected. Fresh upload sample is requested and pending; no upload-health or canonical-readback completion is inferred yet.

Upload observation completed: acceptance resumed, with 101/116 private and 64/78 public HTTP200, zero observed generation errors, but 12 private and 12 public 503s in the same 120 s sample. The private failures are 8 claim lock timeouts and 4 claim statement timeouts. The full upload fix remains incomplete. One read-only statistics observation succeeded (`4a9fd9da`); nested statement capture is disabled (`track=top`, functions `none`), so it cannot identify the inner blocking SQL. No statistics configuration was changed.

Source-control supplement: reviewed forward-fix guard/driver/bracket/verifier snapshots and exact PREPARE/apply/review receipts are included with source provenance and scan review. Customer-neutral receipt filenames replace editorial customer names. Historical lock SQL 653631a3 is superseded by final 2486bf70; no lock samples are asserted by this source-only inclusion.

Five final-query lock samples completed 23:09:04Z–23:09:45Z (93e2e59a), no retries or writes. Observed ingest-to-ingest blocking with receipt-seat-clock context in one sample, and active claim DataFileRead near five seconds in another. No idle/non-ingest blocker was sampled. See the explicit attribution/sampling limits in post-window-lockwait-final-five-summary.md; a per-user-only cause and full repair are not established.
