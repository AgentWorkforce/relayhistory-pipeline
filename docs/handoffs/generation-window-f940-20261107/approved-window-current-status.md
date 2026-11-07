# Approved schema window: committed and verified G2

The direct user approvals were “create the 4 missing NOLOGIN roles” and “schema window approved”. The one role bootstrap committed and verified all 13 prerequisite roles, including the four new NOLOGIN roles, with the nine existing role records unchanged.

The canonical owner apply ran once from frozen source `f9409ccb9e46c3f100cf3017847d8ed62bc716c7`, from 22:23:13Z to 22:31:04Z on 2026-11-07. It returned `owner_apply_verified`, applying all 21 pending migrations and skipping 35 stored migrations. G2 is `e778dbb6cb68bc0e8939797691565db0b1dd8d357d7d790b375036e0070923ce`; the final ledger digest is `502349fe27871cb12de3fb93d8b02c3a1ece691ad500c1d7a6b2176dbed66502`. The canonical verification checked PASS at G2, no pending inventory, and the verified function boundary. Receipt: `approved-window-schema-apply-result.json`, SHA-256 `d690c2b2eaf95f8612bd4b052a54ca87de3c7db7ada53ec00b7aa5926aea25d7`.

The refreshed PLAN/capture and approval metadata are preserved. The actual direct quote is retained; the approval-record timestamp is explicitly the operator's record time, not a fabricated user utterance time. Preapply G1 marker, policy and 1031 admissions were verified before apply; these are historical preapply evidence.

Pipeline-lead and the release owner were notified of committed/PASS G2. The Worker deployment now proceeds through its qualified private-ingest operator, with a fresh metadata bracket. Root must await its G2 deployment/readback before the canonical generation-rebind dry run and execute. Root has not deployed a runtime, rebound admissions, activated a publisher or flipped a feature flag. Upload recovery is not yet verified.

Buffered GC is deferred by pipeline-lead message 244948358983348224. Its generation-checked claim refuses the old generation after G2; no backlog or health assertion is inferred. Publisher remains under the documented inactive disposition.

At the user's explicit “trigger aws access again”, native AWS authentication and fixed GC status succeeded at 22:19:56Z. The session was closed and RAM credentials discarded. No Lambda update was issued; future operator use requires a fresh login. Receipt: `approved-window-aws-user-retry-result.json` (SHA-256 `9816ec74e6ee3a494ea073182364b2c94d4e8410b96064c8138a60eef9998508`).

Earlier plan documents, expired captures and missing-role/runtime records remain historical evidence; this supplement and execution-targets.json describe the current window state. This schema result does not establish complete Neon exit, a healthy upload path, canonical publication, or Brian's readback.
