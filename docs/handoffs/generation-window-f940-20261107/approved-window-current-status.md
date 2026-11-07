# Direct approval, completed role bootstrap and refreshed schema window

Recorded 2026-11-07T22:05:31.909638+00:00. Direct user words in migration-handover-agent's chat: “create the 4 missing NOLOGIN roles”; “schema window approved”. No peer quote was used as this authorization.

The four-role bootstrap **committed once and was verified** at 21:59Z. Read `approved-role-bootstrap-observation.md` and `approved-role-bootstrap-verification.json`: all 13 prerequisite roles are present, all four new roles are NOLOGIN/non-inheriting/non-elevated with no outgoing memberships/settings, and the nine existing role records are exactly unchanged. Only the expected Supabase creator-admin edges appear. No explicit GRANT or existing-role ALTER was run.

A fresh canonical existing-owner PLAN at **2026-11-07T22:02:57.580Z** and read-only capture at **2026-11-07T22:04:06.494Z** succeeded. Exact source, generation, 21 pending names/checksums, starting ledger and ACL preview are unchanged from the reviewed prefix. The old generated capture was preserved before cleaning this owned checkout for PLAN; no user edits were reset. The earlier expired artifacts remain historical evidence.

- Execution source: `f9409ccb9e46c3f100cf3017847d8ed62bc716c7`.
- Schema G2: `e778dbb6cb68bc0e8939797691565db0b1dd8d357d7d790b375036e0070923ce`.
- Starting ledger: `3173f2df7f749fd79c340286f3284d91d8f5a8b988b7f1b1a11e01c3a3307641`.
- Fresh PLAN: `95d9012c2183758fd4a81a05f887a8b395c4287e1b9ef37f7d9e7f55a3c7554e` (`approved-window-refresh-readonly-plan.json`).
- Fresh capture: `695aa5bfcf18646aabf9081c6028662017d42d94d0e4629a283e5b215f479c50` (`approved-window-refresh-capture.json`).
- Capture generation (not runtime G2): `c9bc7700eaf8b9aeacf15bca9c0a7a781c30ceccbffc5b3286c602623b1b7042`.
- ACL preview: `d953e10c3e9c519e559d2728c0665de946ba223c952da1dd9c1ba3e28a3b7aad`; wrapper: `09b1d7804612f38fc121106004752348d56d8781930b66e840e1ef71391c4808`.
- Earliest refreshed expiry: **2026-11-07T23:02:57.580000Z**.

The schema window authorization is recorded. Current runtime preimages, qualified deploy paths and synchronization with pipeline-lead/release owner remain to be reconciled before schema apply. Root asked lead for the latest closed execution-targets/runtime status and requested no duplicate actor. The local execution-targets draft still contains unresolved runtime/health slots; old inactive-publisher assumptions must not substitute for the latest coordinated state. Do not start a window that strands the old generation without its qualified replacement/rebind path.

No canonical apply approval.json, schema apply, generation rebind, runtime deploy, publisher activation or feature flip has been performed by this preparation. The next execution must use fresh current binding records and the current approved window sequence, preserving this direct user authorization and actual capture times. Nothing in the old proposed English template was fabricated as the user's words.

These are the current supplements to `generation-window.md`; its original 18:47 capture/role-missing statements describe earlier preparation. `execution-targets.json` now points to the completed role proof and refreshed plan/capture, while retaining unresolved runtime qualification.
