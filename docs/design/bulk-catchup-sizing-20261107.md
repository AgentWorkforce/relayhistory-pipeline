# Sizing v1: 049000 code, 050000 attester, 051000 catch-up

Status: planning estimate only. Nothing here authorizes work, applies, grants, flags, ceremonies or deploys.
Unit: engineering days for one owning agent lane, covering implementation, tests, one adversarial and one
contextual review pass per PR (AGENTS.md), fixes, and CI to green. Wall-clock waits on other owners
(independent verifier, reviewers, Khaliq OKs, generation outage windows) are excluded and listed separately.

## Observed scope (inputs)
- 049000 scoped claim (design packet on 47425aca, retained code branch b1283b2e):
  - About 5 tables: owner authority, approved plan registry, stage binding/reservation, frontier attestation,
    and plan admission state.
  - 2 public `_v1(jsonb)` RPCs plus internal helpers.
  - The ordinary claim core gains a reservation check, so 060000 claim-core bodies are replaced.
  - Two-connection tests.
  - The retained code branch is 22 files, +6,210 lines, against an older base. Main has since moved:
    040000-043000 landed and the repo was renamed. Its largest files are frontier-plan.ts (584), the
    publication canary (891), the capture source (414) and the history adapter (422).
- 050000 attester (v3 FINAL + SF1-SF8 + S1-S7):
  - Role bootstrap plus the member-exact verifier (SF2).
  - Evidence, approval, audit and settings tables, with B4 append-only guards.
  - attest, renew, revoke and emergency-revoke RPCs.
  - SF1 hook triggers on control receipts, plus a pending-head refusal.
  - SF3 schema pre-validator and SF4 UTF-16 HOLD vectors.
  - SF5 150 s / 330 s DB-clock checks.
  - SF6 new-trigger rollback plus bootstrap-down.
  - S1 fifth readiness verifier: shared/db.ts interface, derived-schema, and the publication-runtime and
    buffered-gc-job count guards, plus migrate.ts and the mark_pass include, all changed together.
  - S5 PG15/16/17 two-session CI, the S3 runbook section, and the S7 split tests.
- 051000 catch-up (this design):
  - 6 tables, about 13 RPCs, 1 trigger on a 050000 audit table, and the `--catchup` runtime mode.
  - The attended driver CLI.
  - The cohort/custody proof and the resumable prefix proof (co-owner constraints 16.1/16.3).
  - Census and metrics, a LaneManifest, a proof-script extension, and real-PG two-session tests.

## Comparable landed changes (main history)
| change | size | elapsed | notes |
|---|---|---|---|
| 036000 consumer lease (#254) | 15 files, +1,117 | same day | closest shape to 051000 lease/checkpoint/dead-letter tables |
| 039000 | 10 files, +908 | ~1 day | single-lane SQL + tests |
| 060000 canonical publication | 30 files, +2,582 | multi-day | claim/publish protocol that 051000 drives |
| 041000 (#289) | 41 files, +5,819 / -102 | assigned 11-06 16:37, merged 11-07 | multiple review rounds |
| 042000 (#294) | 44 files, +6,327 | ~1 day agent wall-clock | rollback-refusal pattern reused by 050000/051000 |
| 040000 / 043000 | +207 / +312 SQL | < 1 day | small guarded migrations |
Agent wall-clock for a ~6k-line security-sensitive migration has been about 1 day plus review rounds. The
estimates below add margin for: novel trust-boundary work (role bootstrap, receipts), cross-repo verifier
coordination, and the two-pass review on every fix round.

## Estimates (engineering days)
| piece | low | likely | high | main drivers of the range |
|---|---|---|---|---|
| (a) 049000 scoped-claim code | 6 | 9 | 14 | rebasing 6.2k lines onto current main; claim-core replacement; dual-generator and two-connection tests; receipt-seat producer still missing (capture stays hold/scan_incomplete) |
| (b) 050000 attester (v3 + SF1-8/S1-7) | 8 | 12 | 18 | role bootstrap and member-exact verifier; S1 fifth readiness verifier composition; SF1 hooks; SF3/SF4 vectors; PG15/16/17 CI; excludes independent-verifier work by its own owner |
| (c) 051000 catch-up | 7 | 10 | 14 | lease/fairness RPCs (036000-like); 4.4 trigger coupling to 050000 names; cohort and prefix proofs; runtime `--catchup` mode and driver; proof-script extension |
| total, serial | 21 | 31 | 46 | |

Review overhead included above:
- Each PR gets one adversarial and one contextual pass, about 0.5-1 day each including fixes. That is about
  1.5 days per PR in the likely case.
- Each piece is assumed to need 1 PR plus 1 consolidated fix round, per the review cadence (one thorough
  flow per logical change).
- The high column assumes a second fix round with both passes repeated, because the diff changed materially.

## Critical path
1. 049000 code lands first: it is the source of the planner codec and frontier evaluator, and S2 gates 050000
   on it.
2. 050000 is next, because it is the only frontier writer and 051000's trigger binds to its audit/approval
   tables (Q7).
3. 051000 is last. Its design, tests and runtime mode can be built in parallel with (b) once the 050000 table
   names freeze, which saves about 3-4 days. Its migration still lands after 050000 in registry order.
- Parallel-adjusted likely path: about 9 + 12 + 6 = 27 engineering days. Low is about 18, high about 40.
- Outside-the-lane gates, not counted above:
  - the independent verifier (separate owner);
  - B1 recovery-import evidence for S4 (~12.9k pre-switch ACKs) and the DLQ cohort;
  - Khaliq own-words OKs for each apply, flag and canary step;
  - one generation outage window if 050000 and 051000 apply together;
  - lead decisions Q1/Q2. Without them, 051000 cannot outpace ingest, so building it before Q1/Q2 are answered
    risks inert work.

## Assumptions
- One owning agent per piece, CI available, no repo-rename or base churn beyond today's rate.
- Estimates exclude the canary/rollout calendar time: a measurement canary of about 1 day, then about 22 h of
  drain at R = 14k/h if Q1 is answered yes.
- Synthetic test counts are mechanics evidence only, never production coverage. fd3f vectors and bigint
  fixtures stay frozen, so no re-baselining effort is included.
- Sizing confidence is low-medium. The largest uncertainties are rebasing the 049000 branch and the S1
  readiness composition.
identifiers-check: 0 (counted with the three-pattern grep from the brief over the Supabase, AWS and Neon host suffixes; the 12-digit pattern is excluded because 14-digit migration timestamps are false positives)
