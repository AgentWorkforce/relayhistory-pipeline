# 049 source/readiness packet against current main

Qualification: SOURCE PREPARATION ONLY. No implementation, build, production capture, owner write, publisher activation, route change, credentials lookup or provider action is qualified by this packet.

## Exact source tuple

- Current GitHub main: `f6ccc2f8e0f4e839f197e333c9ac5fb8d5377038` (042000 landed; no claim about applied production migrations).
- Retained tested offline source: `b1283b2e6c651ea6c2a80b90287e3c0d92ee343b`; previous 24 tests / 101 assertions belong to that old tuple only.
- Canonical 049 design: `95ec68550602925158aa48f9fa73e4d24e0e6b4c`, D1-D10. Its trusted producer and executor questions remain open.
- Merge base: `8741efddbf0836146903f8cc452fe7ca71592ace`.
- Unqualified resolved merge TREE: `1c549604378afa95fb1939d72ea2d71801effe9e`. This is not a commit, branch, installed build or a passing integration test.
- One textual conflict: `src/session-ingest/__tests__/publication.postgres.test.ts`. Keep main's bootstrap array including `session_read_api_roles.sql`; all auto-merged canary additions remain. The artifact contains that resolution and the full candidate patch. Shared worktrees, indexes, branches and refs were not changed.

The source-pin JSON binds 16 exact file blobs and SHA-256 digests. `candidate-main-reconciliation.patch` is preparation for review/testing, not a patch approved to apply to a real database. `git diff --check` passes for the candidate tree; no post-reconciliation compiler, runtime or database suite has run.

Main advanced from `09a1dac574e1cd1c08d56cfa0fe256764c47f7a6` to the pinned snapshot above during preparation (#306 diagnostic-only classifier). This source merge is not deployment, repaired uploads or readback evidence.

## Current-main facts that change the review

1. Main has registry slots 040000, 041000 and 042000. 043000-048000 are not on this main snapshot. Reserved 049000 cannot land before that contiguous prefix. Do not add its SQL now or edit historical 040600.
2. 040600's `ingest_publication_claim` SQL is byte-identical at b128 and main. It is the current source definition, not proof that its caller/helper graph or the managed deployment is unchanged. Retrace the effective body again after the remaining prefix lands.
3. Main's 039000 is the effective GC claim replacement, absent at b128. It tries at most four candidates, uses a GC-only advisory admission hint and NOWAIT acquisition of fence owner/batch/intent/outbox within a subtransaction, and performs at most one complete retention decision per call. This supersedes the old 051000 multi-candidate blocking-loop argument. Child/outbox accesses and the ordinary/scoped/GC interaction still need the actual integrated two-connection proof; no blanket no-cycle claim transfers from either version.
4. The generated source ACL is exactly: `ingest_receipt_seats_for_stage(uuid)` owner-only; `ingest_seed_scope_capture(jsonb)` owner + seeder; ordinary publication/GC claim owner + ingest. Those are source manifest facts, not a new inspection of deployed grants. The current capture role cannot become a receipt reader by convention.
5. Publication runtime source defaults for `INGEST_PUBLISH_ENABLED` and `INGEST_PUBLICATION_GC_ENABLED` are false. Actual deployed values, publisher health/artifact, consumer route and current managed permissions remain UNKNOWN.

## Missing real receipt/source producer contract

The retained `composeCanonicalFrontierCapture` emits `receiptDecisions: []` and no receipt scan. The evaluator requires a real `receipt-decisions` scan (`first-ack-seat-decisions-v1`). Keep `hold/scan_incomplete`; do not edit the empty list into an invented terminal scan.

A future narrow producer design must specify these facts before an implementation or live request:

| Boundary | Required reviewable output |
|---|---|
| Scope/authority | Exact owner-approved plan, tenant/org/workspace/user/origin and target/generation; producer/installer principal and provenance. File hashes and an owner label do not authenticate a capture. Shared-role executor identity beyond the agreed boundary remains unresolved. |
| Capability | A restricted read capability or trusted claim-evidence path that can enumerate the exact receipt/stage domain. No owner-credential fallback, direct table grants or broadening of the seeder is assumed. Installation/activation is a later separately authorized operation. |
| Coverage | Complete independently bounded stage/receipt enumeration with stable key ordering, explicit final empty page and origin inventory. A list of recently uploaded batches is not the full domain. |
| Scan proof | Exact query/adapter version, snapshot, page counts, terminal cursor, item/byte/work limits, page-chain/items hashes and matching opening/closing authority/privacy state; no caller-supplied complete=true or zero count. |
| Decision rows | Actual `FrontierReceiptDecision`: tenant, recordId/revision/revisionId, source/sessionId, seat, bodyAccepted, candidateDigest, firstDigest. Include losing equal-revision candidates; duplicate accepted bodies hold. Bind decisions to immutable accepted maxima/ordinals and verified export bytes. |
| Freshness/install | Bind source identity/generation, capture/evidence/plan hashes and target; reject stale, revoked, mismatched or missing evidence. Owner installation of approved-plan bytes is distinct from producing and admitting current evidence. No synthesized attestation rows. |
| Safety | Read-only bounded transactions; no external await under DB locks; no claim, publish, replay, GC or mutation hidden in a capture adapter. |

The TS shapes are pinned in frontier-plan.ts; the authoritative DB read/producer and attestation installer are not built. This is a concrete producer contract checklist, not a grant or a claim that such evidence exists.

## Admission/interlock design and unexecuted test matrix

Retain D1-D10's immutable owner-installed raw JSON plan (digest over exact UTF-8 JSON.stringify bytes), no runtime self-registration, no public reserve/release/replan capability, durable binding history and no expiry-based release. Use RESTRICT relationships; a GC/ACK row-version change vetoes a stale member and does not erase its reservation obligation. Explicit held/outside-scope members reserve nothing and cannot become implicit eligible work.

For multi-member admission, propose existing-only NOWAIT row locks in the reviewed deterministic order, including installed plan/member/owner/frontier/attestation rows, with all veto checks before the first reservation. Do not call an insert-capable fence helper to manufacture a missing fence. Missing required rows are readiness failures. The actual integrated common-core first-write point must be traced; no wrapper pre-locking that inverts ordinary order.

NOWAIT covers tuple acquisition, not every possible relation/unique/FK/trigger wait. Require a reviewed per-session call envelope (proposed lock_timeout=250ms and statement_timeout=3s set BEFORE the RPC), proof of FK/unique/trigger wait edges, and an exact error map. 55P03 may map to a concurrency refusal; 57014 is a distinct bounded failure, not an automatic retry or release. The values and final lock implementation remain design output, not deployed settings.

Required final-base tests (not executed by this packet):

- Ordinary-first/scoped-first same-stage races: one immutable binding/ticket, no bypass through ordinary claim; unrelated ordinary work continues and its public grant/signature stay unchanged.
- Multi-user admission versus the ACTUAL 039000 GC, both orders: bounded refusal/no deadlock and zero durable reservations on whole-plan veto; stale GC/ACK member refuses later claim without forgetting obligations.
- Plan/owner retirement and attestation revocation versus admission/claim in both orders, with protected terminal history and no plain reread authorization race.
- DDL relation-lock contention bounded by the call envelope; timeout leaves no reservation/attempt/head effect.
- Crash, exact replay, expired ticket, changed plan/evidence/target/generation, lost ACK and in-flight remote effect: no implicit reacquisition, adoption, release or new-plan escape.
- Both ACL generators in both orders, comparing complete outputs and --check; replacements/down use the actual effective prior body after prefix composition.
- Real managed-role fixture, real producer completeness/empty scan, and consumer readback that preserves Amp continuity. Synthetic/compiler READY never substitutes.

## Source preparation sequence

1. Review the candidate conflict resolution and full patch against the pinned main. Reconcile again if main moves.
2. Materialize a normal isolated source branch/worktree under the applicable disk/dependency rules; validate the combined runtime with focused offline/compiler tests, typecheck and actual SQL races. The virtual merge and old b128 tests do not satisfy this step.
3. Resolve and review the producer/capability and trusted installation/executor design; implement only the reviewed source surface. Keep absent evidence fail-closed.
4. After 043000-048000 land, retrace all effective replacement/rollback bodies and generated boundary, then build/review the 049000 implementation at that final base.
5. Only then prepare a separately reviewable live operation naming authoritative source/target/runtime/policy preimages, readback and rollback conditions. There is NO live operation ready in this packet.

Last live evidence remains the timestamped 05:27 buffered receipt and 05:53 missing-frontier/head packet; neither is refreshed here. Current publisher/consumer/routing remains unknown. Preserve the verified Amp selected-thread path; no global workspace reader change or ambiguous outgoing MCP retry.

Packet manifest SHA-256: `ee12b50e6785883028de34e6fc6088a14a9551dcd6d10a2d2f3a7502b0aedcbb`. Candidate patch SHA-256: `e9529b4bf0e16d000a113dacb82bbfd9b5fd81366c8f07d66eca4d2320946ef1`. The patch is a retained source artifact, not applied or runtime-qualified.
