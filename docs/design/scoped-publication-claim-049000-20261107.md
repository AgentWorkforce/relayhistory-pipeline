# Scoped publication claim 049000: source design contract

Status: DESIGN ONLY. This document neither implements nor authorizes a migration, frontier installation, receipt export, publication, backfill, deployment, IAM or routing change. No production adapter exists yet. Preserve the working Amp route and its coverage.

## Base and integration

Retained source is commit `b1283b2e6c651ea6c2a80b90287e3c0d92ee343b`, parent `88d29a7ccaf3d8811dfd5fe90444a5eccd10f2c1`, published on `codex/copilot-frontier-offline-hardening-20261107`. It includes the retained canary/frontier source and reviewed strict offline-input hardening. Its narrow review does not sign off the whole retained branch for production.

That retained branch predates current main and lacks later registered migrations. Use it as an exact DESIGN reference; do not reconstruct or silently rebase it. Root (`migration-handover-agent`) owns reconciliation with main and final source review. A normal reviewed merge/composition must precede production implementation qualification. A draft based here is not eligible to land migration 049000 as-is.

Only one slot is reserved: `supabase/migrations/20261105049000_scoped_publication_claim.sql` and its matching `supabase/rollbacks/20261105049000_scoped_publication_claim.down.sql`. Land only after the contiguous 041000-048000 prefix. Applied `20261104060000_ingest_canonical_publication.sql` is immutable; do not edit it or guess another slot. No competing SQL branch is requested by this document; the current task is the design proposal below.

## Existing authoritative source

Read the exact-base `src/session-ingest/publication-canary.ts`, `frontier-plan.ts`, `publication-protocol.ts`, `publication-rpc.ts`, `docs/runbooks/canonical-publication-canary.md`, `docs/canonical-frontier-plan.md`, `scripts/proof-publication-canary.ts` and associated tests.

`src/session-ingest/__tests__/fixtures/publication-canary-guard.sql` is TEST ONLY. It is evidence of proposed semantics, not a production migration or an approved grant change. In particular its global revoke of the ordinary ingest claim must not be copied into production.

## Seven binding and boundary requirements

1. **Owner and scope.** Existing `ownerId` is a bounded plan string, not an authenticated DB principal or a bearer capability. A binding includes owner, plan ID/digest, stage token, stage tenant/user/origin, the exact frontier primary key, target and generation. The design must identify trusted DB/caller authority for installation and ownership; a caller-provided owner string alone cannot confer it.

2. **Plan and evidence.** Preserve `PublicationCanaryPlan` and `publicationCanaryPlanDigest`: SHA-256 of `JSON.stringify(plan)`, not SQL JSONB reserialization. The plan binds version, UUID plan ID, owner, creation/expiry (at most 24 hours), tenant, target/generation, complete bounded stage list (including user/origin/hash/bytes) and all budgets. Frontier A separately binds capture ID, evidence SHA, plan SHA and `CanonicalFrontierRow`: org/workspace/user/origin, source frontier SHA, record maxima SHA, validity, bucket/region/key root/schema generation. Trusted DB inspection additionally binds accepted ordinals/group-ordinals hash, reset epoch, intent/outbox versions and privacy/admission state. Existing final source/head/pointer CAS is mandatory. File-supplied hashes show candidate consistency, not authenticity. The real receipt-seat/source producer is missing: no empty scan or invented evidence; capture stays `hold/scan_incomplete` until it exists.

3. **Replay and concurrency.** Retained fixture binding is per stage token with immutable exact owner/plan/evidence identity. Exact replay with a still-valid ticket returns that same attempt/ticket; mismatched binding or expired ticket refuses without reacquisition. Concurrent calls may install at most one binding/attempt. Propose explicit terminal/resume states, and causal races in both orders. No retry nonce may bypass the stage binding.

4. **Ordinary claim continuity.** Keep `sessions.ingest_publication_claim` callable with existing grants and behavior for unaffected ordinary work. Do not globally revoke the shared role. A scoped-reserved stage must nevertheless be protected against bypass through the ordinary path: use one atomic DB reservation/interlock with the same protected state and locks. A wrapper check, extra SELECT or operator convention is insufficient. Test both scoped-first and ordinary-first races without stopping unrelated work.

5. **Durable storage.** Propose an RLS side ledger for immutable stage/owner/plan/evidence binding and the original ticket, rather than ownership only inside expiring/replaced attempt JSON. Table schema and reservation lifecycle still require review. No direct runtime table grants. Preserve obligations and reservations over crashes, expiry, retry and rollback; absence must not create adoptable authority or forget late effects.

6. **Caller ACL.** Minimum proposed runtime EXECUTE surface is `bulls96_session_ingest` only; no `service_role`, PUBLIC, anon or authenticated expansion. Internal helpers have no direct grants. This runtime grant does not authorize installing trusted frontier attestations or owner authority. If a distinct scoped runner role is necessary, propose its exact bootstrap/verifier/lifecycle impact before code; no role activation is implied.

7. **Lease and refusal.** Reuse the ordinary `PublicationTicket` lease/parser/heartbeat protocol. Do not invent a separate scoped lease or reacquire an expired attempt silently. Intended split follows existing guards/fixture: malformed binding/expiry `22023`; authorization `42501`; generation/readiness `55000`; stale evidence, owner-plan conflict or concurrent change `40001`. Freeze each production mapping in the eventual reviewed SQL/tests, with zero mutation on refusal. These requirements do not assert that an unimplemented function already guarantees them.

## Inspection and claim interfaces

Existing read-only `inspect` input is exactly `{planId, ownerId, planDigest, stageToken}`. It does not claim or lease. Its trusted response binds the stage identity, evidence digest, eligible/hold decision and Frontier A readiness. All planned stages are inspected before any claim; out-of-tenant work must be an explicit `outside_canary_tenant` hold. Falsely eligible cross-scope work vetoes the entire plan.

Existing `claim` adds `evidenceDigest` and `frontier: {captureId, evidenceSha256, planSha256, row}`. It must atomically recheck trusted inspection evidence and bind owner/plan before returning the exact same binding plus the normal ticket. Preserve current lock ordering, privacy/fence/receipt guards and final source/head/pointer CAS; no external network awaits inside the DB lock transaction.

## Generated function boundary

A separate proposed prefix `sessions.ingest_scoped_publication_` is sensible design input. The retained legacy sync-ingest-publication-boundary script deletes `sessions.ingest_publication_*` ACL keys, so do not place unrelated new generated signatures there accidentally.

Use the unified `scripts/sessions-function-boundary` LaneManifest/generator and registry, `src/hoopdb/sessions-function-acl.json`, boundary wrapper, migration guards and per-role verifiers. Declare all new and replaced signatures, SECDEF attributes and exact grant sources. Fixed SECDEF search path is `pg_catalog, pg_temp`; qualify objects, close input shape and enforce existing caller/generation guards. Test both generators in sequence so one cannot erase the other's entries. Generated output is not a hand-edited ACL exemption.

Rollback must refuse while durable authority/obligations remain, honor later-migration ordering and restore any replaced function/ACL from its authoritative prior version. Preserve the applied historical files.

## Requested design result

Provide a concrete proposal against these exact interfaces: authenticated installation/owner trust, reservation and ordinary-path interlock, state/replay/expiry lifecycle, table/function/role map, generator composition, and offline plus genuine two-connection negative-test matrix. Distinguish retained synthetic proof from real producer/provider evidence. Root resolves main composition and reviews the final implementation tuple. No production SQL, source capture, export, replay, publisher activation or routing action follows from this design document.
