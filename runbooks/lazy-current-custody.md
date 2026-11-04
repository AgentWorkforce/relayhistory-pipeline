# Initial lazy CURRENT custody (040550)

Draft source only. Initial uploads retain bounded temporary Neon metadata, receipt and ordered-control reads. No full metadata prewarm or transcript-body seed is claimed. Canonical publication remains separately held.

The owner installs a `lazy-current` policy with exact compiled PASS generation, trusted source identity/epoch, frozen privacy proof, retention contract and issuer. The owner separately initializes the transactional ACK clock above both attested source allocator frontiers. A private authenticated witness may instantiate an org admission; it never asserts that the org has no old history. Closing the retained admission or policy expiry refuses producer/control writes. GC continues after admission closes.

The witness binds tenant, batch metadata, retained-record ordinals, native source processing ranks, current source custody and source control version. Cold users require complete bounded privacy evidence; a first control carries the durable source PRE packet. Subsequent versions require the permanent USER privacy seal. A SQL-derived seal header is required on all private successful batch/control responses. An old binary with the correct generation but without this witness cannot bypass the policy.

A global native-key slot is initialized once and remains authoritative even when empty. Foreign CURRENT custody is refused. Authorized erasure clears current ownership for session-scoped records while retaining initialization; committed NULL-session custody survives reset. Original source rows never reseed initialized slots. New absent keys under an existing origin remain allowed.

Actual first-body decisions are immutable at ACK. Higher revisions replace current custody; equal/lower candidates remain ignored. Native processing rank preserves source ordering within a batch. Neither old metadata custody nor ignored candidates seed the canonical revision ledger.

ACKed GC retention follows immutable accepted decisions and the exact current native seat/owner/privacy, per original ordinal. Preparing objects retain their original attempt/version/reset fences. Replacement masks and outbox CAS are rechecked before swap; healthy records in a mixed group remain unchanged. SQL adds no runtime table grants.

Current evidence: four PGlite groups exercise real SQL plus receiver, old8/B→ignored1/A→eraseB→fresh1/C, NULL-session reset carry, no-op enable retention, mixed-record GC masks, cold reset-zero sealing and old unqualified control refusal. These checks do not establish real PostgreSQL concurrency, versioned S3 retention, source digest parity, workerd capacity or independent signoff. Those remain mandatory before source readiness and admission.

In-batch same-native-key peers follow source0071: conflicting bodies at or above current revision return409; peers strictly below current revision are ignored. This preflight uses the initialized CURRENT custody slot in lazy mode, never the independent canonical S3 revision ledger. Restoring the prior mode function makes the new below-current discriminator fail with baseline_pending.
