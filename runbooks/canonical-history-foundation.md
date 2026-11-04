# Canonical History authority foundation — HELD

This checkpoint is source for review, not a complete canonical publisher or an
activation instruction. Initial buffered ACK/retention deployment is a separate
release. Ordinary040600 remains its provenance; this620 child also composes
current550 receipt custody,184 large replay pages and187 analysis foundations.

## Implemented

Physical inventory captures ALL references to a carrier, including foreign and
tombstoned pointers, before accepting native-key positions. Pages have explicit
totals/terminal cursors. A pointer mutation changes the inventory epoch; a lease
cannot renew after expiry, including a blocking lock wait. Unknown layouts stay
unscannable rather than breaking existing indexer writes.

The byte-preserving rewrite verifies existing chunk digests/lengths, removes all
versions of the addressed origin/native keys in one pass per carrier, and keeps
unrelated, independent-origin and NULL archival records. It uses the shared raw
frame codec without a second scrub. Every new object intent precedes PUT.

Logical consumers share the existing replay epoch and native PostgreSQL JSONB
source fingerprint. Immutable COW mask pages retain explicit erased keys;
missing keys are unknown, never empty. One changed record updates only its leaf
and ancestors. Current target lifecycle/accepted seats dominate an old raw MAX.
Source-owner and native changes invalidate consumers even with identical physical
pointers. Reconcile obligations survive the new stage/body being GCed.

Model continuations carry compact references. Exact source lock permits couple
registered encrypted summary/state publication to the same source transaction.
Short/long final pointers must use summary-short/summary-long; bob-slice is an
intermediate only. Busy scope raises40001; stale authority returnsNULL/false.
Only the publisher ingest role receives new620 EXECUTE grants. Proposed630 adds
its separately reviewed function-only model role; no model table grants.

Managed replay uses the authoritative canonical fold with its captured winner
mask, registers all artifact objects before PUT, verifies their bytes and
publishes the index through a qualified source/optional large-job lease CAS.
Unqualified managed replay publication refuses. Default Worker raw replies stay
2MiB; trusted Node raw consumers can hydrate one bounded64MiB row using the same
codec and reader-owned ordinal cursor. Exact raw JSON integers use the existing
canonical codec through materialization and trusted Node reads, including nested
signed64-bit values.

Managed large-job discovery/claim/heartbeat/retry are generation-bound through
one function-only RPC. The old physical claimant excludes managed logical
scopes. The actual Node runner uses the same managed capture/mask and source CAS,
with schema generation plus original lease nonce bound through completion.
SESSION_HISTORY_EXPECT_GENERATION selects that explicit managed runtime; the
configured database principal must have the reviewed ingest function profile.
No service-role fallback is attempted. Discovery walks a durable128-scope cursor
including small managed sessions, because every complete graph needs eventual
population. A claim examines at most100 due attempts and may return no claim after\nretiring a stale page; that is not a global completion/readiness proof. The final creator still must enqueue before its metadata commit.

Managed directory state is written only from a matching current logical state;
late physical classifiers cannot replace it. Legacy plaintext short summaries
are cleared for managed scopes. A new native destination invalidates immediately,
even before that native key has any registered scope dependency. Empty
workspaces retain their existing supported identity.

## Proofs and limits

Focused functional tests exercise native PostgreSQL page hashes (PGlite), exact
ordinal seeks, same-source summary CAS, wrong-family refusal, native/control
invalidation without pointer changes and qualified replay. Causal controls must
fail when mask application, native invalidation, intent-before-PUT or registered
index fencing is removed. Real Node24/ownedPG17 proof uses the actual restricted
bulls96_session_ingest LOGIN after all migrations/phase2 checks; it also checks
nonblocking scope contention. This is not live cloud latency or permission proof.

Run own fixtures only:
- bun test src/session-ingest/__tests__/history-mask.pglite.test.ts
- bun test src/session-pipeline/__tests__/replay-pages.test.ts
- bun scripts/sync-ingest-history-boundary.ts --check
- bun build scripts/proof-history-source.ts --target=node --external pg --outfile=src/hoopdb/.proof-history-source-20261104.mjs
- HISTORY_DISPOSABLE=1 node src/hoopdb/.proof-history-source-20261104.mjs canonical-history-source-final-pg17-20261104

The real proof requires a fresh explicitly owned loopback PG17 container with
label com.96bulls.proof=canonical-history-source. It creates only synthetic data.
Do not reuse an author's DB, copy credentials or run this against a service URL.

## Remaining full-service gates

Atomic multi-carrier old/new pointer publication, fresh destinations, complete
global physical ownership including unindexed NULL archives, persistent legacy
retirement/GC, whole-workspace inventory CAS, source79 installation/operator identity and complete target-overlay/body
provenance remain unfinished. The generated689 source SDK and multi-lookup
owner-context checks are composed source; they do not independently establish
those final publication gates. The physical plan capture alone does
not publish an image. The masked logical source/version creator is still part
of that final CAS; fixture owner inserts are explicitly synthetic.

Durable reconcile/retirement workers must consume cleared slots and legacy-only
privacy controls even with no surviving buffered payload. Public logical short/long encrypted summary readers and complete source-bound
Referee facts are not wired by merely storing metadata here. Directory state
uses its existing selected-list path; actual final-caller proof is still required.610 search publication and630 model
integration, exact current-head source review, PG/MinIO races and the largest
1.55GB managed rewrite remain separate mandatory gates. No full read/search/
Bob/Referee switchover or48h retention SLO is established.

Operational rollback stops workers while retaining authority/object ledgers.
The generated down script refuses if any plan, scope, privacy/native obligation
or object intent exists. It must not forget a late immutable PUT.
