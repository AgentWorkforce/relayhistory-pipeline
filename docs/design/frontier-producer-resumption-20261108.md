# Trusted frontier producer: resumed source lane

Root owns `codex/copilot-frontier-offline-hardening-20261107`, retained source
`b1283b2e6c651ea6c2a80b90287e3c0d92ee343b`, reconciled with main
`c858e7fee83648b8275a02a56f5b5017fd91259b`. This is source preparation. It
installs no authority, writes no frontier, and enables no publisher or route.
The separate G4 freeze remains 8bb18be5 / 043700 + 043800.

## Current implementation and concrete gap

The retained offline compiler, source/export capture and canary are present in
this lane. Current main has ACK custody, lazy slots/decisions, receipt seats,
publication lineage and acquisition-owner guards, but no trusted receipt capture
producer, installer, or atomic plan-bound scoped publication adapter. The old
9496310e packet was documentation, not an implementation.

`composeCanonicalFrontierCapture` still supplies no receipt scan. It must
continue to produce `hold/scan_incomplete`. Neither an empty array nor a
caller-supplied artifact establishes a complete receipt domain. The owner-only
VOLATILE `ingest_receipt_seats_for_stage(uuid)` is not a seeder read surface and
must not be called through an owner-credential fallback.

This change adds **the bounded client component** in
`src/session-ingest/frontier-receipt-source.ts`. It is deliberately not wired
into the normal runner while its DB capability is absent. Synthetic tests
qualify client behavior only; they do not qualify a producer, SQL capability,
complete origin, or live capture. There is no executable installer or DDL here.

## Proposed read capability contract to implement next

The fixed surface is
`sessions.ingest_scoped_publication_receipt_capture_v1(jsonb)`, STABLE SECURITY
DEFINER, owned by the independently qualified schema owner, with search_path
exactly `pg_catalog, pg_temp`. The client requires a functionOwner pin separate
from the definition digest; the owner is not learned from the RPC response.
The client verifies the independently reviewed function-definition SHA against
catalogs in each of two read-only transactions. The only proposed client role
is the dedicated NOLOGIN
`bulls96_frontier_receipt_reader` (no superuser, inheritance, creation,
replication, RLS bypass or LOGIN). Seeder and publisher reuse is refused,
including their use as the function owner. This is an inert source expectation:
no role or grant is created here. Capability deployment requires its own later
review and authorized installation; there is no credential fallback.

The RPC must enforce an **owner-installed immutable capture profile** before
reading. The profile binds its UUID and exact digest, allowed authenticated
session-role boundary, exact org/workspace/user/origin, schema generation,
source/control/privacy/reset/fence authority, finite validity and server-side
row/byte/work bounds. A runtime caller can neither install nor broaden it.
The profile label/digest is not a bearer credential. This read profile is
separate from the exact raw approved publication plan and its durable admission
ledger; installing one cannot authorize the other. No generic owner runtime or
arbitrary SQL adapter is exposed.

Request keys are exactly protocol, profileId, profileSha256, tenant, generation,
after and limit. Protocol is `first-ack-seat-capture-v1`. `after` is null or the
four-text cursor `[recordId, revision, revisionId, candidateDigest]`; the SQL
order must use C collation for each field, including the decimal revision text.
No numeric-to-float conversion is permitted. Limit is 1..1000 and cannot enlarge
the immutable profile's budget.

Response keys are exactly protocol, profileId, profileSha256, tenant, generation,
authority, snapshot, decisionCount, decisionsSha256, rows and next. Authority is
the existing six-field `FrontierAuthority` shape with no null facts. Snapshot
identifies the actual transaction snapshot. The count and digest cover the
complete defined candidate domain, not only this page or accepted winners.
Every non-empty page has next equal to its last cursor; EOF requires an actual
empty page with next null, even after a short page. No complete=true assertion
is accepted from SQL.

Rows use the existing exact `FrontierReceiptDecision` shape. The current-origin
candidate domain must include losing equal-revision candidates for every current
receipt-seat maximum. It must independently account for the complete relevant
stage/claim/ordinal and original decision domain, including absent/broken joins,
so a join cannot silently discard an unproven candidate. Duplicate provenance
must be reconciled explicitly; conflicting seat/body/namespace facts refuse,
not disappear under DISTINCT. Old reset/lineage/revision candidates cannot be
mistaken for current maxima. Native-only records must not disappear because
legacy delivery_records lacks them. This domain query and its work bounds still
need implementation and real PostgreSQL proof; the client is not that proof.

The database/client domain hash is SHA-256 over UTF-8 bytes beginning with
`first-ack-seat-domain-v1\n`. For each row in cursor order append `row:`, then
these fields in order: orgId, workspaceId, userId, originId, recordId, revision,
revisionId, source, sessionId, seat, bodyAccepted as `true`/`false`,
candidateDigest, firstDigest. Each non-null field is prefixed by its decimal
UTF-8 byte length and `:`; null is `-1:`. PostgreSQL JSONB text and JavaScript
JSON key ordering are not the shared wire encoding. Separate compiler evidence
hashes continue to use the existing canonical planner ordering. Python-computed
empty and multibyte golden vectors test the wire framing.

The client requires the complete observed rows to match count and domain hash,
keeps losing candidates, rejects cursor regression/duplicates, and performs a
fresh second read-only transaction. The closing authority/count/digest must
match. Both catalog checks must match the independently frozen function SHA.
All failures roll back an open transaction and stop; server error text is never
returned. Query-level timeouts are 3s/250ms; total-duration admission is monotonic.
The caller must also enforce a finite connection/transport timeout: a mock SQL
client cannot prove real network cancellation. Bounds here do not qualify a
server query's execution cost or PG driver's response allocation.

## Remaining source work, owner and integration order

Registry reconciliation (message 245189430090244096) confirms
layered-sessions-pipeline is the 049000 source/implementation owner; Root owns
program/review coordination and this retained client/capture lane. The canary
owner remains migration-publish-canary-20261106. Root does not open a competing
049000 implementation. The registry still marks 049000 DESIGN ONLY, with no
migration bytes; this proposed client contract does not change that disposition.

The next source sequence is:

1. Route the proposed immutable capture-profile/current-candidate RPC contract
   to layered-sessions-pipeline for reconciliation with its 049000 design and
   the separate 050000 attester design. That owner must resolve the capability's
   placement and trust boundary before executable SQL. Root reviews and then
   binds the client to the accepted exact interface. Prove no direct table
   grants, runtime profile installation, owner fallback, missing-join success or cross-scope
   reads. Actual SQL capability/role installation remains a separate operation.
2. Produce verified buffered-stage byte/export bindings and current native
   maxima, then reconcile them with the existing legacy capture. Enclose slow
   object evidence work with fresh receipt and source/privacy closure. Only
   then connect receipt evidence to `composeCanonicalFrontierCapture`. A reader
   page alone cannot promote a complete frontier or authenticate an input file.
3. Layered-sessions-pipeline owns the previously specified 049000 raw-plan
   authority and atomic adapter; Root coordinates/reviews it alongside the
   separate trusted frontier installation, durable whole-plan admission/
   reservation and common ordinary/scoped claim interlock. Reuse the existing
   canary interface;
   no global ordinary-claim revoke, timer-based release or live fixture seeding.
   Recheck current 043850 claim/acquisition bodies and 039000 GC, not obsolete
   historical line numbers. Test both contention orders with real connections.
4. Use only the registry-confirmed 049000 allocation after the required ordered
   044000–048000 prefix exists, with unified/legacy generator composition and
   exact prior-body rollback. Those five prefix slots are absent on this pinned
   main. The confirmed slot is 20261105049000_scoped_publication_claim plus its
   rollback. Existing draft #321 is PREP only and do-not-merge, with no migration
   bytes; 050000 attester and 051000 bulk catch-up are separate design lanes.
   No placeholder migration or new slot is added here. The lead owns landing
   order; Root routes any proposed 049000 SQL to its implementation owner.

The exact-origin row absence already observed read-only explains an unmet
candidate prerequisite. It does not authorize writing a placeholder frontier,
replaying batches, activating global publication or bypassing these producer
requirements. Customer custody queries are not repeated for this source work.

## Validation and review boundary

The client accepts an exclusive idle connection supplied by its eventual caller;
the generic SQL interface cannot prove connection ownership or cancel a stalled
transport. Mark a transaction attempted before awaiting BEGIN, because the
server may have accepted it when its response is lost. Failure always attempts
best-effort ROLLBACK after a BEGIN send, including during the fresh closure
transaction. The eventual real caller must discard an uncertain/broken
connection if cleanup cannot establish idle; this client makes no idle or
transport-health claim after a failure. Its effective-role catalog check is not
a claim of human/process identity: the dedicated reader is NOLOGIN, so its
trusted installation/profile must bind and
guard the actual session principal and permitted role transition. Reusing a
shared credential does not authenticate a publication-plan owner label.

For this source candidate, adversarial review examined role/profile substitution,
missing or forged function authority, cursor/EOF truncation, losing revision ties,
request mutation, duplicate/extra fields, privacy and snapshot races, ambiguous
cross-language hashes, time/byte/page bounds, rollback, and sanitized failure.
Contextual review kept the old runner's HOLD behavior, existing planner hash
ordering and ordinary publisher paths, and identified the missing capability,
buffered byte bindings and atomic adapter as remaining work rather than inferred
readiness. The two added framing vectors were computed independently in Python.

The merge retains main's read-role bootstrap and acquisitionEpoch ticket check;
no applied migration was edited. A pre-existing seed-proof deadline classification
race was corrected: elapsed time reports seed_proof_deadline rather than a size
bound. Tests give the delayed operation a wider causal margin.

A real npm ci was attempted in this T7 worktree; it refused EUSAGE because the
repository has no npm lockfile. Its own Bun 1.4.2 frozen-lock install then succeeded
(192 packages), without borrowing node_modules. All tests/typechecks use aw-heavy.
The focused client/planner/export/canary/seed-proof suite passed 80/0, and root tsc --noEmit
passed. SQL responses are injected synthetic metadata; this is not PostgreSQL,
provider, live producer or hosted-history acceptance evidence.

## Narrow BEGIN-response cleanup follow-up

Independent component review identified that recording an open transaction only
after BEGIN returns skips cleanup when the response is lost. The client now
records the attempt before awaiting BEGIN. Three causal tests cover response
loss in the first and second transactions and loss followed by failed rollback;
all three failed on baseline 1076147b (unchanged 9438a2e7 client) before the fix. There is no automatic retry, fallback
or success/idle claim. Connection disposal remains a required real-caller
integration step, not a capability supplied by this generic SQL client.

Final follow-up validation: 83/0 tests, 323 assertions across the same five
focused suites; root TypeScript and diff checks pass. Tests remain synthetic.

## Consolidation of existing prep lanes

Draft #374 resumes the retained frontier/canary lineage rather than creating a
second implementation. It incorporates draft #321 at exact c6ff786e by a normal
merge, including de7f12d7's deterministic deadline tests and c6ff786e's real
PostgreSQL 15/16/17 publication/frontier job with versioned MinIO. The deadline
comment reflects the later seed-proof error-classification fix. The workflow
adds a comment explaining the intentional embedded-JavaScript shell quoting.
Current main's role, acquisition and constrained-lock proof jobs, triggers,
permissions and runner choices remain unchanged; only the new job is inserted.

The older frontier/canary branches and #321 are historical source inputs to this
resumed lane. This records content consolidation, not a branch deletion, PR
closure or merge-to-main decision. Coordinate overlapping publication fixtures
with #336 by normal main forward-merges when either lands. This PR has no SQL,
and does not implement or change the assigned 049000/050000 design lanes.

Merged-source validation: 83/0 focused tests, 326 assertions; root TypeScript and
diff checks pass. Workflow syntax validates with actionlint. Full actionlint
retains two unrelated existing shellcheck findings (SC2100 and SC2034); excluding
those known baseline findings leaves the added job clean. The real database and
MinIO job has not been executed on this merged head locally; CI must provide
that separate evidence, and its success will still not qualify a live producer.

## Reconciliation decisions and codec conformance

C1: select a dedicated NOLOGIN receipt-reader boundary, not a seeder-reuse waiver.
The proposed source name is bulls96_frontier_receipt_reader; activating or
granting it is not part of this client PR. Group the read RPC, immutable capture
profile and reader boundary with the 049000 prerequisite bundle, subject to the
registry/implementation owner's explicit source inventory and lock/ACL review.
This follows v3 source_plan_reuse_C3's separate qualified least-privilege receipt
adapter prerequisite and leaves the reviewed 050000 writer boundary unchanged.
Do not implicitly broaden 050000 permissions or guess a new 053000 allocation.
No SQL is added here; layered-sessions-pipeline owns that implementation.

C2: keep the planner in #374. After this source lane passes its complete reviews
and checks and merges, 049000 imports the planner from main; no second vendored
copy or independent planner landing. No merge is performed while the remaining
source-contract and whole-lane review conditions are unresolved.

C3: recovered the original Book artifacts with exact pins dc068aef (v1 design),
51d358b2 (v3 design), fd3f8908 (contract vectors) and 867f6b35 (bigint known
answers). The two unmodified synthetic vector sets use their author-published canonical
paths under docs/handoffs/frontier-attester-20261107, merged from 3caccf36, with
their full SHA256 pins enforced by conformance tests. Duplicate docs/contracts
copies are removed; artifact bytes and known answers are unchanged. The original v3
design is also published unmodified under docs/handoffs/frontier-attester-20261107
with its 51d358b2 pin enforced by the same tests. These historical design/test
artifacts grant no operational authority. The v3 design governs:
its source_plan_reuse_C3 explicitly retains the planner's exact existing output
hashes and ordering and retires the v2 column-domain/length-framing proposal.
Do not prepend a new domain or silently re-sort the planner's output. V3 audit
objects bind their own protocol field and use the existing canonical JSON hash;
new audit identity arrays use field-by-field raw UTF8 order. Their comparator is
shared with the receipt RPC cursor, not substituted for planner ordering.

Both published positive component vectors, both >2^53/int64max decimal-string
vectors, and the v3 audit example match the existing codec's exact bytes/hashes.
Literal JavaScript bigint remains unsupported; canonical decimal STRING input
is the contract, with no unsafe Number coercion. The punctuation ordering vector
proves the planner's encoded-tuple order and new audit raw-field order intentionally
differ. Unsupported nonASCII, whitespace/control and lone-surrogate identifiers
now HOLD in planner eligibility and refuse in receipt input. This is fail-closed
eligibility, not a silent output-hash rewrite or a Unicode-support amendment.

Conformance here covers byte/hash behavior, strict duplicate/unsafe-number JSON
rejection, ordering and unsupported identifiers. The remaining typed audit-schema,
whole-user/S4 coverage, independent verifier and approval cases in fd3f8908 still
belong to their consuming implementation; they are not claimed to pass because
four synthetic codec vectors pass. The receipt wire-domain digest is a distinct
page-inventory commitment, never a replacement for READY's frontier columns or
an attester/verifier approval.

The source reader duration cap is now 120000ms. This bounds requested capture
time, not the complete job or network cancellation; its eventual caller must
pass its remaining overall budget and reserve transport/cleanup margin.
Function-definition hashes must be qualified for the actual PostgreSQL version;
no cross-version equality with a raw SQL-file hash is asserted. complete=true
means the client observed EOF, exact count/digest and fresh closure under its
pinned capability; it does not supply the absent SQL domain-completeness proof,
byte evidence, trusted profile installation or v3 independent-verifier result.
The test-only outcomes ledger and fixture attestation table remain disposable
fixtures, not a production audit/identity-retention design.

Reconciliation validation: focused byte/reader/planner/canary/seed suites run
through aw-heavy; the prior 855f8513 PG/MinIO receipt remains historical evidence
only until this new head's CI completes. No missing-producer HOLD was relaxed.

Final source validation for this reconciliation: 98 tests, 0 failures, 380
assertions across six focused suites, with root TypeScript and diff checks.
The unsupported-identifier regression causally fails with the old eligibility
guard (a self-consistent synthetic Unicode maximum otherwise became READY),
and passes with the fail-closed guard. The source was restored immediately
after that negative experiment. This does not qualify real source coverage.

Author publication consolidation: normal merge of 3caccf36 imports the original
vector paths, README and SHA256SUMS. Byte comparison verified both artifact pairs
identical before removing the duplicate paths. Tests now load the author paths
with the same exact three source pins. This is provenance/path consolidation,
not a change to the accepted reader API, planner hashing or coverage claims.
