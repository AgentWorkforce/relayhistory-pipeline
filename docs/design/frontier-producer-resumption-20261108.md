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
DEFINER, owned by postgres, with search_path exactly `pg_catalog, pg_temp`.
The client verifies the independently reviewed function-definition SHA against
catalogs in each of two read-only transactions. The only accepted client role
is the existing restricted `bulls96_session_ingest_seeder` (no superuser,
inheritance, creation, replication or RLS bypass). No new LOGIN role is proposed.
The role currently lacks this capability: deployment requires its own later
review and authorized installation, not a changed client credential.

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

Root owns the following work in this existing lane; no competing branch or new
production permission request is needed to continue source development:

1. Implement/review the immutable capture-profile owner boundary and the exact
   bounded current-candidate RPC above. Prove no direct table grants, runtime
   profile installation, owner fallback, missing-join success or cross-scope
   reads. Actual SQL capability/role installation remains a separate operation.
2. Produce verified buffered-stage byte/export bindings and current native
   maxima, then reconcile them with the existing legacy capture. Enclose slow
   object evidence work with fresh receipt and source/privacy closure. Only
   then connect receipt evidence to `composeCanonicalFrontierCapture`. A reader
   page alone cannot promote a complete frontier or authenticate an input file.
3. Complete the previously specified owner-installed raw-plan authority,
   trusted frontier installation, durable whole-plan admission/reservation and
   common ordinary/scoped claim interlock. Reuse the existing canary interface;
   no global ordinary-claim revoke, timer-based release or live fixture seeding.
   Recheck current 043850 claim/acquisition bodies and 039000 GC, not obsolete
   historical line numbers. Test both contention orders with real connections.
4. Use only the registry-confirmed 049000 allocation after the required ordered
   044000–048000 prefix exists, with unified/legacy generator composition and
   exact prior-body rollback. Those five prefix slots are absent on this pinned
   main. Root asked the registry for its current owner/slot disposition before
   adding executable migration SQL; no new slot is guessed here.

The exact-origin row absence already observed read-only explains an unmet
candidate prerequisite. It does not authorize writing a placeholder frontier,
replaying batches, activating global publication or bypassing these producer
requirements. Customer custody queries are not repeated for this source work.

## Validation and review boundary

The client accepts an exclusive idle connection supplied by its eventual caller;
the generic SQL interface cannot prove connection ownership or cancel a stalled
transport. Its effective-role catalog check is not a claim of human/process
identity: seeder is NOLOGIN, so its trusted installation/profile must bind and
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
