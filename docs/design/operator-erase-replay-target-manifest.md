# Operator erase replay target manifest — draft 3

## Status and ownership

Design for review, not implementation or rollout approval. The registry owner
has reserved forward slot 062360 for a later implementation, after 062320.
This reservation is outside the Friday release window.
That reservation is not design acceptance or erase-equivalence sign-off;
published migration bytes are not rewritten. Nothing here authorizes a real
database apply, replay, hold release, credential change, deployment or feature
activation. Indefinite retention of raw target identifiers is a proposal which
still needs an explicit owner decision (section 8).

Producer design: hard-delete-owner. Consumer: sessions-backlog-integration.
062320 hold contract: sessions-authority-evidence. Erase-equivalence sign-off:
layered-sessions-pipeline. Rollout coordination remains with its owner.

Inputs checked for this draft:

- Main `eb357efedb164fb141818b023aad97b760756f3a`: 050000, 050010,
  050020 and [the runtime contract](../operator-session-erase-runtime.md).
- Hold design revision 4, `892f089eb999ba99f2984ce0b3211ee7431d3741`,
  `docs/design/org-admission-hold.md`, sections 6.1–6.3, on
  `review/org-admission-hold-design`.
- 062320 implementation `e13f85414bd0230182aa97dba4f490f41d2e612a`,
  `review/org-admission-hold-062320`, checked with its owner. Both erase begin
  bodies preserve the marked org check as the first statement after BEGIN.
- Consumer review input `476975bd90e3b9dc92b152155169b34831b36d09`,
  `review/backlog-ops-hold-bridge`, `src/session-ingest/hold-bridge.ts`,
  on authority modes `bab783784fda8eea202a9534613d600b035edcf2`.

The pending branches are interface inputs, not claims that they have landed.

## 1. Problem and limit of this proof

The SQL erase discovers aliases, owners and origins before it removes ownership
and derived rows. A later bridge cannot reconstruct that set from the surviving
receipt, alias fences, or S3 owner stubs alone. Index-only and directory-only
owners can be missing from the stubs. `session_erasure_scopes` stores alias IDs
after the SQL erase call; it is not a pre-cleanup owner/origin witness.

Persist the exact finite target set under the erase's locks, in the transaction
which applies the SQL erase. An immutable completion record distinguishes a
completed capture from an intent, partial attempt, old erase or failed capture.
Retries reuse a saved complete set; an incomplete intent is not that proof and
cannot be completed by rediscovering targets after cleanup.

This is a target-capture proof. It is not a byte-erasure receipt, a user-generation
barrier, or by itself proof that finite S3 selection deletions reproduce every
SQL admission fence. In particular, preserving fences against a new owner or a
first upload from a new origin remains part of the named erase-equivalence
review. The bridge remains dormant/fail-closed until that review accepts the
consumer and producer together.

## 2. Coverage: one shared derivation for capture and erase

For the validated tenant `(org, workspace)`, source `s` and requested session:

1. `A` is the sorted, unique canonical/physical alias closure produced by
   `ingest_operator_erase_ids`, including the requested ID. Preserve its existing
   collision refusal and 256-ID bound; do not derive aliases by another lossy
   layout conversion or replace the requested ID with just one canonical ID.
   Acceptance must prove this resolver covers the physical replay identities
   even when ownership/directory evidence survives without an index link. If
   it does not, the shared authoritative resolver needs a reviewed correction;
   a canonical-only result must not be labelled complete for that case.
2. After the existing owner-lock convergence, `U` is the union produced by
   `ingest_operator_erase_owners`: ownership rows, matching index rows (including
   `canonical_session_id` matches), and directory-user rows over `A`. Preserve
   its existing 64-owner bound and ordered lock acquisition.
3. For each `u` in `U`, `O(u)` is the distinct ownership `origin_id` values for
   that owner, source and alias set, plus `relayhistory:conversation_turns:v1`.
   Thus an index-only or directory-only owner is included even without an
   ownership row, and has at least the turns origin. Do not guess an origin
   from a machine ID or silently substitute S3 owner-stub coverage.
4. The target set is `T = {(u, o, s, a) | u in U, o in O(u), a in A}`.
   Origins are owner-specific; this is not every owner's cross product with
   origins recorded only for another owner.

This matches the existing SQL's `v_pairs` and per-owner origin loop. The
implementation must derive `T` once per invocation and, when capture is
eligible, drive both capture and destructive selection calls from that
materialized set. An ineligible proof retry retains the legacy SQL derivation
without claiming it reconstructs the original set. Copying the SELECT into two
independent implementations would let the witness drift from the effects.
Canonical and physical IDs must both appear in the expanded targets.

No truncation, sampling, dropped directory owner, inferred origin, or page limit
can produce a complete manifest. Do not add a new erase cardinality refusal
without equivalence review. Use normalized storage and bounded reader pages
instead of aggregating an unbounded alias/owner/origin product in Node memory.

An empty SQL owner set does not prove that no replay effect is needed in S3.
The present bridge rejects empty target sets; preserve that behavior. Such an
erase needs a separately approved empty-coverage proof before release. This
draft supplies no such proof and does not turn an empty array into success.

## 3. Transaction and failure boundary

Preserve the actual existing lock order. The 062320 erasure check takes the
per-org hold key SHARED before the existing erase locks. It never refuses an
erasure because of the hold state or isolation level. Existing erase functions'
own isolation checks remain unchanged. Do not move the org check below the
global erase gate or add a later reverse acquisition.

The future patch must retain the global statement gate, alias-writer locks,
receipt lock and sorted owner fence locks in their established order. Its lane
must take the latest replaced begin bodies from 062320, not 050020 or 058010.
Keep this marked line first after BEGIN, even before isolation/role checks:

```sql
PERFORM sessions.ingest_org_admission_hold_check(p_org,'erasure'); -- 062320 org admission hold
```

If another replaced entry point records a new erasure, it must take that check
first too. Read/step helpers do not gain a new erasure gate merely because
their names contain "erase". Add evidence from the live `pg_proc.prosrc` after
a full migrate (or an equivalent later-body assertion): the present 062320
classification test checks only its own file, not a later replacement.
The hold lifecycle retains its `(961996,1)` SHARED gate and fresh-slot check;
the capture path does not acquire an extra lifecycle gate.

The producer sequence is:

1. Under the existing erase gate, validate the request and resolve `A`. Bind the
   capture to the accepted receipt's persisted `request_id`, not a repeated
   caller's new `p_request`. Current receipts make request IDs globally unique
   and reject reuse for another target; preserve that check. A repeated/retried
   event's caller-supplied request ID is not the originating request identity.
2. Persist an immutable capture intent and its alias bindings at the same
   boundary as the new permanent session fences, **outside** the existing inner
   owner-fencing exception block. No targets or completeness are claimed yet.
3. Inside that block, finish owner-lock convergence and resolve all of `U`,
   `O(u)` and `T`. For an eligible capture window as defined below, persist the
   target rows, validate their exact set/binding, and insert the immutable
   completion record before the first destructive `ingest_apply_selection`,
   tombstone or directory-membership cleanup. An ineligible proof retry does
   not issue that record.
4. Execute the existing erase using that same `T`. For an eligible capture,
   commit the matching witness and SQL effects together. An ineligible proof
   retry keeps the existing SQL erase behavior without a new seal; it does
   not substitute the intent's older aliases. Do not capture from a later
   worker pass.

The completion record is inserted before cleanup but is visible to other
transactions only when the transaction commits. Cleanup failure must not commit
a completion record without the effects it witnesses. An existing caught RH010
rolls back the inner capture/cleanup together, while its intent and permanent
fences can remain. The reader then reports incomplete; a persisted post-install
intent is **not**, by itself, permission for a later attempt to seal it.

For any attempted first seal on retry, require exact equality between the
persisted original alias declaration `A0` and the newly resolved, sorted unique
closure `A_retry`: `A_retry = A0`. Test both a larger and a **smaller** set. A
subset check is insufficient. Do not edit the intent/bindings to fit a new set,
or use a retained superset as permission to widen the SQL erase.

Equality is necessary but not sufficient. Owner/origin evidence can disappear
after the failed inner transaction releases its locks. Reacquiring those locks
in a later transaction does not prove continuity, even if aliases are unchanged.
A first uninterrupted capture/cleanup call may seal the intent it created in
that same call under the locks. After an unwind or a committed incomplete
attempt, sealing additionally needs an independently validated continuity
witness: bound to this intent and full scope, covering the entire gap and every
alias/ownership/index/directory/origin mutation, proving that original evidence
was not lost (or preserving a complete authoritative pre-cleanup set).

No such continuity witness is supplied by this draft. The org hold, a timestamp,
an unchanged alias set, current owner rows or S3 stubs are not substitutes.
**Default: retrying an incomplete intent cannot produce a seal.** Keep replay
incomplete unless a separately reviewed continuity protocol proves it eligible.
An already sealed manifest is reused as immutable evidence; it is not recaptured
or tested against metadata which normal cleanup has removed.

This proof gate must preserve existing SQL erase behavior. On an alias mismatch
or unproven continuity, the existing erase can retry/proceed with its existing
target derivation, refusals and status behavior, but no completion witness is
issued and the bridge stays blocked. Do not narrow, widen, refuse or falsely
complete that erase merely to make the manifest fit. Any other change to erase
semantics requires separate equivalence review.

Distinguish that case from an old fence with no capture intent. It is not safe
to manufacture an intent for old, already-cleaned metadata and label it complete.
Existing non-failed-receipt early returns must not synthesize a witness.

All witness binding/eligibility checks occur before destructive cleanup.
Ambiguous or mismatched bindings never become complete. The precise
capture-failure/receipt mapping must be reviewed against existing erase behavior
before implementation is accepted; a proof failure is not a new erase refusal.

## 4. Proposed immutable records

Use `sessions.session_erasure_replay_*` names. Do not introduce `ingest_%` or
`upload_%` tables with `org_id`, `workspace_id` or `user_id`: lazy bootstrap and
repair scan those names as prior authority.

| Record | Required binding and purpose |
| --- | --- |
| `session_erasure_replay_requests` | `manifest_id` (generated UUID), accepted receipt `request_id`, `org_id`, `workspace_id`, `source`, `requested_session_id`, capture-format version, and creation time. Intent only. |
| `session_erasure_replay_aliases` | The immutable original alias declaration `A0`; `(org_id, workspace_id, source, alias_session_id)` identifies a counted fence and resolves to its originating manifest. It is not rewritten on a retry. |
| `session_erasure_replay_targets` | Unique `(manifest_id, user_id, origin_id, source, session_id)` rows, where source equals the parent source and session ID belongs to that manifest's alias set. |
| `session_erasure_replay_completions` | One immutable seal per manifest: format/derivation version, exact alias/owner/origin-pair/target counts, canonical target digest and completion time. Exists only after a complete, nonempty capture. |

The implementation must enforce the full receipt/tenant/source/request binding
through compound keys/constraints and reader validation; two unrelated foreign
keys must not be mistaken for proof that a request and scope name the same row.
Alias bindings are unique per fence identity. No `LIMIT 1` resolves competing
manifests. Every binding of a capture must name the same request and target set.

The stored `manifest_id` is the proposed immutable replay-group identity. Its
accepted `request_id` remains available to correlate the original erase.
The UUID does not replace tenant validation or the target digest. All three
must be bound together by the reader and replay receipt.

Define the digest over a versioned, length-framed encoding of the complete
binding and unique targets in bytewise `COLLATE "C"` order. Do not use delimiter
concatenation, unordered JSON aggregation, or consumer locale collation.
Publish exact encoding test vectors with the implementation.

UPDATE, DELETE and TRUNCATE are refused on all four tables. Also reject new
alias/target INSERTs into a sealed manifest: immutable existing rows alone do
not prevent later extension of a previously hashed set. Completion insertion
must verify the exact counts/digest, and constraints/guards must prevent partial
commits or post-seal extension, including concurrent insert attempts. The
producer's existing lock and isolation discipline is part of that proof.

## 5. Repeats, aliases and old fences

- A repeat for the same receipt uses its original intent/manifest and targets.
  A different incoming request ID must not overwrite the persisted identity.
- An incomplete intent is not a saved complete target set. A retry can seal
  only with exact original/retry alias equality **and** the continuity proof
  described in section 3; otherwise it remains incomplete while the existing
  SQL erase retains its own behavior.
- Each alias fence resolves to the same originating complete manifest. Replay
  may coalesce these rows into one group, but a group receipt covers **all** its
  targets, not just the alias used for that lookup.
- If a later API request uses another already-fenced alias, do not rebind that
  fence to a fresh request or infer a smaller target set from cleaned rows.
  A fully covered repeat can refer to the existing immutable proof; its
  original fence/request binding remains authoritative for bridge replay.
- A closure overlapping different manifest groups, a new alias outside a
  purported complete group, or inconsistent receipt binding is not proof.
  Reconciliation of those cases must not silently union unrelated erases or
  pick an arbitrary group. Keep the bridge blocked and require a separately
  reviewed resolution before claiming equivalence.
- A pre-install fence/receipt without a recorded intent is historical-missing,
  even if the current SQL or S3 inventory happens to look empty.

There is **no automatic historic backfill in this proposal**. An independently
complete pre-cleanup witness could support a separate import design with bound
provenance; present-day owner stubs, receipts or surviving rows alone cannot.

## 6. Owner read interface for the bridge

Reader name, signature and bounded transport accepted by the consumer; the
response contract below is specified for its parser before SQL implementation:

```text
sessions.session_erasure_replay_read(
  org text, workspace text, source text, fence_session text,
  after_cursor jsonb default null, page_size integer default 512
) -> jsonb
```

It is an owner-only, SECURITY INVOKER, STABLE read with a fixed search path,
fully qualified objects and no writes, locks, sequences, clock-based leases or
side effects. It must run in a READ ONLY transaction. One parameterized SELECT
per page is the accepted bounded transport; the consumer will replace its
single-array seam with a complete draining/preparation adapter.
Require `1 <= page_size <= 512`; a page boundary is never a coverage boundary.

Every page carries the same manifest identity, accepted `request_id`, full
tenant/source/requested-session binding, format version, target count and digest.
The response includes `complete`, a closed reason code, expanded targets
`{user_id, origin_id, source, session_id}`, and a next cursor. The cursor binds
the manifest identity/digest and last tuple; no raw IDs go to logs or receipts.
Source/alias validation and parent/alias/seal joins are mandatory on every page.

`complete` means capture completeness, not that this one page contains every
target or that bytes have been erased. It is false for missing intent, missing
seal, invalid binding, unsupported format, inconsistent counts/digest or empty
coverage. No such response authorizes an S3 call or a replay receipt.

Pagination uses immutable keyset order, not OFFSET. Pages must be strictly
advancing and non-overlapping. A page limit or a partial response must never be
adapted directly into today's `{complete: true, targets: [...]}` result.

**A complete validated plan is required before the first S3 mutation, not just
before a replay receipt.** A valid prefix is not permission to start fencing.
Use two phases:

1. **Prepare in SQL only, with zero S3 calls.** Do not read a replay receipt
   from S3 during preparation. Start at the beginning, drain every page to
   the terminal cursor, and validate all identities, scope/alias/target
   membership, ordering, uniqueness, counts and the streamed full digest
   against the immutable seal. Reject an absent or invalid terminal page.
   Only after all checks succeed create an internal verified-plan descriptor
   binding manifest UUID, request, complete tenant/source scope, format,
   original alias declaration, target count and digest. A caller-supplied
   `complete: true` or descriptor is not a substitute for this preparation.
   A bad final page means **zero S3 calls, zero mutations and zero receipts**.
2. **Apply only that verified plan.** A bounded consumer can reread the same
   immutable sealed set, selected by the verified manifest identity, full
   scope and digest, from its first cursor. Do not resolve a new live target
   set or choose a different manifest on this pass. Every page and target must
   be bound to that exact prevalidated set before dispatch; mismatched scope,
   binding, seal or cursor never authorizes a new target. The immutable rows,
   parent/alias membership constraints and post-seal insertion guard are
   prerequisites for safe rereading, not optional optimizations.

The bridge needs a draining/preparation adapter or two-pass streaming seam;
streaming validation and mutation together is forbidden. Bounded memory can
hold the descriptor, the at-most-256 alias declaration, rolling validation
state and one page, rather than all targets or raw-ID files on disk. If the
reader cannot guarantee an identical sealed set for the second pass, stop:
the interface needs a different verified-plan mechanism before any effects.

Once a complete plan is prepared, a read/transport/S3 failure during application
can leave only effects from that already validated plan; it creates no receipt.
An unvalidated prefix must never be justified as "safe because idempotent".
Retry application idempotently from the same immutable plan, and issue a group
receipt only after all planned effects finish and final applied count/digest
match. Never count a partial application as complete.

The same owner reader is used at replay time and before `bridge_complete`.
There is no new app endpoint, runtime grant, end-user reader or bearer token.

### 6.1 Exact JSONB response and cursor keys

The following defines JSON property names and types, not executable code or
proof that a producer exists. Every listed key is required; reject unknown
keys, an unsupported wire/capture version, or a malformed response. Reader
wire version `v: 1` is independent of the bridge's proposed v2 receipt format.

```ts
type Target = {
  user_id: string;
  origin_id: string;
  source: string;
  session_id: string;
};
type Cursor = {
  v: 1;
  manifest_id: string;
  manifest_sha256: string;
  after: Target;
};
type ProvenPage = {
  v: 1;
  complete: true;
  reason: "complete";
  manifest_id: string;
  request_id: string;
  scope: {
    org_id: string;
    workspace_id: string;
    source: string;
    requested_session_id: string;
  };
  capture_version: 1;
  alias_ids: string[];
  target_count: string;
  manifest_sha256: string;
  targets: Target[];
  next_cursor: Cursor | null;
};
type UnprovenPage = {
  v: 1;
  complete: false;
  reason:
    | "manifest_missing"
    | "manifest_unsealed"
    | "manifest_binding_invalid"
    | "manifest_format_unsupported"
    | "manifest_integrity_invalid"
    | "manifest_empty_unproven"
    | "manifest_cursor_invalid"
    | "reader_input_invalid";
  manifest_id: null;
  request_id: null;
  scope: null;
  capture_version: null;
  alias_ids: [];
  target_count: null;
  manifest_sha256: null;
  targets: [];
  next_cursor: null;
};
type ReaderResponse = ProvenPage | UnprovenPage;
```

`manifest_id` is the persisted UUID; `request_id` is the accepted originating
receipt ID. `scope` must match the query's org/workspace/source.
`scope.requested_session_id` names the original request, not necessarily the
queried fence alias: that queried `fence_session` must occur in `alias_ids`.
The alias declaration is sorted, unique, nonempty and at most 256 IDs.

`target_count` is a positive, canonical base-10 integer string
(`^[1-9][0-9]*$`), at most PostgreSQL bigint's positive maximum and parsed
without floating-point coercion. Zero is unproven, not a proven empty plan.
`manifest_sha256` is exactly 64 lowercase hexadecimal
characters for the complete binding/target digest, not just this page's digest.
All proven pages repeat identical metadata; only `targets` and `next_cursor`
change. An unsealed intent includes retries with unproven continuity or changed
aliases; `manifest_unsealed` does not invent a more specific historical cause.

Targets use bytewise `COLLATE "C"` order on
`(user_id, origin_id, source, session_id)`, with no duplicates. Each target's
source equals `scope.source` and its session occurs in `alias_ids`. A proven
page has between one and `page_size` targets. `next_cursor` is null only on
the nonempty terminal page; otherwise it repeats the manifest UUID/digest
and `after` equals that page's last target. Do not emit an extra empty terminal
page. Validate a cursor against the same sealed set; it cannot select another
scope, skip preparation, or stand in for the total count/digest check.

Unproven responses expose no partial targets or reusable descriptor. SQL errors
or no JSON response also fail closed. IDs and cursors stay in SQL/the bridge
process; logs and receipts must not serialize this response. A proven page
still grants no effects by itself: both passes in section 6 remain mandatory.

## 7. Replay receipt and release coupling

The consumer must prepare the complete validated plan from section 6 **before**
this erasure replay's first S3 call, including its receipt lookup. Only afterward
may a receipt authorize `already` or a plan authorize mutations. At review
input `476975bd`, it checks only that `resolveSessionTargets` exists before an
identity-derived receipt lookup; the actual target resolution still happens
later. Callback presence is not proof, and an old receipt cannot bypass plan
preparation. Release readiness must independently validate the same plan and
receipt binding; a previously cached replay outcome is not sufficient.

Propose a new receipt version/key domain binding hold epoch, manifest UUID,
accepted request ID, complete scope, capture version and full target digest.
Only hashes and aggregate counts leave SQL/the bridge process. A v1 receipt
keyed only by the synthesized `(workspace, source, session)` identity is not
valid evidence for this new group. Receipt validation rejects another scope,
manifest, digest, generation of the format, incomplete target count or wrong
erasure kind. Create-only writes remain idempotent and collisions fail closed.
The user-erase prerequisites in section 9 apply before receipt reuse too; a
structurally valid cached user receipt is not itself generation or byte proof.

Several counted alias fences may share that one validated group receipt. The
bridge must still account for each fence row in the release watermark. Do not
count manifest headers, target rows, distinct requests or replay API calls.
Keep the 062320 release count exactly:

```text
count(operator_user_erasures for org) + count(session_erasure_fences for org)
```

It is the total org count recomputed under the exclusive hold key, not the
number of records inside a FULL-time replay window. Use the settled status
`erasureCount`, not a window's `set.length` or a receipt-group count, for the
release watermark. Adding this manifest must not hide a mismatch. A new erase
takes the shared org key, so an erase racing
release is either in its recomputed count or occurs after release. No marker
or count row is deleted, and all manifest tables are excluded from that count.

`complete` does not approve `bridge_complete`; all replay receipts, the current
hold epoch/state and the existing final count check are still required.

## 8. ACL, retention and down

Enable RLS; REVOKE ALL from PUBLIC, anon, authenticated, service_role,
bulls96_session_ingest and bulls96_session_ingest_seeder on the new tables and
reader/helper functions. Give no new runtime grants. Existing trusted erase
entry points write through their owner context; the backlog owner reads via a
READ ONLY connection/transaction. Defining objects must not inherit default
PUBLIC EXECUTE or permissive default table grants.

Persist identifiers and coverage evidence only: no transcript content, title,
summary, object credentials or provider text. The bridge's receipts and logs
carry hashes/counts/closed codes only. Raw identifiers are restricted to the
owner-only SQL records and the bridge process.

**Retention is an unresolved owner decision.** Indefinite owner-only retention
of these raw target identifiers is the proposal, not an approved default.
The benefit is an immutable replay/audit witness beyond any hold or retry;
the cost is retaining identifiers after transcript bytes are gone. Obtain an
explicit owner decision on that trade-off before treating the producer design
as accepted. This document creates no records and grants no retention approval.

Any chosen policy must keep proof available beyond the hold and all supported
retries. This proposal adds no TTL, automatic GC or cleanup cascade. A bounded
retention/export alternative needs a separately reviewed availability and
completeness protocol; it cannot silently discard replay evidence, delete the
never-deleted fence/user-erase markers, or change the release count. Do not
describe the raw-identifier policy as approved merely because RLS/immutability
or 062320 compatibility was reviewed.

A future down should retain the manifest records, immutability guards and a
safe owner read path (or a fail-closed unsupported-version result). It must not
silently drop the only replay witness. Its exact retained schema, ACL manifest,
bootstrap verifier and reapply behavior require tests and registry review.
062360's down runs first and restores the **062320** replaced bodies, with the
marked first erasure check. The 062320 later-migration guard refuses its down
while 062360 remains applied; only after 062360 is down does 062320 restore its
own pre-062320 bodies. Test that full sequence and the restored live prosrc.
Do not add a lifecycle lock to capture or invent a reverse order for the down.

## 9. User erases: proof gates cache reuse and release too

This proposal supplies no `userGenerationBoundary` proof. A captured user ID,
SQL reset count, timestamp, request ID or observed fence epoch alone does not
prove that an old-generation object write cannot land later, or that retrying
a reset after release will preserve a new-generation upload. The consumer must
continue returning `erasure_generation_unproven` without that separate proof.

This requirement applies to **existing receipts as well as new replay**. Both
`replayErasure` and release readiness (the `bridgeCompleteReady` contract,
including the pinned implementation's `bridgeCompleteVerdict` receipt counter)
must share the same mandatory user-evidence validation:

1. Resolve the exact trusted SQL user erasure and validate its org, workspace,
   user, accepted request ID and required byte-verification state. A receipt's
   existence, counts or claimed erasure kind do not establish that state.
2. Require an actually validated generation-boundary proof for that same
   identity and relevant fence/generation/hold context: no old-generation write
   can land after replay, and replay/retry cannot remove post-release uploads.
   Missing, false, unsupported, stale or mismatched proof remains unproven.
   A callback's presence or an unverified caller boolean is not evidence.
3. Only then may a bound receipt authorize `already` or be counted toward
   release. The receipt must match the expected user kind, complete identity
   and evidence, not merely have an allowed `erasure` enum and request hash.
   No manifest/session receipt may stand in for this user proof.

At review input `476975bd`, the replay's existing-v1-receipt return precedes
`bytesVerified`/`userGenerationBoundary`, and the release verdict separately
counts v1 receipts. Both bypasses must be closed. Do not validate only the
uncached path, rely on an earlier in-process replay result, or let a separate
release receipt counter grandfather old receipts. The final erasure-set read
and readiness verdict recheck these prerequisites before the owner release.

In particular, with `bytes_verified = true` and no valid generation proof, a
preseeded v1 receipt must **not** produce `already`, contribute a receipted user
erase, or permit release. Return an unproven/not-ready outcome, perform zero
reset/payload mutations, write no new receipt, and do not call the release
function. `bytes_verified = false` likewise fails its prerequisite even if a
receipt exists. Stored v1 objects are not deleted or rewritten by this design;
they simply cannot authorize a bypass.

User generation isolation requires its own writer/S3 fence protocol and
late-write/retry-after-release evidence, plus erase-equivalence sign-off.
Neither the session manifest nor 062320's shared org lock can stand in for it.

## 10. Required implementation evidence

- Native PostgreSQL interleavings for alias/owner discovery versus writes,
  cleanup, retry and release. Include index-only and directory-only owners,
  physical aliases without a surviving index link, multiple aliases,
  owner-specific origins and turns-only targets.
- Inject failure before capture, during capture, after sealing and during
  selection/cleanup. Prove no complete witness without committed effects;
  prove caught owner failure keeps fences plus intent but no seal. On retry,
  test larger **and smaller** alias closures against the immutable original
  set; exact equality is required before sealing. Test unchanged aliases with
  intervening owner deletion, origin cleanup and index/directory cleanup after
  the owner locks unwind. Without validated continuity evidence all remain
  incomplete, while original SQL erase retry/refusal/status behavior is
  preserved. Equality alone and a post-install intent do not establish proof.
  An already sealed repeat uses its witness after metadata is removed.
- Cross-request/tenant/workspace/source binding rejection, same-request repeat,
  alias-input repeats, multiple counted aliases in one group, conflicting
  groups and old fences without intents. No request identity reassignment.
- Exact set equivalence against the production SQL effect path, not a fixture
  which mirrors the new producer. Include new-origin/new-owner admission in
  the separate whole-erase equivalence review.
- Read-only invocation, deterministic digest vectors, multiple pages,
  dropped/duplicate/reordered/forged-cursor pages, target counts exceeding a
  page, and full replay retries after partial effects of a **prevalidated** plan.
  A valid first page followed by a wrong-scope or absent terminal page yields
  **zero S3 calls and zero receipts**, not merely no final receipt. Test the
  identical-binding two-pass reader and refusal of changed second-pass binding
  or target membership. No streaming mutation during preparation.
- V1, wrong-kind, wrong-digest, wrong-scope and partial receipts cannot bypass
  manifest resolution or satisfy release. Logs/receipts contain no raw IDs.
- Preseed a structurally valid v1 **user** receipt with byte state verified
  and no generation proof. Invoke both replay and the independent release
  readiness path: neither succeeds or counts it, and no S3 mutation, new
  receipt or SQL release occurs. Also cover unverified bytes, false/stale or
  wrong-scope generation proof and the separate receipt counter; no warm-cache
  or previously-replayed exemption.
- A racing new erase, alias-fence versus manifest-group cardinality, an erase
  predating the replay window, and unchanged 062320 count under its exclusive
  key. Missing historical and empty coverage keep the bridge blocked.
- RLS/default-ACL/role negatives; UPDATE/DELETE/TRUNCATE and post-seal INSERT
  refusal; stale/concurrent mutation attempts; retained down/reapply guards.
- After full migrate, assert the marked erasure check is first in the live
  latest replaced bodies, not merely present in 062320's source file. Test
  062360 down restores 062320 bodies and only then 062320 down restores theirs.

## 11. Decisions before implementation

1. Consumer has accepted the reader signature, bounded keyset transport and
   manifest UUID/accepted-request/scope/digest receipt identity. Confirm the
   exact response keys above and the complete preparation before receipt reuse
   **and before any mutation**, including both user-proof paths. No identity-only
   or pre-proof old receipt fallback in replay or release.
2. Erase-equivalence owner accepts the exact target derivation, failure and
   alias-repeat rules, empty/historical refusals, and the distinction from
   global admission and user-generation proofs.
3. 062320 owner has confirmed compatibility and successor `e13f8541`; preserve
   its latest-body, first-statement, live-prosrc and down requirements above.
   Slot 062360 is reserved by the registry owner, not implementation approval.
4. The owner explicitly decides raw-target identifier retention. Indefinite
   retention remains a proposal, separate from ACL/lock compatibility approval.

Until these are agreed, this remains a review draft. It changes no SQL body,
bridge behavior, release gate or live state.
