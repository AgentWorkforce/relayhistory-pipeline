# Operator erase replay target manifest — draft 1

## Status and ownership

Design for review, not implementation or rollout approval. No migration slot is
claimed. A future forward migration must follow 062320; published migration
bytes are not rewritten. Nothing here authorizes a real database apply, replay,
hold release, credential change, deployment or feature activation.

Producer design: hard-delete-owner. Consumer: sessions-backlog-integration.
062320 hold contract: sessions-authority-evidence. Erase-equivalence sign-off:
layered-sessions-pipeline. Rollout coordination remains with its owner.

Inputs checked for this draft:

- Main `eb357efedb164fb141818b023aad97b760756f3a`: 050000, 050010,
  050020 and [the runtime contract](../operator-session-erase-runtime.md).
- Hold design revision 4, `892f089eb999ba99f2984ce0b3211ee7431d3741`,
  `docs/design/org-admission-hold.md`, sections 6.1–6.3, on
  `review/org-admission-hold-design`.
- 062320 implementation `d1190aa9833c4792213551decb2d210d8d829859`,
  `review/org-admission-hold-062320`. The consumer also cites a `1c95258e`
  successor; it was not the fetched remote head and must be reconciled before
  implementation. This draft preserves the interface confirmed by its owner.
- Consumer `729cc83bc018f451124def177a1bb119db8d0d80`,
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
Retries read the saved set rather than rediscovering targets after cleanup.

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
implementation must derive `T` once and drive both capture and destructive
selection calls from that materialized set. Copying the SELECT into two
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
receipt lock and sorted owner fence locks in their established order. Any
migration-lifecycle gate must follow the repository's established order too;
062320's lifecycle gate and fresh-slot checks are not replaced by this design.

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
   `O(u)` and `T`. Persist the target rows. Validate their exact set and binding;
   insert the immutable completion record before the first destructive
   `ingest_apply_selection`, tombstone or directory-membership cleanup.
4. Execute the existing erase using that same captured target set. Commit the
   capture and SQL effects together. Do not capture from a later worker pass.

The completion record is inserted before cleanup but is visible to other
transactions only when the transaction commits. Cleanup failure must not commit
a completion record without the effects it witnesses. An existing caught RH010
rolls back the inner capture/cleanup together, while its intent and permanent
fences can remain. The reader then reports incomplete. A later successful retry
can fill that *known, post-install intent* under the same locks.

Distinguish that case from an old fence with no capture intent. It is not safe
to manufacture an intent for old, already-cleaned metadata and label it complete.
Existing non-failed-receipt early returns must not synthesize a witness.

All new binding checks occur before destructive cleanup. Ambiguous or mismatched
bindings never become complete. The precise refusal/receipt mapping must be
reviewed against existing erase behavior before an implementation is accepted.

## 4. Proposed immutable records

Use `sessions.session_erasure_replay_*` names. Do not introduce `ingest_%` or
`upload_%` tables with `org_id`, `workspace_id` or `user_id`: lazy bootstrap and
repair scan those names as prior authority.

| Record | Required binding and purpose |
| --- | --- |
| `session_erasure_replay_requests` | `manifest_id` (generated UUID), accepted receipt `request_id`, `org_id`, `workspace_id`, `source`, `requested_session_id`, capture-format version, and creation time. Intent only. |
| `session_erasure_replay_aliases` | The manifest's exact alias set; `(org_id, workspace_id, source, alias_session_id)` identifies a counted fence and resolves to its immutable originating manifest. |
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

Proposed reader (name/signature to be agreed with the consumer before SQL):

```text
sessions.session_erasure_replay_read(
  org text, workspace text, source text, fence_session text,
  after_cursor jsonb default null, page_size integer default 512
) -> jsonb
```

It is an owner-only, SECURITY INVOKER, STABLE read with a fixed search path,
fully qualified objects and no writes, locks, sequences, clock-based leases or
side effects. It must run in a READ ONLY transaction. One parameterized SELECT
per page is the proposed bounded transport; the consumer must confirm this
extension of its current single-result array seam.
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
advancing and non-overlapping. Start at the beginning; consume to the terminal
cursor, validate the complete count and streamed digest, and only then issue a
group replay receipt. A page limit or a partial response must never be adapted
directly into today's `{complete: true, targets: [...]}` result. The bridge
needs a draining adapter or streaming seam which cannot treat one page as the
whole manifest. Retries can safely reread immutable pages; partial replay is
not receipted and must remain idempotent.

The same owner reader is used at replay time and before `bridge_complete`.
There is no new app endpoint, runtime grant, end-user reader or bearer token.

## 7. Replay receipt and release coupling

The consumer must resolve and validate the manifest **before** the session
receipt lookup. At `729cc83b`, it reads an identity-derived receipt before
calling `resolveSessionTargets`; an old receipt must not bypass the new proof.

Propose a new receipt version/key domain binding hold epoch, manifest UUID,
accepted request ID, complete scope, capture version and full target digest.
Only hashes and aggregate counts leave SQL/the bridge process. A v1 receipt
keyed only by the synthesized `(workspace, source, session)` identity is not
valid evidence for this new group. Receipt validation rejects another scope,
manifest, digest, generation of the format, incomplete target count or wrong
erasure kind. Create-only writes remain idempotent and collisions fail closed.

Several counted alias fences may share that one validated group receipt. The
bridge must still account for each fence row in the release watermark. Do not
count manifest headers, target rows, distinct requests or replay API calls.
Keep the 062320 release count exactly:

```text
count(operator_user_erasures for org) + count(session_erasure_fences for org)
```

It is the total org count recomputed under the exclusive hold key, not the
number of records inside a FULL-time replay window. The bridge must reconcile
its window/baseline accounting with that total; adding this manifest must not
hide a mismatch. A new erase takes the shared org key, so an erase racing
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

Initial retention is indefinite and at least longer than any hold or replay
retry. There is no TTL, automatic GC or cleanup cascade. The new records do
retain target identifiers; this is an explicit audit/replay retention choice,
not a claim that all metadata at rest disappears with transcript bytes.
A future retention/export policy needs its own reviewed completeness proof;
it cannot delete the never-deleted fence/user-erase markers or change the count.

A future down should retain the manifest records, immutability guards and a
safe owner read path (or a fail-closed unsupported-version result). It must not
silently drop the only replay witness. Its exact retained schema, ACL manifest,
bootstrap verifier and reapply behavior require tests and registry review.
Do not invent a new lock order for the down; reconcile its lifecycle and live
capture serialization with 062320 before implementation.

## 9. User erases: not solved by this manifest

This proposal supplies no `userGenerationBoundary` proof. A captured user ID,
SQL reset count, timestamp, request ID or observed fence epoch alone does not
prove that an old-generation object write cannot land later, or that retrying
a reset after release will preserve a new-generation upload. The consumer must
continue returning `erasure_generation_unproven` without that separate proof.

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
  prove caught owner failure keeps fences plus intent but no seal, then retries
  capture before cleanup. No reconstruction from deleted rows.
- Cross-request/tenant/workspace/source binding rejection, same-request repeat,
  alias-input repeats, multiple counted aliases in one group, conflicting
  groups and old fences without intents. No request identity reassignment.
- Exact set equivalence against the production SQL effect path, not a fixture
  which mirrors the new producer. Include new-origin/new-owner admission in
  the separate whole-erase equivalence review.
- Read-only invocation, deterministic digest vectors, multiple pages,
  dropped/duplicate/reordered/forged-cursor pages, target counts exceeding a
  page, and full replay retries after partial effects. No partial receipt.
- V1, wrong-kind, wrong-digest, wrong-scope and partial receipts cannot bypass
  manifest resolution or satisfy release. Logs/receipts contain no raw IDs.
- A racing new erase, alias-fence versus manifest-group cardinality, an erase
  predating the replay window, and unchanged 062320 count under its exclusive
  key. Missing historical and empty coverage keep the bridge blocked.
- RLS/default-ACL/role negatives; UPDATE/DELETE/TRUNCATE and post-seal INSERT
  refusal; stale/concurrent mutation attempts; retained down/reapply guards.

## 11. Decisions before implementation

1. Consumer accepts manifest UUID plus persisted receipt request ID and full
   scope/digest as identity, resolves before receipt lookup, and agrees the
   paged/streaming read contract. No identity-only old receipt fallback.
2. Erase-equivalence owner accepts the exact target derivation, failure and
   alias-repeat rules, empty/historical refusals, and the distinction from
   global admission and user-generation proofs.
3. 062320 owner confirms its successor head, unchanged lock/count contract and
   down interaction. Registry owner allocates a later forward slot; none is
   claimed by this document.

Until these are agreed, this remains a review draft. It changes no SQL body,
bridge behavior, release gate or live state.
