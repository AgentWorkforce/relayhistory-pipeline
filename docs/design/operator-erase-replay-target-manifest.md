# Operator erase replay target manifest — draft 8

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

Refreshed 062340 handoff inputs, inspected without applying SQL:

- `6d60dc4fcd92957354e86c59600f82002b7c2751`,
  `supabase/migrations/20261105062340_ingest_org_erasure_history.sql`:
  `_bind` at 331–375 and `_release` at 389–425. The history fence is the
  hold's install, and the selected FULL is a separate sealed binding.
- `f98d54af681f06a7030492168461b83b246b7b1f`,
  `src/session-ingest/erasure-window.ts`: the same-statement status/source
  read and the executable history/FULL/coverage checks. Its legacy introductory
  two-table comments are not the authoritative 062340 field contract.
- `eaa5e2f01cfce16da1b73d202edb33f6e62b0ae3`,
  `src/session-ingest/hold-bridge.ts`: the manifest-missing refusal and existing
  resolver seam. This is not an implemented paged reader or v2 receipt adapter.

Section 7 replaces this draft's old surviving-two-table watermark guidance.
The closed section 6.1 reader v1 remains unchanged. The separate replay proof
wrapper below needs consumer/kernel co-review before it is wired into code.

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

### 2.1 Missing-link qualification hold

The review of producer `3d78e4aa` found that removing the only index link from
a physical ID to the requested logical ID leaves physical ownership outside
`A`, while a surviving logical directory owner can still produce a nonempty
manifest. Equality with the legacy SQL selection effects does not prove full
physical replay coverage in that case. The index resolver is not a retained
alias authority.

The current disposition is **no production seal without proved authoritative
alias coverage**. A workspace/source scan for ownership rows without an index
match is a useful negative disqualifier, not an accepted positive proof.
Neither adding directory rows to that scan nor finding no orphans establishes
that already-removed links or evidence never existed. Do not infer aliases
from a shared owner. No authoritative registry or closed-world capture
precondition has yet been accepted for this increment; its absence keeps the
production capture unsealed. A retained mapping or another sufficient proof
needs a separate reviewed design before enabling completion.

The production capture must continue the legacy erase without attempting a
seal that would abort it. The completion mutation path must also fail closed
when coverage authority is absent: an empty scan, caller flag or guessed
physical identity cannot authorize a seal. Keep the v1 reader shape for future
qualified captures; an intent is still `manifest_unsealed`, never a weaker
meaning of `complete:true`. Synthetic sealed fixtures may test the reader and
digest, but cannot demonstrate production alias-coverage qualification.

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

Associate effect provenance with that **same accepted request**, in the locked
capture/effect transaction. It must identify the actual SQL effect scope and
source-record identities/operations needed to correlate the capture to an
authoritative replay-set member; a caller-provided request string alone is
not proof that those effects occurred. Capture, seal and effect evidence
must commit or unwind together under section 3's existing failure rules.
The physical provenance columns or append-only sidecar, exact encoding and
per-source coverage relation must be reviewed with the implementer. They
must have the same compound scope/request binding and post-seal immutability
as the manifest. A transaction ID or timestamp is supporting provenance,
not a unique effect identity or completeness certificate by itself.
Keep provenance to the minimal identity/proof metadata allowed by section 8;
it must not introduce lease credentials, object paths or copied payloads.

Capture is independent of hold lifecycle: it must work before seal/FULL
selection and outside any hold. An active epoch observed during capture may
be saved as provenance only; absence of an active epoch is valid, not an erase
refusal. Neither a non-null capture epoch nor a guessed future FULL is part
of the target seal's authority. This metadata does not change reader v1 keys.

The proposed target-digest encoding is SHA-256 over the ASCII domain tag
`rh-erase-replay-manifest/v1`, followed by length-framed fields in this order:
capture version, canonical manifest UUID, accepted request ID, org, workspace,
source, requested session, alias count and each sorted alias, then target count
and each sorted target's user/origin/source/session fields. Each frame is a
four-byte unsigned big-endian **octet length** followed by the exact UTF-8
bytes. Counts and capture version are canonical decimal text in those frames,
not 32-bit binary counts. Target count remains a positive PostgreSQL bigint;
the tag/version and original bytes must not be silently normalized.

Aliases and targets use bytewise `COLLATE "C"` order. Do not use delimiter
concatenation, unordered JSON aggregation, Unicode normalization, JavaScript
UTF-16 sort assumptions, or consumer locale collation. No capture-time epoch
or future FULL enters this digest. Publish fixed non-ASCII/ordering and
boundary vectors with the implementation and freeze the encoding with the
consumer/kernel before v1 data is created. A later incompatible encoding
requires explicit format versioning. Effect-binding bytes have their own
separate versioned contract; a target digest is not an effect proof.

The first implementation's fixed 64 MiB encoding cap is a **proof resource
bound**, not an erase cardinality limit. Origins per owner remain unbounded.
Preflight the exact framed octet size without constructing the bytea, including
the raw tag, every four-byte prefix, UTF-8 field and decimal count. Arithmetic
must not overflow; prefer preflight before materializing the target product.
An oversized capture remains unsealed and uses the unchanged legacy erase
loop, receipts, events and 062340 recording. Do not compute the digest first
or introduce a runtime/caller-adjustable cap. The digest helper must enforce
the bound before aggregation too, so a seal argument or direct helper call
cannot allocate the oversized value before a trigger checks it. This does not
change the encoding or any digest below the cap. Test an injected small bound
below, at and above the exact size, multibyte fields and decimal digit changes,
direct oversize seal refusal, and full legacy-effect/history equality.

UPDATE, DELETE and TRUNCATE are refused on all four tables. Also reject new
alias/target INSERTs into a sealed manifest: immutable existing rows alone do
not prevent later extension of a previously hashed set. Completion insertion
must verify the exact counts/digest, and constraints/guards must prevent partial
commits or post-seal extension, including concurrent insert attempts. The
producer's existing lock and isolation discipline is part of that proof.
The child and completion mutation guards must reject non-READ COMMITTED
transactions (`40001`): a lock on an unchanged parent does not refresh an old
repeatable-read snapshot. This restriction applies to mutations, not the pure
reader, which retains REPEATABLE READ and SERIALIZABLE READ ONLY support.
Native controls must cover both stale-snapshot orders and a sealer waiting on
a child that commits, as well as rollback. After both transactions finish,
every persisted seal must match the actual rows, counts and digest; a refused
seal must leave no completion record.

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

### 7.1 Separate capture evidence from epoch/FULL qualification

The producer captures request-bound targets and associated effects before
cleanup, without requiring a hold or a selected FULL. A later replay must
independently qualify that immutable capture for its **current** epoch and
selected FULL. A capture-time epoch cannot substitute for that qualification,
and an outside-hold capture is neither automatically excluded nor automatically
admitted. Membership comes from the current authoritative erasure set and
complete request/effect coverage, not from a capture timestamp filter.

062340 `_bind` is allowed only after the epoch is sealed. It binds the history
fence to `hold.installed_at` and records the selected FULL's run, integer
`started_ms` and receipt digest. It is therefore incorrect to require a
pre-seal producer to bind a future FULL or to block a normal erase until that
FULL exists. Do not stamp later context into an already sealed target manifest.

### 7.2 Owner-only proof wrapper/co-read, outside reader v1

Preserve the exact section 6.1 page keys, nested shapes, cursor binding and
canonical target-count text. The epoch/FULL proof is a **separate** owner-only
co-read owned by the **bridge consumer**, not extra fields silently added to
a v1 page and not a new 062360 qualifier migration function. The consumer owns
that one-statement SQL and its versioned wrapper, v2 receipts, and final release
transaction. The producer owns immutable target/effect provenance and the
owner-only v1 reader. It must expose documented owner-readable provenance keys
or a pure STABLE read helper, and byte-exact effect encoding, for that statement.
The consumer implements the co-read in TypeScript like `ERASURE_WINDOW_SQL`.

One SQL statement reads `session_erasure_replay_read`, authoritative
`ingest_org_admission_hold_status`, the effect provenance and full-precision
window comparisons. The agreed outer field structure is `v:1`,
`kind:"session_replay_qualification"`, `page`, `window`, `window_checks`,
and `effect`. `page` is the section 6.1 response verbatim; `window` is status
verbatim. `window_checks` contains boolean `epoch_matches`,
`history_cutoff_matches`, and `history_full_ordered` computed in SQL; NULL or
unknown comparisons become false. The bridge distinguishes `history_unbound`
from mismatch using null `window.erasure_history.fence_at` or `selected_full`,
not a nullable boolean. There are no caller-supplied FULL
values in the producer reader: the bridge compares selected FULL values with
its own dispatch.

`effect` contains `v:1`, a closed `status`, boolean `request_id_matches`,
canonical nonnegative bigint-text `alias_fence_count`, and `binding_sha256`.
`binding_sha256` is null **exactly when** status is not `effect_proven`;
`alias_fence_count` is `"0"` for every unproven verdict. The initial missing
verdict also has a false request-match field and `status:"effect_mapping_missing"`.
A positive verdict requires a real complete effect association, the exact
declared/covered alias count and a lowercase 64-hex binding digest computed
**in the SQL co-read** over the specified versioned, length-framed actual effect
proof. The bridge also checks the count against `page.alias_ids.length` without
lossy conversion. It is not a hash of booleans or counts. The v2 receipt binds
that digest. The exact refusal enumeration, provenance-read schema and
effect bytes must be reviewed before any positive verdict is implemented.
A source lacking a proved request-to-effect join stays unproven. No raw
plural `effects` object or xid/count summary substitutes for this verdict.
No new runtime grant or mutating reader is proposed.

At replay, bind the expected org and epoch to the caller's selected FULL
dispatch and verify all of the following from the qualified co-read:

- The live hold is the same epoch in the required sealed/marker-attempted
  state. Compare the installed history fence with `hold.installed_at` at
  full SQL timestamptz precision, not rounded client milliseconds.
- `selected_full.run`, `selected_full.started_ms` and
  `selected_full.evidence_sha256` equal that exact dispatch's FULL run,
  integer start and verified receipt digest. Require the SQL comparison
  `installed_at < FULL start <= sealed_at`. The fence and FULL start are
  different concepts; neither substitutes for the other.
- History is bound, covers the installed fence and is recording. Every
  `by_source` key is from the shared authoritative source inventory, all
  counts are valid and their sum equals status `erasures`. Validate the
  supported count range losslessly; an unsupported range refuses rather
  than rounding. Any corroborating all-source snapshot checks still apply.
- Match each authoritative session-erasure member to the original accepted
  request, complete manifest UUID/scope/aliases/digest and its committed
  effect provenance. Do not infer membership or full coverage from an equal
  request string, current survivors, generic event count, timestamp or xid.
- Any source/operation, missing identity, overwritten/transient evidence or
  overlap that cannot be reconciled with a complete request-bound effect
  proof keeps the positive window blocked. This session manifest does not
  establish replay coverage for every other erasure type in the inventory.

062340 history deliberately stores coarse org/source/op/time/xid events,
not target identities. Its scalar count cannot supply the missing effect
mapping. The implementation must supply a reviewed per-source association
from actual erase effects; without it the wrapper remains unproven. A
matching count or an array containing only the surviving fence rows is not
a completeness proof for the whole window.

#### Request attribution and bidirectional reconciliation

An eligible effect witness records the validated org, the **actual emitting
transaction's xid8**, accepted request/manifest binding, source/operation,
the mutation identities needed by that source's covering proof, and exact
multiplicity. The xid8 comes from the writer transaction, never caller input
or a guessed 32-bit row xmin. Preserve its exact textual/type value; do not
round it through a JavaScript number. Per-source/op counts are canonical
nonnegative bigint values, checked without lossy conversion.

The producer must prove unique request-to-effect attribution under that
transaction's locks, committing the witness with the effects it attests to.
It may not assign every row for `(org,xid8)` to whichever erase finishes first.
Multiple requests sharing a transaction are permitted only if every covered
mutation has an unambiguous request attribution; otherwise qualification
refuses. Unrelated same-org writes, nested calls, or outer runtime writes
after the capture helper returns cannot be silently absorbed by its certificate.
The call/request association must be established by reviewed actual mutation
provenance or an equivalently enforced writer protocol, not reconstructed by
source/op totals, timestamps, SQL call ordering assumptions or current rows.

For the qualified epoch/window, let `H(org,xid8,source,op)` be the authoritative
history multiplicities and `E` be the multiset projection of the genuinely
attributed, validated effect witnesses. Before object I/O the bridge must
reconcile **both directions**:

1. Every in-window history occurrence has exactly one covering attribution
   under its applicable per-source proof. No missing source/op, unmatched
   transaction, undiscovered request or partial multiplicity may be ignored.
2. Every claimed witness occurrence maps to that same qualified history
   window and request/effect domain. No orphan certificate, extra occurrence,
   double credit, cross-org member or unsupported operation is allowed.

The bridge's owner-only co-read derives `H` from the actual 062340 history
rows in the same statement, not merely status `by_source` totals. This adds
no runtime grant or v1 page fields and does not change status JSON; xid/op
details remain internal to the qualified SQL proof and hash-only diagnostics.

`E` here projects only effects claimed for this window. An earlier complete
capture may remain the immutable identity/target anchor without its old
effects being counted again after the fence. Do not exclude an otherwise
qualified outside-hold capture solely because its capture transaction predates
the window, or import those old occurrences just to balance a later count.

After unique attribution is proved, projected `E` and `H` must match in every
org/xid8/source/op multiplicity, not just in their grand total or `by_source`
sum. That equality is necessary corroboration, not the identity proof itself.
A source row can disappear or change after its mutation; an after-the-fact
join to survivors is not a substitute for missing mutation evidence. History
and the effect witnesses must be read with the same epoch/FULL/coverage
binding as the manifest preparation. Any ambiguity means no receipt lookup,
mutation or release, even if a cached receipt looks valid.
The consumer performs qualification on a read-only transaction/connection
separate from an uncommitted producer write transaction. Seeing one's own
uncommitted seal during a diagnostic SELECT is not replay authority; no object
I/O may run before the source effects and witness have committed. This does
not change the pure v1 reader's supported transaction-mode behavior.

#### Covering proof or required zero for every history source

The following is the scope of the first prospective session-capture increment,
not a statement that any covering implementation already exists. A candidate
proof below is usable only after its actual request joins, operation identities
and multiplicities are reviewed. If that proof is absent, its authoritative
history multiplicity must be zero; otherwise qualification remains missing or
partial. Absence of a `by_source` entry means zero only after validating the
full 062340 status and shared source inventory, never by dropping an unknown key.

| Source | Required covering proof, or zero in this increment |
| --- | --- |
| `operator_user_erasures` | Required zero for session-manifest qualification. A separate user replay still requires its bytes and generation proof; no session certificate covers it. |
| `session_erasure_fences` | Actual INSERT identities bind the full org/workspace/source/alias keys to the manifest's accepted request and complete alias declaration. Existing historical fences cannot be retroactively assigned to a fresh intent. UPDATE/DELETE or unproved alias/request attribution refuses. |
| `session_erasure_receipts` | Actual INSERT/UPDATE effects bind the exact receipt compound key and its persisted accepted request, including every claimed mutation occurrence. A matching current receipt alone does not identify all historical updates; DELETE or unproved mutations refuse. |
| `session_erasure_events` | Actual append event identities bind their receipt scope and accepted request, with any incoming retry ID explicitly distinguished. Every claimed INSERT is uniquely attributed. Later audit/step events are not presumed covered merely because they name the same session; unsupported operations refuse. |
| `session_erasure_runs` | Required zero unless a separately reviewed per-mutation runtime/run-to-request proof covers the full lifecycle. A run count or current lease is not that proof; the capture helper does not cover later outer-runtime inserts by implication. |
| `session_erasure_scopes` | Required zero unless separately reviewed immutable run/alias/request provenance and actual mutation attribution cover it. Do not infer it solely from an alias count. |
| `session_erasure_writes` | Required zero unless separately reviewed write-obligation-to-request/effect provenance covers each begin/finish/failure/cleanup transaction. An expired obligation or its disappearance is not evidence of coverage. |
| `upload_deletions` | Actual selection mutations must bind the full org/workspace/user/origin/source/session key to the freshly materialized target set and accepted request. Each claimed source/op occurrence must come from that reviewed selection effect path. Unrelated selection/re-enable or lost identity refuses. |
| `upload_resets` | Required zero. A whole-user reset is not proved by a finite session target set; a future user/reset proof is separate. |
| `upload_erasure_rearm` | Required zero. Rearm/re-admission and later writes need separate authority/generation coverage, not a session-alias certificate. |
| `relayfacts_erasure_fences` | Required zero. Relayfacts/user erasure is a distinct effect domain, not implied by this session capture. |
| `session_analysis_purge_fences` | Required zero without an independently reviewed analysis-purge effect proof; target capture is not proof that derived bytes were purged. |
| `session_analysis_purge_authority` | Required zero without its own reviewed authority-to-effect proof. A grant/authority row is not a purge receipt. |
| `ingest_frozen_source_deletions` | Required zero without a reviewed frozen-source lineage/control effect proof. Session target membership alone does not establish it. |
| `ingest_frozen_source_resets` | Required zero without a reviewed frozen-source reset proof; a broader reset cannot be inferred from finite aliases. |

Required zero means **refuse proof when nonzero**, not prevent, skip or delete
the underlying erase operation. Ordinary runtime paths do write sources not
yet covered by the first capture-call certificate. Therefore the scoped first
increment can persist useful target witnesses while remaining unable to
qualify a positive runtime window. Do not claim it completes wave-2 release
or expand it to all runtime writers without an explicit reviewed scope change.
Future covering proofs can extend this matrix only with versioned evidence,
qualified request attribution and the same bidirectional reconciliation.

At the inspected `6d60dc4f` source, runtime scopes and runs are inserted after
`_session` returns (062320:1706–1708). Their `target_session_id` and storage
scope need comparison to the sealed targets or independent proof. Lease-only
run updates (050020:161/173/194) are merely bookkeeping candidates: a durable
witness must bind the exact run to the accepted request and establish unchanged
target/storage scope. Classify each event writer and transition before claiming
audit-only coverage. Write registration, finish and failure (050020:58/69/77)
use `writer_id`; sharing a session does not bind that writer to an erase request.
No later source mutation is qualified as bookkeeping solely by its history
row. Capture missing attribution at its actual writer in a separately reviewed
increment, or leave it uncovered; this partial increment requires no runtime
rewrite.

The consumer-owned co-read computes `binding_sha256` **in SQL**, only over the
concrete evidence actually validated, using the producer/consumer's agreed
byte-exact versioned, length-framed encoding and shared test vector. It must
bind the accepted manifest/request/scope and attributed
effect facts, not a guessed complete set or a hash of the expected counts.
Until the physical provenance/read contract and vectors exist, keep it null
and qualification unproven. Missing required coverage also leaves the runtime
binding digest null; a hash of a validated subset is not promoted to complete
proof. The v2 receipt cannot turn that null or a target digest into a complete
effect proof.

Drain every manifest page in the SQL-only preparation phase and pin its
identity, accepted request, full scope, capture version, aliases, count and
digest. Pin the wrapper's epoch, installed fence, selected FULL triple and
authoritative history watermark across that preparation too. Refuse mixed
snapshots/bindings, missing pages, incomplete effect mapping or changed
coverage. A mismatch on a final page still means **zero S3 calls**, including
receipt reads. A new history watermark requires a fresh complete preparation,
not acceptance of a mixed prefix. Read-only isolation alone is not a claim
that authority remains current forever.

The application pass rereads the same sealed manifest and qualifies the same
epoch/FULL and effect binding. If authority, coverage or watermark changes,
stop and refresh the qualified plan; do not create a success receipt from a
stale wrapper. Effects already applied from a completely prevalidated set
retain section 6's idempotent partial-application semantics. Release readiness
must freshly qualify all current members and receipts again, even after a
prior successful replay. No warm-cache or receipt-only exemption exists.

### 7.3 Receipts are group evidence, not the history watermark

The consumer must prepare the complete validated plan from section 6 **before**
this erasure replay's first S3 call, including its receipt lookup. Only afterward
may a receipt authorize `already` or a plan authorize mutations. Current
hold-bridge (`8818b764` and the `f98d54af` landing unit) already resolves and
validates the complete session target array before receipt lookup. Preserve
that placement when replacing the legacy array seam with the paged wrapper;
the earlier callback-presence-only review input is superseded. An old receipt
cannot bypass plan preparation. Release readiness must independently validate
the same plan and receipt binding; a previously cached replay outcome is not
sufficient.

Propose a new receipt version/key domain binding hold epoch and its installed
fence/selected-FULL context, manifest UUID, accepted request ID, complete scope,
capture version, full target digest and the validated effect-proof binding.
Only hashes and aggregate counts leave SQL/the bridge process. A v1 receipt
keyed only by the synthesized `(workspace, source, session)` identity is not
valid evidence for this new group. Receipt validation rejects another scope,
manifest, digest, generation of the format, incomplete target count or wrong
erasure kind. Create-only writes remain idempotent and collisions fail closed.
The user-erase prerequisites in section 9 apply before receipt reuse too; a
structurally valid cached user receipt is not itself generation or byte proof.

Several aliases or source events may resolve to one validated group receipt.
That coalescing never changes the authoritative history watermark or proves
that unrelated events are covered. Do not count manifest headers, target rows,
distinct requests, fence aliases, receipts or replay API calls as that watermark.
With 062340, pass the co-read status `erasures`, whose meaning is:

```text
count(ingest_org_erasure_history rows for org at or after bound fence_at)
```

The baseline 062320 surviving-two-table count is **not** sufficient authority
for this 062340 handoff. No fallback to that count, a caller FULL cutoff,
`set.length`, or a group count may authorize release. Manifest/provenance tables
are proof storage, not new erasure sources: explicitly classify them in the
shared inventory/exclusions so their own inserts do not recursively count as
erase effects. Preserve the 062340 recorder and source semantics.

The final release, through the org lifecycle lock, rechecks epoch/state,
bound history coverage/recording and the fresh authoritative count under the
exclusive org key. A racing source write is counted before release or waits
until after it. A count match is necessary but not a replacement for the
wrapper's independent effect/manifest/receipt checks. No marker or history
row is deleted to hide a mismatch. An observed watermark stored with a group
receipt is not valid evidence for later unseen events without fresh coverage.

`complete` does not approve `bridge_complete`; all replay receipts, the current
hold epoch/state and the existing final count check are still required.

### 7.4 Required lifecycle cases

- **Pre-seal erase:** capture and SQL cleanup succeed together before a FULL
  is selected. No future FULL is fabricated. Once sealed/bound, replay obtains
  fresh epoch/FULL qualification against the original manifest and effects.
- **Outside-hold erase:** capture succeeds with no active epoch. A later hold
  admits it only when its authoritative set and effect evidence require and
  prove that request; absence of capture-time epoch is not itself missing proof.
- **Already-cleaned historical request without capture:** refuse with
  `session_manifest_missing`, even if bytes are gone or an unrelated current
  hold/FULL/count looks valid. No automatic historical backfill is introduced.
- **Epoch/FULL mismatch or coverage gap:** no S3 preparation effects or receipt
  lookup, no success receipt, and no release. A partial capture stays partial.
- **Qualified empty window:** it may use the independently reviewed empty-window
  path without a manifest when all-source history/snapshot and final release
  checks qualify. This producer neither establishes nor overrides that proof.

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
062360's down restores its recorded pre-062360 erase-entry bodies, preserving
the **062320** marked first erasure check. In the pinned 062340 integration,
062340 replaces hold status/release, not those erase-entry bodies: leave its
status/release, history, coverage, bindings and guards intact. If a later
integration changes an erase body, record that latest original in the lane
instead of blindly restoring an older 062320 copy. The 062320 later-migration
guard still applies: roll back all later applied slots in the proper order
before 062320 restores its own pre-062320 bodies. Do not skip intervening
062340/other slots merely because this manifest has been rolled back. Test
that full sequence and the restored live prosrc.
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
- Capture before seal/FULL selection and outside a hold: no future FULL or
  mandatory capture-time epoch. Later qualification must use the selected
  epoch/FULL and actual effect membership. A capture-time epoch alone cannot
  approve replay or exclude a valid outside-hold capture.
- Cross-page changes to epoch, installed fence, selected FULL run/start/digest,
  history watermark or coverage refuse before any S3 call. Test a complete
  valid v1 manifest with incomplete/mismatched effect provenance, ambiguous
  xid/time matches and an unresolved non-session source: none can authorize
  receipt lookup, mutation or release. No silent reader-v1 field additions.
- Same-org/same-xid multiple requests or unrelated control writes, equal totals
  with swapped request/row identities, duplicate/orphan witnesses, missing or
  extra per-source/op occurrences, and outer-runtime writes after capture all
  refuse unless uniquely attributed and reconciled in both directions. Cover
  each of the 15 source rows in the matrix, including required-zero failures.
  Preseeded receipts never bypass these negatives or allow any object I/O.
- Explicit qualification fixtures: two same-org erase requests in one xid,
  with no unique per-mutation attribution; and a later transaction finishing
  or failing a registered writer that names the same session but lacks a
  durable request relation. Both retain `effect_mapping_missing` and a null
  binding digest, with zero object calls and no qualifying receipt or release.
- Wrapper negatives: non-null effect digest for unproven status; null/non-hex
  digest for proven status; alias-count mismatch or malformed/overflow text;
  and unknown SQL comparisons. Unproven counts are `"0"`, strict booleans
  remain false, and unbound is diagnosed from structured window nulls. Prove
  the effect-digest vector against the actual SQL encoding before enabling
  a positive verdict, not a hash reconstructed from a client summary.
- A racing source write between proof and release, transient/deleted source
  rows, history-event versus alias/group cardinality, and an erase predating
  the replay window. The final exclusive-key check uses authoritative 062340
  history, never the old two-table count or replay-group count. Unrecorded
  historical capture remains `session_manifest_missing`; a qualified empty
  window remains a separate path. Missing/empty target proof does not become
  an empty-window proof.
- RLS/default-ACL/role negatives; UPDATE/DELETE/TRUNCATE and post-seal INSERT
  refusal; stale/concurrent mutation attempts; retained down/reapply guards.
- After full migrate, assert the marked erasure check is first in the live
  latest replaced bodies, not merely present in 062320's source file. Test
  062360 down restores its recorded latest erase bodies with the 062320 check,
  while retaining the installed 062340 history contract. Only the subsequent
  proper rollback of later slots permits 062320 down to restore its originals.

## 11. Decisions before implementation

1. Consumer has accepted the reader signature, bounded keyset transport and
   closed section 6.1 v1 pages. Separately qualify/version the section 7
   window/effect wrapper and the epoch/FULL-bound v2 receipt domain with the
   consumer and kernel reviewers. Confirm complete preparation before receipt reuse
   **and before any mutation**, including both user-proof paths. No identity-only
   or pre-proof old receipt fallback in replay or release.
2. Erase-equivalence owner accepts the exact target derivation, failure and
   alias-repeat rules, empty/historical refusals, and the distinction from
   global admission and user-generation proofs.
3. 062320 owner has confirmed compatibility and successor `e13f8541`; preserve
   its latest-body, first-statement, live-prosrc and down requirements above.
   Slot 062360 is reserved by the registry owner, not implementation approval.
   This refreshed handoff also requires 062340 history/selected-FULL binding,
   the current per-source effect coverage relation, and pinned integration
   originals. Old baseline two-table watermark guidance is superseded.
4. The owner explicitly decides raw-target identifier retention. Indefinite
   retention remains a proposal, separate from ACL/lock compatibility approval.

Until these are agreed, this remains a review draft. It changes no SQL body,
bridge behavior, release gate or live state.

## 12. Producer implementation handoff clarifications

- **Eligibility:** without a separate continuity proof, only the uninterrupted
  call that created both receipt and intent may seal. Failed-receipt retries
  keep an existing intent unsealed and never invent capture for a pre-install
  receipt. A full transaction rollback leaves no committed receipt/effects;
  a later genuinely fresh attempt is distinct from a committed failed intent.
- **Org lock:** the replaced `_session` records an erasure and therefore takes
  section 3's marked erasure check first too. Owner-only ACL does not enforce
  runtime-wrapper-only invocation. A same-key SHARED reacquisition is reentrant;
  a direct owner call must not take GLOBAL X before needing org S. Do not
  blanket-add new erasure gates to read/step helpers. Verify the live body and
  direct-owner/lifecycle ordering in native PostgreSQL.
- **Overlap/empty:** a fresh closure overlapping another request's alias group
  gets no partial intent/A0 or silent union. An older seal is not proof of the
  new request's effects; only a genuinely fully covered repeat can reuse it.
  An empty owner set or unwind with no seal may return `manifest_unsealed`;
  no extra marker table or false empty completion is needed just for diagnosis.
  A pre-existing alias fence without a complete original capture is historical
  proof debt too, even when it has no alias-to-manifest binding yet. A fresh
  receipt must not launder that alias into complete history by discovering only
  its surviving owners; preserve legacy erase behavior and keep proof unproven.
- **Compound keys/down:** full request/scope FKs may be added only in the new
  migration, never by changing 050000 bytes. A retained FK to receipts can
  prevent 050000's DROP TABLE even with empty histories. Its populated and
  empty down/reapply behavior needs a reviewed explicit plan; preserving all
  FKs is not automatically equivalent to restoring older rollback behavior.
  Never cascade away proof or bypass the older guards to make a down pass.
- **Reader:** read-only behavior and support inside a READ ONLY transaction
  are required; this does not add an undocumented `transaction_read_only`
  mode precondition that rejects a valid pure SELECT in a read-write transaction.
  Cursors must identify an existing non-final target of the same manifest and
  digest, while the consumer starts at null and validates the complete chain.
  Origins have no declared fixed bound: 64 owners and 256 aliases do not make
  full-set work per page constant. Budgets fail closed without truncating proof
  or inventing a new erase cardinality refusal.
- **Shared fence body:** a six-argument owner-only implementation and retained
  five-argument NULL-manifest wrapper can share one effect path. No ambiguous
  DEFAULT overload. The manifest path uses T freshly materialized after owner
  convergence under this call's locks, seals before selection, and unwinds with
  failed effects. A saved seal is replay evidence, not a way to narrow new SQL
  effects. The NULL path preserves legacy convergence/refusals and mutations.
- **Partial phase:** a first code-only increment may implement prospective
  targets/reader while effect qualification remains missing. It does not
  satisfy positive-window release. In particular, grouping history by org and
  current xid/source/op is not request attribution: the transaction can contain
  multiple erases, unrelated writes, and outer runtime work after `_session`
  returns. Later runtime effects require an independently reviewed association
  or remain unmapped. Do not silently broaden the scope to every runtime entry.
- **Classification/ownership:** the producer branch adds a closed explicit
  `ERASURE_PROOF_TABLES` classification and causal inventory tests with consumer
  review, atomically with its new relations. Org IDs are allowed in that proof
  class, but it never becomes an erasure source. No blanket prefix exemption:
  unknown future matching tables must still fail classification. The consumer
  owns draining, preparation, v2 receipts and release; no frozen reader change
  or producer-owned migration qualifier is required.
  The classification edit touches the consumer landing unit's
  `src/session-ingest/erasure-window.ts` and its test. Stack it on the
  `f98d54af` landing unit, or on main after that unit lands; do not rewrite
  the frozen head or copy an older incompatible file over it. Coordinate
  ancestry and review with the consumer before the producer push.
