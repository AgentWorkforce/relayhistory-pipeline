# Guarded publication retirement for user erase

Draft 1 — design comparison only. **No clearance path or implementation is
selected.** The combined `RH040` / `RH011` hold remains in force. This work
covers never-published abandoned claims, not general erasure of published
buffered sessions. It changes no SQL, grants, storage authority, or runtime.
The erase lead is `hard-delete-owner`; `layered-sessions-pipeline` is the
publication-kernel co-reviewer and erase-equivalence owner. The T25 builder's
scope is unchanged. This is separate from replay-manifest slot 062330; no
migration slot is claimed here.

## 1. Source and evidence boundary

Main source reviewed: `8840659b77211c24277bc62ed6675830653caaea`.
The accepted hold disposition is on
`review/hard-delete-owner-t25-erasure-disposition`, with its attribution
correction at `121f164f`. The publisher's named paused-fleet test covers
pre-abandon refusal and frontier revocation. Abandon, repeated `RH040`, and
`RH011` are separately reported disposable PG16 probes at local T25 head
`4eb7dc0b`; ordinary-path parity on `fde1fe24` is builder-reported. These
are refusal evidence, not proof that a new clearance works.

Relevant current source:

- [058010 user erase](../../supabase/migrations/20261105058010_operator_user_erase.sql):
  presence probes, ledger states, purge order, completion, and writer fence.
- [Canonical publication](../../supabase/migrations/20261104060000_ingest_canonical_publication.sql):
  table relationships, locked attempt, heartbeat, abandon, and generation GC.
- [043850 acquisition](../../supabase/migrations/20261105043850_ingest_acquisition_owner.sql):
  replacement ordinary claim, including owner authority checks.
- [059000 provider publication](../../supabase/migrations/20261105059000_ingest_publication_gcs_provider.sql):
  replacement prepare and publish bodies.
- [050010 erase fence](../../supabase/migrations/20261105050010_operator_session_erase_fence_check.sql):
  global gate before the user-fence row and pointer fencing.
- [062010 origin fence](../../supabase/migrations/20261105062010_ingest_origin_claim_fence.sql):
  latest main `ingest_lock_fences`, including its marked-origin shared row lock.
- [Publication image writes](../../src/session-ingest/publication-image.ts) and
  [user byte eraser](../../src/session-erase/user-erase.ts): independent storage
  effects and the current machine-prefix inventory.

Two held interfaces also constrain the design, without being treated as
installed: org-hold branch `review/org-admission-hold-062320` at `e13f8541`,
and its `supabase/proposals/continuous-publication/062280-scoped-kernels.up.sql`
and `062290-scoped-recover.up.sql` review components. Their bodies and grants
must be rechecked against the actual integration heads before implementation.

## 2. Why a state-only exception is insufficient

User begin, step, and final purge reject any publication head or generation.
Abandon retains those rows. Current user purge deletes images and attempts,
but never deletes heads, generations, or publication-object registrations.
Changing only begin's predicate would strand step, break foreign keys, or
misstate completion. The current user byte inventory does not enumerate
`upload-generations/g=<generation>/`; it cannot certify that namespace empty.

Main's `writePublicationImage` awaits prepare and registration before its
storage PUTs. That ordering is useful evidence for a known cooperating
writer, but SQL alone does not enforce the behavior of every holder of storage
authority. An empty registration table, a rejected attempt, or a stopped
process is insufficient without a proven closed writer/authority inventory.
Lease expiry is SQL authority expiry, not storage revocation. Generations and
immutable raw keys may also have readers or references outside the attempted
publication.

The narrow design must retain all other `RH040` blockers. It must never use
`state IN ('building','retired')` alone as authority to erase. A retired head
may have a publication history; a building head may contain a legacy base
pointer. Prepared state is outside this first design, even if later rejected,
when it has left images or registrations.

## 3. Shared eligibility contract

Both alternatives use one closed, request-bound eligibility definition.
For a normalized `(request_id, org, workspace, user)` and a fresh
read-committed snapshot under section 4's locks, prove **all** of the following:

1. No other active erase owns the user, and the request ID is either new or
   already bound to exactly this user and scope. Existing completed requests
   return their receipt; they cannot authorize cleanup of a later footprint.
2. Every existing non-publication `RH040` blocker is absent, including any
   `session_index` row (soft-deleted too), record revisions, custody/frozen
   evidence, history ownership/outbox, and published history plans. Matching
   physical identities and base-pointer carriers must also be checked when
   their canonical/user spelling differs. Ambiguous alias or owner coverage
   refuses; it never widens the deletion target.
3. Every candidate head is exactly bound to this user and one captured
   generation closure, is `building` or `retired`, and has null `current_key`,
   `current_raw_sha256`, and `base_pointer`, with `pointer_revision = 0`.
   No candidate generation is live, no known generation was published, and
   each head's current generation exists with the same full logical owner.
   Missing historical lineage is unproven, not an empty history.
   Its physical tuple, generation, fence/reset epochs, pointer revision and
   row version are captured, not recomputed from a lossy identifier.
4. All attempts that reference any candidate through a build token,
   generation, stage, or JSON head snapshot are accounted for. Each is
   `rejected`, has no `published_at` or completion digest, and has no live
   attempt, stage-GC, head-build, or object-GC lease. A stale process with an
   `admitted`/`prepared` attempt must first use a supported abandon/recovery
   path; this design does not silently reject it. All times are checked again
   after waits using the database clock. Missing or inconsistent provenance
   refuses rather than inventing a terminal attempt.
5. No publication image or object registration belongs to any generation in
   the closure, including retired generations not currently named by the
   head. No published attempt or image outside the candidate set references
   it. No index, sibling head, base pointer, derived object, carrier, or
   immutable-key reference outside that set depends on it. A shared reference
   blocks the whole user request; it is not cascaded or partially cleared.
6. The closure includes all of this user's candidate heads, all generations
   linked by the full logical key, and all touching attempts/stages. A single
   attempt may span several heads. Special publication identities such as
   `_none` / `_origin` are not skipped. Every dependency is either wholly in
   this scope or an explicit blocker. Bounded discovery that overflows its
   implementation budget refuses without a partial permit.
7. The scoped authority path, when installed, can be permanently invalidated
   for these old admissions/attempts. Disabled admissions, recovery tickets,
   and registrations are inventoried even after an attempt is rejected.
   Refuse unknown newer kernel versions or unsupported dependency tables.

These are proposed conditions, not an executable predicate proven complete.
The kernel co-review must supply the full reference inventory, exact SQL
joins, and a bound for the closure before acceptance. Empty FK reference
sets alone do not prove absence of JSON/object references. For this narrow
version, any image or registration is a refusal even if already redacted.

## 4. Lock and authority order

No storage call occurs while holding a SQL transaction or these locks.
Use read committed and preserve the existing release/schema gate protocol.
Where 062320 is installed, the first statement after `BEGIN` of an
erase-recording entry point remains
`PERFORM sessions.ingest_org_admission_hold_check(p_org,'erasure');`.
This takes the org hold key shared before erase locks and must not be moved
behind a user or publication lock. A helper called by that entry point does
not introduce a second, reversed lifecycle-gate order.

The clearance transaction then follows this order:

1. Global erase advisory key `(961996,50000)` **exclusive**.
2. Existing privacy-authority key `(1919249529,96)` shared.
3. Exact erase ledger row, if it exists, `FOR UPDATE`.
4. Exact user's `ingest_fence_owners` row `FOR UPDATE`, creating it through
   the existing guarded path when absent. Do not use a shared origin mark to
   weaken the erase-side lock.
5. Scoped admission/recovery rows when applicable, then the relevant batches,
   stages/outbox rows and publication frontiers. Revoke only this user's
   frontiers within the erase transaction.
6. Existing physical publication-pointer advisory keys, acquired with the
   existing nonblocking/abort behavior in deterministic bytewise physical
   tuple order, then heads, generations, and touching attempts/dependencies
   in deterministic key order. Re-read after any wait before authorizing.

All eligible mutating writers must acquire the global shared key before
their user or publication rows. Main's owner INSERT statement trigger does
this for `ingest_lock_fences`; the held scoped gate calls that helper before
its authority rows. The exclusive gate is the serialization argument, not
the order in which an initial unlocked discovery query happens to see rows.
Do not optimize the gate away on an apparently unfenced session. Discovery
outside locks is advisory only; missing/new rows are re-enumerated under X.

The exact writer inventory must include claim, heartbeat, abandon, recovery,
prepare, register, final publish, GC claim/finish, owner/frontier writers,
and any direct owner operation admitted by the intended integration tree.
If one mutator can reach these rows without the gate, neither alternative
is accepted until it is fenced or given a proven compatible order. Never
wait for global X while holding admission, stage, or head rows. Lock errors,
deadlocks, schema changes, and snapshot mismatches abort and retry the whole
transaction; a partial candidate set is never committed as clearance.

## 5. Alternatives for the shared contract

| Concern | A: erase-owned clearance | B: guarded kernel interface |
| --- | --- | --- |
| Entry | Extend owner-only user begin/step/complete with the exact closed eligibility predicate. | Introduce an owner-only kernel helper that verifies and invalidates the candidate closure, invoked by an erase-recording transaction. |
| Guard ownership | Erase owns the predicate and purge inventory; publication co-review pins every dependency and token gate. | Kernel owns the closure and its invalidation witness; erase consumes that exact witness and retains all other blockers. |
| Atomic boundary | New ledger fence and accepted candidate set commit together at begin. Cleanup happens only under that same request's later locked phase. | Helper and erase begin commit together, or an equivalent durable fenced handoff is proved. There is no free-standing delete-then-begin window. |
| Compatibility cost | Replaces begin, step, remaining, purge, and completion consistently; changes the job's storage inventory. | Adds the kernel helper/witness contract and its versioning, plus matching erase probes, purge/job support, and rollback guards. |
| Main risk | Erase duplicates kernel reference and authority knowledge as publication evolves. | A stale, reusable, or caller-forged witness becomes a destructive capability, or new kernel semantics are missed by erase. |

For B the witness binds the accepted request, full tenant/user, exact head
and generation IDs, versions/epochs, all attempt/admission dependencies, and
the kernel contract version. It is owner-created in SQL, not a caller boolean,
GUC, token supplied by the publisher, or an unsigned JSON list. The helper
cannot grant runtime publisher roles erase/GC authority. A stale or changed
set invalidates the witness; it is not widened on retry. Physical deletion
before a durable user-erase fence is expressly forbidden.

For either option, the earliest durable success is **erase admitted and old
publication authority invalidated**, not “erased.” Heads/generations and the
captured target inventory remain available until the storage boundary and
metadata cleanup are proven. Both options require the same external-write
proof; selecting B does not manufacture it. This comparison selects neither
option. Joint owners must choose the boundary and resolve section 9 first.

## 6. Begin, step, bytes, complete, and retry semantics

**Begin:** Under section 4's locks, validate all ordinary blockers and the
full candidate set before effects. Bind the candidate closure and exact
generation storage targets to the accepted request in durable owner-only
SQL cleanup state. Each target uses the generation's recorded bucket,
region/root and exact prefix, with the provider pinned from the trusted
deployment configuration and checked against those recorded locations
(the generation table has no provider column). Never trust operator-supplied
prefixes or a caller-selected provider.
Commit that state, scoped admission invalidation, frontier revocation, token
revocation, and the ordinary `purging` erase ledger fence atomically. A
refusal or exception rolls them all back. No partially admitted user or
clearance-only success is returned. Existing requests retain their original
binding. A retry does not rediscover and authorize new generations.
The stored comparison images are the expected **post-invalidation** versions;
own version increments are recorded in the same transaction, never treated
as a reason to accept arbitrary later changes.

**Writer quiescence:** Once begin has fenced SQL writers, establish a durable
storage-write boundary for every captured generation before removing its
metadata. SQL lease rejection cannot revoke an already issued PUT. Require
a provider-enforced fence/revocation or a proven finite capability horizon
and drained in-flight requests, including multipart completion and delayed
requests. Record the bound authority/epoch and its proof; a caller's assertion,
process exit, timer, or one empty listing is insufficient. Ordinary credentials
with unbounded generation write power do not satisfy this condition. If that
boundary cannot be supplied, stay `purging` and refuse destructive clearance.
This proof mechanism does not currently exist in this design as an accepted
implementation; it is a blocking dependency, not a promised property.

**Step:** Revalidate the frozen set and versions under the same locks, with
all original non-publication blockers still strict. New metadata, active
leases, changed versions, or outside references stop the step. Preserve the
existing lazy-slot tombstones and shared-seat checks. Once writer quiescence
is proved, remove candidate dependency rows in verified FK order: scoped
recovery children and terminated authority references as required, then
images/attempts where eligible, generations after all references are absent,
then heads. The narrow version expects no image/object rows and refuses
unexpected ones. Do not use cascading deletion or remove unrelated members
of a multi-head attempt. Extend counts and remaining probes to the same
inventory. The final purge rechecks that inventory under a fresh snapshot
before `metadata_purged` commits. Any exception rolls back that whole step;
the earlier begin fence stays active across transactions.

The held `publication_private.expired_tickets` table has an FK to attempts;
its admission parent also records the stage/attempt. Current purge does not
handle that table. A concrete B or A implementation must specify safe removal
or nonblocking terminal representation for those references, without keeping
usable old tokens or silently losing kernel evidence. No new indefinite
raw-identity retention policy is established here.

**Bytes:** The job must erase every version and delete marker under both its
existing user machine prefixes and the captured generation prefixes, even
when SQL registrations were empty. Validate the entire exact target set before
the first effect. No org-wide generation-prefix deletion is allowed. Keep the
inventory durable through crashes and retries after heads/generations have
been removed. Provider list/delete failures do not advance the state. A fresh
empty verification after the proven write boundary is required for every
target; metadata absence is never byte verification.

**Complete:** In the ordinary completion transaction, recheck no candidate or
new publication metadata remains, no upload roots have returned, no outside
references survive, and the generation target proofs match the accepted
request and writer boundary. Add these checks explicitly: current completion
and `remaining` do not cover this new inventory. Refuse late rows or invalid
proofs and keep `metadata_purged`. Only then transition to `bytes_verified`
and lift the user's fence using the current adoption rules. Clear raw cleanup
targets when no longer needed according to the agreed cleanup-state retention
policy; retain the ordinary erase receipt/counts. Do not delete erase markers
or alter 062320's total user-erasure-plus-session-fence release count.

**Repeat:** A repeated completed request returns its bound result without
executing any new target deletion. A later upload is a new footprint requiring
a new request. Old ordinary and scoped capabilities must be unable to publish,
recover, renew, register, or recreate an old generation after completion,
including after adoption of the same user identity. This requires durable
revocation/absence semantics in addition to lifting the temporary user fence.
No generation-boundary proof for the hold bridge is implied by this proposal;
that consumer stays fail-closed until separately accepted evidence exists.

## 7. Access, integration, and rollback

All new helpers and cleanup state are owner-only, RLS enabled where relevant,
with no PUBLIC, service, ingest, seeder, scoped-publisher, or recovery grants.
Logs carry refusal codes and counts/hashes, not leases, storage credentials,
raw tenant identifiers, or a caller-controlled SQL error body. Cleanup state
must outlive partial erase and be inaccessible to ordinary readers. It must
not accidentally become a new lazy authority source through the ingest/upload
table-name bootstrap scan.

An eventual migration is append-only at a newly reserved slot. It takes the
latest integration bodies as its replaces, including 062320's first org-hold
check and 062010's marked-origin semantics; it does not restore 058010 over
later work. Test live `pg_proc.prosrc`, function ACLs, and release/down order.
A down must refuse while a cleanup admitted under this contract is unfinished
or the old bodies cannot safely interpret retained state. Never discard
cleanup targets, re-enable old admissions, or remove the user fence to make
rollback pass. Keep pending state and safe recovery available, or require a
reviewed forward recovery. No migration apply is authorized by this document.

## 8. Required acceptance evidence

Run a native PostgreSQL matrix with the exact integrated kernels and real
user-erase state machine, plus a versioned synthetic object store for bytes:

- Scoped and ordinary stopped claims: supported abandon, admitted clearance,
  actual user erase through `purging` → `metadata_purged` → `bytes_verified`,
  repeated complete/job calls, and a new request for a later footprint.
- Active attempt/head/stage/GC lease, prepared or registered image, live or
  historically published generation, legacy base pointer, index tombstone,
  shared generation/key/sibling reference, unknown provenance, and foreign
  tenant or physical-alias collision: all refuse without destructive effects.
- Both interleavings of renewal, scoped recovery, ordinary re-claim, prepare,
  register, and final publish against begin/step/complete. A winning publish
  causes refusal; a winning erase invalidates the old writer. Assert exact
  state and counts, not just an expected exception.
- Crash/rollback after eligibility, ledger insertion, authority invalidation,
  dependency cleanup, metadata purge, each storage page, and final verify.
  Restart from a fresh connection must use the original target set, keep the
  fence when incomplete, and return a stable completed receipt on repeat.
- An unregistered generation object, multiple versions/delete markers,
  delayed PUT/multipart completion, and a stale capability after the last
  listing: no premature completion; unsupported storage proof stays blocked.
- Old scoped and ordinary token calls after completion and same-user adoption
  all fail; unrelated user/session data, frontier, admission, and object
  versions remain byte-for-byte unchanged. No extra publisher/recovery grant.
- Inventory parity across eligibility, FK order, remaining probes, byte
  targets, completion, rollback, and both ordinary and held scoped interfaces.
  Repeat in the supported PostgreSQL versions before implementation sign-off.

This draft supplies no new execution evidence. Existing repeated refusals
and frontier protections do not count as successful-clearance tests.

## 9. Decisions required before implementation

Kernel and erase co-review must settle A versus B, the complete reference
inventory and exact joins, and how terminated scoped recovery references are
represented. It must also settle the provider-enforced or bounded storage
write boundary for **both** ordinary and scoped claims, the durable cleanup
target/proof schema and retention, and a safe down/recovery contract. If any
cannot be proved for the intended deployment, retain the stranded-user hold.

Neither a relaxed `RH040` filter nor a hand-cleanup prerequisite is selected.
No deployment, IAM/credential action, real database apply, live erasure,
replay, hold release, or flag change is authorized here. Any eventual operator
procedure follows accepted code and synthetic proof in the private ops repo.
