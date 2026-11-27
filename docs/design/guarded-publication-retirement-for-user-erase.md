# Guarded publication retirement for user erase

Draft 2 — design comparison only. **No clearance path or implementation is
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

The kernel review's source map supplies the additional **held 062285**
issuance/cohort interface: issuance lines 49–59 declare an admission UUID
primary key referencing `publication_private.admission`; lines 62–68 attach
`keep_issuance` before UPDATE, DELETE, or TRUNCATE, at statement level. Issue
lines 175–196 and gate lines 97–129 establish the cohort-first lock order
described below. These excerpts were supplied by the kernel co-reviewer;
the full 062285 file/local T25 object is not available in this checkout.
They are an explicit design input, not independently executed source evidence.
The exact installation head, file digest, columns and trigger bodies must be
pinned and reviewed before an implementation can pass the inventory gate.

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
8. Supported durable storage-write quiescence for every exact target and an
   approved terminal evidence/retention representation already exist and can
   be validated by begin. If either is unavailable, refuse **before** a new
   cleanup ledger is inserted or any clearance state is committed. An unknown
   capability horizon or unresolved raw-ID retention policy cannot create a
   request that is knowingly stuck in `purging`.

These are proposed conditions, not an executable predicate proven complete.
The kernel co-review must supply the full reference inventory, exact SQL
joins, and a bound for the closure before acceptance. Empty FK reference
sets alone do not prove absence of JSON/object references. For this narrow
version, any image or registration is a refusal even if already redacted.

The minimum reference inventory is closed across SQL and embedded data:

| Carrier | Required closure or refusal |
| --- | --- |
| Heads and generations | Full logical-owner FK, all historical generation rows, the head's current generation and full physical unique key. No alias coercion may equate unrelated physical owners. |
| Attempts | Build-token links, stage/batch ownership, and every entry in `head_snapshots`, including generation, previous key/hash, and embedded base-pointer fields. Close over all heads of a multi-head attempt. |
| Images and objects | Images reference both attempt and generation; objects reference generation. Reject any candidate image/registration and any outside JSON/key reference into the candidate namespace. |
| Head `base_pointer`, index and derived carriers | Interpret stored JSON references and canonical/physical aliases, not just declared FKs. Reject old current/base keys, sibling/shared immutable keys, shared physical tuples, and unresolved external references. |
| 062280 admission | Bind the original `binding`, `frontier_image`, `admission_image`, stage, attempt and lease. `enabled=false` invalidates use; it does not erase these identifiers or images. |
| 062285 issuance | Its admission PK/FK requires the referenced admission to remain. Preserve `keep_issuance` and the original issuance evidence: no UPDATE, DELETE, TRUNCATE, cascade, reparenting to a dummy admission, or trigger bypass. |
| 062290 recovery | `expired_tickets` references both admission and attempt. Explicitly account for the child before deleting an attempt, and preserve the intended recovery/audit semantics. |

At terminal cleanup a scoped admission must remain disabled permanently; it
must not be marked consumed as if publication succeeded. Keeping the current
binding/frontier/admission images indefinitely is **not approved** by this
draft. Before entry, the owners must select either an explicitly approved
retention policy for the retained evidence, or a kernel-reviewed terminal
representation that preserves the issuance FK/evidence while safely removing
disallowed raw fields from mutable admission data. That representation needs
exact field/constraint/reader changes; setting JSON or IDs to NULL ad hoc is
not a design. If immutable issuance itself contains raw identifiers that
cannot be retained under the approved policy, the case remains refused.
No issuance evidence is deleted to make erasure appear complete, and the
receipt must truthfully distinguish permitted retained evidence from deleted
user data. The separate 062330 retention request grants no policy here.

## 4. Lock and authority order

No storage call occurs while holding a SQL transaction or these locks.
Use read committed and preserve the existing release/schema gate protocol.
Where 062320 is installed, the first statement after `BEGIN` of an
erase-recording entry point remains
`PERFORM sessions.ingest_org_admission_hold_check(p_org,'erasure');`.
This takes the org hold key shared before erase locks and must not be moved
behind a user or publication lock. A helper called by that entry point does
not introduce a second, reversed lifecycle-gate order.

Use these phase-specific graphs; do not add privacy S to begin by copying
step's order. `GLOBAL` is `(961996,50000)`, `PRIVACY` is `(1919249529,96)`;
`LEDGER` and `USER` mean the exact request and user-fence rows `FOR UPDATE`.

| Erase phase | Lock graph after any existing schema/org gate |
| --- | --- |
| New `operator_user_erase_begin(request,org,ws,user)` | 062320 org S first when installed → GLOBAL X → existing matching LEDGER → USER → closure rows. Main 058010 begin has **no** PRIVACY S. New ledger insertion and candidate acceptance are atomic later in this same transaction. |
| Existing-request begin retry | Same begin order; return the bound state or validate its original frozen closure. No new target discovery permit. |
| `operator_user_erase_step(request,limit)` | GLOBAL X → PRIVACY S → LEDGER → USER → closure rows. Keep the existing slot and GC-lease checks. |
| Final metadata purge inside step | Same transaction and locks as step; dependency cleanup → fresh strict remaining/probe checks → state transition. |
| Byte job | Short owner SQL reads of bound state/proofs, then storage work outside SQL locks; no transaction held over list, delete or verification. |
| `operator_user_erase_complete(request)` | GLOBAL X → PRIVACY S → LEDGER → USER → closure/remaining checks → atomic verified transition. A failed check preserves the prior request state. |

Create USER through the existing guarded path if absent. Do not use a shared
origin mark to weaken an erase-side row lock. Under GLOBAL X, closure rows
are scoped admission/recovery dependencies, batches, stages/outbox, frontiers,
then physical pointer/head/generation/attempt dependencies in a deterministic
order documented by the eventual implementation. Revoke only this user's
frontiers. The current pointer key is
`hashtextextended(concat_ws(chr(31),'96bulls:publication-pointer',org,workspace,physicalSource,physicalSession,machineId),0)`;
acquire it through `pg_try_advisory_xact_lock` with abort-on-busy behavior,
ordered by the full bytewise physical tuple. Re-read after waits. Do not
turn the try-lock into a blocking wait on a pointer holder.

The **cohort lock is a row lock, not a hashed advisory key**. Held 062285
issuance locks one `cohort_logins` row ordered by `login_oid`, `LIMIT 1 FOR
SHARE`, before `ingest_lock_fences`; then it locks stage/frontier/buffer
admission rows FOR SHARE and fleet/scope registry rows FOR SHARE. Its runtime
gate calls `caller_cohort()` (authenticated login `FOR SHARE OF l`) before
`gate_unfenced` and `lock_scope_binding`. The underlying 062280 order is
GLOBAL S via owner INSERT → USER → admission FOR UPDATE → stage/frontier/
buffer FOR SHARE. Recovery uses that gate too.

This draft's erase-side constraint is **no cohort/fleet/scope registry lock
or mutation while holding GLOBAL X or USER**. Cleanup does not reauthorize
the runtime login and must not call `caller_cohort`, the scoped runtime gate,
or issue/recover as an owner helper after taking erase locks. It uses the
already persisted issuance/admission binding for target identity; unlocked
registry reads are inventory hints, never current authorization or proof of
absence. The proposed helper neither deletes issuance nor changes its FK,
and adds no FK from cleanup state to a cohort row. Invalidating a captured
admission requires an owner-only invalidator with this explicit contract,
not entry through the runtime gate.

If an implementation needs a cohort/registry lock to validate or mutate
cleanup, this no-cohort variant is unsupported: refuse before entry until a
separate cohort-first protocol and all owner paths are proved. Do not simply
acquire cohort SHARE/UPDATE after GLOBAL X. A writer may hold cohort SHARE
while waiting for GLOBAL S; erase waiting on an incompatible cohort lock
would reverse that edge. Likewise an owner holding cohort UPDATE can block
a new cohort reader. No row-level lock compatibility is assumed to solve
these cycles. Cohort provisioning, revocation, and registry owner operations
stay outside the erase transaction in this comparison.

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

The implementation's required writer/owner map must distinguish ordinary
claim/locked operations (GLOBAL S → USER → batch/stage/outbox → frontier →
pointer/head/attempt), the scoped cohort-first gate above, buffered GC
(GLOBAL S → USER → batch/stage), generation GC (GLOBAL S → USER → head or
object), and each direct owner/frontier/admission/evidence operation. The
shown suffixes are source-path inventories, not permission to reorder their
existing statements. Read-only evidence access must not hide a write or
locking helper. New cleanup paths must not acquire a schema, cohort, or
owner lock late that another path holds while waiting on GLOBAL. Pin the
complete per-API graph and exercise both wait directions in native PG tests.

## 5. Alternatives for the shared contract

| Concern | A: erase-owned clearance | B: guarded kernel interface |
| --- | --- | --- |
| Entry | Extend owner-only user begin/step/complete with the exact closed eligibility predicate. | Introduce an owner-only kernel helper that verifies and invalidates the candidate closure, invoked by an erase-recording transaction. |
| Guard ownership | Erase owns the predicate and purge inventory; publication co-review pins every dependency and token gate. | Kernel owns the closure and its invalidation witness; erase consumes that exact witness and retains all other blockers. |
| Atomic boundary | New ledger fence and accepted candidate set commit together at begin. Cleanup happens only under that same request's later locked phase. | Helper and erase begin commit in one owner transaction with the same closure and request fence. A separate handoff protocol is outside this comparison; no free-standing delete-then-begin window. |
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
Validate the pre-entry storage and retention prerequisites below before a new
request is inserted. Commit the frozen accepted closure, scoped capability
invalidation, terminal-evidence plan, frontier revocation, token
revocation, and the ordinary `purging` erase ledger fence atomically. A
refusal or exception rolls them all back. No partially admitted user or
clearance-only success is returned. Existing requests retain their original
binding. A retry does not rediscover and authorize new generations.
The stored comparison images are the expected **post-invalidation** versions;
own version increments are recorded in the same transaction, never treated
as a reason to accept arbitrary later changes.

**Exact storage target inventory:** For each candidate generation capture
`(provider,bucket,region,key_root,org,generation,prefix)` from the validated
SQL/deployment binding. Require the stored generation prefix to equal
`key_root + '/' + org + '/upload-generations/g=' + generation`, and enumerate
only its slash-terminated descendant prefix. A different root, owner,
provider or generation is a refusal, not a repaired input. Include every
generation in the accepted logical-head closure, not only `head.generation`,
and reject shared references before treating a whole prefix as deletable.
Use the existing exact `userErasePrefixes` machine-target construction for
the captured user origins and legacy-export origin as the other target class.
No bucket-wide, org-wide or bare `upload-generations/` target is executable.

The durable target set is unique and bytewise ordered by provider/location/
prefix and bound with its count and full digest to the accepted request,
scope, closure digest and contract version. Validate all rows and the binding
before the first storage effect. Keep it available after metadata deletion
and across job restarts. A prefix string or an old empty-list result alone
is not an authority token. Each list/delete/verify checks the same bound set;
no runtime rediscovery widens it. The concrete target/proof schema is still
an implementation-design gate, not an existing API.

**Writer quiescence — prerequisite, not an unbounded recovery phase:** This
comparison chooses refusal before entry for deployments lacking a supported
write boundary; it does not admit a request and hope a boundary can later be
invented. Begin must validate owner/provider-authenticated, durable proof
already bound to every accepted generation and machine target. Existing
machine-prefix lease/adoption checks may be reused only within a qualified
closed-writer proof; a current settle delay alone is not that proof. This
prerequisite must distinguish old outstanding writes from a new footprint
legitimately admitted after completion. The proof is not an
operator boolean or an unauthenticated job result. It must establish either:

- A provider-enforced durable denial of old-authority writes and multipart
  completion to the captured targets, including already issued/in-flight
  requests, plus prevention of retired-generation recreation; or
- A closed writer/issuer inventory with a proven maximum capability lifetime,
  no renewal or new grants for those retired generations, and a proven bound
  on completion of issued requests. The last possible write is bounded by
  the last allowed issuance plus maximum credential validity, allowed renewal
  and request-completion/consistency/clock bounds. An unbounded term makes
  this proof unavailable. Waiting a guessed timeout cannot replace it.

The proof records the target-set/closure binding, issuer and capability
epoch, invalidation/issuance cutoff, applicable finite bounds or durable
provider denial, validation version and durable no-renewal commitment. A
timestamp is only one field; it does not prove quiescence. It must exclude a
new grant or refresh that can recreate any retired generation after complete
or same-user adoption. A qualified authority must be able to check that
commitment through completion; SQL missing-attempt behavior is insufficient.

Both the ordinary publisher and T25 scoped cycle prepare/register before
PUT and use create-only writes, but a delayed `ifNoneMatch:true` PUT can
succeed once an eraser has removed an object. The scoped SQL gate does not
by itself revoke cloud credentials. Ordinary refreshable credentials with
unbounded generation write power likewise fail this prerequisite. Neither
process pause nor lease expiry, stage rejection, or an empty list qualifies.
SQL begin's under-lock recheck must prove the accepted closure has not changed
since the pre-entry proof; any mismatch refuses the whole entry. No clearance
operation in this draft itself changes IAM or obtains credentials.

No accepted concrete mechanism satisfying this prerequisite is supplied for
either deployment path yet. Until one is qualified, novel cleanup returns a
distinct unsupported-prerequisite refusal with zero new erase rows and zero
clearance changes. The refusal code/schema is not reserved here. Pre-entry
qualification has a declared finite work/deadline budget; exhaustion refuses.
After a supported entry, transient job failures use finite retry budgets and
retain a visible fenced request on exhaustion, requiring the reviewed forward
recovery for that same accepted target set. They cannot re-enable authority,
drop a target or record verification to escape a failed proof. A surprise
loss of the promised durable boundary is an invariant failure, not ordinary
unsupported configuration to be discovered for the first time in `purging`.

**Step:** Revalidate the frozen set and versions under the same locks, with
all original non-publication blockers still strict. New metadata, active
leases, changed versions, or outside references stop the step. Preserve the
existing lazy-slot tombstones and shared-seat checks. Once writer quiescence
is proved, remove candidate dependency rows in verified FK order: scoped
recovery children and mutable terminated authority references as approved, then
images/attempts where eligible, generations after all references are absent,
then heads. The narrow version expects no image/object rows and refuses
unexpected ones. Do not use cascading deletion or remove unrelated members
of a multi-head attempt. Extend counts and remaining probes to the same
inventory. The final purge rechecks that inventory under a fresh snapshot
before `metadata_purged` commits. Any exception rolls back that whole step;
the earlier begin fence stays active across transactions.

The held `publication_private.expired_tickets` table has an FK to attempts;
its admission parent also records the stage/attempt. Current purge does not
handle that table. The 062285 append-only issuance→admission FK additionally
forbids simply deleting the referenced admission. A concrete B or A
implementation must distinguish deletable recovery children from preserved
issuance and its permanently disabled terminal admission, under section 3's
approved representation. The predicate and `remaining` probe must explicitly
account for permitted retained evidence; it cannot silently call those raw
bindings erased or count them as executable authority. No new indefinite
raw-identity retention policy is established here.

**Bytes:** The job must erase every version and delete marker under both its
existing user machine prefixes and the captured generation prefixes, even
when SQL registrations were empty. Validate the entire exact target set before
the first effect. No org-wide generation-prefix deletion is allowed. Keep the
inventory durable through crashes and retries after heads/generations have
been removed. Provider list/delete failures do not advance the state. A fresh
empty verification after the proven write boundary is required for every
target; metadata absence is never byte verification.
All versions and delete markers, pagination and exact target coverage are
verified separately for machine and generation targets. Empty registration
does not permit skipping the latter. A late write or marker restarts bounded
verification under the same proof/targets; no stable result means no complete.

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
Issuance evidence and its disabled admission remain according to the approved
terminal policy; complete does not delete them or release their old capability.

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
or the old bodies cannot safely interpret retained state or preserve terminal
admission invalidation. Never discard cleanup targets, re-enable old
admissions, or remove the user fence to make rollback pass. Keep pending
state and safe recovery available, or require a
reviewed forward recovery. No migration apply is authorized by this document.

The atomic unit is accepted closure + capability invalidation + request fence
and its proof binding. Precommit fault injection must leave all four absent
or unchanged, never a committed clearance without a ledger fence. After an
ambiguous commit, re-read by the accepted request ID and compare full bindings;
do not issue a replacement request or recapture a smaller/larger closure. Each
step commits its dependency/metadata changes and accounting together; a retry
starts with the expected post-invalidation images and the durable proof.
Failed complete changes neither the verified state nor the fence. Rollback
or down cannot remove append-only issuance, retained proof needed for recovery,
or durable no-renewal semantics, even for an already verified request.
Here atomic capability invalidation means the SQL admission/frontier/ticket
state and the reference to the already qualified storage boundary. No claim
of a distributed SQL/provider transaction is made. Any pre-existing durable
provider denial remains in force on SQL rollback; there is no compensating
re-enable operation to make a failed begin resemble its initial state.

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
- 062285 issuance/admission FK and `keep_issuance` remain intact, terminal
  admission stays disabled, and approved binding/image retention or projection
  is enforced. Unapproved retention, missing storage boundary and finite-budget
  exhaustion before entry each produce zero new erase rows or cleanup effects.
- Phase-specific lock assertions distinguish begin from step/complete. Native
  cohort SHARE/UPDATE, issuer, owner, GC and erase interleavings show no reverse
  waits; a helper that tries a late cohort lock is refused by qualification.
- Inventory parity across eligibility, FK order, remaining probes, byte
  targets, completion, rollback, and both ordinary and held scoped interfaces.
  Repeat in the supported PostgreSQL versions before implementation sign-off.

This draft supplies no new execution evidence. Existing repeated refusals
and frontier protections do not count as successful-clearance tests.

## 9. Decisions required before implementation

Kernel and erase co-review must settle A versus B, the complete reference
inventory and exact joins, the pinned 062285 evidence schema, and how recovery
children and permanently disabled admissions preserve issuance without
unapproved raw binding/image retention. It must qualify the no-late-cohort
owner invalidator or replace it with a fully proved cohort-first protocol.
It must also settle the pre-entry provider-enforced or bounded storage-write
boundary for **both** ordinary and scoped claims, the durable target/proof
schema and its terminal retention, and a safe down/forward-recovery contract.
If any cannot be proved for the intended deployment, refuse entry and retain
the stranded-user hold.

Neither a relaxed `RH040` filter nor a hand-cleanup prerequisite is selected.
No deployment, IAM/credential action, real database apply, live erasure,
replay, hold release, or flag change is authorized here. Any eventual operator
procedure follows accepted code and synthetic proof in the private ops repo.
