# T25 operator-erasure disposition

Status: **HOLD — no supported clearance established.** This is a code-only
disposition of the combined `RH040` / `RH011` case, not a retirement design,
implementation assignment, or operator procedure. It is separate from the
replay-target manifest work reserved as 062360.

## Ownership and evidence boundary

The operator-session erase owner owns this combined-gap disposition.
`layered-sessions-pipeline` is the publication-kernel counterpart and owns
erase-equivalence sign-off. The T25 publisher owner supplied the synthetic
publication evidence below; that report identifies the same kernel owner.

The source review is pinned to main
`8840659b77211c24277bc62ed6675830653caaea`. The reported T25 evidence is pinned
to the **local, unpublished** branch `review/rebel-scoped-publisher-t25` at
`4eb7dc0bee1a4755f0c432ea59d25bf920178331`, based on `fde1fe24`.
The publisher owner also reports an ordinary-path probe on unmodified
`fde1fe24`. Those PostgreSQL 16 runs used disposable synthetic databases;
this document's author has inspected the main SQL, but has not fetched the
local T25 branch or independently rerun its tests. The reported runs have not
been repeated on the pinned main head. Source agreement is not a substitute
for that execution evidence.

## Current API and state evidence

The existing [user-erase contract](../operator-user-erase.md) already holds
published-and-buffered users. The blocker also covers a never-published
claim that leaves publication metadata behind:

| API or predicate | Relevant behavior at the reviewed main head |
| --- | --- |
| `sessions.operator_user_erase_begin(text,text,text,text)` | Takes the user fence and deletes publication frontiers, then probes for canonical/publication state. Any matching `ingest_publication_heads` or `ingest_publication_generations` row causes `RH040`, without a state filter. The new erase ledger row is inserted only after the probe. |
| `sessions.operator_user_erase_step` and final purge used by complete | Recheck the publication-state blockers. Starting or resuming a different phase does not provide a bypass. |
| `sessions.ingest_publication_abandon(jsonb)` | Requires `attemptToken`, `leaseToken`, and `generation`. For a non-published attempt it sets the attempt to `rejected`, retires qualifying building generations, and clears matching build/GC leases. It retains heads and generations. For a published attempt it returns `abandoned:false`. |
| `sessions.ingest_operator_erase_ordered(text)` | True if the singleton lazy policy exists, the org has buffer admission, or the org has a buffered batch. Stopping a claim or abandoning an attempt does not clear these predicates. |
| `sessions.ingest_operator_erase_session` | Refuses the ordered/lazy/buffered case with `RH011`. |
| `sessions.ingest_publication_retire_fence()` | Marks matching heads and generations retired and updates derived visibility. It retains the rows, so retirement alone cannot satisfy the user-erase presence probe. |

The relevant source files are:

- [058010 user erase](../../supabase/migrations/20261105058010_operator_user_erase.sql):
  begin, step, and final-purge probes and the new-request ledger insertion.
- [Canonical publication](../../supabase/migrations/20261104060000_ingest_canonical_publication.sql):
  `ingest_publication_abandon`.
- [050000 session erase](../../supabase/migrations/20261105050000_operator_session_erase.sql):
  `ingest_operator_erase_ordered` and the `RH011` refusal.
- [044070 retirement trigger](../../supabase/migrations/20261105044070_selfhost_erase_search_text_rebuild.sql):
  the replacement `ingest_publication_retire_fence` body.

For a fresh begin that raises `RH040`, the earlier frontier deletion rolls
back with the failed transaction; it is not evidence that the user has been
durably stopped. A separately committed stop or abandon is a separate fact.
Zero new erase-ledger rows means the request never entered `purging`; the
existing contract's resume-from-`purging` guidance applies to requests that
actually reached that state. Optional relayfacts work committed before begin
is a separate boundary already described in the existing contract.

## Publisher-reported synthetic PostgreSQL 16 results

The named T25 test in `publication.postgres.test.ts`,
“T25 scoped fleet … a paused fleet holds no erase path of its own …”, covers
the pre-abandon `RH040` refusal and frontier revocation. The publisher's
**separate PG16 probes**, rather than that committed test, report the fuller
abandon-and-retry sequence below:

1. Issue through `ingest_publication_scoped_issue_v1`, then run the scoped
   fleet through `ingest_publication_scoped_candidates_v1` and
   `ingest_publication_scoped_claim_v1`.
2. Stop at the first stage read, before prepare. There is one `admitted`
   attempt and one `building` head.
3. Call `operator_user_erase_begin`: `RH040`, with zero erase-ledger rows.
4. Call `ingest_publication_scoped_abandon_v1` with its binding and tokens:
   `abandoned:true`, admission disabled, but one `building` head remains.
5. Retry user begin and repeat: `RH040` each time, still zero erase rows.
6. Try per-session erase for the buffered org: `RH011`.

Ordinary-path parity is builder-reported. That separate probe uses
`ingest_publication_claim` followed by
`ingest_publication_abandon`; it likewise reports a remaining `building`
head and `RH040`. Thus the reported gap is not introduced solely by the
scoped T25 wrapper. Repeated refusal is evidence of the hold, **not** positive
evidence of successful repeat erase.

The publisher also reports that scoped publish refuses after an erase row
is recorded: `purging` is blocked by the tenant fence, and `bytes_verified`
by the scoped gate. A revoked frontier also blocks publish. These cases
leave zero published results, no consumed admission, and an empty index;
the scoped login cannot call erase, GC, or frontier deletion (`42501`). A
separate stop-after-prepare/recovery case publishes exactly once. These are
useful existing protections, but do not prove that any proposed retirement
or relaxed erase guard preserves active or published heads.

## Disposition and required kernel decision

No supported retirement or clearance prerequisite was identified in the
reviewed main source or by the T25 publisher owner. Preserve the combined
hold. Stopped processes, rejected attempts, expired leases, retired states,
and an empty index do not individually authorize metadata removal or a
weaker `RH040` probe. Neither hand deletion nor an ingest-mode change is a
clearance procedure.

The publication-kernel and erase owners must choose a separately reviewed
design before implementation is assigned:

- An erase-owned path that proves which never-published abandoned metadata
  is purgeable under the existing lock and writer-fence model, with matching
  begin, step, and completion semantics; or
- A publication-owned guarded retirement interface, coordinated with the
  erase guards so its retained evidence or removed metadata has a defined
  meaning. Merely marking a row `retired` leaves today's blocker intact.

A prerequisite saying “no open or abandoned claim” can describe an eligible
initial state, but cannot clear this already stranded state without a
supported way to establish that prerequisite. The recommendation is to keep
this hold until the owners select and prove a guarded path; no API change is
selected by this note.

Before calling a path supported, require synthetic native-PostgreSQL evidence
covering both scoped and ordinary claims: the stranded-state transition,
successful erase and idempotent repeated erase, rollback/retry boundaries,
and races with claim, prepare, lease renewal, recovery, and final publish.
Protect active and published heads explicitly, including existing canonical
or base pointers, generations, registered objects, and shared references.
Prove that stale capabilities cannot publish or recreate erased state and
that unrelated users and sessions remain unchanged. Record exact source
heads, API inputs without secrets, before/after states, refusal codes, and
receipt/ledger outcomes. Existing T25 refusal tests do not discharge these
new-path obligations.

This disposition authorizes no live mutation, migration apply, replay,
deployment, flag change, or cleanup. Any eventual operator clearance
procedure belongs in the private ops repository after the implementation
and equivalence evidence are accepted.
