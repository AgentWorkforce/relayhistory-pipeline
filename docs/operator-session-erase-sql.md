# Operator erase SQL boundary

Source-only, default-off operator implementation. Applying migrations or enabling any
runtime still needs Khaliq's explicit approval. 050000/050010 are not a public route;
Their mutators are owner-only helpers from creation; the ingest login gets only receipt
read. 050020 grants admission/lease-gated runtime calls and cannot be bypassed through
legacy helper grants.

Every session_index or session_directory INSERT, UPDATE or DELETE takes a shared advisory gate in a BEFORE STATEMENT
trigger, before row/conflict locks. Erase owns the same gate exclusively before resolving
aliases, fencing owners or tombstoning pointers. Concurrent pointer writers remain
concurrent with each other; operator erase serializes against pointer statements globally.
This deliberate coarse gate covers direct UPDATE and arbitrary existing SQL writers,
including writes that started before the fence committed, without adding a lock to the
claim/commit hot-path fence lookup. Erases should be rare and their SQL transaction short.

A non-MVCC sequence epoch records that an erase crossed the writer boundary. Once an
erase has occurred, a higher-isolation pointer writer fails with 40001 and must retry
under READ COMMITTED; it cannot use a repeatable-read snapshot that predates the fence.
The eraser itself also requires READ COMMITTED before any write, so an old erase
snapshot cannot miss newly committed aliases. The dedicated consumer already uses READ COMMITTED. The sequence is owner-only and has
no content. No erasure means the epoch remains uncalled and existing isolation behavior
is retained. A used fence cannot be rolled back to remove enforcement.

Alias resolution rejects a physical ID associated with distinct canonical IDs before
any receipt or fence write. More than 256 connected aliases also refuses before writes.
The separate 64-owner cap preserves a failed receipt and fence for retry. Artifact state
assertions alone cannot establish unreadability: settlement independently checks live
pointer and directory rows.

Lazy mode is a database-wide singleton in 20261104055000, not an org setting. Existing
canonical admission already refuses globally when it exists (043850/050010). Buffer
admission is per organization and does not block a different direct-mode organization.
Changing a deployment with permanent operator fences to an unsupported independent
writer/control mode needs a separate drain protocol; this implementation refuses such
operations instead of weakening an existing deletion. No mode change is authorized here.

Proofs include lossy alias rejection, cross-source/org preservation, false unreadability
refusal, and an actual PostgreSQL forced writer-first/erase-first race. The race observes
pg_blocking_pids, exercises INSERT and direct UPDATE, and starts a repeatable-read writer
snapshot before erase to prove the stale-snapshot refusal. CI repeats it on PG15/16/17.

Directory statements use the same shared gate and permanent row guard. A projector
whose statement snapshot predates erase cannot restore a tombstone after waiting.
The physical object namespace is permanently reserved by an erase: a later canonical
ID that encodes to the same physical prefix is refused before object I/O. Allowing
reuse would let an old erase job delete the new session's objects. This layout limit
requires a separate identity/layout migration to remove safely.

The gate is database-wide, with no network I/O inside its transaction. Existing
self-host statement/lock timeouts bound contention. The sequence
epoch is nontransactional: an attempted erase can require READ COMMITTED thereafter
even if its SQL transaction rolls back. SQL-lane tombstones retain metadata at rest;
the runtime purge removes the rows before it reports completion.

The global gate precedes user-fence locks and pointer/directory statements. A BEFORE STATEMENT
trigger on ingest_fence_owners covers ingest_lock_fences (whose first operation is
INSERT ... ON CONFLICT) and direct first-owner seeders. Thus selection, reset, claim,
commit, self-host erase and publication retirement all share the gate before taking
a user fence. This necessarily adds one shared advisory acquisition to the existing
user-fence path; the permanent-fence lookup itself remains a plain primary-key read.
Concurrent ordinary writers still share the gate. Operator erase is the exclusive user.

The direct NOWAIT owner-lock sweep found publication scope, pointer guards and buffer
GC. Publication scope and buffer GC now acquire GLOBAL first; pointer guards run under
the session_index statement gate (including DELETE) already and retain their nonblocking NOWAIT checks. Replacements preserve role, owner and ACL,
and rollback restores their exact preceding bodies. Actual PostgreSQL tests drive the
publication retirement trigger through both selection and reset in both lock orders.
