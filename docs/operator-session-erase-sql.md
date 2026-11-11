# Operator erase SQL boundary

Source-only, default-off operator implementation. Applying migrations or enabling any
runtime still needs Khaliq's explicit approval. 050000/050010 are not a public route;
Their mutators are owner-only helpers from creation; the ingest login gets only receipt
read. 050020 grants admission/lease-gated runtime calls and cannot be bypassed through
legacy helper grants.

Every session_index INSERT or UPDATE takes a shared advisory gate in a BEFORE STATEMENT
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
