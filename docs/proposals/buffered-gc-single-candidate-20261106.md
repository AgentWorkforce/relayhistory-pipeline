# GC single-candidate migration 039: draft promotion evidence

Source draft only; no push, merge, deployment, managed migration, production
query, real credential or cloud operation was performed. Root assigned stem
`20261105039000_buffered_gc_single_candidate`. This draft starts at freshly
fetched main `795f5ddd717e895da6118ca8602896069f6952fe`; it does not modify any
existing migration or GC runtime. Concurrent registry slots 033/034/038 remain
the registry owner's responsibility. Rebase, recalculate the generation and
repeat relevant rehearsals against the actual combined source before promotion.

## Behavior and lock authority

The unmodified G2 `claim(1)` can evaluate four users while retaining the seat
clock SHARE lock from its first candidate. A producer/control can hold the next
user's fence while waiting for that clock exclusively. GC then waits for that
user fence: a cycle. The new claim evaluates at most **one complete retention
decision per transaction**, including healthy and prospective-child rotations.
It returns at most one ticket, within the released caller's requested limit.

Up to four acquisition identities can be considered. A GC-only nonblocking
advisory hint precedes NOWAIT acquisition of the existing authoritative user
fence, batch, intent and outbox. Only the acquisition subtransaction catches
`lock_not_available`; a failed attempt releases all its locks and performs no
checked, lease, outbox or row-version mutation. Missing authoritative rows are
corruption, not implicit enrollment. The existing user-fence row coordinates
with released producers; the new advisory hint alone is never authority.

After acquisition, the exact G2 privacy, retention, immutable ACK, lineage,
generation, row-version, replacement, lease and byte checks remain unchanged.
Successful healthy branches and ticket issuance end the scan. Retention errors,
deadlines and corruption still abort the RPC. The function is byte-identical to
the reviewed offline prototype `8a82c8d492f048af7c42b24f61e0a3330df6b74e`
from `CREATE OR REPLACE FUNCTION` onward:
`859fd581a7d6ca099c28c4d6116816f5541b58df5cf622a97555d54e68a33414`.

## Migration, rollback and generated boundary

The new up is
`supabase/migrations/20261105039000_buffered_gc_single_candidate.sql`:
`eb04d062b0b612978ffa9da4dc42b63f4d6e2a1c1b0223f3ef7c903b3f1b0cde`.
It only replaces the existing SECURITY DEFINER claim and preserves its ACL,
owner and configuration. The unified function-boundary generator represents it
as one replacement, zero new functions and zero grants. Every pre-existing
generated ACL, boundary, guard and verifier artifact remains byte-identical;
boundary SHA stays
`efd38ada85eadb9100a36bcb22480a6989fbba5e39319fbe38a1e6bafeb8fe2f`.

The same generator emits
`supabase/rollbacks/20261105039000_buffered_gc_single_candidate.down.sql`:
`db15fc4f79928e1348905b18000ad11d846675c06a5af9ced33baf4c0b466d47`.
The down uses an explicit transaction and the real migrator's advisory-lock
namespace. It requires the ledger and validation marker, locks the ledger,
refuses a later applied upstream migration and requires this up's exact applied
checksum. It atomically sets the readiness marker PENDING, restores the exact
latest prior claim from `20261104055000_lazy_current_custody.sql`, and removes
only its own matching ledger row. A failed guard requires the caller to
ROLLBACK or disconnect; it cannot commit a partial reversal. After a successful
down, current-generation workers remain fenced until the real migrator
reapplies and validates the registered source. The test also checks the latest
prior defining migration so a future replacement cannot silently use this down.

Migration sentinel, runbook inventory and profile descending-down/reapply
coverage include 039. Derived schema assets include it automatically. At this
exact draft base the 49 source migration names produce generation
`439240991a9c01a2df5c5747721654929a4707a428fe354551862d3fb371eaa0`.
That is a source/owned-fixture generation, not a managed database attestation.

## Owned validation

A real `npm ci` was attempted in this new T7 worktree and refused the absent
package lock. Bun 1.4.2 installed 192 packages with
`--frozen-lockfile --backend=copy`; no other checkout's node_modules was used.
Root TypeScript `tsc --noEmit`, unified generator `--check` and diff whitespace
checks passed.

The PostgreSQL regression is opt-in with a passwordless loopback fixture owned
independently of the URL. Its strict URL guard intentionally excludes passworded
CI fixtures, and without opt-in it skips; ordinary CI is not claimed as PG proof.
The rehearsal used a unique owned T7 Homebrew PostgreSQL 17.11 cluster bound
only to 127.0.0.1, with UTF8 encoding, locale C, pg_trgm 1.6 and btree_gin 1.3.
Vector 0.8.7 was actually created in its own admin database as a toolchain probe;
the current migration chain does not require vector. The test created and
dropped its own random `gc_single_<uuid>` database and synthetic roles/data.
There were zero remaining test databases before the owned cluster was stopped.
Synthetic object bytes used the FS ObjectStore on T7, not S3.

Final relevant results:

| Check | Result |
| --- | --- |
| Actual PostgreSQL 17 migration/GC matrix | 19 pass, 134 assertions, 8.66 s |
| Current PGlite sentinel/inventory/descending down-up profile cases | 5 pass, 263 assertions, 52.23 s |
| Unified generator and exact rollback cases | 9 pass, 51 assertions |
| Existing GC tick and job cases | 12 pass, 69 assertions |
| Existing AWS GC template cases | 5 pass, 40 assertions |

The real PG test runs the actual migrator over all currently landed migrations
plus 039 and executes all four actual read-only consumer verifiers as the ingest
role, with `schemaVerdict` PASS. Down/up proves exact prior function body, owner,
ACL and configuration; checksum drift, a later ledger row and a missing own row
refuse. Down marks PENDING; reapplication applies exactly one migration and
restores PASS. The PGlite profile separately descends through the existing
rollback chain and restores the gap-free source prefix through the real migrator.

The remaining PG cases reproduce the original unmodified G2 `40P01` using the
actual lazy-clear helper and synchronized `pg_blocking_pids`, then prove the new
interleaving completes. They cover all five busy acquisition categories and
lock release before outer commit; four-busy-window refusal; preparing and
prospective-child exits; 500/499 ordinals; mixed groups; delete/reset privacy;
foreign-owner lineage; a real post-mutation timeout rollback; an aborted claim
and a response lost after real committed claim; generation and immutable ACK
corruption; missing authoritative fence; and earlier RPC progress surviving a
later fatal RPC. Test-only 100 ms deadlock detection and deliberate corruption
are confined to the disposable fixture. The unknown-response case injects the
transport failure after a real commit and verifies no object IO or false finish.

Seven 500-record server-time samples were 46.616, 53.046, 46.664, 44.859, 45.901,
48.161 and 44.177 ms (median 46.616, max 53.046). The actual
SemanticPostgresExecutor also completed one healthy 500-record claim within the
existing 1.8-second context. These unloaded owned results do not establish a
production SLA or explain the participants in any specific production deadlock.

## Limits and release gates

The decoder still runs its original work twice; the original lease filter/index
can scan a larger backlog to find four acquisition identities. Busy heads are
never advanced, so persistent contention can starve later entries beyond that
window. The default five client claims now fully evaluate at most five healthy
objects instead of twenty; ticket-dense behavior was already one ticket per RPC.
No scheduler, timeout, client budget, role, IAM or retention bypass is introduced.
A separate bounded client follow-up may improve throughput only with measured
round trips and object work under the existing invocation deadline.

No managed role/profile, S3 versioned erasure, late-PUT lifecycle, source baseline,
production queue latency, live rollback, or packaged deployment is certified by
this owned SQL/FS rehearsal. Normal marker fencing and draining old function
executions, combined-prefix generation coordination across every consumer,
managed-profile/eraser rehearsal, independent exact-source/package review and
the root operator's release gates remain required. Existing applied G2 files
must not be rewritten or this prototype body applied directly to a real DB.

## Own final diff reviews

Adversarial pass followed every skip/exit and lock lifetime, compared the entire
replacement to the frozen prototype and prior G2, and inspected rollback
transaction, migrator-lock identity, checksum/later-prefix guards, marker fence
and exact body restoration. The owned negative cases passed with no material
finding remaining. There is no mutation before an acquisition-only busy skip.

Contextual pass checked released limit/ticket compatibility, real producer and
control fence-before-clock order, replacement and privacy consumers, unchanged
ACL output, derived generation, sentinel/bootstrap inventory and descending
profile behavior. The branch remains a draft at the assigned stem pending root
integration review; registry/generation drift and runtime/managed proof limits
are explicit. No runtime or applied migration file is changed.
