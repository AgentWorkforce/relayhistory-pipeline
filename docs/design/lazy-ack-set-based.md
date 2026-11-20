# Set-based staged ACK (migration `20261105062155`)

Status: code, migration file and tests only. Applying the migration to any real database and flipping the gate
anywhere each need Khaliq's explicit OK in his own words.

## Why

Every staged buffered ACK runs `sessions.ingest_lazy_ack_batch` from the `ingest_receipt_seat_ack` trigger inside
`ingest_commit_buffered_batch`. It takes the singleton `sessions.ingest_receipt_seat_clock` row `FOR NO KEY UPDATE`
(`043800`) and holds it until COMMIT, so every ACK in every org queues behind the one in flight. Under the ingest
role's 2 s `lock_timeout` the waiting commits fail with `55P03` (uploads get 503). Measured on PostgreSQL 16
(uncontended, synthetic): the per-record loop is about 98% of the hold for batches of 10 or more records.

## What the clock protects (unchanged)

- Global seat numbering. A seat is `accepted_seq`, which decides "last accepted wins" for one `record_id` across
  origins. Because the clock is held to commit, seat order is commit order.
- `last_seat`, the bound that `ingest_lazy_stage_seats` / `ingest_receipt_seats_for_stage` check seats against.
- Exclusion from clock holders that need ACKs quiet: `set_live`, `control_changed`, `lazy_clear` and
  `receipt_seat_initialize` (`FOR UPDATE`); `lazy_policy_set`, `buffer_gc_ordinals` and custody begin/release
  (`FOR SHARE`).

## What changed

Migration `062155` does a `CREATE OR REPLACE` of `ingest_lazy_ack_batch(jsonb)` and touches nothing else. The text
before and after the per-record loop is byte-for-byte `043800`. The loop gains a set-based twin:

| Step | Loop (gate off) | Set body (gate on) |
|---|---|---|
| candidates | one read, ordered by processing rank | the same read, ordered by (rank, ord) |
| slot locks | `SELECT ... FOR UPDATE` per record | one `SELECT ... ORDER BY first position FOR UPDATE` (same order) |
| decisions | per record | one window-function query after the locks: running revision, accept, lineage, running seat/digest, seat numbers |
| seats | `INSERT` per allocation | one `INSERT` |
| slots | `UPDATE` per accept | history switch live: one `UPDATE` per accept, in order; switch off: one `UPDATE` per round |
| decisions | `INSERT ... ON CONFLICT DO NOTHING` + re-read per record | one `INSERT ... ON CONFLICT DO NOTHING`; adoption checked before writing |
| errors | first failing record | the writes of the records before it, then the same SQLSTATE and text |

Both bodies take the clock `NO KEY UPDATE` first, at the same place, so they can run side by side during a flip.
A call keeps the body it started with. The lock order is fence → clock → origin → stage row → slots, then seats,
slots and decisions are written.

The differential suite (`src/session-ingest/__tests__/lazy-ack-set-based.pglite.test.ts`) runs each case in both
modes from the same state, with the history switch off and on. It compares the result or first error, every row of
every `sessions` table (timestamps excluded) and every `sessions` sequence.

### Known differences that store nothing

1. **Open, under review.** On an error path the set body takes every slot lock before it raises; the loop stops at
   the failing record. If a later slot were held by another transaction, the set body would wait (possibly `55P03`)
   where the loop would already have raised, e.g. `P0W01`. No known production writer of slots runs alongside an ACK
   of the same origin (see 3), but this is not yet proven on PostgreSQL. The real-PostgreSQL concurrency tests are
   to settle whether only a synthetic direct row lock can reach it.
2. Records with tied processing ranks are taken in `ord` order. The loop's order for ties is unspecified, so this is
   one of the orders it already allows.
3. Concurrent writers: `ingest_lazy_records` (claims) takes the origin lock, `ingest_lazy_clear` the clock, and the
   operator user erase `(961996,50000)` exclusive, which every ACK holds shared. `ingest_bookkeeping_cleanup_decisions`
   deletes only terminal claims' decisions. None of them can change what the ACK reads between its statements.

## The gate

`bulls96.lazy_ack_set_based`, a setting held in the function's own `proconfig`. This is the first function-level
`SET` of a custom setting in this repository (the `bulls96.` prefix was used before only through
`current_setting`). It is read once at the top of the body; exactly `on` selects the set body, and anything else or
no value runs the loop. `proconfig` is applied when the call starts and overrides session, role and database values,
and only the owner may `ALTER FUNCTION`, so the runtime role cannot switch bodies (tested).

```sql
-- read
SELECT proconfig FROM pg_proc WHERE oid = 'sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure;
-- on / off (owner only; transactional; affects calls that start afterwards)
ALTER FUNCTION sessions.ingest_lazy_ack_batch(jsonb) SET bulls96.lazy_ack_set_based = 'on';
ALTER FUNCTION sessions.ingest_lazy_ack_batch(jsonb) SET bulls96.lazy_ack_set_based = 'off';
```

A later migration that runs `CREATE OR REPLACE` on this function resets `proconfig` to that file's own `SET`
list, so the gate silently returns to whatever that file declares.

## Deploy order and rollback

1. Apply `062155` (gate off: behaviour unchanged).
2. Flip the gate on separately. Flip it off to revert at once, with no migration.
3. Full rollback: `supabase/rollbacks/20261105062155_lazy_ack_set_based.down.sql`, generated by the
   `lazy-ack-set-based` lane, restores the exact `043800` body and `proconfig`. It refuses if a later migration is
   applied or the checksum differs.

## Expected effect (PostgreSQL 16, local, uncontended, ACK time)

With `ingest_history_switch.live = true` (its installed default) the middle column applies: the set body saves about
9% at 500 records and is slower below about 25 records. Only with the switch off does it save about a quarter.

| records | loop (ms) | set, history switch live (ms) | set, switch off (ms) |
|---|---|---|---|
| 1 | 0.42 | 0.55 | 0.55 |
| 10 | 1.92 | 2.01 | 1.81 |
| 100 | 17.6 | 16.6 | 13.7 |
| 500 | 94.3 | 86.2 | 69.8 |

Most of what remains is index and FK work on the rows that must be written while the clock is held: the seats'
four indexes, the five slot indexes (every accept changes indexed columns, so its update is non-HOT) and the
decisions' three indexes plus three FKs. Shortening the hold further needs those writes moved out of it, which is
a separate design.
