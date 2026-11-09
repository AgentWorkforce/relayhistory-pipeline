# Operator packet: ingest_claim_sessions side table (slot 044080, PR 1 of 2)

Status: code only. Nothing here has been applied to any database. Applying
migration 044080, enabling the writer, or running the backfill against any real
database needs Khaliq's separate, explicit OK in his own words.

## What the migration adds (inert)

`supabase/migrations/20261105044080_ingest_claim_sessions.sql`:

- `sessions.ingest_claim_sessions`: one row per distinct `(claim_token, source,
  session_id)` taken from `sessions.ingest_claim_attempts.records`, with the claim's
  tenant/batch columns and `accepted_at` (the batch's `committed_at`, a sort hint only).
  FK to the claim with `ON DELETE CASCADE`.
- `sessions.ingest_claim_sessions_policy`: a singleton row, `write_enabled=false`.
  Changing `write_enabled` clears `backfill_cursor` and `backfill_complete_at`;
  `backfill_complete_at` cannot be set while the writer is off; the row cannot be deleted.
- Trigger `ingest_claim_sessions_sync`, `AFTER INSERT OR UPDATE OF records` on
  `sessions.ingest_claim_attempts`. It writes nothing while `write_enabled` is not true.

Both tables are owner-only (RLS on, no policies, no grants), the same as
`ingest_claim_attempts`. No reader uses the table yet; PR 2 adds the reader behind its
own default-off flag. Applying 044080 therefore changes no runtime behaviour.

## Order

Run every step as the schema owner. Report counts only: never select or log tokens,
tenant ids, session ids or records.

1. Apply 044080 with the normal migrator (transactional, ledger
   `upstream:20261105044080_ingest_claim_sessions.sql`). Confirm the inert state:

   ```sql
   SELECT write_enabled, backfill_cursor IS NULL AS cursor_clear,
          backfill_complete_at IS NULL AS not_complete
     FROM sessions.ingest_claim_sessions_policy;          -- expect: false, true, true
   SELECT count(*) FROM sessions.ingest_claim_sessions;   -- expect: 0
   ```

2. Dry run of the backfill (read-only transactions, writes nothing). The owner URL is
   read from 1Password into the pipe and never written to disk:

   ```sh
   op read "op://<vault>/<schema owner>/url" \
     | jq -Rc '{ownerUrl: .}' \
     | bun scripts/backfill-ingest-claim-sessions.ts --dry-run
   ```

   The report is one JSON line of counts (`claimsScanned`, `rowsMissing`, ...).

3. Enable the writer. Turning it on takes `SHARE` on `sessions.ingest_claim_attempts`
   inside the guard trigger, so it waits for in-flight claims and blocks new claims
   until the enabling transaction commits. Keep it short and bounded:

   ```sql
   BEGIN;
   SET LOCAL lock_timeout = '5s';
   UPDATE sessions.ingest_claim_sessions_policy SET write_enabled = true;
   COMMIT;
   ```

   On a lock timeout nothing changes; retry later. From the commit on, every new claim
   (and every `records` rewrite, including retirement to NULL) maintains its side rows.

4. Backfill existing claims (after step 3, so nothing written before the writer was on
   is missed; the enable cleared any earlier cursor):

   ```sh
   op read "op://<vault>/<schema owner>/url" \
     | jq -Rc '{ownerUrl: .}' \
     | bun scripts/backfill-ingest-claim-sessions.ts --batch-size 500 --statement-timeout-ms 30000
   ```

   Each batch is one transaction (claims locked `FOR SHARE`, side rows inserted or
   `accepted_at` refreshed, rows of retired claims deleted, cursor advanced). It is
   resumable: rerun the same command after an interruption or a `--max-batches` stop.
   It marks `backfill_complete_at` only when it reaches the end with the writer on;
   otherwise it exits 0 with `completionMarked:false` and a
   `completionNotMarkedReason` (`write_disabled`, `max_batches`, `dry_run`).

5. Verify, counts only:

   ```sql
   BEGIN READ ONLY;
   SET LOCAL statement_timeout = '60s';
   -- Expected distinct pairs from live claims vs side rows (should match).
   SELECT (SELECT count(*) FROM (
             SELECT DISTINCT a.claim_token, e->>'source', e->>'session_id'
               FROM sessions.ingest_claim_attempts a
               CROSS JOIN LATERAL jsonb_array_elements(
                 CASE WHEN jsonb_typeof(a.records) = 'array' THEN a.records ELSE '[]'::jsonb END) e
              WHERE jsonb_typeof(e) = 'object'
                AND jsonb_typeof(e->'source') = 'string'
                AND jsonb_typeof(e->'session_id') = 'string') x) AS expected_pairs,
          (SELECT count(*) FROM sessions.ingest_claim_sessions) AS side_rows;
   -- Side rows whose claim no longer carries records (should be 0).
   SELECT count(*) AS stale_rows
     FROM sessions.ingest_claim_sessions s
     JOIN sessions.ingest_claim_attempts a USING (claim_token)
    WHERE a.records IS NULL;
   SELECT write_enabled, backfill_complete_at IS NOT NULL AS complete
     FROM sessions.ingest_claim_sessions_policy;           -- expect: true, true
   COMMIT;
   ```

## Rollback

- Stop writing (keeps the rows; clears cursor and completion):

  ```sql
  BEGIN;
  SET LOCAL lock_timeout = '5s';
  UPDATE sessions.ingest_claim_sessions_policy SET write_enabled = false;
  COMMIT;
  ```

  Re-enabling later requires steps 3 to 5 again (the backfill reconciles rows that went
  stale while the writer was off).

- Remove everything: `supabase/rollbacks/20261105044080_ingest_claim_sessions.down.sql`
  drops the trigger, both tables and both functions and deletes the ledger row. It
  refuses (55000) unless 044080 is applied with its exact checksum, and while any later
  upstream migration is applied. Only after PR 2's reader is off or rolled back.

## Caveats for PR 2

- `accepted_at` is NULL for rows the trigger writes (the claim precedes the batch
  commit) and is filled by the backfill only. It is a sort hint, not authority: readers
  must still join `sessions.ingest_batches` (`state='committed'`), or PR 2 adds a
  commit-time refresh.
