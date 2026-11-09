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
  commit-time refresh. PR 2 (slot 044090, below) adds that refresh; the readers still
  join `sessions.ingest_batches` and judge every stage as before.

## PR 2 / enabling the reader (slot 044090)

Status: code only, like PR 1. Applying 044090 or setting `read_enabled` against any
real database needs Khaliq's separate, explicit OK in his own words.

`supabase/migrations/20261105044090_claim_sessions_reader.sql` adds:

- `sessions.ingest_claim_sessions_policy.read_enabled boolean NOT NULL DEFAULT false`,
  with CHECK `ingest_claim_sessions_policy_read_check`: `read_enabled` only while
  `write_enabled` is true and `backfill_complete_at` is set. Any `write_enabled`
  change now clears `read_enabled` too (with the cursor and completion).
- Trigger `ingest_batch_claim_sessions_refresh`, `AFTER UPDATE OF state, committed_at`
  on `sessions.ingest_batches`: when a batch commits, sets its side rows' `accepted_at`
  to the batch's `committed_at`, only while `write_enabled`. An error inside it is
  swallowed (the commit never fails because of it; the row stays NULL, which the
  reader handles).
- `sessions.buffered_sessions_for` and `sessions.buffered_session_stages_for` replaced
  (signature, owner, SECURITY DEFINER and ACL unchanged). While the gate
  (`read_enabled AND write_enabled AND backfill_complete_at IS NOT NULL`) is off they
  run 044040's query unchanged. While it is on they find candidate sessions and the
  session's claims through `sessions.ingest_claim_sessions` instead of scanning the
  user's whole outbox / the records GIN, and judge each stage with the same
  predicates. The results are the same rows, order and cursor, provided the side
  table is complete (which the gate requires). No index is added.

Applying 044090 changes no runtime behaviour: `read_enabled` starts false.

1. Apply 044090 with the normal migrator (ledger
   `upstream:20261105044090_claim_sessions_reader.sql`). Confirm:

   ```sql
   SELECT write_enabled, read_enabled, backfill_complete_at IS NOT NULL AS complete
     FROM sessions.ingest_claim_sessions_policy;   -- expect: read_enabled = false
   ```

2. Complete PR 1 steps 3 to 5 (writer on, backfill to completion, verification). The
   CHECK refuses step 3 below until then (SQLSTATE 23514).

3. Enable the reader (a single-row update; no table lock beyond the policy row):

   ```sql
   BEGIN;
   SET LOCAL lock_timeout = '5s';
   UPDATE sessions.ingest_claim_sessions_policy SET read_enabled = true;
   COMMIT;
   ```

4. Verify, counts only (never select or log tokens, tenant ids or session ids):

   ```sql
   BEGIN READ ONLY;
   SET LOCAL statement_timeout = '60s';
   SELECT write_enabled, read_enabled, backfill_complete_at IS NOT NULL AS complete
     FROM sessions.ingest_claim_sessions_policy;   -- expect: true, true, true
   -- Side rows under committed batches with no accepted_at (missed refreshes; the
   -- reader handles them, but a growing number means the refresh is failing).
   SELECT count(*) AS null_accepted
     FROM sessions.ingest_claim_sessions s
     JOIN sessions.ingest_batches b USING (org_id, workspace_id, origin_id, batch_id)
    WHERE b.state = 'committed' AND s.accepted_at IS NULL;
   -- Side rows whose accepted_at differs from the batch's committed_at (expect 0).
   SELECT count(*) AS drifted
     FROM sessions.ingest_claim_sessions s
     JOIN sessions.ingest_batches b USING (org_id, workspace_id, origin_id, batch_id)
    WHERE s.accepted_at IS NOT NULL AND s.accepted_at IS DISTINCT FROM b.committed_at;
   COMMIT;
   ```

   Re-run PR 1 step 5's pair counts as well; `expected_pairs` must still equal
   `side_rows`. If they ever diverge, disable the reader (below) first.

5. Disable the reader (a single-row update; both readers go back to 044040's query at once):

   ```sql
   UPDATE sessions.ingest_claim_sessions_policy SET read_enabled = false;
   ```

   Turning the writer off also turns the reader off. Re-enabling needs a completed
   backfill again (PR 1 steps 3 to 5) and then step 3 above.

- Remove PR 2: `supabase/rollbacks/20261105044090_claim_sessions_reader.down.sql`
  restores 044040's readers and 044080's guard, drops the refresh trigger and function,
  the CHECK and the column, and deletes its ledger row. It refuses (55000) unless 044090
  is applied with its exact checksum, and while any later upstream migration is applied.
  044080's own down refuses while 044090 is applied.
