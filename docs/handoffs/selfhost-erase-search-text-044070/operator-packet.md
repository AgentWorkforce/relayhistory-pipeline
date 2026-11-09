# Operator packet: erase and retire rebuild search_text (slot 044070)

Status: code only. This change does not apply anything to any database. Applying
migration 044070, or running the one-off rebuild below, against any real database
needs Khaliq's separate, explicit OK in his own words.

## What the migration changes

`supabase/migrations/20261105044070_selfhost_erase_search_text_rebuild.sql` does a
`CREATE OR REPLACE` of two SECURITY DEFINER functions with 044050's bodies byte for
byte, except that each `UPDATE sessions.session_directory` SET list also sets

```sql
search_text=sessions.session_directory_search_text(d.title,d.task_title,d.agent_name,d.project_id,d.git_repo,d.git_branch,d.model,NULL)
```

- `sessions.ingest_selfhost_set_session_uploads(jsonb)`: the self-host per-session erase.
- `sessions.ingest_publication_retire_fence()`: the managed-head retire trigger function
  (triggers `ingest_publication_retire_version` and `ingest_publication_retire_reset`).

Why (privacy): both functions clear `short_summary` and its provenance, but before
044070 neither rebuilt `search_text`, which embeds the summary. On a directory row
that is not tombstoned (another live pointer keeps `deleted_at` NULL, so the
tombstone trigger does not empty `search_text`), the erased summary text stayed
matchable through `sessions.search_session_directory` (`d.search_text ILIKE`).

Unchanged: signature, VOLATILE, SECURITY DEFINER, `search_path=pg_catalog,pg_temp`,
owner, ACL, both triggers. No new feature flag (bug fix of an existing default path).
Rollback: `supabase/rollbacks/20261105044070_selfhost_erase_search_text_rebuild.down.sql`
restores 044050's exact bodies; it refuses (55000) unless 044070 is applied with its
exact checksum and while a later upstream migration is applied.

## One-off rebuild for rows erased before 044070

The migration fixes future erases only. Rows erased or retired before it may still
carry the old summary inside `search_text`. Candidates: `short_summary IS NULL`,
`deleted_at IS NULL`, `search_text <> ''`, and `search_text` differs from the
metadata-only document. Rows with an empty `search_text` carry no summary text and
are left alone (rebuilding them would newly expose them to metadata search).

The candidate set can also include rows whose `search_text` is stale for another
reason (for example written by an older builder). Rebuilding those is still correct:
the result is the one definition with a NULL summary, which is what every current
writer produces for a row without a summary.

Run as the schema owner (the table owner; no runtime role has UPDATE on it), after
044070 is applied, so no erase in flight re-introduces the old behaviour. Report
counts only; never select or log `search_text`, `short_summary` or titles.

### Step 1: dry run, count only

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '60s';
SELECT count(*) AS stale_rows
  FROM sessions.session_directory AS d
 WHERE d.short_summary IS NULL
   AND d.deleted_at IS NULL
   AND d.search_text <> ''
   AND d.search_text IS DISTINCT FROM sessions.session_directory_search_text(
         d.title, d.task_title, d.agent_name, d.project_id,
         d.git_repo, d.git_branch, d.model, NULL);
COMMIT;
```

Record the count. If it is 0, stop: nothing to rebuild.

### Step 2: batched rebuild (only with Khaliq's explicit OK)

Each batch is one short owner transaction. Repeat until it reports `UPDATE 0`.

```sql
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
WITH batch AS (
  SELECT d.org_id, d.workspace_id, d.harness, d.session_id
    FROM sessions.session_directory AS d
   WHERE d.short_summary IS NULL
     AND d.deleted_at IS NULL
     AND d.search_text <> ''
     AND d.search_text IS DISTINCT FROM sessions.session_directory_search_text(
           d.title, d.task_title, d.agent_name, d.project_id,
           d.git_repo, d.git_branch, d.model, NULL)
   ORDER BY d.org_id, d.workspace_id, d.harness, d.session_id
   LIMIT 500
   FOR UPDATE SKIP LOCKED
)
UPDATE sessions.session_directory AS d
   SET search_text = sessions.session_directory_search_text(
         d.title, d.task_title, d.agent_name, d.project_id,
         d.git_repo, d.git_branch, d.model, NULL)
  FROM batch AS b
 WHERE d.org_id = b.org_id AND d.workspace_id = b.workspace_id
   AND d.harness = b.harness AND d.session_id = b.session_id
   -- Re-checked under the row lock: a summary written meanwhile is left alone.
   AND d.short_summary IS NULL
   AND d.deleted_at IS NULL;
COMMIT;
```

On a lock_timeout (55P03) or statement_timeout (57014) error, the batch rolls back
with no change; rerun it. Rows skipped by `SKIP LOCKED` are picked up by a later batch.

### Step 3: verify, count only

Rerun the Step 1 query. Expected: 0. Report the before count, the number of batches,
the sum of `UPDATE n`, and the after count.

## Validation done in the PR (PGlite, synthetic data)

Regression tests in `src/session-ingest/__tests__/selfhost-server.pglite.test.ts`
(044070) cover both the erase and the retire-fence path on an untombstoned row: the
summary phrase is searchable before, and after the clear the five summary columns are
NULL, `deleted_at` is NULL, `search_text` equals the metadata-only document, the phrase
no longer matches through `sessions.search_session_directory`, and the title still does.
