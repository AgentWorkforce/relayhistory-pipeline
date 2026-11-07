# Session-list brief titles off Neon (F2)

Cloud's session list shows each session's cached model-written brief title. Today Cloud reads it with direct SQL on Neon (`sessions.session_analysis_requests` joined to `session_analysis_jobs`, `kind = 'brief'`). F2 moves that read to this pipeline. Everything is default-off. Nothing here is applied, flagged, deployed or backfilled by this repository.

## Pieces

**Migration `20261105043000_brief_titles.sql` (registry slot 043000).**
- `session_briefs` gains `origin` (`native` | `neon_backfill`) and `legacy_job_id`.
- A BEFORE trigger derives `origin` from the brief text: `engine_version LIKE 'neon-backfill:%'`.
  - Native writes (`src/session-brief/store.ts`) are unchanged.
  - A native brief, or a quarantine clear, turns a row native.
- For `neon_backfill` rows only, three CHECKs are restated so a backfilled row may have no state, no description, or a line break in its title.
- `sessions.session_brief_titles_for` is the read path:
  - EXECUTE for `bulls96_read_api` only;
  - the tenant is pinned by `session_directory_read_api_scope`;
  - it resolves a RelayHistory id through `coalesce(canonical_session_id, session_id)` of a live pointer;
  - a native brief wins over a backfilled one, then the newest;
  - titles are trimmed and never blank.

**Read API: `GET /v1/sessions/titles?workspace=<token's>&key=<source>:<sessionId>`** (1-100 keys).
- It is gated by its own allowlist, `READ_API_BRIEF_TITLES_WORKSPACES` (unset = none, `*` = all), separate from the P3 directory canary.
- It answers 501 until 043000 is applied.
- A blank or over-long title drops only that key.
- Callers keep each URL bounded: 100 ids of up to 255 characters can exceed a front-door URL limit. Cloud's client splits batches at 8,000 characters.

**Cloud.**
- Switch: `CLOUD_BRIEF_TITLES_BACKEND=sessions_pipeline` in `app/dashboard/sessions/session-brief-titles.ts` (cloud#6145). Var plumbing is cloud#6151. Unset keeps the Neon read.
- There is no Neon fallback, and any failure leaves the list on History summaries.

**Backfill: `src/migration/neon-brief-titles/`.**
- It uses Cloud's exact Neon selection across every scope, resolved like the read path.
- It writes `origin = 'neon_backfill'` rows. It never overwrites a native title and never refills a quarantined one.
- Modes:
  - dry run (default; each batch rolls back);
  - `--execute --expect-supabase-ref=<ref>` (guarded);
  - `--verify`, which is read-only and exits 2 when any session with a live pointer is missing its title or serves a different one, or when the verify output does not cover every key exactly (a missing, duplicate or malformed row).
  - Keys that a well-formed row says have no live pointer are reported as `no_pointer` and **excluded from the title comparison**. That count is not a proof of copy completeness; the copy verifier owns that question.
- Only counts and keys are printed, never titles.

## Containment: backfilled titles stay out of the directory and search

- A backfilled title has **two readers**, and neither is the directory:
  - `session_brief_titles_for`: Cloud's session list, gated by `READ_API_BRIEF_TITLES_WORKSPACES`;
  - **`sessions.session_brief()`** (20261102000000, last replaced in 026000): relayhistory-cloud's `GET /v1/sessions/:id/brief`, granted to `session_directory_search_worker`. After a backfill it returns the Neon title (line breaks included) as `stale`, with a NULL description and state and `engine_version 'neon-backfill:v1'`. Before `--execute`, this response shape needs the Cloud brief-endpoint owner's acceptance, or containment in `session_brief()`. That decision belongs to the migration lead.
- The directory projector (`src/hoopdb/session-directory-projector.ts`) reads native brief titles only. It filters on `engine_version NOT LIKE 'neon-backfill:%'`, which also works before 043000 adds `origin`.
- So a Neon title never reaches `session_directory.title`, `search_text`, `list_session_directory_for`, or the brief-document/search publication built from the directory row.
- The runtime writer role could set the reserved `neon-backfill:` prefix itself (finding A5). That would only widen the legacy-row CHECKs for that row, and the row would stay out of the directory like any backfilled one. Accepted; no owner-role pin.
- Proof: `src/hoopdb/__tests__/session-directory-brief-provenance.test.ts`. It covers native precedence and the pointer fallback, ordinary and line-break negatives, a causal run of the unfiltered pre-fix SQL, and pre-043000 projection.

## Freshness

- `sweepUnbriefed` arms only sessions with no brief row, so a backfilled session keeps its Neon title until new evidence arrives.
- Indexing of that evidence arms a wakeup. The native brief then replaces the backfilled one, and `origin` becomes native.
- `briefs.sweepSeconds` (stack input, absent = off) briefs history that has neither.

## Activation order (each live step needs Khaliq's explicit OK)

1. Land in slot order after 040000-042000.
2. Apply 043000 in a generation window and rebind the readers.
3. Load the pipeline model key from 1Password and deploy with `briefs.enabled: true`.
4. Lead: backfill dry run, then `--execute`, then `--verify`, which must exit 0.
5. Deploy the read API with `SESSION_READ_BRIEF_TITLES_WORKSPACES`: a canary workspace first, then `*`.
6. Cloud: #6145 and #6151, then `CLOUD_BRIEF_TITLES_BACKEND=sessions_pipeline` (canary first), then the #6125 `brief_titles` Neon off-switch.

## Rollback

`supabase/rollbacks/20261105043000_brief_titles.down.sql` refuses (55000):
- while a later migration is applied;
- while any `neon_backfill` row remains. Dropping `origin` would make Neon titles look native.

Delete those rows deliberately first. The down then restores the exact prior CHECKs.

## Relationship to `runbooks/analysis-title-cache.md`

That runbook covers the analysis-cache title path. This repo's read Worker serves it (`packages/session-read-worker/src/analysis-cache.ts`, `view=title`) from Neon→S3 analysis snapshots, behind its own default-off flags. F2 **coexists** with it and does not supersede it: F2's titles come from `session_briefs` on Supabase through `GET /v1/sessions/titles`. Cloud picks one backend per list read (`CLOUD_BRIEF_TITLES_BACKEND`).
