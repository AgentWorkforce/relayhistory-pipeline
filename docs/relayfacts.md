# Relayfacts

Relayfacts were called memory documents until 2026-11-17.

Proposal and first slice, 2026-11-17. Owner: layered-sessions-pipeline. Requested by Khaliq Gant. The code ships off by default; nothing is applied, deployed or turned on from this document.

## Purpose

Agents keep rediscovering decisions that an earlier session already made: which store a feature uses, which flag gates a route, why an approach was dropped. Relayfacts are small, per-topic records of those decisions, with pointers back to the sessions that made them. An agent or a person pulls a topic's relayfacts document before starting work and sees what was decided, whether it was superseded, and where it was implemented.

The first slice has three parts:

1. Storage: two tables in the `sessions` schema, written and read only through SECURITY DEFINER functions.
2. An extractor that turns the decisions already present in a session-analysis result into decision records with evidence.
3. A read API that lists topics and renders one topic as markdown.

## Source of decisions

The extractor reads the structured session-analysis snapshot (`SessionAnalysisSnapshot`, `src/session-analysis/engine/session-analysis-result.ts`), specifically `understanding.decisions`. Each decision is an `AnalysisClaim` with `text`, `evidenceIds`, `confidence` and `basis`. The evidence ids point into `evidence.references`, which carry the session id and the reference order.

It does not re-parse transcripts, and it does not read Bob Costas summaries: those are unstructured text (`Summary.text`) with no decision list.

Mapping:

| Record field | Derived from |
|---|---|
| `topic` | Slug of the first two directory segments of the most-edited file in `metrics.filesChanged` (for example `src-session-read-api`), else `general`. A relative path starts at its first segment. An absolute path starts at its first known source root (`src`, `packages`, `apps`, `lib`, `docs` and similar), so no home directory or user name becomes a topic; an absolute path with no source root, or any path with `..`, gives `general`. Lowercase `[a-z0-9-]`, at most 64 characters. |
| `title` | The first sentence of the decision text, at most 200 characters. |
| `statement` | The decision text with whitespace collapsed to single spaces, at most 1000 characters. |
| `status` | `decided`. |
| evidence `claim` | The decision text, at most 300 characters. |
| evidence `turn_start`, `turn_end` | The lowest and highest 0-based position, among the snapshot's references for that session, of the references the decision cites. These are reference positions, not transcript turn numbers, so they do not address `/v1/sessions/{id}/turns`; the markdown prints them as `refs a-b`. The column names are kept from the shipped migration. |
| evidence `ref` | The first GitHub commit, pull request or issue URL in the decision text, if any. |

Only decisions with `confidence` `high` or `medium` and at least one cited reference become records. Records are deduplicated by workspace, topic and normalized title (lowercase, whitespace collapsed, trailing punctuation removed). A repeated decision adds evidence to the existing record instead of creating a second one. Within one analysis snapshot a record gets at most one evidence row per session: a repeated decision widens that session's range instead of adding a row.

Deduplication limits (best effort, accepted for this slice):

- The title is the model's first sentence, so a re-analysis that words a decision differently creates a new record. Nothing merges near-duplicates.
- Across snapshots, evidence is deduplicated only on the exact (`source`, `session_id`, `turn_start`, `turn_end`). A re-analysis of the same session whose reference positions shift adds another row for that session, up to the 50-row cap per record.
- `sessions.relayfacts_supersede_v1` has no caller in this repository. Every record stays `decided` until a follow-up (for example the coordination advisor) marks one superseded.

## Data model

Migration slot `20261105062080` (`supabase/migrations/20261105062080_memory_documents.sql`), with a checksum-pinned rollback in `supabase/rollbacks/`.

`sessions.relayfacts_v1`:

| Column | Type | Bound |
|---|---|---|
| `id` | `uuid` primary key | |
| `org_id`, `workspace_id` | `text` | non-empty, at most 512; the scope of every read and write |
| `topic` | `text` | `^[a-z0-9][a-z0-9-]{0,63}$` |
| `title` | `text` | 1 to 200 characters |
| `title_key` | `text` | normalized title, used for deduplication |
| `statement` | `text` | 1 to 1000 characters |
| `status` | `text` | `open`, `decided` or `superseded` |
| `supersedes` | `uuid`, nullable | a record in the same workspace |
| `decided_at` | `timestamptz` | |
| `implemented_at` | `timestamptz`, nullable | |
| `implemented_ref` | `text`, nullable | at most 500 characters |
| `created_at`, `updated_at` | `timestamptz` | |

A unique index on (`org_id`, `workspace_id`, `topic`, `title_key`) enforces deduplication. Indexes on (`org_id`, `workspace_id`, `topic`) and (`org_id`, `workspace_id`, `updated_at`) serve the read API.

`sessions.relayfacts_evidence_v1`:

| Column | Type | Bound |
|---|---|---|
| `id` | `bigint` identity | |
| `decision_id` | `uuid` | references `relayfacts_v1` on delete cascade |
| `source` | `text` | a history source name, at most 32 characters |
| `session_id` | `text` | 1 to 512 characters |
| `turn_start`, `turn_end` | `integer` | `0 <= turn_start <= turn_end` |
| `claim` | `text` | 1 to 300 characters |
| `ref` | `text`, nullable | `https://github.com/` commit, pull or issue URL, at most 500 characters |
| `created_at` | `timestamptz` | |

Evidence is unique on (`decision_id`, `source`, `session_id`, `turn_start`, `turn_end`), so rerunning the extractor on the same snapshot adds nothing. A record holds at most 50 evidence rows; further evidence is dropped by the insert function. The extractor sends at most one row per session per record in one call (see Deduplication limits).

Both tables have row-level security enabled and no table privileges for any role. Access goes through SECURITY DEFINER functions with `search_path` pinned and PUBLIC EXECUTE revoked:

| Function | Role | Purpose |
|---|---|---|
| `sessions.relayfacts_insert_v1` | `service_role` | Insert or merge one record and its evidence for one workspace. |
| `sessions.relayfacts_supersede_v1` | `service_role` | Mark one record superseded by another record in the same workspace. No caller yet. |
| `sessions.relayfacts_topics_v1` | `bulls96_read_api` | List topics with record counts for one workspace. |
| `sessions.relayfacts_list_v1` | `bulls96_read_api` | List one topic's records with evidence for one workspace, oldest first, keyset paginated. |
| `sessions.relayfacts_evidence_erase_session_v1` | `bulls96_session_ingest` | Operator per-session erase (`20261105062090`, see Retention). |
| `sessions.relayfacts_user_erase_v1` | owner only (the per-user erase job) | Hosted operator per-user erase (`20261105062100`, see Retention). |

Writes go to the worker role. The task asked for service_role only; the read API runs as `bulls96_read_api`, so the two read functions are granted to that role instead, following the coordination advisories template (`20261105062000`). Both read functions refuse with SQLSTATE `42501` unless `sessions.session_directory_read_api_scope(workspace)` equals the requested org, so a read-API caller cannot read another workspace even with a forged org.

## Extractor runtime

The extractor runs in the session-analysis tick (`src/session-analysis/worker.ts`), as a hook that `runAnalysisJob` calls once after `store.complete` has succeeded. It never runs for a failed, lease-lost or refused job, and it writes to the completed job's own org and workspace. A failing write is contained: the job stays completed, the tick counts `decision_records_fenced` when the write was refused with `RH016` (a reference to an erased, fenced session; see Retention) and `decision_records_failed` for any other failure, and nothing is logged. The tick counts the records it sent as `decision_records`. With the flag off the tick has no hook, so it makes the same store calls as before and adds no count.

## What is not stored

- No transcript text and no message bodies. The reference `summary` and `detail` fields of the analysis evidence are never read by the extractor.
- No secrets. The full decision text and each stored cut of it (title, statement, claim) pass through `scrubText` (the relayscrubber engine via `src/shared/vendor.ts`). A decision the scrubber would change in any way is not recorded at all, so no stored string holds a secret-like value or a redaction marker.
- Evidence is limited to the source, session id, a reference-position range, a claim of at most 300 characters and an optional GitHub URL.
- No people's names beyond what a decision statement itself says, no file contents and no tool output.

## Flags

| Flag | Where | Default | Effect when exactly `true` |
|---|---|---|---|
| `SUMMARY_DECISION_RECORDS_ENABLED` | session-analysis worker | off | After an analysis job completes, extract decisions and call `relayfacts_insert_v1`. Off: no extractor call, no DB call and no log line. |
| `SUMMARY_DECISION_RECORDS_ENABLED` | self-host ingest (operator erase worker) | off | The operator per-session erase also calls `relayfacts_evidence_erase_session_v1` before its purge, and startup requires that function. Off: the erase pass makes the same calls as before. |
| `SUMMARY_DECISION_RECORDS_ENABLED` | operator user-erase job (`src/session-erase/user-erase-main.ts`) | off | Before `operator_user_erase_begin`, the job calls `relayfacts_user_erase_v1` for the user, and logs and returns its counts. Off: the job makes the same calls as before, with no `relayfacts` result key and no relayfacts log line. |
| `READ_API_RELAYFACTS_ENABLED` | read API (Lambda, Cloudflare worker, self-host) | off | Serve `GET /v1/relayfacts` and `GET /v1/relayfacts/{topic}`. Off: both routes answer `501 {"error":"relayfacts_disabled"}`. |

## Retention

A record holds no transcript text, so it is not bound to the transcript retention windows. Superseded records are kept so a topic's history stays readable. Deleting a record deletes its evidence (`ON DELETE CASCADE`). No workspace-level delete path reaches these tables yet; that is a follow-up.

Evidence rows carry session ids and a claim taken from that session. Migration `20261105062090` adds `sessions.relayfacts_evidence_erase_session_v1`, which the self-host operator per-session erase calls with `SUMMARY_DECISION_RECORDS_ENABLED=true`:

- It runs inside the erase pass's search lock, after the search delete and before the purge, under the pass's lease, and only as `bulls96_session_ingest`.
- It deletes every evidence row of the erased session and its canonical ids (`session_erasure_scopes`), within the request's org, workspace and source only.
- A record left with no evidence is deleted. A record that keeps evidence from other sessions keeps its title (the dedupe key, shared by those sessions) and gets the oldest remaining claim as its statement, so no text taken only from the erased session remains.
- A trigger refuses any later evidence insert or update for an erased (fenced) session with SQLSTATE `RH016`, so a late extractor write fails as a whole instead of restoring the session.
- A failure is not swallowed: the pass records `searchDocuments` as `failed_retryable` with code `relayfacts_erase_failed`, skips the purge, and the request is retried. A retry after success deletes nothing more.
- The rollback drops the function and trigger; it does not restore erased evidence.

Hosted: the extractor runs in the hosted session-pipeline Lambda, and the operator per-session erase runs only in self-host. On hosted, these tables are reached by the operator per-user erase job (`docs/operator-user-erase.md`, Relayfacts step). Migration `20261105062100` adds `sessions.relayfacts_user_erase_v1`, which that job calls with `SUMMARY_DECISION_RECORDS_ENABLED=true`:

- It is owner-only (no grant, not `SECURITY DEFINER`), and the job calls it in its own read-committed transaction before `operator_user_erase_begin`. It cannot run after begin, which refuses (`RH040`) while any `session_index` row of the user exists.
- It resolves the user's sessions from `session_index` for the request's org, workspace and user, soft-deleted rows included, under both the session id and the canonical session id, and records each (source, id) pair in `sessions.relayfacts_erasure_fences`. Fence rows are permanent; only the rollback removes them.
- It then applies the per-session rules above to every pair: the evidence rows go, a record left with no evidence is deleted, and a kept record gets the oldest remaining claim as its statement.
- A trigger refuses (`RH016`) any later evidence insert or update for a fenced pair, so a late extractor write fails as a whole.
- Every insert or update statement on the two tables takes a shared advisory lock that the erase takes exclusively, so the erase waits for in-flight writes and blocks new ones until it commits.
- With the flag on, the job first checks that the function exists and refuses with `relayfacts_migration_missing` (exit 2) where `20261105062100` is not applied, before any other statement.
- A failure is not swallowed: the job logs `{"event":"user_erase_relayfacts","outcome":"failed","sqlstate":"…"}` (the five-character code only), never calls begin, and exits 4 (`22023`, a malformed id or a request id that belongs to another user or already fences another workspace, exits 2). A rerun after success adds no fence and deletes nothing.
- A user with no `session_index` row returns zeros without taking the exclusive lock, so the common buffered-only erase does not stop relayfacts writers.
- Deadlock with the per-session erase: `relayfacts_evidence_erase_session_v1` (`20261105062090`) locks a record `FOR UPDATE` before its update takes the shared lock, while this erase holds the lock exclusively and then locks records `FOR UPDATE`. When both touch the same record at once, Postgres aborts one with `40P01`. Both sides are retried (this job exits 4; the per-session pass records `failed_retryable`), and the aborted transaction leaves nothing behind.
- The rollback drops the function, the triggers and the fence table; it does not restore erased evidence. It refuses (`55000`) while any fence row exists, because dropping the fences lets the analysis tick rewrite relayfacts from an erased user's sessions. To drop them anyway, run `SET relayfacts.rollback_drop_fences = 'true'` in the same session before the rollback file (the file opens its own transaction).

Known limitation: the fence guards refuse a whole `relayfacts_insert_v1` batch when any of its evidence rows references a fenced session. An analysis of a live session that cites an erased session therefore writes no record at all, on every re-analysis; the tick counts it as `decision_records_fenced`, separate from `decision_records_failed`, so it does not mask other failures. Filtering fenced references out before the insert is a follow-up.

Per-user (data-subject) erase: see Hosted above. `20261105062100` adds it, and it reaches the relayfacts tables only where that migration is applied and `SUMMARY_DECISION_RECORDS_ENABLED=true` is set on the user-erase job; self-host uses the per-session erase.

## Surfacing

The first slice is pull only: `GET /v1/relayfacts` lists topics with counts, and `GET /v1/relayfacts/{topic}` returns `{topic, records, nextCursor, markdown}`, where `markdown` renders one page of the topic's records. Each route requires the workspace attestation and the canary allowlist, rejects unsupported parameters, returns at most 200 records per page (oldest first) with a cursor, and renders only stored fields. Every line of stored text is escaped, so it cannot open a heading, list, link, HTML tag or code span, or add a line.

The "relayfacts canary" is not a separate allowlist. It is the shared read-API canary allowlist `READ_API_DIRECTORY_WORKSPACES`, checked through `requireCanary(identity, "relayfacts")`; the `"relayfacts"` argument only sets the error label (`relayfacts_workspace_not_enabled`). Adding a workspace to that allowlist also enables, for that workspace, every other read-API route gated by the same check (directory, timing, turns, analysis, output, summary, digest, coordination, catalog and login).

Out of scope, as follow-ups:

- A kit hook that pulls the topic document at the start of a turn (layered-agents kit).
- DM supersede events when a new decision supersedes an old one (coordination advisor).
- A Desktop or Cloud view (relayhistory-cloud).
- A workspace-level delete of records and evidence (this repository).

## Threat model

| Threat | Mitigation |
|---|---|
| Cross-workspace leak | Every function takes `org_id` and `workspace_id` and filters on both. The read functions also require the read-API scope to match. The routes attest the workspace against the caller's identity. A pglite test proves a workspace A row is never returned for workspace B. |
| Prompt injection through extracted text | Records are data, not instructions. The markdown renders stored fields only, escapes markdown control characters at line starts and collapses newlines, and labels the document as recorded decisions. Bounds keep any injected text short. The consumer still treats the document as untrusted evidence. |
| Oversized documents | Column checks bound every field; each record holds at most 50 evidence rows; each page holds at most 200 records; the topic slug is at most 64 characters, and a longer one is answered with 400 before any database call. |
| Secrets in claims | `scrubText` runs on the full decision text and every stored cut; a decision the scrubber would change is not recorded at all. |
| Unattended activation | Both flags default off and must be exactly `true`. The migration enables nothing. |

## Activation (gated on Khaliq)

Each step needs Khaliq's own explicit OK; a relayed quote is not that OK.

1. Apply migrations `20261105062080` and `20261105062090` (and, for hosted, `20261105062100`) with `.github/workflows/apply-session-schema.yml`: run it with mode `plan`, then with mode `apply` (environment `session-schema-production`), then confirm the sentinels.
2. Set `SUMMARY_DECISION_RECORDS_ENABLED=true` on the ingest process that runs the operator erase, confirm it starts (startup requires the erase function), and only then turn on the extractor. On hosted, the extractor is the session-pipeline Lambda, and its switch is the sessions stack input `relayfacts` (`stacks/aws/session-pipeline.ts`). Turn it on only where an erase reaches these tables (see Retention): on hosted, only after `20261105062100` is applied and `SUMMARY_DECISION_RECORDS_ENABLED=true` is set on the operator user-erase job.
3. Set `READ_API_RELAYFACTS_ENABLED=true` on the read API, either through the read-API stack input `relayfacts` (`stacks/aws/session-read-api.ts`) or through the `relayfacts=on` input of `.github/workflows/deploy-session-read-worker.yml`. That input is not preserved from live, so repeat it on later dispatches. Then add the workspace to `READ_API_DIRECTORY_WORKSPACES`, the shared canary allowlist that `requireCanary(identity, "relayfacts")` checks (see Surfacing). That also enables the other directory routes for that workspace, so add only a workspace that is cleared for them too.

Rollback: unset the flags, then run the rollback files if the tables must go, `20261105062100` first, then `20261105062090` (each rollback refuses while a later migration is applied). The `20261105062100` rollback also refuses while erasure fences exist, unless `relayfacts.rollback_drop_fences` is set to `true` in the same session (see Retention).

## Implementation plan

1. Design (this document).
2. Migration, rollback, function boundary lane and sentinel, with pglite tests for up, down and workspace scope.
   Implemented: `supabase/migrations/20261105062080_memory_documents.sql`, its rollback, the `relayfacts` lane, and `src/hoopdb/__tests__/relayfacts.pglite.test.ts`.
3. Extractor in the session-analysis path, behind `SUMMARY_DECISION_RECORDS_ENABLED`.
   Implemented: `src/session-analysis/decision-records.ts`, the `onCompleted` hook of `runAnalysisJob`, the `decisionRecords` sink of `analysisTick`, wired in `src/session-pipeline/lambda.ts`; tests in `src/session-analysis/__tests__/decision-records.test.ts` and `processor.test.ts`.
4. Read API routes, behind `READ_API_RELAYFACTS_ENABLED`.
   Implemented: `src/session-read-api/relayfacts.ts`, registered once in `app.ts` and wired by the Lambda and self-host entries (`lambda.ts`) and the Cloudflare worker; tests in `src/session-read-api/__tests__/relayfacts.test.ts`, `facts-backends-wiring.test.ts` and the worker suite.
5. Self-host composition rows and the runbook.
6. Operator per-session erase (`20261105062090`), behind `SUMMARY_DECISION_RECORDS_ENABLED`.
   Implemented: the migration and rollback, the `relayfacts-erase` lane, `decisionRecords` in `src/session-erase/runtime.ts` and `selfhost-runtime.ts`; tests in `relayfacts.pglite.test.ts`, `runtime.test.ts` and `selfhost-runtime.test.ts`.
7. Hosted operator per-user erase (`20261105062100`), behind `SUMMARY_DECISION_RECORDS_ENABLED` on the user-erase job.
   Implemented: the migration and rollback, the `relayfacts-user-erase` lane, the pre-begin relayfacts step in `src/session-erase/user-erase.ts` and `user-erase-main.ts`; tests in `relayfacts.pglite.test.ts`, `user-erase-main.test.ts`, `migration-profile.test.ts` and `migration-target.pglite.test.ts`.
