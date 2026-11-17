# Memory documents

Proposal and first slice, 2026-11-17. Owner: layered-sessions-pipeline. Requested by Khaliq Gant. The code ships off by default; nothing is applied, deployed or turned on from this document.

## Purpose

Agents keep rediscovering decisions that an earlier session already made: which store a feature uses, which flag gates a route, why an approach was dropped. A memory document is a small, per-topic record of those decisions, with pointers back to the sessions that made them. An agent or a person pulls the document for a topic before starting work and sees what was decided, whether it was superseded, and where it was implemented.

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
| `topic` | Slug of the first two path segments of the most-edited file in `metrics.filesChanged` (for example `src-session-read-api`), else `general`. Lowercase `[a-z0-9-]`, at most 64 characters. |
| `title` | The first sentence of the decision text, scrubbed, at most 200 characters. |
| `statement` | The decision text, scrubbed, at most 1000 characters. |
| `status` | `decided`. |
| evidence `claim` | The decision text, scrubbed, at most 300 characters. |
| evidence `turn_start`, `turn_end` | The lowest and highest 0-based position, among the snapshot's references for that session, of the references the decision cites. |
| evidence `ref` | The first GitHub commit, pull request or issue URL in the decision text, if any. |

Only decisions with `confidence` `high` or `medium` and at least one cited reference become records. Records are deduplicated by workspace, topic and normalized title (lowercase, whitespace collapsed, trailing punctuation removed). A repeated decision adds evidence to the existing record instead of creating a second one.

## Data model

Migration slot `20261105062080` (`supabase/migrations/20261105062080_memory_documents.sql`), with a checksum-pinned rollback in `supabase/rollbacks/`.

`sessions.decision_records_v1`:

| Column | Type | Bound |
|---|---|---|
| `id` | `uuid` primary key | |
| `org_id`, `workspace_id` | `text` | non-empty, at most 256; the scope of every read and write |
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

`sessions.decision_evidence_v1`:

| Column | Type | Bound |
|---|---|---|
| `id` | `bigint` identity | |
| `decision_id` | `uuid` | references `decision_records_v1` on delete cascade |
| `source` | `text` | a history source name, at most 32 characters |
| `session_id` | `text` | 1 to 512 characters |
| `turn_start`, `turn_end` | `integer` | `0 <= turn_start <= turn_end` |
| `claim` | `text` | 1 to 300 characters |
| `ref` | `text`, nullable | `https://github.com/` commit, pull or issue URL, at most 500 characters |
| `created_at` | `timestamptz` | |

Evidence is unique on (`decision_id`, `source`, `session_id`, `turn_start`, `turn_end`), so rerunning the extractor on the same snapshot adds nothing. A record holds at most 50 evidence rows; further evidence is dropped by the insert function.

Both tables have row-level security enabled and no table privileges for any role. Access goes through SECURITY DEFINER functions with `search_path` pinned and PUBLIC EXECUTE revoked:

| Function | Role | Purpose |
|---|---|---|
| `sessions.decision_records_insert_v1` | `service_role` | Insert or merge one record and its evidence for one workspace. |
| `sessions.decision_records_supersede_v1` | `service_role` | Mark one record superseded by another record in the same workspace. |
| `sessions.decision_records_topics_v1` | `bulls96_read_api` | List topics with record counts for one workspace. |
| `sessions.decision_records_list_v1` | `bulls96_read_api` | List one topic's records with evidence for one workspace, oldest first, keyset paginated. |

Writes go to the worker role. The task asked for service_role only; the read API runs as `bulls96_read_api`, so the two read functions are granted to that role instead, following the coordination advisories template (`20261105062000`). Both read functions refuse with SQLSTATE `42501` unless `sessions.session_directory_read_api_scope(workspace)` equals the requested org, so a read-API caller cannot read another workspace even with a forged org.

## What is not stored

- No transcript text and no message bodies. The reference `summary` and `detail` fields of the analysis evidence are never read by the extractor.
- No secrets. Every stored string passes through `scrubText` (the relayscrubber engine via `src/shared/vendor.ts`), and a claim that scrubbing changed is dropped rather than stored redacted.
- Evidence is limited to the source, session id, a turn range, a claim of at most 300 characters and an optional GitHub URL.
- No people's names beyond what a decision statement itself says, no file contents and no tool output.

## Flags

| Flag | Where | Default | Effect when exactly `true` |
|---|---|---|---|
| `SUMMARY_DECISION_RECORDS_ENABLED` | session-analysis worker | off | After an analysis job completes, extract decisions and call `decision_records_insert_v1`. Off: no extractor call, no DB call and no log line. |
| `READ_API_MEMORY_DOCUMENTS_ENABLED` | read API (Lambda, Cloudflare worker, self-host) | off | Serve `GET /v1/memory/documents` and `GET /v1/memory/documents/{topic}`. Off: both routes answer `501 {"error":"memory_documents_disabled"}`. |

## Retention

A record holds no transcript text, so it is not bound to the transcript retention windows. Records and their evidence are kept until the workspace's history is deleted. Superseded records are kept so a topic's history stays readable. Deleting a record deletes its evidence (`ON DELETE CASCADE`).

Evidence rows carry session ids. The operator per-session erase does not reach these tables in this slice. Wiring it in is a follow-up, and it is a precondition of activation step 2 below: no record is written until an erased session's evidence can be removed.

## Surfacing

The first slice is pull only: `GET /v1/memory/documents` lists topics with counts, and `GET /v1/memory/documents/{topic}` returns a markdown rendering. Each route requires the workspace attestation and the `memory` canary, rejects unsupported parameters, returns at most 200 records per page (oldest first) with a cursor, and renders only stored fields.

Out of scope, as follow-ups:

- A kit hook that pulls the topic document at the start of a turn (layered-agents kit).
- DM supersede events when a new decision supersedes an old one (coordination advisor).
- A Desktop or Cloud view (relayhistory-cloud).
- Per-session erase of evidence rows (this repository; required before activation step 2).

## Threat model

| Threat | Mitigation |
|---|---|
| Cross-workspace leak | Every function takes `org_id` and `workspace_id` and filters on both. The read functions also require the read-API scope to match. The routes attest the workspace against the caller's identity. A pglite test proves a workspace A row is never returned for workspace B. |
| Prompt injection through extracted text | Records are data, not instructions. The markdown renders stored fields only, escapes markdown control characters at line starts and collapses newlines, and labels the document as recorded decisions. Bounds keep any injected text short. The consumer still treats the document as untrusted evidence. |
| Oversized documents | Column checks bound every field; each record holds at most 50 evidence rows; each page holds at most 200 records; the topic slug is at most 64 characters, and a longer one is answered with 400 before any database call. |
| Secrets in claims | `scrubText` runs on every stored string; a claim scrubbing would change is dropped. |
| Unattended activation | Both flags default off and must be exactly `true`. The migration enables nothing. |

## Activation (gated on Khaliq)

Each step needs Khaliq's own explicit OK; a relayed quote is not that OK.

1. Apply migration `20261105062080` to the target database, then confirm its sentinel.
2. After the per-session erase removes evidence rows (see Retention), set `SUMMARY_DECISION_RECORDS_ENABLED=true` on the session-analysis worker.
3. Set `READ_API_MEMORY_DOCUMENTS_ENABLED=true` on the read API, and add the workspace to the `memory` canary.

Rollback: unset the flags, then run the rollback file if the tables must go.

## Implementation plan

1. Design (this document).
2. Migration, rollback, function boundary lane and sentinel, with pglite tests for up, down and workspace scope.
3. Extractor in the session-analysis path, behind `SUMMARY_DECISION_RECORDS_ENABLED`.
4. Read API routes, behind `READ_API_MEMORY_DOCUMENTS_ENABLED`.
5. Self-host composition rows and the runbook.
