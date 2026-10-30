# Session search and index contract

Production uses Supabase for HoopDb and AWS S3 for canonical session objects.
EKS, Aurora and the other cloud stacks are self-hosting conformance targets, not
the hosted production data plane. This contract is provider-neutral PostgreSQL;
the same audit must pass for every self-hosted database adapter.

The contract comes from the query surfaces that people already use, not from
the columns that happen to exist. Cloud's Sessions board, Cloud's shared-session
MCP and Relay Desktop were inspected together. An index is required only for a
server-side durable query shape. Relay Desktop's ephemeral/local dimensions stay
in its SQLite projection and must not add write amplification to Supabase.

## Remote query shapes

| Consumer | Filter or order | Database shape | Required coverage |
| --- | --- | --- | --- |
| Cloud Sessions | newest workspace sessions and opening summaries | `org_id`, `workspace_id`, `last_ts DESC`, stable session/source cursor; oldest nonblank user prompt | workspace rollup recency and opening-prompt lookup |
| Cloud Sessions | harness/source | workspace + exact source + recency | workspace/source rollup recency |
| Cloud Sessions | project | workspace + exact project + recency; existing RelayHistory `projectContains` is substring | workspace/project recency and project trigram |
| Cloud Sessions | agent name | normalized exact name now; prefix is part of the source contract | tenant/workspace/name-prefix/recency agent catalog |
| `search_shared_sessions` | transcript query | `left(content, 4096) ILIKE` or `task_title ILIKE` | direct-write trigram GIN indexes |
| `search_shared_sessions` | source, project, since/until | workspace plus optional equality and event time range | workspace event, source event, and project event keysets |
| `get_shared_session` | session and optional source | workspace-scoped ordered replay | workspace/session/time replay keyset; source stays after the ordered cursor so omitted-source reads remain index ordered |
| `get_shared_session_context` | query or timestamp window within one session | transcript trigram plus replay/time keyset | both paths above; PostgreSQL may bitmap-combine them |
| `list_session_timings` | source, project, since/until | same session/event filters, then timing lookup | source/project/time keysets and session-marker lookup |
| `list_team_activity` | declared session/source latest event | top-one workspace session replay | workspace/session/time replay keyset |

The executable gate is `bun run audit:search-indexes` with
`HOOPDB_URL` supplied by the approved runtime secret mechanism. It reads only
PostgreSQL catalogs plus the agent lookup rollout marker, and prints only
names/readiness failures, never connection data or session content. Missing,
invalid, unfinished or structurally different indexes, unexpected partial
predicates, and an unfinished agent lookup projection rollout fail the command.

Large-table indexes are not created by that command. They must use the migration
owner's resumable `CREATE INDEX CONCURRENTLY` rollout. In particular, hot
transcript GIN indexes require `fastupdate=off`; this bounds delivery work and
avoids an unbounded pending-list merge.

## API gaps that need plumbing before an index is justified

Cloud currently applies several filters after fetching a bounded page. That can
produce an incomplete page and means an index alone would be unused:

- `person` on the Sessions board and `user_id` in `list_session_timings` need a
  server-side filter over the rollup's `user_ids`. Add the API/query first, then
  add a GIN or source-owned projection proven by its plan.
- `created` needs a server-side `first_ts` range contract before a created-time
  index is added.
- Cloud's `project` text also matches project display metadata not owned by
  RelayHistory. Keep that join/search separate from exact `project_id`.
- General dashboard words and agent terms must be split before forwarding so a
  transcript word never silently changes into an identity lookup.

Those gaps are cutover correctness work, not permission to index every array or
text column preemptively.

## Relay Desktop dimensions that stay local

Relay Desktop supports `title`, `project`, `harness`/`source`, `path`, `id`,
`agent`, `is`, `upload`, `updated`, `after`, `before` and free text, plus sorts
for session, project, status, updated, relay, upload, harness, model, reasoning,
created, directory and session ID. Its local SQLite projection already provides
trigram FTS, facet keys and sort indexes.

The following remain local by design:

- `is:active`, `is:inactive`, `is:on-relay`, `is:off-relay`: live relay state;
- `upload:*`: uploader/pipeline state;
- path/directory and substring session ID: local filesystem/catalog metadata;
- model/reasoning and every alternative table sort until a remote API exposes
  that exact ordered query.

Agent, source, project, updated time and transcript text have durable remote
counterparts and are covered by the remote contract above. Local parity does not
mean the two stores must use the same physical index.
