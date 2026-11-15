# Message origin: human-typed vs agent-to-agent

Shared session histories mix messages a person typed with traffic between
agents: an agent prompting its subagent, a peer agent's message, a background
task's notification. This page is how the pipeline tells them apart and how
search filters to human-typed messages only.

## Where the origin comes from

Claude Code records each user message's `origin.kind` (`human`, `peer`,
`task-notification`, ...). relayhistory stores it verbatim as
`session_events.origin_kind`, and the change feed exports it with the row, so
the desktop uploads it like any other `session_event` column. The delivery
ingest keeps every column the client sent (`servicePayload`), so the value is
stored in the canonical records and in each projected event's
`record.payload.origin_kind`.

Uploads from a client before origin capture carry no `origin_kind`.

## `actor_name` and `subagent_id`

`packages/pipeline/src/lib/pipeline/message-origin.ts` turns the payload into
one label per event, written to `actor_name` where a projected event becomes an
index row (`projectedToNeonRow`, shared by the canonical indexer, the buffered
search feed, canonical event reads and owner-buffered reads):

| Event | `actor_name` |
| --- | --- |
| `history` (the harness's log of prompts typed at its input) | `human` |
| user `session_event`, `origin_kind = human`, not a control/meta row | `human` |
| user `session_event`, `origin_kind = peer` | `peer` |
| user `session_event`, `origin_kind = task-notification` or `control_kind = task_notification` | `task-notification` |
| user `session_event`, `is_sidechain` (a parent agent prompting its subagent) | `subagent` |
| user `session_event`, other control or meta row | `system` |
| user `session_event`, an unknown `origin_kind` | `other` |
| assistant `session_event` | `agent` |
| user `session_event` with no origin recorded, tool calls, file edits | none |

`human` is only ever claimed from positive evidence. A message whose author is
unknown gets no label and is never answered to a human-only search.

`subagent_id` is `subagent_session_id`, else, on a sidechain row only, its
`agent_id`. A root-session row's `agent_id` is not used (it can name the
delegated child a tool result reports on).

Sidechain user rows and control rows are not projected as events at all (the
hosted projection's rule, `relayhistory-projection.ts`), so subagent prompts
and classified task notifications never reached search; the labels matter for
the peer messages and unclassified notices that do.

`relayhistory-projection.ts` is unchanged: it is the parity port of the hosted
plpgsql projection, which leaves `actor_name` NULL.

## Search: `origin=`

`GET /v1/events?origin=<label>` keeps only events with that `actor_name`, e.g.
`origin=human&q=yes` for human confirmations. It composes with every other
filter. An unknown label is a 400. A search backend must declare the `actor`
filter (`SearchIndexCapabilities.filters`) for it to be served; one that does
not answers 501 `param_origin`, never an unfiltered page:

| Backend | `actor` filter |
| --- | --- |
| Postgres (`SEARCH_INDEX_BACKEND=postgres`, self-host) | yes, on the existing `actor_name` column |
| ClickHouse (hosted) | yes, on the existing `actor_name` column |
| OpenSearch | no (`actor_name` is stored, not indexed) |

`/v1/sessions/:id/events` does not take the filter (501 `param_origin`).

No schema change: `actor_name` already exists in every index.

## Existing rows

Rows indexed before this change keep an empty `actor_name` until their scope is
re-indexed. A re-index labels `history` and assistant rows from the columns
already uploaded; a user message uploaded
without `origin_kind` stays unlabeled, because nothing recorded who wrote it.
Labelling those needs the client to re-export the rows with the origin, which
is a separate decision.
