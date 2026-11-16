# Message origin: human-typed vs agent-to-agent

Shared session histories mix messages a person typed with traffic between
agents: an agent prompting its subagent, a peer agent's message, a background
task's notification. This page explains how the pipeline tells them apart and
how search filters to human-typed messages only.

## Switch

Everything below is **default off**. `MESSAGE_ORIGIN_LABELS_ENABLED`, exactly
`true`, turns it on. Any other value or unset is off. It is read by:

| Service | Effect when on |
| --- | --- |
| session pipeline Lambda (`SEARCH_INDEX_ENABLED=1`) and its buffered search feed | index rows carry the labels below |
| self-host `session-pipeline-consumer` | the same, on the Postgres index |
| read API (Lambda, self-host service, Cloudflare Worker) | serves `origin=` on `GET /v1/events` |

When the flag is off, index rows are written exactly as before, fingerprints
included. `origin=` answers 501 `param_origin`.

The large-read worker (`large-read-main.ts`), which the pipeline Lambda hands
large scopes, reads the same variable. Set it on both together, so that a
scope moving between them keeps one fingerprint and is labelled either way.

**Rollout order.** One flag spans separately deployed services.

1. Turn it on in the writers first: the pipeline Lambda, the large-read
   worker and the self-host consumer.
2. Turn it on in the readers once scopes have been re-indexed.

A reader turned on earlier answers `origin=` over rows that are not labelled
yet, which leaves them out of a human-only search. Scopes not indexed since the
writers turned it on stay unlabelled.

Browsers see `X-Sessions-Read-Origin` because it is in
`Access-Control-Expose-Headers` when the flag is on.

Turning it on in a writer changes the fingerprint of every labelled row. Each
scope's next index pass therefore rewrites that scope's rows once, in the
search index and in the directory events. Nothing is backfilled: a scope is
relabelled only when it is next indexed. The read paths that build events
straight from canonical objects (session reads, owner-buffered reads) are not
labelled.

## Where the origin comes from

Claude Code records each user message's `origin.kind` (`human`, `peer`,
`task-notification`, ...). relayhistory stores it verbatim as
`session_events.origin_kind`, and the change feed exports it with the row. The
desktop therefore uploads it like any other `session_event` column. The
delivery ingest keeps every column the client sent (`servicePayload`), so the
value is stored in the canonical records and in each projected event's
`record.payload.origin_kind`.

Uploads from a client before origin capture carry no `origin_kind`. They are
not backfilled.

## `actor_name` and `subagent_id`

`packages/pipeline/src/lib/pipeline/message-origin.ts` turns the payload into
one label per event. With the flag on, it is written to `actor_name` where a
projected event becomes an index row (`projectedToNeonRow`):

| Event | `actor_name` |
| --- | --- |
| user `session_event`, `origin_kind = human`, not a control/meta row | `human` |
| user `session_event`, `origin_kind = peer` | `peer` |
| user `session_event`, `origin_kind = task-notification` or `control_kind = task_notification` | `task-notification` |
| user `session_event`, `is_sidechain` (a parent agent prompting its subagent) | `subagent` |
| user `session_event`, other control or meta row | `system` |
| user `session_event`, an unknown `origin_kind` | `other` |
| assistant `session_event` | `agent` |
| user `session_event` with no origin recorded | none |
| `history` (prompt log), tool calls, file edits | none |

`human` is only ever claimed from positive evidence, which is
`origin_kind = human` on the message row. A `history` row carries no origin. A
prompt a broker typed at the harness input also lands in the prompt log, and
the same prompt is already its own `session_event` row. So prompt-log rows are
not labelled, and a human-only search answers each typed prompt once.

A message whose author is unknown gets no label and is never answered to a
human-only search.

An uploaded `actor_name` cannot claim a label: a value that spells one is
dropped unless it was derived here. A free-form name is kept.

`subagent_id` is `subagent_session_id`, else, on a sidechain row only, its
`agent_id`. A root-session row's `agent_id` is not used, because it can name
the delegated child a tool result reports on.

Sidechain user rows and control rows are not projected as events at all (the
hosted projection's rule, `relayhistory-projection.ts`). Subagent prompts and
classified task notifications therefore never reached search. The labels
matter for the peer messages and unclassified notices that do.

`relayhistory-projection.ts` is unchanged. It is the parity port of the hosted
plpgsql projection, which leaves `actor_name` NULL, so rows from the hosted
Neon tail stay unlabelled.

## Search: `origin=`

`GET /v1/events?origin=<label>` keeps only events with that `actor_name`, for
example `origin=human&q=yes` for human confirmations. It composes with every
other filter. An unknown label is a 400.

It is served only when both of these hold; otherwise the answer is 501
`param_origin`, never an unfiltered page:

- the read API has `MESSAGE_ORIGIN_LABELS_ENABLED=true`;
- the search backend declares the `actor` filter
  (`SearchIndexCapabilities.filters`).

| Backend | `actor` filter |
| --- | --- |
| Postgres (`SEARCH_INDEX_BACKEND=postgres`, self-host) | yes, on the existing `actor_name` column |
| ClickHouse (hosted) | yes, on the existing `actor_name` column |
| OpenSearch | no (`actor_name` is stored, not indexed) |

`/v1/sessions/:id/events` does not take the filter (501 `param_origin`).

No schema change: `actor_name` already exists in every index.

### What a client must check

- Every answered filtered page carries `X-Sessions-Read-Origin: <label>`.
- A read API from before this change ignores `origin=` and answers every
  author, without that header. **Treat a page without the header as
  unfiltered.**
- When a filtered search reaches the history floor, its `continuation` carries
  `originFilter: "not_applied"`. The backend the rest is handed to
  (relayhistory) does not apply the filter. A 501 (`history_floor`,
  `session_partial`) on an `origin=` search means the same thing for a
  fallback read: it would be unfiltered.
