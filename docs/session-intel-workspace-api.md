# Session Intelligence: the workspace view API

Session Intelligence M3-4a ([proposal](proposals/session-intelligence.md), "Delivery", the dashboard bullets). One
read route gives a dashboard what it needs for a workspace: live alerts, live status and progress, and recent
handoffs, with a `coverage` object that says what is missing. It is the data side of the "Session intelligence" view;
the view itself (relay-webapp) and the adjudication buttons are not part of this slice.

Off by default. Nothing ships enabled from this document.

## Route

```
GET /v1/session-intel/workspace?workspace=<id>
```

- **Gating.** Unless `READ_API_SESSION_INTEL_ENABLED` is exactly `true`, the route answers `501` with the read API's
  standard envelope, `{"error":{"code":"unsupported_by_backend","message":"not served by this backend: session_intel_disabled","reason":"session_intel_disabled"},"correlationId":"<id>"}`,
  and reads nothing. The bearer's `401` comes first, as on every `/v1` route.
- **Auth and scope, as `GET /v1/coordination/advisories`.** `attestWorkspace`: `workspace` exactly once and equal to
  the token's (`403` otherwise; absent is `501 org_scope`). Then `requireCanary(identity, "coordination")`, the
  `READ_API_DIRECTORY_WORKSPACES` allowlist (`501 coordination_workspace_not_enabled`), and `rejectUnsupported`.
  No other parameter is served: any other one is `501 param_<name>`. Every read is bound to the token's org and
  workspace; nothing crosses a workspace.
- **Served on** the self-host service, the hosted Lambda and the read Worker, wherever the advisories GET is. GET
  only; CORS stays GET-only.
- **Contract:** `session-intel-workspace/1`, in the response's `contract` field. `nextCursor` is always `null` in v1.
- **Other answers.** `501 directory_not_configured` or `directory_not_migrated` without the directory;
  `501 session_intel_not_migrated` until migration `20261105062170` is applied; `503 backend_unavailable` when the
  alerts or directory read fails; `500` when a directory row of another tenant comes back.

## Response

```json
{
  "contract": "session-intel-workspace/1",
  "alerts": [
    {
      "advisoryId": "9f3c2a71d04be6e5",
      "urgency": "high",
      "kind": "duplicate_work",
      "class": "collision",
      "artifact": "supabase/migrations/062130_x.sql",
      "sessions": [{ "source": "claude", "sessionId": "<id>" }, { "source": "codex", "sessionId": "<id>" }],
      "createdAt": "2026-11-19T10:04:00.000Z",
      "deliveredAt": null,
      "acked": [{ "source": "claude", "sessionId": "<id>", "acked": false }, { "source": "codex", "sessionId": "<id>", "acked": true }],
      "evidence": [{ "sessionId": "<id>", "eventId": "<id>", "ts": "2026-11-19T09:58:12.000Z", "kind": "tool_result" }]
    }
  ],
  "status": [
    {
      "source": "claude", "sessionId": "<id>", "projectId": "<id or null>", "title": "<title or null>",
      "shortSummary": "<scrubbed short summary or null>", "summaryAt": "<ISO or null>", "state": "active",
      "lastActivityAt": "2026-11-19T10:03:00.000Z", "openIntents": ["<intent title>"], "hasClaims": true
    }
  ],
  "handoffs": [
    {
      "source": "codex", "sessionId": "<id>", "title": "<title or null>", "shortSummary": "<or null>",
      "summaryAt": "<ISO or null>", "state": "done", "lastActivityAt": "2026-11-18T16:00:00.000Z",
      "decisions": ["<decision title>"], "openIntents": [], "hasClaims": true
    }
  ],
  "coverage": {
    "sessionsActive": 1, "sessionsWithSummary": 1, "sessionsWithClaims": 1,
    "claimsNewestAt": "<ISO or null>", "summariesNewestAt": "<ISO or null>",
    "claimsExtraction": "unknown",
    "notes": ["handoff search is not served here; use GET /v1/sessions/summaries/search"]
  },
  "nextCursor": null
}
```

### alerts

The workspace's open advisories (`sessions.coordination_advisories`, migration `062000`), **one entry per advisory
id**, newest first (the earliest `created_at` of its rows), at most **100**.

- **Open** means at least one recipient row is `pending` or `delivered` and younger than the 7-day retention. An
  advisory acked by every recipient, expired, or older than 7 days is not listed. Acked rows of an open advisory are
  read only for the `acked` flags.
- `sessions`: the distinct recipient and peer sessions of its rows, byte-ordered. Two for a pair; at most 4 when the
  recipients name different peers (a planned-fleet conflict sent to two orchestrators).
- `acked`: one entry per recipient row, with whether that recipient acked.
- `urgency`: `high` if any recipient row is HIGH. `deliveredAt`: the earliest delivery of any recipient (an ack also
  sets it). `evidence`: the distinct `{sessionId, eventId, ts, kind}` entries of its rows, at most 20.
- `class`: the alert taxonomy's class of the kind at its urgency, as `src/session-intel/replay/classes.ts`
  `classOfKind` maps it (`duplicate_work` and `superseded_work` are `collision`; `concurrent_edit` is `collision` at
  HIGH and `overlap` at LOW; `nearby_work` and `similar_summary` are `overlap`; a kind that names a class is that
  class). A kind without a class is `null`, with the note "alert kind without a class", never a silent `none`. The
  read API carries a mirror of the mapping (classes.ts reads its own file at load, which the bundled Lambda and the
  Worker cannot), pinned to `classOfKind` by the route test.

### status

Live status and progress: the directory's sessions whose `last_activity_at` is within the last **2 hours** and whose
`state` is not ended (`done` or `abandoned`), newest first. The directory page is at most **200** sessions.

- `title`, `projectId`, `state`, `lastActivityAt` come from the directory (`sessions.list_session_directory_for`, the
  existing read-API path; no new grant).
- `shortSummary` is the directory's stored short summary, which is scrubbed at the producer; `summaryAt` is its
  `short_summary_generated_at` (null when there is no summary).
- `openIntents`: up to **3** current (not superseded) intent claims whose evidence names the session
  (`sessions.relayfacts_claims_v1`, migration `062150`), newest first, **titles only**, at most 120 characters.
- `hasClaims`: whether the session has any current intent or decision; `null` when the claims read failed.

### handoffs

Idle or ended sessions of the last **7 days**, newest first, at most **100**: the ended sessions of the 2-hour page,
plus the sessions whose last activity is between 2 hours and 7 days ago (a second directory page of at most 100).
Each has the status fields above except `projectId`, plus `decisions`: up to **5** current decision records whose
evidence names the session (`sessions.relayfacts_v1`, migration `062080`), titles only.

**Search is out of scope for this slice.** To find a handoff by text, use the existing short-summary search,
`GET /v1/sessions/summaries/search?workspace=<id>&q=<text>` (behind `READ_API_SUMMARY_SEARCH_ENABLED`), and the
directory list's `q` (`READ_API_SESSION_QUERY`). The view always carries the note saying so.

### coverage

What the view could and could not see, so a dashboard never implies full coverage.

| Field | Meaning |
|---|---|
| `sessionsActive` | The number of `status` sessions |
| `sessionsWithSummary` | Of those, how many have a short summary |
| `sessionsWithClaims` | Of those, how many have a current intent or decision. **`null` when the claims reader is unavailable** (missing function, `42883`, or any error); that is not `0` |
| `claimsNewestAt` | The newest `updated_at` of the claims read, or `null` (none, or unavailable) |
| `summariesNewestAt` | The newest `summaryAt` of `status` and `handoffs`, or `null` |
| `claimsExtraction` | `"unknown"`: the read API cannot see whether the claims extractor runs. It is never `"on"` |
| `notes` | Fixed strings, never with an id or a value |

The notes:

- `claims unavailable`: the claims read failed;
- `alerts capped at 100`, `status capped at 200`, `handoffs capped at 100`: a list reached its bound, so there may be
  more;
- `alert kind without a class`;
- `handoff search is not served here; use GET /v1/sessions/summaries/search` (always).

**A failed claims read never fails the route.** Status and handoffs are still served, with `openIntents` and
`decisions` empty, `hasClaims` null, `sessionsWithClaims` and `claimsNewestAt` null, and the note. The failure is
logged with its error code only. A failed alerts or directory read fails the route (`503`).

### No transcript text

Nothing in the response is transcript content. Alerts carry ids, kinds, artifacts (a path, migration name or commit
sha) and timestamps. Sessions carry the directory's title and its stored scrubbed short summary. Claims carry their
title only: never the statement and never the evidence's claim text.

## Storage: migration 20261105062170 (provisional slot)

Two read-only (`STABLE`) SECURITY DEFINER functions, `search_path` pinned, PUBLIC EXECUTE revoked, EXECUTE for
`bulls96_read_api` alone. Each refuses (`42501`) unless the org is the read API's request scope
(`session_directory_read_api_scope`, as `coordination_advisories_pending_for_v1` enforces it). No table, index or
table grant is added; nothing else changes. The rollback is checksum-pinned and refuses while a later step is
applied.

- `sessions.session_intel_workspace_alerts_v1(org, workspace, limit)`, limit 1 to 100: the alerts above.
- `sessions.session_intel_workspace_claims_v1(org, workspace, session_keys text[], limit)`: 1 to 300
  `"<source>:<sessionId>"` keys (the source is a harness name, so the first colon splits), limit 1 to 2400. Per
  session at most 3 intents and 5 decisions, newest first; rows of `(source, session_id, kind, title, updated_at)`,
  with the title cut to 120 characters. The route asks for 8 rows per key, so the limit never cuts a session.

The `bulls96_read_api` role's transactions are read-only by default; these readers only read.

## Flag

| Flag | Default | Turns on |
|---|---|---|
| `READ_API_SESSION_INTEL_ENABLED` | off | `GET /v1/session-intel/workspace` on the self-host service, the hosted Lambda and the read Worker |

Applying migration `062170` to any real database, and enabling the flag in production, each need Khaliq's own
explicit OK.
