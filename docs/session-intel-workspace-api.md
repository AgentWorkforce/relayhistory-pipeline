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
      "priority": 1,
      "notifyEligible": false,
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
- `priority` and `notifyEligible` are present only with `READ_API_ADVISORY_PRIORITY_ENABLED` exactly `true` (default
  off); with it off an alert has neither field, exactly as before M3-7.
- `priority`: 1, 2 or 3, from the same shared function as `GET /v1/coordination/advisories`
  (`src/session-read-api/advisory-priority.ts`; see [the advisories doc](coordination-live-advisories.md), "Priority
  and delivery"), applied to the advisory's urgency and kind. Desktop dedupes system notifications by `advisoryId`
  per human per device (M3-9); until then the pull path is at-least-once.
- `notifyEligible`: true only when the advisory's exact `<kind>:<urgency>` is on `READ_API_ADVISORY_NOTIFY_ROWS`, the
  same server allowlist and parser as the advisories GET. Desktop raises an OS notification only when it is true and
  its local setting allows. Empty by default (nothing eligible); a row is added only with its eval result and
  Khaliq's OK.
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
  `short_summary_generated_at` (null when there is no summary). Served only where the directory list serves it:
  with `READ_API_LIST_SHORT_SUMMARY_ENABLED` exactly `true` (the gate the list route's `includeShortSummary`
  checks). Otherwise both are `null` for every session, `sessionsWithSummary` and `summariesNewestAt` are `null`,
  and the view carries the note `short summaries not served`, so the view never shows a summary the list route
  would refuse.
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
| `sessionsWithSummary` | Of those, how many have a short summary. **`null` when short summaries are not served** (`READ_API_LIST_SHORT_SUMMARY_ENABLED` off) |
| `sessionsWithClaims` | Of those, how many have a current intent or decision. **`null` when the claims reader is unavailable** (missing function, `42883`, or any error); that is not `0` |
| `claimsNewestAt` | The newest `updated_at` of the claims read, or `null` (none, or unavailable) |
| `summariesNewestAt` | The newest `summaryAt` of `status` and `handoffs`, or `null` (none, or not served) |
| `claimsExtraction` | `"unknown"`: the read API cannot see whether the claims extractor runs. It is never `"on"` |
| `notes` | Fixed strings, never with an id or a value |

The notes:

- `claims unavailable`: the claims read failed;
- `alerts capped at 100`, `status capped at 200`, `handoffs capped at 100`: a list reached its bound, so there may be
  more;
- `alert kind without a class`;
- `short summaries not served`: `READ_API_LIST_SHORT_SUMMARY_ENABLED` is off, so no summary is shown;
- `handoff search is not served here; use GET /v1/sessions/summaries/search` (always).

**A failed claims read never fails the route.** Status and handoffs are still served, with `openIntents` and
`decisions` empty, `hasClaims` null, `sessionsWithClaims` and `claimsNewestAt` null, and the note. The failure is
logged with its error code only. A failed alerts or directory read fails the route (`503`).

### Claim and decision titles: what gated them before, and erase

Migration `062170` is the first path that lets the read-API role read **intent** titles: before it,
`sessions.relayfacts_claims_v1` (`062150`) had no read-API function and no grant to `bulls96_read_api`, so intent
titles were never served by the read API. **Decision** titles were already served by `GET /v1/relayfacts` and
`GET /v1/relayfacts/{topic}` (`sessions.relayfacts_list_v1`, `062080`), behind `READ_API_RELAYFACTS_ENABLED` and
`requireCanary(identity, "relayfacts")`. This view serves both behind a different pair of gates,
`READ_API_SESSION_INTEL_ENABLED` and `requireCanary(identity, "coordination")`; both canaries read the same
`READ_API_DIRECTORY_WORKSPACES` allowlist. So turning `READ_API_RELAYFACTS_ENABLED` off does not hide decision
titles from this view: turn `READ_API_SESSION_INTEL_ENABLED` off too.

Erase, precisely ([relayfacts](relayfacts.md), Retention, and its claims section). Where an erase reaches the
relayfacts tables (the self-host operator per-session erase, and the hosted per-user erase job, each only with
`SUMMARY_DECISION_RECORDS_ENABLED=true`), it deletes every evidence row of the erased session from both the decision
and the claim evidence tables, deletes a record or claim left with no evidence, and rewrites a kept one's statement
from its oldest remaining evidence. It does **not** change a kept record's or claim's **title**: the title is the
dedupe key shared by the sessions that still cite it. So a title first written from an erased session can still
appear in this view, under another (live) session whose evidence cites the same record or claim. It never appears
under the erased session itself: that session's evidence rows are gone, and the directory no longer lists it.
Where no erase reaches these tables (`SUMMARY_DECISION_RECORDS_ENABLED` off on the erase path; on hosted, the
operator per-session erase is self-host only) the evidence rows stay, but the session is no longer listed by the directory, so the view
does not ask for it; its titles still show under any live session citing the same record or claim.

### Not gated by the session-intelligence controls

The route does not read the `062160` controls (`sessions.session_intel_controls_v1`): turning a feature off there
does not turn this view off. Only `READ_API_SESSION_INTEL_ENABLED` does. Two reasons, both design questions rather
than code: the controls function is `service_role`-only and takes any org and workspace (it has no read-API scope
pin), so the read API's role cannot call it without a new grant or a new scoped reader; and none of the five
features (`advisor`, `conflicts`, `dm`, `claims`, `regen`) names this view, so which one gates it (or whether it
needs its own) is a product decision. The controls do stop the producers: `claims` off stops new claim writes and scheduled analyses, `advisor`
off stops new advisories, so the view then shows only what was written before.

### No transcript text

Nothing in the response is transcript content. Alerts carry ids, kinds, artifacts (a path, migration name or commit
sha) and timestamps. Sessions carry the directory's title and its stored scrubbed short summary. Claims carry their
title only: never the statement and never the evidence's claim text.

## Storage: migration 20261105062170

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
| `READ_API_ADVISORY_PRIORITY_ENABLED` | off | `priority` and `notifyEligible` on alerts (and the advisories GET's priority order), shared with the advisories GET |
| `READ_API_ADVISORY_NOTIFY_ROWS` | empty | `notifyEligible` rows, shared with the advisories GET; malformed: this route and the advisories GET answer 503 on the read Worker, and the self-host service and the Lambda fail to start |
| `READ_API_SESSION_INTEL_ENABLED` | off | `GET /v1/session-intel/workspace` on the self-host service, the hosted Lambda and the read Worker |

Applying migration `062170` to any real database, and enabling the flag in production, each need Khaliq's own
explicit OK.
