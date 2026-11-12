# Live coordination advisories

Proposal, 2026-11-12. Owner: layered-sessions-pipeline. Requested by Khaliq Gant via relay-desktop. Design only; nothing ships from this document.

## The question

Can a continuously running job find overlapping or adjacent work across **live** sessions and send those agents a message before the work collides? It would use two urgency levels:

- **HIGH** is a peer message: an Agent Relay DM, sent now.
- **LOW** is an advisory that the desktop delivers at the natural end of a turn.

## What exists, and what the evidence says

The pipeline already has a retrospective detector, `outcome-facts` v1 (#211), served at `GET /v1/coordination/overlaps` behind `READ_API_COORDINATION_OVERLAPS_ENABLED`. The decision record is `docs/coordination-overlap-detection.md`. Its evidence argues against a live alert:

- **No approach warned before the harm** at a usable alert budget, on 5,008 sessions.
- **Planned fleets were 50 of the 60 first-pool alerts.** Review fleets and one person splitting work across agents were the largest source of false alarms.
- **The record says not to interrupt work in progress.** The detector is for reconcile or close-one follow-ups.

Live precision will be poor until the facts carry relay-message evidence and normalized tool-call facts, and until stored facts are reprojected with #485 so Codex edits count. This proposal therefore ships **off by default**, and HIGH is restricted to the high-confidence tiers. Everything below is shaped so that a replay over the frozen snapshot can measure precision before anyone turns it on.

## Detection

A new worker, `SERVICE=coordination-advisor`, built on the same `runWorker` pattern as `bob-costas` (`src/bob-costas/main.ts`, `src/shared/worker.ts`). Its side `tick` runs every `COORDINATION_ADVISOR_INTERVAL_SECONDS` (default `120`) for each org and workspace. It inherits the worker's live gate (ledger, marker, phase 2 and the sessions-function boundary), so a closed gate stops the scan as it stops every claim path. It needs a new `docker/entrypoint.sh` case.

Each scan:

1. **Candidate lanes.** Sessions whose session-directory `last_activity_at` is within the last 2 hours, grouped by `project_id`. Only sessions in the same project are paired.
2. **Planned fleets are excluded.** A pair whose directory `parent_session_ids` intersect is never a candidate. The retrospective detector demotes planned fleets; the live advisor drops them, because an interruption is costlier than a list entry.
3. **Detection.** Read the candidates' facts through the existing `CoordinationBackend.readFacts` and run `detectCoordination` (`packages/coordination-detect`) over the resulting `LaneFacts`. The existing evidence caps apply unchanged: `COORDINATION_FACTS_ROW_CAP` (10,000), `COORDINATION_OUTPUT_ROW_CAP` (50,000) and the seven-day `COORDINATION_EVIDENCE_LOOKBACK_MS`. A truncated read is logged with its notes and produces no advisory from the truncated stream.
4. **Pre-filter for LOW only.** A cheap check gates LOW and nothing else: the same repo or project plus overlapping top-level directories in `files_touched`, or `short_summary` similarity. It never promotes a pair to HIGH and never suppresses a HIGH finding.

The worker reads facts and directory rows and writes only `sessions.coordination_advisories`. It reads no transcript body.

## Urgency rubric

| Urgency | Signal | Delivery |
|---|---|---|
| HIGH | T1 `duplicate_work`: the same migration number or filename written by two session roots | Relay DM now, and the pull path |
| HIGH | T2 `superseded_work`: one root reverts a commit the other root made | Relay DM now, and the pull path |
| HIGH | The same file written by two roots on different branches within 60 minutes | Relay DM now, and the pull path |
| LOW | Same project and overlapping top-level directories, with no shared file yet | Turn-end advisory (pull path only) |
| LOW | Similar short summaries, with no shared file yet | Turn-end advisory (pull path only) |
| SILENT | Everything else, and **any** pair inside a planned fleet | Nothing |

T1 and T2 are the detector's high-confidence tiers. The third HIGH row is the detector's T3 `concurrent_edit` narrowed from 48 hours to 60 minutes; the detector itself rates T3 low, and on the snapshot exact shared-file pairs within 60 minutes numbered zero. The replay harness must show its precision separately before `COORDINATION_ADVISOR_RELAY_DM_ENABLED` is enabled for it, and it drops to LOW if the replay does not support it.

## Dedupe and rate limits

- **Deterministic id.** The advisory id is the finding id: `findingId(workspaceId, kind, pair, artifact)`, the FNV-1a `stableHex` already used by `outcome-facts`. It is module-private today; the worker PR exports it unchanged rather than copying it. LOW findings come from the pre-filter, not the detector, and use the same function with their own `kind` and artifact.
- **Per-pair cooldown:** `COORDINATION_ADVISOR_PAIR_COOLDOWN_SECONDS`, default `21600` (6 hours). No new advisory for the same pair inside it, whatever the artifact.
- **Per-session cap:** `COORDINATION_ADVISOR_SESSION_HOURLY_CAP`, default `3` advisories per recipient per hour. Excess findings stay unwritten and are counted in the scan log.
- **Never resend an acked finding.** A finding whose row is `acked` is never re-inserted or redelivered, even after the cooldown.
- **HIGH is never downgraded.** Once a finding has been sent as HIGH, a later scan that would rate it LOW leaves the row as it is.

The cooldown and the cap apply to both urgencies. HIGH is ordered first when the cap would cut a scan's output.

## Storage

Table `sessions.coordination_advisories`, one row per finding per recipient:

| Column | Type | Notes |
|---|---|---|
| `advisory_id` | `text` | The finding id |
| `org_id`, `workspace_id` | `text` | Tenant, from the scan |
| `harness`, `session_id` | `text` | The recipient |
| `peer_harness`, `peer_session_id` | `text` | The other session |
| `urgency` | `text` | `check (urgency in ('high','low'))` |
| `kind` | `text` | A `FindingKind` |
| `artifact` | `text` | A path, migration name or commit sha |
| `evidence` | `jsonb` | Bounded: at most 10 `{sessionId, eventId, ts, kind}` entries |
| `created_at`, `delivered_at`, `acked_at` | `timestamptz` | `delivered_at` and `acked_at` nullable |
| `status` | `text` | `check (status in ('pending','delivered','acked','expired'))` |

- **Key.** Both agents in a pair receive the same finding, so the primary key is `(advisory_id, harness, session_id)`: `advisory_id` stays the finding id, and the recipient makes the row unique. The ack route names the recipient for the same reason.
- **Content is scrub-bounded like `short_summary`:** no transcript content. `artifact` and `evidence` hold only paths, migration names, commit shas, session and event ids, and timestamps, with a hard `char_length` bound on `artifact`.
- **Retention is 7 days.** A sweep marks unacked rows older than that `expired`, and then deletes them.
- **Migration:** the migration slot is assigned from the registry at implementation time. The migration adds the table, its indexes and the readers, and nothing else.
- **Readers.** All reads and writes go through SECURITY DEFINER functions in the sessions-function boundary style: tenant-filtered, PUBLIC EXECUTE revoked, and granted only to the worker and read-API roles. Neither runtime role gets direct table privileges.

## Delivery contract

This is the contract the desktop hook builds against.

### Pull: list pending advisories

```
GET /v1/coordination/advisories?workspace=<id>&source=<harness>&sessionId=<id>&status=pending
```

- **Gating.** If `READ_API_COORDINATION_ADVISORIES_ENABLED` is not `true`, the route answers `501 {"error":"advisories_disabled"}`. (The overlaps route answers 404 when disabled; this route follows the read API's `501` convention for a route that is not configured.)
- **Auth, as the overlaps route does.** The workspace comes from the verified identity: `attestWorkspace` requires `workspace` exactly once and equal to the token's workspace. Then `requireCanary(identity, "coordination")` and `rejectUnsupported(c)`.
- **Parameters.** `source` and `sessionId` are each required exactly once. `status` defaults to `pending`, and `pending` is the only value served in v1.
- **Contract:** `coordination-advisories/1`, returned as the response's `contract` field, as overlaps returns `coordination-overlaps/1`.
- **Response.** At most 10 advisories, oldest first, and `nextCursor` is always `null` in v1:

```json
{
  "contract": "coordination-advisories/1",
  "advisories": [
    {
      "advisoryId": "9f3c2a71d04be6e5",
      "urgency": "low",
      "kind": "concurrent_edit",
      "artifact": "src/session-read-api",
      "peer": { "source": "codex", "sessionId": "<peer session id>" },
      "createdAt": "2026-11-12T10:04:00.000Z",
      "message": "<templated, at most 600 characters, no transcript text>",
      "evidence": [{ "sessionId": "<id>", "eventId": "<id>", "ts": "2026-11-12T09:58:12.000Z", "kind": "tool_result" }]
    }
  ],
  "nextCursor": null
}
```

The GET is read-only, like every read-API route on the read pool. `status=pending` means not yet acked: it returns rows whose status is `pending` or `delivered`, so a row the desktop has not acked stays visible until it expires, and a crashed turn sees it again.

### Ack

```
POST /v1/coordination/advisories/:advisoryId/ack
{"source": "<harness>", "sessionId": "<id>"}
```

- **Success:** `204`. It sets `acked_at`, sets `delivered_at` if the DM path had not, and moves the row to `acked`. It is idempotent: acking an acked row is also `204`. This is the only write the read API makes, through one SECURITY DEFINER writer.
- **Not the recipient:** `404` if no row matches `advisoryId` for that source, session and the token's workspace. The route never discloses whether the id exists for someone else.
- **Trust boundary.** The token is workspace-scoped, not session-scoped. `source` and `sessionId` select the recipient's rows but do not prove the caller is that session, so any member of the workspace can read or ack them. That matches the overlaps route, which already shows workspace-wide findings. Nothing crosses a workspace, and an ack can hide an advisory but never disclose one.
- **Same gates as the GET.** The read API's CORS preflight allows `GET` only today, so this PR also adds `POST` for this route.

### Push: HIGH by Relay DM

For HIGH, the worker also sends an Agent Relay DM to **both** agents from the service identity `relayhistory-coordination`. The relay sender credential is provisioned by Khaliq. Without it, the worker refuses to send and leaves the row `pending`, so the pull path still delivers it. A sent DM sets `delivered_at`; the row stays visible to the pull path until it is acked.

Message template (HIGH):

```
relayhistory-coordination: possible duplicate work.
Your session <harness>:<sessionId> and <peer_harness>:<peer_sessionId> both <verb> <artifact>
(<kind>, first seen <ts>). Please check with the other agent before continuing.
Advisory <advisoryId>; ack it at the end of your turn.
```

`<verb>` is fixed per kind: "wrote migration" for T1, "reverted or were reverted on" for T2, and "wrote" for the 60-minute shared file. The LOW template uses the same shape with "may be working near" and no request to stop.

**Sender identity rule.** Every message names the pipeline as its sender, cites both sessions by harness and id, and never quotes transcript text. Its only variable content is ids, timestamps, kinds and artifacts.

## Flags and gates

Every new behaviour is default off:

| Flag | Default | Turns on |
|---|---|---|
| `COORDINATION_ADVISOR_ENABLED` | off | The worker's scan and its writes |
| `READ_API_COORDINATION_ADVISORIES_ENABLED` | off | The GET and ack routes |
| `COORDINATION_ADVISOR_RELAY_DM_ENABLED` | off | HIGH Relay DMs (also needs the sender credential) |

Enabling any of them in production, provisioning the relay credential, and deploying the worker each need Khaliq's own explicit OK. Merging this document or the implementation PRs enables nothing.

## Open questions for Khaliq

1. **Should LOW advisories exist at all?** The evidence says planned parallelism dominates broad signals, and LOW is exactly that class of signal.
2. **Is the relay sender a new service identity** (`relayhistory-coordination`) or an existing one?
3. **Is 7 days the right retention** for advisory rows?

## Implementation plan

Three PRs, each with tests and each doc-linked to this proposal:

1. **Migration and readers.** The table, its checks and indexes, and the SECURITY DEFINER readers and writers, with role-grant, tenant-isolation, bound and retention tests. The slot comes from the registry.
2. **Worker.** `SERVICE=coordination-advisor`: candidate selection, fleet exclusion, the rubric, dedupe, cooldown, the cap, the no-downgrade rule, and the DM sender that refuses to run without its credential. Pure tests cover the rubric and limits table-driven.
3. **Read-API routes.** The GET and ack routes, with tests for the disabled `501`, workspace attestation, canary, unsupported params, not-recipient `404`, idempotent ack and the 10-row bound.

Before any production enablement, a **replay harness** runs the worker's scan over the frozen snapshot process in `docs/coordination-overlap-detection.md`: harvest, ground truth from outcomes, and blind adjudication. It steps a simulated clock through the window, records every advisory it would have sent and when, and reports precision and lead time per urgency row against the labeled incidents and hard negatives. Those numbers, not this document, decide whether any flag is proposed for enablement.
