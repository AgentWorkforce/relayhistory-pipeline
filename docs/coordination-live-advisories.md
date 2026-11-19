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

The cooldown and the cap apply to both urgencies. HIGH is ordered first when the cap would cut a scan's output. The cooldown is kept per urgency: a HIGH advisory for a pair starts the HIGH cooldown and a LOW one the LOW cooldown. A HIGH waits only on the HIGH cooldown, so a LOW advisory never holds back a later HIGH for the same pair; a LOW waits on either. (A LOW and a HIGH finding have different ids, because the id includes the kind, so the upsert's LOW-to-HIGH upgrade cannot cover this case.)

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

- **Gating.** If `READ_API_COORDINATION_ADVISORIES_ENABLED` is not `true`, the route answers `501` with the read API's standard envelope: `{"error":{"code":"unsupported_by_backend","message":"not served by this backend: advisories_disabled","reason":"advisories_disabled"},"correlationId":"<id>"}`, logged with reason `advisories_disabled`. This is the same code, `unsupported_by_backend`, the path answered before the route existed (that answer carried reason `route`, message "route not served by this backend"), so clients that fall back on the code keep working. (The overlaps route answers 404 when disabled; this route follows the read API's `501` convention for a route that is not configured.)
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

M3-1 also requires an enabled global **and** workspace `advisor` control row
before a scan writes, and both `dm` rows before it sends. Missing rows or a
failed control read mean off. These controls supplement the flags below;
see [session intelligence controls](session-intel-controls.md) for operator
SQL and the next-tick stop contract.

| Flag | Default | Turns on |
|---|---|---|
| `COORDINATION_ADVISOR_ENABLED` | off | The worker's scan and its writes |
| `READ_API_COORDINATION_ADVISORIES_ENABLED` | off | The GET and ack routes |
| `COORDINATION_ADVISOR_RELAY_DM_ENABLED` | off | HIGH Relay DMs (also needs the sender credential) |
| `SESSION_INTEL_CONFLICTS_ENABLED` | off | The conflict rows (see "Conflict rows"); also needs the workspace's `conflicts` control |

Enabling any of them in production, provisioning the relay credential, and deploying the worker each need Khaliq's own explicit OK. Merging this document or the implementation PRs enables nothing.

## Conflict rows (M2-4)

With `SESSION_INTEL_CONFLICTS_ENABLED` exactly `"true"` **and** the M3-1 `conflicts` control on (global and workspace), each workspace scan also runs the conflict detectors of [session-intel-conflicts.md](session-intel-conflicts.md). With either off, the scan is byte-identical to the scan without M2-4: no claims query, no conflict code, the same stats keys. A recorded golden of every store, backend and sender call proves it (`src/coordination-advisor/__tests__/conflicts-gate.test.ts`).

**The claims read.** One read per scanned workspace: `sessions.relayfacts_claims_v1(org, workspace, NULL, NULL, since, 500)` (062150). `since` is the candidate window's start minus the facts' 7-day lookback, compared with each claim's `updated_at`. A claim gains evidence and a new `updated_at` each time a session restates it.

Claims not updated within the window (7 days plus 2 hours, by `updated_at`) drop out of the live read. A long-lived constraint must be restated, or captured by the M1-4 policy pack.

The read is **fail-open for the advisor**: any of these skips conflicts for the workspace with a counted warning (`conflictsSkipped`), and the collision and overlap rows go on:

- a failed read, including `42883` when 062150 is not applied;
- a full read (500 rows, so it may be cut);
- truncated facts;
- any error in the conflict code.

**Live inputs, shared logic.** The candidate lanes, the facts the scan already read, and the claims are mapped onto the replay harness's as-of sessions (`src/coordination-advisor/conflicts.ts`). The decisions come from the same `candidates.ts`, `classify.ts` and rule judge as the replay detectors. There is no second copy of the logic. A claim is attached to every session its evidence names; its time is `observed_at`, else `created_at`. Only the rule judge runs live. The model judge is not wired: no transport, model or budget is approved.

**Rows.**

| Decision | Kind | Urgency | Rows |
|---|---|---|---|
| A propagating constraint broken by an action of the other session | `constraint_violation` | HIGH | The violator and the constraint's holder, each naming the other; DMed (HIGH) |
| The same, by a stated intent only | `constraint_violation` | LOW | The same two rows, pull only |
| A decision or intent against one of the other session, with the acting side writing in the topic | `intent_conflict` | HIGH | Both sessions; DMed |
| The same without the action | `intent_conflict` | LOW | Both sessions, pull only |
| Compatible claims on a shared topic | `overlap` | LOW | Both sessions, pull only; dropped when the advisor already has a row for the pair |
| A conflict inside a planned fleet | the conflict kind | LOW | Only the orchestrating sessions, pull only, naming the acting worker as the peer; never the workers, never a DM |
| An overlap inside a planned fleet | | SILENT | none |

A `repo`-policy owner with no holding session gets no row and no DM. Until M2-4's follow-up defines "repo owner", no recipient is invented for it.

- **Kind.** 062000 bounds `kind` by format only, so no migration is needed for the new kinds.
- **Artifact.** The topic slug.
- **Evidence.** The cited claim ids (`kind` `claim_constraint`, `claim_intent` or `claim_decision`) and the action event, within the existing bounds.
- **Id.** 32 hex digits, hashed from the workspace, kind, pair and deciding claims. A constraint violation is keyed by its constraint, so its LOW (an intent) and its HIGH (a write) are one advisory, and the upsert upgrades LOW to HIGH.
- **Limits.** The cooldown, the cap and the no-downgrade rule apply unchanged.
- **Precedence.** A conflict never suppresses a collision: a pair with both in one tick gets both rows. With conflicts on, `deliver` checks each advisory's cooldown against the pair state the tick started with, plus what its own source (the advisor, or the conflict detector) touched that tick. So a collision and a conflict for one pair both go through, and within a source the check is exactly as before. Every write touches the pair as before, so across ticks the cooldown is unchanged. With conflicts off, the live state is used, as before. The per-recipient cap still applies, HIGH first. A conflict does suppress the pair's LOW overlap rows that tick.
- **Shared claims.** A stored claim whose evidence names both sessions is agreement. It is left out of both sides' judge input (`shared`), so it can never be cited.

**DMs.** HIGH conflict rows use the existing DM path, behind `COORDINATION_ADVISOR_RELAY_DM_ENABLED` and the `dm` control. The template names the class:
- "possible constraint violation", "may be acting against a constraint on";
- "possible conflicting plans", "have conflicting claims about";
- "possible nearby work".

It may add the cited claim ids and claim excerpts while the message stays within 600 characters. An excerpt is at most 200 characters and is included only when the scrubber leaves it unchanged (the decision-records rule); otherwise the message carries ids only.

Excerpts are scrub-clean but untrusted peer text. The template marks each one as quoted: `Quoted <kind> claim: "..."`. The read API's `message` for the same row has neither extra. The sender itself (`relay-sender.ts`) is unchanged. Its Relaycast contract is M3-8 (docs/session-intel-dm-transport.md): an advisory counts as delivered when its state is in the set {`delivered`, `consumed`, `replied`} or `accepted_at` is set, never by an ordered chain. The tests' fake sender models exactly that.

## Open questions for Khaliq

1. **Should LOW advisories exist at all?** The evidence says planned parallelism dominates broad signals, and LOW is exactly that class of signal.
2. **Is the relay sender a new service identity** (`relayhistory-coordination`) or an existing one?
3. **Is 7 days the right retention** for advisory rows?

## Implementation plan

Three PRs, each with tests and each doc-linked to this proposal (a fourth, the worker's readers, was added during implementation):

1. **Migration and readers.** The table, its checks and indexes, and the SECURITY DEFINER readers and writers, with role-grant, tenant-isolation, bound and retention tests. The slot comes from the registry.

   **Implemented: PR 1.** Slot `20261105062000` (`supabase/migrations/20261105062000_coordination_advisories.sql`, rollback in `supabase/rollbacks/`). It adds the table and five SECURITY DEFINER functions, with PUBLIC EXECUTE revoked and no table privileges for either runtime role. The worker role `service_role` gets `sessions.coordination_advisories_upsert_v1`, `sessions.coordination_advisories_mark_delivered_v1` and `sessions.coordination_advisories_sweep_v1`. The read-API role `bulls96_read_api` gets `sessions.coordination_advisories_pending_for_v1` and `sessions.coordination_advisories_ack_v1`, which also require the read-API scope to match the requested org. Retention is a fixed 7 days in the functions. `bulls96_read_api` defaults to read-only transactions, so the PR 3 ack route must open a `READ WRITE` transaction. This enables nothing: no worker, no routes and no flags.
2. **Worker.** `SERVICE=coordination-advisor`: candidate selection, fleet exclusion, the rubric, dedupe, cooldown, the cap, the no-downgrade rule, and the DM sender that refuses to run without its credential. Pure tests cover the rubric and limits table-driven.

   **Implemented: PR 2.** `SERVICE=coordination-advisor` (`src/coordination-advisor/`), off unless `COORDINATION_ADVISOR_ENABLED` is exactly `true`. Every `COORDINATION_ADVISOR_INTERVAL_SECONDS` (default `120`) it reads live directory sessions with a project, active in the last 2 hours, from the workspaces in `READ_API_DIRECTORY_WORKSPACES` (the routes' canary allowlist, which must also be set on the worker: unset or empty means no workspace is scanned, `*` means every workspace). It reads at most 200 per workspace and 2000 overall, newest first. It then reads the facts (`COORDINATION_ADVISOR_FACTS_BACKEND`: ClickHouse by default, or `postgres` through the PR 4 readers), applies the rubric, the fleet exclusion and the limits, and writes through `upsert_v1`. Before the write, an artifact is clipped to the column's 512 characters and evidence entries are dropped from the end until the stored text fits 4096. A row the database still refuses (a constraint violation, class 23, or an over-long value or untranslatable text, 22001 or 22P05) is logged and counted as `rejected`, and the scan goes on with the next row. Any other error, including the upsert's own `22023` argument checks, fails the workspace for that tick. The HIGH DM sender is off unless `COORDINATION_ADVISOR_RELAY_DM_ENABLED` is exactly `true`, and then it needs `COORDINATION_ADVISOR_RELAY_TOKEN` and `COORDINATION_ADVISOR_RELAY_URL`. The URL must be https (http only on loopback) with no credentials, query or fragment in it (the token has its own variable), and a bad value refuses to start without echoing it. The DM body is `{from, orgId, workspaceId, to: {harness, sessionId}, text, advisoryId}`, so the relay can route by tenant. The sender never follows a redirect (`redirect: "error"`) and discards the response body. A DM is marked delivered only after the send succeeds. The relay endpoint contract has not been verified against a live relay.
3. **Read-API routes.** The GET and ack routes, with tests for the disabled `501`, workspace attestation, canary, unsupported params, not-recipient `404`, idempotent ack and the 10-row bound.

   **Implemented: PR 3.** `GET /v1/coordination/advisories` and `POST /v1/coordination/advisories/:advisoryId/ack`, off unless `READ_API_COORDINATION_ADVISORIES_ENABLED` is exactly `true`. Off, both answer the standard `501` envelope above. The GET is served by the self-host service, the hosted Lambda and the read Worker. The ack runs in one `READ WRITE` transaction and is served by the self-host service only (exactly `true`): the Lambda URL answers `405` and the read Worker is GET-only. CORS adds `POST` for the ack path only while the flag is on.
4. **Worker readers and durable limits** (added during implementation).

   **Implemented: PR 4.** Slot `20261105062070` (`supabase/migrations/20261105062070_coordination_advisor_readers.sql`, rollback in `supabase/rollbacks/`). It adds four SECURITY DEFINER functions granted to `service_role` only, and no table, index, column or backfill. `coordination_advisor_{write,output,usage}_facts_v1` mirror the `047000` and `045040` readers for the worker, taking the tenant explicitly instead of the read-API scope. `coordination_advisories_recent_v1` returns, per tenant, the latest advisory time per (recipient, peer, urgency) and each recipient's count in the last hour. With 062070 applied, the worker seeds its cooldown and cap from it before each workspace writes, so a restart keeps both. Without it (`42883`), the worker keeps in-memory limits with a warning, and a restart resets them. Any other error fails that workspace with nothing written.

   Known limits: `recent_v1` counts rows by `created_at`, so a LOW-to-HIGH upgrade and a cooldown touch with no write are not durable; the durable cap is rounded up to the latest send time; the limits are not shared across replicas (run one).

   **Follow-up operator step (Khaliq-gated).** The candidate read filters `session_directory` on `last_activity_at`, and no index leads with it, so on a large directory the scan is a sequential scan every interval. The per-workspace cap keeps the result bounded but not the read. An index on `session_directory (last_activity_at)` (partial on `deleted_at IS NULL AND project_id IS NOT NULL`) is not in 062070: the migrator is transactional, so it must be built with `CREATE INDEX CONCURRENTLY` outside the migrator, as for `047000`, and only with Khaliq's explicit OK on a real database.

Before any production enablement, a **replay harness** runs the worker's scan over the frozen snapshot process in `docs/coordination-overlap-detection.md`: harvest, ground truth from outcomes, and blind adjudication. It steps a simulated clock through the window, records every advisory it would have sent and when, and reports precision and lead time per urgency row against the labeled incidents and hard negatives. Those numbers, not this document, decide whether any flag is proposed for enablement.

The harness is [session-intel-replay.md](session-intel-replay.md).
