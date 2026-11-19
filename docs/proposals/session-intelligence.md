# Session intelligence: conflict alerts, not just overlap

Proposal, 2026-11-18. Owner: sessions-intel-lead. Requested by Khaliq Gant, from Will Washburn's ask.
Reviews and landing: layered-sessions-pipeline. Board: sessionspipelinegrok (Roma).
Design only. Nothing is applied, deployed or turned on from this document.

## The ask

Khaliq's original request (Claude session, 2026-11-12 21:42Z, found by codex-relayhistory-pipeline-session)
was a continuous pipeline job that looks for overlapping or adjacent work and sends proactive messages
at two urgencies:

1. a peer message;
2. a message inserted at the natural end of a turn.

On 2026-11-13 he moved detection away from the SessionStart teammate-context hook, which asked agents
to read their teammates' summaries, to the pipeline. That changes where detection happens. It does not
change the turn-end delivery of advisories the pipeline has already classified.

> We need to get the sessions pipeline working: real-time analysis and summaries of chats, with
> alerts that distinguish conflicting intents and constraints from overlapping work. ... a strong
> path into enterprise: running analysis on their existing session traces.

So we are building four things:

1. **Near-real-time summaries and claims** for live sessions. Claims are the intents, constraints and
   decisions a session holds.
2. **Alerts that separate four situations** that look alike when the only signal is shared files.
3. **A batch path** that runs the same analysis over a customer's existing traces and produces a
   report of what would have been caught, and when.
4. **A measured eval**, so every alert class proves its precision before anyone proposes a flag for it.

Khaliq's direction (2026-11-18): this is top priority, and it "should be default on in our cloud too.
Make sure it doesn't break anything." Self-host stays default off. See
[Hosted default-on plan](#hosted-default-on-plan).

## What exists, and what failed

Read [coordination-overlap-detection.md](../coordination-overlap-detection.md) and
[coordination-live-advisories.md](../coordination-live-advisories.md) first. In short:

| Piece | State |
|---|---|
| `outcome-facts` v1 (T1 duplicate migration, T2 superseded commit, T3 concurrent edit) and `GET /v1/coordination/overlaps` | Shipped. Retrospective. Found 0 of 7 dev incidents on session evidence alone |
| Live advisor `SERVICE=coordination-advisor`, the advisories table (062000, 062070), and the GET and ack routes | Code only, all flags off. The replay harness that must measure it first was never built |
| Bob Costas summaries, summary regen on turns or idle (`SUMMARY_REGEN_TURNS_ENABLED`), and short-summary search | Code exists, flag-gated |
| Session analysis (`SESSION_ANALYSIS_ENABLED`), which already emits structured `intents`, `constraints` and `decisions` as evidence-cited claims (`src/session-analysis/engine/session-analysis-result.ts`) | Runs on request, not live |
| Relayfacts decision records (`SUMMARY_DECISION_RECORDS_ENABLED`, 062080) | Code only. Records decisions only, not intents or constraints. `supersede_v1` has no caller |

**The negative results, stated plainly.** On a frozen snapshot of 5,008 sessions:

- No approach warned before harm at a usable alert budget.
- The model judge (P5 C) found 0 of 4 held-out incidents.
- The Relay Chief classifier labelled all 65 pairs "overlap". All 24 planned pairs scored at or above
  its 0.80 gate.
- Planned fleets made up 50 of the first 60 alerts.

Two lessons follow. Overlap, defined as touching the same thing, is mostly benign. And a model asked
"do these two transcripts collide?" cannot tell benign overlap from harmful collision.

**What is different this time, and why it might work:**

1. **We judge structured claims, not raw transcripts.** The analysis engine already extracts
   intents, constraints and decisions, each with cited evidence. A conflict is a relation between two
   specific claims ("A decided X; B intends not-X"). It is not a property of two 50k-token transcripts.
   The classifier must cite one claim id from each side, or it returns "no conflict".
2. **The target class changed.** P1 to P6 hunted overlap. Overlap becomes a low-urgency,
   informational class, and alerting is reserved for *contradiction*. A planned fleet can overlap
   freely; it is still worth knowing when two of its workers contradict a decision.
3. **Candidates are few and local.** P5 ranked about 300,000 candidate pairs. Live candidates are
   pairs of sessions active in the same project within 2 hours, at most dozens per workspace per tick.
   They are further filtered by shared topic before any model call.
4. **Constraints are first-class.** "Never apply migrations to a real DB" in one session, or in
   AGENTS.md, is a rule another session can break. Prior work never modelled rules.

This may fail as well. The plan is built so that failure shows up in the replay numbers (M0, M2), not
in someone's inbox.

## Alert taxonomy

Every pair of sessions that the scan considers ends in exactly one class:

| Class | Definition | Example | Urgency | Delivery |
|---|---|---|---|---|
| `constraint_violation` | B acts, or states an intent to act, against a constraint in force for B's scope. The constraint comes from repository policy (AGENTS.md, CLAUDE.md and the like) or from a *project-scoped* constraint a human gave in session A | A was told "no new migrations until the 062 renumber lands"; B writes `supabase/migrations/…` | HIGH when B has acted (a write, commit or command); LOW when it is only an intent | DM to both B and the constraint's owner (session A, or the repo owner for a policy constraint); pull; dashboard |
| `intent_conflict` | A's current decision or intent and B's current intent or decision cannot both hold, on the same topic | A decided "advisories use 501 when off"; B intends "return 404 when off" in the same route | HIGH when both are current (neither superseded) and B is writing in the topic; else LOW | DM to both; pull; dashboard |
| `collision` | Existing T1 and T2 duplicate or superseded work, and T3 within 60 minutes (`outcome-facts`) | Two roots write migration `062130` | HIGH (existing rubric) | DM to both; pull; dashboard |
| `overlap` | Same project and shared topic or files, with no conflicting claims | Two agents both editing `src/session-read-api` on different routes | LOW, informational | Turn-end pull advisory and dashboard. Never a DM |
| `planned_fleet` | The pair shares an orchestrating parent | A review fleet | SILENT for overlap. A conflict *inside* a fleet goes to the orchestrator only, as LOW | Orchestrator's pull and dashboard |
| `none` | Everything else | | SILENT | |

Rules that make the classes sharp:

- **Supersession beats conflict.** A later claim by the *same human*, or a decision record marked
  superseded, retires the earlier claim. A pair whose conflicting claim is retired is not a conflict.
- **Scope gates constraints.** A session-local constraint ("don't touch my branch") never propagates.
  Only claims classified `project` or `repo` scope do. Scope is part of M1's extraction, and
  ambiguity defaults to `session` (no propagation).
- **Self-conflict is out of scope.** A session breaking its own constraints is a session-quality
  signal, not coordination. It may appear on the dashboard later, but never as an alert.
- **Actions outrank words.** HIGH needs observed action evidence in the topic: a `files_touched`
  write, a commit, or an outcome. An intent alone caps at LOW.
- **Every alert cites.** Each alert carries both claim ids, their evidence ids, and the action event
  id. The message template uses claim text only after it has passed the scrubber unchanged
  (decision-records rule), capped at 200 characters each.

## Inputs

| Input | Source today | Gap closed by |
|---|---|---|
| Short summary timeline | Bob Costas plus regen on turns or idle | Already flag-gated; default-on plan below |
| Intents, constraints and decisions, each with evidence ids | Session analysis `understanding.{intents,constraints,decisions}`, on request only | **M1**: an incremental claims pass on the regen trigger, and claims stored as facts |
| Decision records with supersession | Relayfacts 062080, decisions only | **M1**: add constraint and intent kinds and scope, and call `supersede_v1` |
| Repository policy constraints | Not extracted | **M1**: a constraint pack parsed from the instruction files the session's harness loaded. Enterprise customers can also upload a policy file |
| Actions: writes, commits, outcomes | `session_event_facts` and `session_output_facts` (`files_touched` complete for Codex since #485) | none |
| Fleet structure | Directory `parent_session_ids` | none |
| Relay messages between the agents | Not in facts | Later. `communication` stays `unknown` |

All inputs are scrubbed derived data. No detector reads transcript bodies. The claims pass reads
canonical evidence through the same reader policy as session analysis.

## Latency target

"Real time" here means *before the conflicting work lands*, not sub-second.

| Stage | Target |
|---|---|
| Turn → claims available | p50 ≤ 3 min, p95 ≤ 10 min (regen trigger: 10 user turns, or 600 s idle; M1 adds a *claims* trigger at a lower threshold, default 3 turns, under a per-workspace budget) |
| Claims available → alert written | ≤ one scan interval (120 s) |
| Alert written → DM delivered | ≤ 30 s |
| **End to end, p95** | **≤ 12 min from the turn that creates the conflict** |

The eval measures lead time against harm (merge, revert or duplicate PR), not wall-clock latency, and
that is the number that matters.

## Delivery

- **HIGH → Agent Relay DM** from the pipeline service identity, using the existing sender
  (`relay-sender.ts`) and template rules: ids, kinds, artifacts and scrub-clean claim excerpts only.
  It also stays on the pull path until acked.
  - **The DM transport is not verified.** `relay-sender.ts` was never tested against a live relay.
    Sending is Relaycast's: every sender is a Relaycast agent with its own token, and a DM names the
    recipient agent. Only Cloud can resolve a (harness, session id) to that agent, from Desktop's
    identity sync. Until the contract is confirmed with Relaycast and Cloud, alerts are delivered
    through the pull path only, and a session with no attached agent has an explicit "not attached"
    outcome.
  - **Delivery is at least once.** A DM and the turn-end pull can show the same advisory. M3-7 adds
    the delivery state to the GET response, so the hook can tell them apart.
- **LOW → turn-end advisory** through `GET /v1/coordination/advisories`, which the desktop hook polls
  at the end of each turn, then acks.
- **Dashboard → "Session intelligence" view**, which extends the dashboard's Overlaps tab. It shows
  live alerts by class, each with its two claims and evidence links, plus a daily digest per
  workspace. Adjudication buttons (real / planned / wrong) feed the labelled set.
  - **Live status and progress:** per workspace, each active session's latest short summary, its
    current intents, and its state, built from the live summaries and claims. No transcript text.
  - **Searchable handoffs:** finished or idle sessions, with their summaries, decisions and open
    items, searchable through the existing short-summary search, so another agent or a person can
    pick up the work.
- **Limits.** The existing cooldown (6 h per pair per urgency), the cap of 3 per recipient per hour,
  and never resending an acked alert all apply to every class. New: a cap of 20 DMs per workspace per
  day, and a model budget per workspace per day (see M2).

## Enterprise batch path

The batch path is the same detectors run by the M0 replay driver over imported traces with a
simulated clock. Its output is a report: "these N alerts would have fired, this far before the harm".

1. **Import (M4).** Adapters for Claude Code JSONL, Codex rollouts, and a generic JSONL / OpenTelemetry
   GenAI-span format. They map into canonical sessions through the existing upload path, into an
   isolated tenant (or the customer's own self-host install, for customers who won't ship data).
   Scrubbing runs through relayscrubber at ingest, as for every session. The offline converter
   (M4-1a) never reaches ingest: it keeps no message text, and it hashes every id and each file-path
   segment with a per-import salt. Depth, extension, well-known source roots and migration number
   prefixes stay in clear, so equal paths still match across sessions.
2. **Analyse.** Summaries, the claims pass, and facts projection run as backfill jobs under a
   per-import budget.
3. **Replay.** `session-intel replay` steps the clock over the import window, runs every enabled
   detector, and writes the alerts with their simulated send times.
4. **Adjudicate and report.** Blind adjudication of the top-K alerts (M0 packets), then a customer
   report: confirmed conflicts with citations, lead time, cost of the duplicated or contradicted work,
   and what the alert would have said.

Reading any customer's traces needs Khaliq's explicit OK and the customer's written authorization,
each time.

## Evaluation: the bar before any flag is proposed

The method is the one in [coordination-overlap-detection.md](../coordination-overlap-detection.md),
with new labels for the new classes:

1. **A frozen snapshot**, with a manifest of hashes and a delete-by date. The previous one was deleted
   on 2026-11-17, so a new harvest of our own workspace needs Khaliq's OK (it reads session data).
2. **Ground truth from outcomes and corrections**, never from a detector:
   - duplicate or superseded PRs, reverts, duplicate migrations;
   - *explicit human corrections* ("no, we decided X", "stop, that's not allowed");
   - follow-up sessions that undo another root's change.
3. **A dev/held-out split by workstream.** Each detector is frozen by hash and scored on held-out
   once; the harness enforces this.
4. **Blind adjudication.** Top-K alerts, pooled across detectors, shuffled, with detector names
   stripped. Two adjudicators judge each alert; their agreement is reported.
5. **Habit ablation.** The run is repeated without Relay banners, agent naming conventions or
   AGENTS.md phrasing, so the result generalizes to enterprise traces.

**Bars, per alert row (class × urgency), on held-out data:**

| Metric | HIGH (DM) | LOW (pull) |
|---|---|---|
| Adjudicated precision | ≥ 0.70 | ≥ 0.50 |
| Adjudicated alerts in the sample | ≥ 20 (otherwise "insufficient evidence", and the row stays off) | ≥ 20 |
| Adjudicator agreement | ≥ 0.80 | ≥ 0.80 |
| True alerts sent before harm | ≥ 50%, median lead ≥ 15 min | n/a |
| Alert budget | ≤ 5 per 100 sessions per day | ≤ 15 per 100 sessions per day |
| Planned-fleet share of alerts | ≤ 20% | ≤ 40% |
| Precision drop under habit ablation | ≤ 0.15 | ≤ 0.15 |

A row that misses a bar ships SILENT (dashboard only) or not at all. The live advisor's existing
HIGH rows (T1, T2, T3 at 60 minutes) are held to the same bars. That is the advisor replay the
live-advisories document required and nobody ran.

## Hosted default-on plan

Khaliq's direction is default on in our hosted cloud. "Doesn't break anything" is the bar. Each
enablement ships with all of the following:

- **CI full lane plus pipeline e2e green** on the exact commit deployed.
- **Hard caps:** the existing cooldown and hourly cap, the per-workspace DM cap, and the per-workspace
  model budget.
- **A kill switch with no redeploy (new, M3).** A control row
  (`sessions.session_intel_controls`, one per scope, read through a SECURITY DEFINER getter every
  tick) can turn each feature off for a workspace or globally.
  - A failed read counts as *off* for alerting and delivery.
  - Ingest, summaries and the read API never read it.
- **Fail-open:** an intelligence failure never blocks ingest, summaries or the read API.
  - Each worker is its own service.
  - Read routes answer the standard `501` when off.
  - The claims pass runs after the summary and never in its transaction.
- **A staged rollout:** stage 0 is our own workspace, then all hosted tenants.
  - The advisor and the read routes are already scoped by the `READ_API_DIRECTORY_WORKSPACES`
    allowlist.
  - Summary regen, session analysis and the claims pass have no workspace scope today. For them,
    stage 0 is scoped by the M3-1 control row, which holds a per-feature workspace allowlist and
    denies by default: a workspace not listed, or a failed read, means off.
  - Until M3-1 exists, rows 1 to 5 can only be enabled for all tenants at once, so they wait for
    M3-1.
- **A soak before each widening:** 48 h at stage 0, with metrics checked: alert volume vs budget,
  model spend, worker tick failures, read-API p95 and error rate, ingest lag unchanged, and
  dashboard-adjudicated precision.

**Flags, in enablement order.** Every flag's code default and stack input default stays off, and with
the flag off the code is byte-identical to today. "Default on in hosted" is a per-stage value set in
that stage's gated deploy, not a changed default. Any change to a default in code or in a stack input
lands only with that stage's confirmation; otherwise an unrelated deploy would turn a feature on.
Self-host defaults stay off.

| # | Flag | Turns on | Precondition | Rollback |
|---|---|---|---|---|
| 1 | `SUMMARY_REGEN_TURNS_ENABLED` | Summaries refreshed on turns and idle | The summary model budget is set | Set false. Summaries freeze at the last version |
| 2 | `READ_API_LIST_SHORT_SUMMARY_ENABLED`, `READ_API_SUMMARY_SEARCH_ENABLED` | Live summaries in lists and search | 1 soaked | Set false. Fields are omitted |
| 3 | `SESSION_ANALYSIS_ENABLED` | The analysis runner | Model key present; completeness gate unchanged | Set false. Jobs stay queued |
| 4 | `SUMMARY_DECISION_RECORDS_ENABLED` | Relayfacts writes | 062080, 062090 and 062100 applied, with both erase lanes (per session and per user) working; see [relayfacts.md](../relayfacts.md), Retention. Real-DB migrations: Khaliq OK | Set false. Rows stay; reads unaffected |
| 5 | `SESSION_INTEL_CLAIMS_ENABLED` (new, M1) | The incremental claims pass plus claim facts | Everything row 4 needs, plus the M1 migration (062150) applied, its erase coverage proven, and the claims budget set | Kill switch, then the flag. Claims stay; detectors see none |
| 5b | `SESSION_INTEL_CLAIMS_SCHEDULER_ENABLED` (new, M1-3) | Live claims: scheduled analysis of active sessions | Row 5 soaked; 062180 applied (real-DB migration: Khaliq OK); the M3-1 `claims` control on for the workspace; the budget values set (the hosted daily spend cap is Khaliq's open decision) | Kill switch (`claims` control), then the flag. Scheduled jobs stop; existing claims stay |
| 6 | `COORDINATION_ADVISOR_ENABLED` | Advisor scan and writes (collision and overlap) | 062000 and 062070 applied; M0 replay meets the bars per row; rows that miss are SILENT | Kill switch, then the flag. Rows expire in 7 days |
| 7 | `SESSION_INTEL_CONFLICTS_ENABLED` (new, M2) | Intent and constraint classification in the advisor scan | M2 replay meets the bars | Kill switch. The scan falls back to the collision and overlap rows |
| 8 | `READ_API_COORDINATION_ADVISORIES_ENABLED` | The pull and ack routes (turn-end advisories) | 6 soaked | Set false. Routes answer 501; the desktop hook treats that as "none" |
| 9 | `COORDINATION_ADVISOR_RELAY_DM_ENABLED` | HIGH DMs | The relay sender credential (Khaliq); HIGH rows meet the bars; stage 0 soak of 48 h | Kill switch (DM scope), then the flag. Rows remain on the pull path |
| 10 | Dashboard "Session intelligence" setting | The view in relay-webapp | 8 on | Setting off |

Khaliq's direction covers making these default on in hosted. Each *actual* production flip, deploy,
real-DB migration and credential still needs a fresh confirmation from Khaliq, in his own words, at
the moment it is done. They are batched into one ready-to-flip ask per stage that lists the evidence:
the CI and e2e runs, fail-open tests, caps, the kill-switch test, the rollback, and the replay scores.

## Milestones

Tickets live on the Roma board (sessionspipelinegrok). This list is the source of truth for their
scope. "Gate" names what needs Khaliq's explicit OK.

### M0: Eval harness and advisor replay

| Ticket | Owner | Acceptance | Gate |
|---|---|---|---|
| M0-1 Replay harness: snapshot format and loader (hashes, delete-by refusal), as-of views with no future leak, a detector interface, an advisor adapter driving the real `scanOnce` with a simulated clock, the scorer (per-row precision, recall, lead time, budget), a held-out-once ledger, blind adjudication packets and verdict import, CLI and synthetic fixture | sessions-intel-lead (M0 builder) | Tests: leak, fleet silence, scorer maths, held-out-once, packet stripping, fixture e2e; typecheck; identifier guard; `docs/session-intel-replay.md` | none |
| M0-2 Snapshot harvester for our own workspace, writing the M0-1 format from the shared-session tools and the read API | sessions-intel-lead | Writes outside the repo; manifest; delete-by ≤ 14 days | **Reading session data: Khaliq OK** |
| M0-3 Labelling kit: outcome mining (reverts, duplicate migrations and PRs) and correction mining, with a split by workstream | sessions-intel-lead | Labels file in the M0-1 format; ≥ 30 incidents across the classes, or an honest count | follows M0-2 |
| M0-4 **Advisor replay report**: the existing HIGH and LOW rows scored against the bars | sessions-intel-lead | A report per row: pass, fail or insufficient | follows M0-3 |
| M0-5 Class-aware scoring: map detector kinds, incident kinds and verdicts onto the taxonomy classes; score in pair-match and class-match modes; rows per class × urgency, which are the rows the bars apply to; a confusion table; the class map's hash recorded in reports and the held-out ledger | sessions-intel-lead (M0 builder) | Tests: mapping, misclassification, confusion counts, benign overlap and planned fleet as correct classes, verdict agreement on class sets | none |

### M1: Intent and constraint extraction into facts

| Ticket | Owner | Acceptance | Gate |
|---|---|---|---|
| M1-1 Claims schema: extend relayfacts with `kind` (`decision`, `intent`, `constraint`), `scope` (`session`, `project`, `repo`) and `observed_at`, behind SECURITY DEFINER functions, in slot 062150 (assigned by the lander) | M1 builder | Migration plus rollback, role and tenant tests, pg-proof 15/16/17. The erase lanes reach the new kinds and the v2 writer: the 062090 per-session erase with its RH016 late-insert fence, and the 062100 per-user erase. `relayfacts_list_v1`, `GET /v1/relayfacts`, the relayfacts MCP tools and the GitHub push module keep returning decisions only. The down file states what happens to intent and constraint rows | **Applying to a real DB: Khaliq OK** |
| M1-2 Extractor: map analysis `intents`, `constraints` and `decisions` into claims, with scope classification (default `session`), scrub-clean only (existing rule), and supersession via `supersede_v1` when the same human revises a claim | M1 builder | Table-driven tests including scope and supersession; no transcript text stored beyond scrub-clean claim text | none |
| M1-3 Incremental claims pass: triggered on the regen cadence at a lower threshold, a delta-only prompt, a per-workspace hourly token budget, fail-open from summaries. It and the M1-2 extractor read the M3-1 `claims` control (global AND workspace, fail-closed) before any model call or write; until that is wired, the claims kill switch is **not enforced** and only the env flags gate extraction | M1 builder | Latency p95 ≤ 10 min on the replayed snapshot; budget tests; a failure never fails a summary | none |
| M1-4 Repository policy constraint pack: parse the instruction files seen in session evidence into `repo`-scope constraints; customer policy upload format | M1 builder | Parser tests over fixture AGENTS.md files | none |
| M1-5 Claims in the snapshot: the harvester and loader carry the claim timeline, so detectors replay claims as of their time | M0 builder | Leak test covers claims | none |

### M2: Conflict classifier vs overlap

| Ticket | Owner | Acceptance | Gate |
|---|---|---|---|
| M2-1 Candidate generator: same project, shared topic (relayfacts topic slug or a shared top-level directory), not superseded, with fleet routing | sessions-intel-lead | ≤ 50 candidate pairs per workspace per tick on the snapshot | none |
| M2-2 Pair classifier over structured claims only: a closed-choice output (`constraint_violation`, `intent_conflict`, `overlap`, `none`) that must cite a claim id from each side, deterministic vetoes (supersession, scope, self-conflict), and a per-workspace daily model budget | sessions-intel-lead | Detector registered in M0; dev-tuned, frozen by hash | none |
| M2-3 Held-out replay and adjudication of M2 against the bars, with habit ablation | sessions-intel-lead plus 2 adjudicator agents | Report per row; any row that fails ships SILENT | none |
| M2-4 Wire into `SERVICE=coordination-advisor` behind `SESSION_INTEL_CONFLICTS_ENABLED`, with the new kinds in the advisories rubric and templates | sessions-intel-lead | Advisor tests; off by default until the hosted plan's step 7 | none |

### M3: Real-time delivery and safety rails

| Ticket | Owner | Acceptance | Gate |
|---|---|---|---|
| M3-1 Kill-switch control row and getter (global, workspace, and per-feature: claims, conflicts, DM), with a per-feature workspace allowlist that denies by default; fail-closed for alerting; slot 062160 | codex-relayhistory-pipeline-session | Migration and tests; the e2e proves the flip takes effect within one tick | **Real-DB migration: Khaliq OK** |
| M3-2 Per-workspace DM cap and model budget, durable | sessions-intel-lead | Tests; metrics emitted | none |
| M3-3 Desktop turn-end hook consumes `GET /v1/coordination/advisories` and acks | relay-app's agent (Khaliq, 2026-11-19); sessions-intel-lead briefs it with the hook contract | Hook treats 501 as none; never blocks a turn; sends `workspace` on the ack. **External dependency:** the hook exists on relay-app `review/teammate-context-all-advisories` and waits on relay PR #2166 (TurnEndAdvisories) | none |
| M3-4 Dashboard "Session intelligence" view: alerts with adjudication buttons, live status and progress per workspace, and searchable handoffs. M3-4a is the read route ([session-intel-workspace-api.md](../session-intel-workspace-api.md), slot 062170); M3-4b is the view in relay-web | sessions-intel-lead (Khaliq, 2026-11-19) | Behind a setting; no transcript text | none |
| M3-5 Pipeline e2e lane covering claims, then a conflict alert, then pull and ack, and the kill switch | codex-relayhistory-pipeline-session | `e2e/pipeline` step green in the CI full lane | none |
| M3-7 Delivery state on the pull path: an additive `delivery {dm, deliveredAt}` on each GET item; the hook deduplicates by advisory id across restarts | sessions-intel-lead (route), relay-app's agent (hook) | Route and hook tests; at-least-once is documented until both ship | none |
| M3-8 DM transport contract ([session-intel-dm-transport.md](../session-intel-dm-transport.md)): register a Relaycast service identity, resolve a session to its agent through Cloud, and define the "not attached" outcome | sessions-intel-lead, with layered-relaycast; the Cloud owner is Khaliq's call | Contract written and tested against a fake; no real token | **Service identity and token: Khaliq OK** |
| M3-9 Desktop notification to the human for HIGH alerts: a ranked, dismissable notice in Agent Relay Desktop, fed by the same advisories (distinct from the web dashboard) | relay-app's agent, with sessions-intel-lead for the feed | Ranked HIGH first; never blocks a turn; respects the kill switch. **Scope to be confirmed with Khaliq**: his original wording about a human alert has not been found yet | none |
| M3-6 Stage 0 ready-to-flip packet (hosted plan steps 1 to 9 for our workspace) | sessions-intel-lead | Evidence list complete | **Each flip and deploy, and the DM credential: Khaliq OK** |

### M4: Enterprise trace import

| Ticket | Owner | Acceptance | Gate |
|---|---|---|---|
| M4-1a Offline converter: Claude Code JSONL, Codex rollouts and generic JSONL into the replay snapshot format | M4 builder | Synthetic fixtures; no message text; salted HMAC of ids and of each path segment (depth, extension, source roots and migration number prefixes in clear); salt from stdin or a 0600 file, never argv; replay runs unchanged on the output | none |
| M4-1b Upload-path import: the same adapters into canonical sessions through the upload path, plus OTel GenAI spans | M4 builder | Scrubbing via relayscrubber; idempotent re-import | none |
| M4-2 Batch runner: backfill the analysis and claims for an imported tenant under budget, then replay, then report | M4 builder | Runs on a synthetic import end to end | none |
| M4-3 Customer report template (confirmed conflicts, lead time, cost) | sessions-intel-lead | Rendered from the M0 report JSON | none |
| M4-4 First real customer run | sessions-intel-lead | | **Customer data: Khaliq OK and customer authorization** |

### M5: Demo for Will

| Ticket | Owner | Acceptance | Gate |
|---|---|---|---|
| M5-1 Scripted live demo: two agents in our workspace, one given a project constraint, the other told to break it, then the DM and the dashboard entry within the latency target | sessions-intel-lead | A recorded run with timings | stage 0 enabled (M3-6) |
| M5-2 Batch demo: the M4 report over a synthetic "enterprise" import, plus our own snapshot's replay numbers, honestly stated | sessions-intel-lead | A shareable page; no production identifiers | none |

## Decisions and open questions

Khaliq answered the first open questions on 2026-11-19, in his own words, to layered-sessions-pipeline:

1. **Owners.** relay-app's agent builds the desktop turn-end hook (M3-3). sessions-intel-lead builds
   the dashboard view (M3-4).
2. **Constraint violations DM both sessions**: the violator and the constraint's owner, the same as a
   conflict.
3. **Still open: model and daily spend cap** for the claims pass and the classifier in hosted. The
   proposed default is the same model as summaries, with the cap set from the replay's measured cost.
   No model, provider key or budget is approved yet.
4. **M0-2 approved**, scoped to our own workspace's existing session data:
   - the snapshot is written outside the repository, with a manifest of hashes and a delete-by date at
     most 14 days out;
   - nothing from it reaches the public repository: no transcript text, ids or names;
   - it does not cover any customer workspace or the M4 import.

Hosted default-on for any `SESSION_INTEL_*` flag is not approved. Code lands default-off.
