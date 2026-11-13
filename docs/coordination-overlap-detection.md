# Detecting overlapping agent work

Decision record, 2026-11-10. Owner: fab-pipe, with final approval delegated by Khaliq Gant.

## The question

Where did two or more agent sessions duplicate or collide with each other's work, at a point where a message between them (Agent Relay) would have helped? And can the sessions pipeline find these by itself, for any customer's data?

## Decision

The pipeline ships one detector, `outcome-facts` v1. It is deterministic, cites its evidence and is **retrospective**: it reports work that was already duplicated. It does not predict overlap.

| Tier | Signal | Kind | Confidence |
|---|---|---|---|
| T1 | The same migration number or filename written by two different session roots in one repo | `duplicate_work` | high |
| T2 | One root's reverted outcome is a commit another root made earlier | `superseded_work` | high |
| T3 | The same file written by two roots on different branches within 48 hours, with hot paths ignored and rarity damping | `concurrent_edit` | low |

- **Planned fleets sink.** Sessions that share an orchestrating parent (the session directory's `parent_session_ids`) drop to the bottom of the list at low confidence.
- **Communication is reported as `unknown`.** Production facts hold no evidence of agent-to-agent messages.
- **Where it runs:** `GET /v1/coordination/overlaps` on the session read API (contract `coordination-overlaps/1`), computed on request from existing facts.
  - It needs no new workload, secret or model. Self-host installs add migration
    `20261105047000_coordination_fact_readers.sql`: two filtered SECURITY DEFINER readers and two
    partial indexes, with no table, backfill or stored finding.
  - On a populated facts table, the transactional migrator refuses a blocking index build. Operators
    prebuild the exact packet below with `CREATE INDEX CONCURRENTLY`, then the migration adopts it;
    `HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true` is the explicit non-production opt-in.
  - It is served only when `READ_API_COORDINATION_OVERLAPS_ENABLED=true`. The hosted read API ships with it on (Khaliq, 2026-11-10), and so does the self-hosted Helm chart. The dashboard's Overlaps view is a separate setting that is off by default, so the endpoint runs without the tab appearing until it is turned on.
- **Where it shows:** an Overlaps tab in the dashboard (`relay-webapp`). It appears only when the dashboard's Overlaps setting is on (off by default) and the endpoint answers; when the endpoint is off the tab also stays hidden.

Two things are deliberately not shipped:

- **Model-proposed findings.** The model judge (P5 C) and the Relay Chief classifier both failed (below). The model report job, its migration and its lambda wiring are removed from #211.
- **Alerts before harm.** No approach produced one on held-out data.

## How it was tested

1. **Snapshot.** A frozen snapshot of 5,008 agentworkforce sessions, 2026-10-26 to 2026-11-10T00:00Z. It was harvested through the owner-authorized shared-session tools: about 1,600 requests and 108 minutes. It is kept outside any repository and deleted by 2026-11-17.
2. **Ground truth, from outcomes rather than from any detector.**
   - Sources: duplicate or superseded PRs, competing heads and duplicate migrations on GitHub, explicit transcript statements, and the pipeline lead's coordination log.
   - Size: 11 PR-level incidents (7 dev, 4 held-out) across seven repos, of which 3 could be mapped to sessions on both sides, plus 63 hard negatives.
   - Split: by workstream, 60 percent dev and 40 percent held-out. Detector agents never saw held-out labels. Each detector was tuned on dev, frozen by hash, then scored on held-out exactly once.
3. **Blind adjudication.**
   - Each detector's top 15 alerts, at a budget of 5 per 100 sessions, were pooled, shuffled and stripped of detector names.
   - Two adjudicator agents and the labeling agent judged each pair: real overlap, planned parallel work, unrelated, or unclear.
   - Independent cross-checks agreed 6 of 6 and 3 of 3.
4. **Habit ablations.** Each detector was rerun without Agent Relay message banners, agent naming conventions, PR conventions or AGENTS.md phrasing.

## Results

| # | Approach | Dev PR incidents | Held-out PR incidents, before / after harm | Blind top 15: real / planned / unrelated | Cost per 1,000 sessions |
|---|---|---|---|---|---|
| P1 | Shared artifact (#211's 60-minute rule, generalized) | 0 of 7 | 0 / 0 of 4 | 0 / 12 / 3 | 90 ms, no model |
| P2 | Outcome-anchored (forge plus sessions) | 2 of 2 session incidents, circular | 0 / 1 of 4 | 0 / 10 / 5 | 1.75 s, no model |
| P3 | Task similarity | 0 of 7 | 0 / 1 of 4 | 0 / 15 / 0 | about 3 s, no model |
| P4 | Investigation overlap (untuned) | 0 of 2 | 0 / 0 of 4 | 0 / 13 / 2 | 10.6 s, no model |
| P5 | Two-stage with model judge | candidates 100%, ranked about 7,000th | A/B: 1 / 1 of 4, but ranked outside every budget; C (judge): 0 / 0 of 4 | 0 / 1 / 14 | model judge on 400 pairs |
| P6 | Outcome-anchored, ranked by duplication evidence, fleets suppressed | 0 of 7 in top 50 | 0 / 1 of 4 | 1 / 7 / 7 | forge calls |
| Jev | Relay Chief classifier, gate at 0.80 | n/a | n/a | labeled all 65 pairs overlap; 24 of 24 planned pairs at 0.80 or more | 168 ms per pair |

What the results mean:

- **No approach warned before the harm at a usable alert budget** on held-out data. P5's broad candidate stage did contain one held-out incident 41 minutes before the harm, but ranked about 300,000 candidates deep, far below anything that would be shown. The early signal exists in the candidates; ranking is what's missing.
- **Planned parallel work dominates.** It made up 50 of the 60 first-pool alerts: review fleets, and one person or a lead splitting work across agents. In this workspace that's the biggest single source of false alarms.
- **Outcome evidence is the only signal that found real incidents.** It did so after the fact, and with forge access.
  - P2's dev recall is circular, because the labels were also mined from GitHub outcomes.
  - P2's session-evidence-only variant found nothing.
- **Written-file evidence was nearly absent.** Only 8.65% of sessions had any file-edit evidence, and exact shared-file pairs within 60 minutes numbered zero. Codex code-mode edits never reached `files_touched`; #485 fixes that for new sessions.
- **The forge-only signals can't map to facts.** Closed-as-duplicate PRs, competing heads and merge conflicts are absent from production facts, which is why v1 keeps only T1, T2 and T3.
- **The shipped detector found nothing on this snapshot.** Restricted to session evidence, the P6 variant (the same tiers as `outcome-facts`) found 0 of 7 dev incidents; all 15 of its top alerts were T3, and T1 and T2 never fired. With forge access it attributed 4 of 7, after the fact, but none in its top 50. The likely reason T1 found nothing: the duplicate-migration incidents were Codex work, and Codex code-mode writes were invisible before #485. That explanation is unverified until #485 data and the filtered readers exist.

## Why this approach

1. **Precision over recall.** A duplicate migration number or a cross-lane revert almost always means real duplicated or conflicting work. The broader signals we tested mostly flagged planned parallelism. A view full of false alarms would teach people to ignore it.
2. **It runs anywhere the pipeline runs.** It uses only facts the pipeline already stores, in both the cloud and self-host backends, with no model endpoint. That matters for Automation Anywhere, whose LLM choice is still open and whose security team distrusts probabilistic controls.
3. **It's honest about what's unknown.** The view never claims "they didn't talk". Communication stays `unknown` until the pipeline stores relay-message evidence.

## Limits

- **On today's evidence it would have found none of the known incidents.** It is shipped off by default as a precise, honest framework; the offline process below is what finds incidents today. Re-evaluate it once #485 and the filtered readers have produced a week of facts.
- **Recall is low by design.** It counts only against known incidents, and the labeled set is small and biased toward PR-visible duplicates and toward this team's habits: one person running many agents, PR-per-change.
- **It is retrospective.** Use it for reconcile or close-one follow-ups, as Relay Chief plans to, not to interrupt work in progress.
- **Existing stored facts lack Codex code-mode edit paths** until they're reprojected. That's a separate job with separate approval.
- **Evidence streams are filtered and independently bounded.** Each request reads up to 10,000 rows
  whose `files_touched` array is non-empty and up to 50,000 `commit`/`outcome` rows. It also reads at
  most 10,000 unfiltered event rows only to price duplicate work. A capped evidence stream makes
  coverage partial and can omit findings; a capped usage stream makes cost/tokens `null` rather than
  undercounting them. `coverage.sessionsTruncated` is the contract's 0/1 truncation indicator, not a
  count of sessions: `0` means no stream cap was observed and `1` means one or more streams hit a cap;
  `coverage.notes` names each capped stream.
- **The evidence read includes a seven-day lookback.** The response window is unchanged, and every
  finding's `alertAt` must be inside it. Earlier evidence can anchor a T1/T2/T3 alert at the left edge;
  later evidence never moves an alert backward or removes it through rarity damping. Readiness covers
  the lookback too.

### Coordination fact-reader index packet

The migration records these byte-frozen definitions (the partial `WHERE` predicates are part of the
contract):

```sql
CREATE INDEX session_event_facts_coordination_writes_idx ON sessions.session_event_facts USING btree (org_id, workspace_id, ts, row_id) WHERE (cardinality(files_touched) > 0);
CREATE INDEX session_output_facts_coordination_outcomes_idx ON sessions.session_output_facts USING btree (org_id, workspace_id, ts, row_id) WHERE (fact_kind = ANY (ARRAY['commit'::text, 'outcome'::text]));
```

On a populated database, run the same statements out of band with `CREATE INDEX CONCURRENTLY` in place
of `CREATE INDEX`, one statement per transaction. Verify that both indexes are valid and ready, then run
the migration; it compares `pg_get_indexdef` byte-for-byte and adopts them without rebuilding.

## Showing a customer where Relay helps (the repeatable process)

This runs offline, with the customer's authorization. The automated steps narrow the candidates and adjudication confirms them.

1. **Harvest.** Pull the customer's sessions for a window through the shared-session tools into a frozen snapshot, using `~/overlap-eval/tools/harvest`. Atomic writes, a manifest of hashes, and a delete-by date.
2. **Mine outcomes.** With read access to their forge, find duplicate or superseded PRs, competing heads and duplicate migrations, and map them to sessions. This is the P2/P6 approach.
3. **Narrow candidates.** Run `outcome-facts` and P6 ranking, suppressing planned fleets.
4. **Adjudicate.** Agents judge the candidate pairs blind against the transcripts, with a cross-check.
5. **Report.** Confirmed incidents with citations, cost of the duplicated work, and what a message would have changed.

For 5,008 sessions, the harvest took about 108 minutes and roughly 1,600 requests.

## Re-running the evaluation

The repository harness verifies the frozen snapshot manifest, maps its retained
lane, write, commit, outcome, branch and parent evidence into the detector input
shape used by the read API, runs
`outcome-facts` (T1, T2 and T3) and the selectable `shared-write-60m` baseline,
then scores development and held-out labels separately:

```sh
bun run coordination-eval \
  --snapshot /path/to/snapshot-v2 \
  --truth /path/to/truth-v1/split.json \
  --projection snapshot \
  --out /path/to/private-output
```

`report.json` and `report.md` are deterministic: they include the verified input
hashes, repository SHA and detector parameter hashes, but no run timestamp or
measured duration. The command prints wall-clock runtime to stderr so two runs
produce byte-identical report files. Reports contain aggregate scores and incident
IDs, never transcript or session text.

`--projection snapshot` uses the frozen snapshot's derived write paths.
`--projection current` instead passes retained raw tool inputs through the current
relayhistory projection, including Codex code-mode `apply_patch` and Claude
`MultiEdit`; it never falls back to stored `files_written`. If a frozen snapshot
contains only `command_or_target` text capped at 600 characters and lacks complete `args_json`, the
report is marked `projection_unavailable` and names that evidence gap precisely.

Proposed cadence: run this evaluation every Monday over the trailing 14 days.
That schedule is only a proposal. Khaliq must approve retention for each rolling
harvest and designate who labels newly discovered incidents before it is adopted.

## Next steps that would raise recall

- **Normalized tool-call facts:** `git revert`, reset, force push and rebase, and `gh` PR and CI refs, so T2-style evidence exists without forge access. Estimated 3 to 5 days, and it needs a schema decision.
- **Relay-message evidence in facts,** so `communication` can become observed or not observed.
- **Reprojecting stored facts** with #485, so Codex edits count.
- **A larger, multi-person labeled set** before anyone calibrates an alert threshold.
