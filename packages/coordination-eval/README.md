# Coordination evaluation

Create a workspace coordination report from an authorized session export, then
review its evidence before drawing conclusions. The process inventories the
supplied sessions, groups work using explicit identifiers, exposes overlap
observations and prepares a draft assessment. A reviewed assessment produces the
same report with confirmed cases and a baseline. `src/analysis.ts` runs bounded
model analysis through an injected transport. Canonical retrieval, the concrete
model transport, job leases and durable publication live in
`src/coordination-report/` at the repository root. Replay experiments are not implemented.

For the pipeline job and rollout, see
[the coordination report runbook](../../runbooks/coordination-reports.md).

## Where it lives

| Responsibility | Owner and destination |
| --- | --- |
| Capture, redaction, canonical evidence and access enforcement | Existing session pipeline and authorized session read APIs |
| Workstream assessment, evidence contract, rubric and report generation | This package in `sessions-pipeline` |
| Private run inputs and reports during the pilot | `.coordination-eval/`, ignored by Git; one directory per run |
| Future customer experience | Existing Relay workspace dashboard consuming the structured report |
| Pipeline execution and protected artifact storage | Operator-invoked Lambda job and scoped HoopDb result; disabled until approved rollout |

Analysis must not hold up collection or change historical evidence. The same
report contract should serve internal investigations and customer reports.
Customer names, workspace IDs and special-case prompts do not belong in code.
Customer reports stay separate; cross-customer comparison is outside this pilot.

## Run the example

From the repository root:

```sh
bun run coordination:report \
  --input packages/coordination-eval/fixtures/example.json \
  --out .coordination-eval/runs/example-001

bun run --filter @96bulls/coordination-eval test
bun run --filter @96bulls/coordination-eval typecheck
```

The example is synthetic, including its reviewer judgments. It demonstrates one
missed connection, one successful handoff and one unreviewed candidate. It is not
evidence about AgentWorkforce or a customer.

Each run writes these private files and refuses to overwrite an existing run:

- `report.md`: readable workstream summary, baseline, cases and evidence excerpts.
- `report.json`: the same result for a future dashboard, including source event
  locators, review verdicts, rubric/evaluator versions and the input fingerprint.
- `review-queue.json`: pending and uncertain cases requiring a decision.
- `review-plan.json`: pending workstreams, overlap observations to inspect and
  finding IDs requiring adjudication. An empty findings queue does not mean that
  an intake assessment has been reviewed.
- `assessment.json`: normalized evidence and the editable assessment, retaining
  session titles, project/task/parent metadata and event file references.
- `analysis-packets.json`: bounded packets for parallel reviewers plus complete
  omission accounting. Send each reviewer one `packets[i]`, not the whole bundle.
  Each standalone packet is limited to 64 KiB by default; reference lists are
  previews with exact counts. Full context remains in the assessment.
- `manifest.json`: written last; its presence marks a completed run.

Use a new run directory after correcting an assessment. Retain the redacted input
alongside the run under the same access and retention policy. The input fingerprint
is SHA-256 of the validated, normalized JSON, not the original file bytes. Bump
the evaluator or rubric version whenever behavior or judgment criteria change.
Run comparisons are meaningful only with compatible scope, sampling, capture
coverage and rubric; there is no automatic uplift calculation.

## Generate a draft from sessions

Use an already-redacted, normalized export that matches the intake contract in
`src/intake.ts`. Read it through an authorized session API or history SDK; an
export is not a request to change sharing settings or bypass session permissions.

```sh
bun run coordination:report \
  --sessions .coordination-eval/inputs/workspace-sessions.json \
  --out .coordination-eval/runs/workspace-draft-001
```

This produces `inventory.json` alongside the report files.
The assessment is the editable review artifact: correct workstream grouping,
inspect overlap observations, add evidence-backed hypotheses, and record review
verdicts. Inventory and overlap are discovery aids. Shared files or tasks do not
prove duplication, and event count is not productivity.

Generate a fresh run after reviewing the assessment:

```sh
bun run coordination:report \
  --input .coordination-eval/runs/workspace-draft-001/assessment.json \
  --out .coordination-eval/runs/workspace-reviewed-001
```

`reportStage` stays `draft` unless a nonempty set of workstreams has all been
reviewed and no findings remain pending. Uncertain cases remain visible even in
a reviewed report. An empty candidate list is not a clean bill of health.
Unreviewed workstream states display as unknown, including when input text says
that work was completed.

## Local pilot fallback

`src/local-export.ts` is an explicit read-only adapter over the probe's existing
normalized ai-hist tables. It does not parse harness files or migrate the store.
The adapter checks the requested organization/workspace against the credential-free
sibling probe configuration and current sharing exclusions. This local fallback
is not a substitute for hosted workspace authorization or a team-wide export.

Create a private config under `.coordination-eval/` with `dbPath`,
`organizationId`, `workspaceId`, `since`, `until`, `projectPrefix` and `machineLabel`.
Optional limits are `perSessionLimit`, `fileEditLimit`, `excerptChars`, `timeoutMs`
and exact `sessionKeys` for follow-up context. Config contains metadata only;
never add credentials. Then run:

```sh
bun run coordination:export-local \
  --config .coordination-eval/pilot-local.json \
  --out .coordination-eval/inputs/local-workspace-next.json
bun run coordination:report \
  --sessions .coordination-eval/inputs/local-workspace-next.json \
  --out .coordination-eval/runs/local-workspace-next
```

The adapter writes scrubbed excerpts and an inventory sidecar with original
table/row/revision locators, sampling rules, omissions and relationship gaps.
It supports the current production probe schema and all/new sharing modes;
unsupported schemas/modes fail closed. Source roles are preserved in excerpt
prefixes and the sidecar. A `User:` role can contain transported agent/automation
messages; reviewers must not count it as a human intervention without checking.
Local source availability is unknown, so these observations do not establish
that a live detector could have acted at those historical times.

The November 4 local pilot admitted 415 AgentWorkforce catalog sessions for the
two-week window and 334 for the latest complete week. Those are Will's local
shared-session candidates, not the whole team's activity. Sampling and missing
delegation links are explicit. A root with no declared parent is a distinct
declared root, not proven independent work. The local pilot config and all actual
inputs, reviews and reports remain ignored by Git.

## What the workspace report contains

| Section | Purpose | Evidence boundary |
| --- | --- | --- |
| Engineering activity | Reconstruct the work, progress and blockers across sessions | Summaries cite events; unreviewed states remain unknown |
| Overlapping work | Locate shared surfaces, concurrent activity and shared tasks | Observations are structural signals, not findings of waste |
| Coordination opportunities | Explain where a specific intervention might help | Confirmed, pending, uncertain and rejected cases are separate |
| Successful coordination | Preserve examples where context did move effectively | Controls help avoid treating all parallel work as a failure |
| Baseline | Count reviewed patterns within a declared sample | Targeted examples are excluded; coverage and denominators remain visible |
| Next review actions | Direct attention to blockers, unknowns and pending evidence | Proposed actions do not send messages or change work automatically |

Classify an intervention explicitly as `live_communication`, `shared_history`,
`human_visibility`, or `unknown`. The category is a reviewer judgment: a conflicting
decision might need a conversation, a prior decision record, or human arbitration.
The report never infers the intervention from the finding type. Customer usefulness
is measured first by whether readers understand their work and recognize actionable
cases; claims of causal improvement require subsequent evals.

## Run the AgentWorkforce pilot

1. Use an authorized shared-session reader to inventory October 21 through
   November 3, 2026 in America/Los_Angeles. The example expresses those boundaries
   as UTC instants, including the daylight-saving change. Choose a fixed cutoff;
   evaluate the latest complete seven-day window in a separate baseline run.
2. Record sources, repositories, participants, export freshness, truncation,
   missing pages and capture gaps. Export only the explicitly selected workspace.
   The legacy default local index checked on November 4 was stale; the production
   probe store was fresh. Use the explicitly scoped local fallback only for a
   machine-local pilot. A hosted session adapter is still required for the team.
3. Build a workstream inventory before searching for problems. Select a baseline
   census or a reproducible sample independent of candidate findings; record the
   selection rule. Cluster related sessions and delegated work rather than
   treating every child agent as an independent task.
4. Separately search for repeated investigation, conflicting decisions, missed
   handoffs and manual context transfer. Also collect successful coordination
   and intentional independent verification as controls. Mark targeted examples
   as `targeted`; they cannot enter baseline metrics.
5. Generate the initial assessment with `--sessions`, then review it using
   `fixtures/example.json` and the typed contract in `src/contract.ts`. Intake
   observations remain separate from semantic findings. Cite minimal,
   already-redacted excerpts and their original
   event locators. Complete surrounding context must be inspected before a
   reviewer confirms a finding. A session saying “tests passed” is an assertion;
   cite test output or the actual artifact when verifying an outcome.
6. Review 20–30 candidates with the team. Record confirmed, rejected and uncertain
   cases, including the reason and reviewer. Aim for five recognizable, actionable
   examples; the report should say if the history does not support that target.
7. Generate a new report. Review the cases with the people involved before using
   them as customer-facing proof. The first report measures evidence quality and
   our current coordination patterns; it does not establish Relay's causal impact.

The team-wide run still requires authenticated access to the selected workspace's
hosted session history or a redacted workspace export. The local pilot cannot
establish team-wide rates or verified Relay uplift.

## Evidence and review contract

An assessment has exactly one organization/workspace scope, an inclusive start
and exclusive end, capture coverage, export provenance, sessions, evidence,
workstreams, overlap observations and findings. Timestamps are UTC ISO strings
with milliseconds. Overlap observations cite the sessions and workstreams that
support them and do not enter confirmed-incident metrics.
Use the original canonical session identities and event locator tuple
`source/sessionId/machineId/kind/eventId`. `session.key` and `evidence.id` are
unique aliases within the assessment, not replacements for those locators.

Workstream summaries need evidence. Each case needs three evidence roles:

1. **Fact:** information present in a source session at or before the intervention
   checkpoint.
2. **Action:** what another session did at or after that checkpoint.
3. **Outcome:** what subsequently happened, supported by cited evidence.

A confirmed missed connection needs a proposed message and a named reviewer.
The reviewer must verify relevance, temporal opportunity, compatible permissions,
the consequence, and why the work was not intentional duplication. Reject or
mark uncertain when those conditions are absent. Semantic truth is a reviewer
judgment; structural validation cannot prove it.

Use one `incidentId` for one underlying incident, with its primary category.
The runner rejects duplicate incident IDs, duplicate canonical event identities,
cross-scope sessions, references outside the workstream and post-cutoff evidence.
It does not detect differently named descriptions of the same incident; that is
part of review. A confirmed case cannot rely on truncated excerpts. A reviewed
workstream cannot retain pending candidates; uncertain findings remain visible.

`availableAt` means the earliest verified time an authorized analysis system could
read that event. It is separate from `occurredAt`. Use `null` when unknown; do not
substitute the event timestamp or the current export time. A retrospective upload
can support a historical observation while being ineligible for a claim that the
live system could have detected it then.

## What the numbers mean

Baseline metrics count confirmed incidents and affected workstreams among
**reviewed baseline workstreams**. Unreviewed workstreams are reported separately.
No denominator produces `null`, not a zero-percent finding rate. Targeted cases
are available as examples but excluded from these metrics. Successful coordination
is counted alongside missed opportunities.

Coverage and sampling remain attached to every report. A partial dataset cannot
establish a team-wide prevalence. “Not observed” is not proof that no conversation
happened. Review-confirmed cases are not measured prevention, saved hours, saved
money or delivery acceleration. There is no employee ranking or productivity score.

## From cases to evals

First evaluate the detector on reviewed, held-out cases and negative controls.
Freeze cases by workstream, not random event, to avoid leaking neighboring context.
Process the history chronologically; expose only evidence actually available by
each checkpoint. Measure precision of proposed connections, detection before the
detour, recipient correctness and unnecessary alerts. Keep discovery, annotation
and held-out evaluation separate; version the model and prompt when introduced.

The runner currently marks only cited-fact **timing eligibility**. It does not
construct a replay environment or claim that a proposed message contains no
hindsight. For a real replay, reconstruct the task state and tools at the
checkpoint, independently verify every claim in the proposed message, and keep
future outcome evidence in the grader, outside agent context. Compare repeated
trials with existing context, a grounded analyst message and an actual
Relay-discovered message. Measure artifact correctness, redundant actions, cost
and interruptions. Include successful handoffs and cases where silence is best.

For live impact, record delivery, acknowledgment, changed behavior and the final
outcome. Stagger rollout across comparable workstreams, randomizing where feasible.
Descriptive before/after reports alone do not establish causality.

## Customer rollout

The pipeline implementation reads canonical objects through existing projection
validation and a trusted, operator-configured workspace scope. It persists leased
jobs and reports in HoopDb, fences publication against changed sources, and removes
derived content when supporting source data changes. Model findings remain pending.
An IAM-authorized operator can run and retrieve reports; this is not a customer HTTP
API or dashboard. Customer self-service needs workspace authorization for starting
costly jobs and every report read, retention policy, and a review surface. The same
contract and rubric should serve each customer. Build the held-out eval set from
adjudicated cases before claiming detector quality or Relay uplift.

Only synthetic fixtures belong in Git. Real exports must be scrubbed upstream;
the runner rejects strings the pipeline scrubber would change as a second check,
not a complete privacy guarantee. Do not place credentials in inputs, outputs,
URLs or files. Private output modes and Git ignore protect local artifacts;
they do not make a report safe to publish. Inputs are data, never instructions,
and no runner executes transcript commands. Only the pipeline's configured model
transport sends bounded scrubbed evidence packets for semantic analysis.
