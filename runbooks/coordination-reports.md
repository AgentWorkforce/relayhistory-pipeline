# Workspace coordination reports

This is an operator-invoked job inside the existing sessions pipeline. It reads
canonical shared sessions for one configured organization/workspace and historical
time window, builds bounded analysis packets, asks a configured model for cited
proposals, and saves a draft report in HoopDb. It needs no chat connector, local
session export, new harness parser, or change to session sharing.

Implementation: `src/coordination-report/`. Evidence/rubric and report contract:
`packages/coordination-eval/`. Dispatch: `src/session-pipeline/lambda.ts`.

## Report contents and interpretation

- Engineering activity, proposed progress/blockers and cited workstream summaries.
- Shared surfaces and tasks, with intentional delegation/review distinguished from
  possible coordination failures.
- Candidate missed handoffs, repeated investigation, conflicting decisions and
  human context transfer, each with evidence and an alternative explanation.
- Successful coordination controls, missing-context requests and a review queue.
- Source fingerprints, capture limits, packet omissions, model/prompt provenance
  and successful-call token usage where returned by the provider.

Job `ready` means a draft artifact was published. Analysis coverage and human review
status are separate. Failed or skipped model packets, omitted context and truncated
excerpts produce explicit partial coverage, even if every model call succeeds.
Model outputs never confirm their own findings. Overlap is not rework;
user-role messages may come from automation. Savings, causal impact, and replay
performance are not measured by this job.

## Deployment preparation

Code and fixtures can be tested locally. Applying the migration, changing runtime
configuration/permissions or deploying requires explicit approval for that rollout.
No production settings are enabled by this change.

1. Apply the repository's normal reviewed migration sequence, including
   `20261105047000_coordination_report_jobs.sql`, and its runtime ACL/readiness
   validation. Do not run the new SQL alone as a substitute for the migrator.
2. Configure `transcriptStore.sessionPipeline.coordinationReports` in the approved
   stack inputs. `enabled`, `organizationId` and `workspaceId` are required;
   optional fields are `model`, `maxSessions`, `maxEvents`, `maxPackets`, `maxCalls`
   and `concurrency`. The default model is `gpt-6.1-sol`; verify availability on the
   existing Responses endpoint. Credentials remain in the pipeline's existing
   managed secret, loaded in memory. No token belongs in inputs or files.
3. Deploy the existing pipeline function with that configuration. No report
   schedule or new public endpoint is created. IAM invocation is the operator
   authorization boundary; deployment configuration supplies tenant authority.
4. Run a short historical window first, review capture/analysis coverage, then run
   a week. A second customer requires a separately authorized configured scope.

Runtime switches map to `COORDINATION_REPORT_ENABLED`, `COORDINATION_REPORT_ORG_ID`,
`COORDINATION_REPORT_WORKSPACE_ID`, `COORDINATION_REPORT_MODEL` and
`COORDINATION_REPORT_MAX_SESSIONS/MAX_EVENTS/MAX_PACKETS/MAX_CALLS/CONCURRENCY`.
Absent or disabled configuration performs no report work.

Rollback: disable report execution, then use
`supabase/rollbacks/20261105047000_coordination_report_jobs.down.sql` before
rolling back event-directory metadata or older session migrations. The down
migration deletes stored report artifacts and jobs. Applying it to a real
database is destructive and requires separate explicit rollback approval; it is
not an automatic response to a failed report job. Up/down/up is covered locally.

## Invoke and retrieve

Invoke the existing pipeline Lambda through the approved operator mechanism with
this body (timestamps are UTC, `until` exclusive):

```json
{
  "coordinationReport": {
    "idempotencyKey": "baseline-2026-10-28-v1",
    "since": "2026-10-28T07:00:00.000Z",
    "until": "2026-11-04T08:00:00.000Z"
  }
}
```

This window is midnight October 28 through midnight November 4 in Los Angeles,
including the daylight-saving transition. The broader discovery window begins
October 21 at `07:00:00.000Z`; keep targeted case discovery separate from baseline
review. Invocation bodies cannot override the configured workspace or budgets.
Windows must be complete, historical and at most 31 days.

The response contains `outcome.jobId`, job execution status and analysis status.
Reusing the same idempotency key with a different request is rejected. Running jobs
are leased; completed jobs are not reanalyzed on duplicate invocation. Fixed error
codes are safe to log; transcript contents and provider error bodies are not.

Retrieve a private report chunk with the returned job ID:

```json
{
  "coordinationReportRead": {
    "jobId": "REPLACE-WITH-RETURNED-UUID",
    "format": "markdown",
    "offset": 0
  }
}
```

Use `format: "json"` for the complete structured artifact. Evidence is stored once
in `assessment.evidence`; `report.evidenceArtifact` references that field.
Concatenate `content`
chunks using each returned `nextOffset` until it is null. Read only `ready` jobs;
stop and discard a download if its status or `artifactSha256` changes. Reads are scoped to the same
configured workspace, and results are bounded chunks rather than large Lambda
response payloads. Save real reports only to approved private storage; local
development artifacts belong in ignored `.coordination-eval/` with private modes.

## Limits, review and next run

Defaults: 500 sessions, 10,000 events, 12 model packets/calls, concurrency 4.
Hard limits: 1,000 sessions, 10,000 events, 24 packets/calls, concurrency 4;
30-second model timeout, 240-second worker deadline and a 300-second lease.
Canonical reads enforce 128 MiB scan budgets and an 8 MiB normalized export budget.
The stored artifact is capped at 16 MiB. These are admission limits, not claims
that arbitrary customer weeks fit one invocation. Split oversized date windows;
do not treat a failed collection as a complete team report. Source readiness gaps
fail collection rather than silently dropping sessions. A read already in flight
may finish after cancellation, but cannot publish or dispatch further reads.
Unrelated new work does not invalidate a historical report. Changes to a supporting
session do invalidate it, including later appends to that same session; newly
discovered in-window events also invalidate it. Historical cross-agent visibility
is unknown: ingestion timestamps are preserved as metadata, never substituted for
verified availability at a proposed intervention checkpoint.

Review candidate evidence and negative controls before confirming cases. To use
the existing review workflow, export the artifact's `assessment` field to private
JSON, record reviewer judgments, and run `coordination:report --input … --out …`.
This regenerates a local reviewed report; it does not overwrite the pipeline draft.
A customer review/publishing API remains separate work.

For an initial benchmark, annotate a held-out set of real incidents and ordinary
parallel work, then evaluate citation correctness, useful-connection precision,
timely availability and unnecessary alerts. A later live Relay rollout can measure
delivery, acknowledgment and changed behavior. Do not sell a retrospective
counterfactual as measured time saved.
