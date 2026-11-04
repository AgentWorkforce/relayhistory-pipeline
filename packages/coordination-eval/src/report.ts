import { createHash } from "node:crypto";
import {
  KINDS,
  parseAssessment,
  type Evidence,
  type Finding,
} from "./contract.ts";

export const EVALUATOR_VERSION = "coordination-review/0.2.0";
export const RUBRIC_VERSION = "coordination-evidence/1";
export const READABLE_EVIDENCE_LIMIT = 200;
export const READABLE_EXCERPT_CHARS = 1500;

function selectReadableEvidence(
  evidence: Evidence[],
  findings: Finding[],
  observations: { evidenceIds: string[] }[],
  workstreams: { summaryEvidenceIds: string[] }[],
) {
  const citedIds = new Set([
    ...findings.flatMap((f) => [
      ...f.factEvidenceIds,
      ...f.actionEvidenceIds,
      ...f.outcomeEvidenceIds,
    ]),
    ...observations.flatMap((o) => o.evidenceIds),
    ...workstreams.flatMap((w) => w.summaryEvidenceIds),
  ]);
  const includedIds = [...citedIds].slice(0, READABLE_EVIDENCE_LIMIT);
  return {
    limit: READABLE_EVIDENCE_LIMIT,
    excerptMaxChars: READABLE_EXCERPT_CHARS,
    citedCount: citedIds.size,
    includedCount: includedIds.length,
    omittedCitedCount: citedIds.size - includedIds.length,
    uncitedCount: evidence.length - citedIds.size,
    includedIds,
  };
}

function replayExclusions(
  evidence: Map<string, Evidence>,
  f: Finding,
): string[] {
  const exclusions: string[] = [];
  if (f.review.verdict !== "confirmed")
    exclusions.push("finding_not_confirmed");
  if (f.kind === "successful_coordination")
    exclusions.push("successful_coordination_control");
  const facts = f.factEvidenceIds.map((id) => evidence.get(id)!);
  if (facts.some((e) => e.availableAt === null))
    exclusions.push("historical_availability_unknown");
  if (facts.some((e) => e.availableAt !== null && e.availableAt > f.checkpoint))
    exclusions.push("fact_available_after_checkpoint");
  if (facts.some((e) => e.truncated)) exclusions.push("fact_truncated");
  return exclusions;
}

export function buildReport(input: unknown) {
  const a = parseAssessment(input);
  const reviewed = a.workstreams.filter(
    (w) => w.selection === "baseline" && w.assessment === "reviewed",
  );
  const baseline = a.workstreams.filter((w) => w.selection === "baseline");
  const reviewedIds = new Set(reviewed.map((w) => w.id));
  const baselineFindings = a.findings.filter((f) =>
    reviewedIds.has(f.workstreamId),
  );
  const confirmed = baselineFindings.filter(
    (f) => f.review.verdict === "confirmed",
  );
  const evidence = new Map(a.evidence.map((e) => [e.id, e]));
  const workstreams = a.workstreams.map((w) => ({
    ...w,
    reportedState: w.assessment === "reviewed" ? w.state : ("unknown" as const),
  }));
  const observations = a.observations ?? [];
  const findings = a.findings.map((f) => {
    const exclusions = replayExclusions(evidence, f);
    return {
      ...f,
      intervention: f.intervention ?? "unknown",
      replay: {
        status: "not_run" as const,
        timingEligible: exclusions.length === 0,
        exclusions,
      },
    };
  });
  return {
    schemaVersion: 1 as const,
    reportStage:
      a.workstreams.length > 0 &&
      a.workstreams.every((w) => w.assessment === "reviewed") &&
      !a.findings.some((f) => f.review.verdict === "pending")
        ? ("reviewed" as const)
        : ("draft" as const),
    evaluatorVersion: EVALUATOR_VERSION,
    rubricVersion: RUBRIC_VERSION,
    inputSha256: createHash("sha256").update(JSON.stringify(a)).digest("hex"),
    synthetic: a.synthetic,
    scope: a.scope,
    window: a.window,
    coverage: a.coverage,
    provenance: a.provenance,
    counts: {
      sessions: a.sessions.length,
      evidence: a.evidence.length,
      workstreams: a.workstreams.length,
      findings: a.findings.length,
    },
    activity: {
      sources: [...new Set(a.sessions.map((s) => s.source))].sort(),
      reviewedWorkstreams: workstreams.filter(
        (w) => w.assessment === "reviewed",
      ).length,
      states: ["active", "blocked", "done", "unknown"].map((state) => ({
        state,
        workstreams: workstreams.filter((w) => w.reportedState === state)
          .length,
      })),
    },
    observations,
    opportunities: {
      confirmed: findings.filter(
        (f) =>
          f.review.verdict === "confirmed" &&
          f.kind !== "successful_coordination",
      ).length,
      successfulControls: findings.filter(
        (f) =>
          f.review.verdict === "confirmed" &&
          f.kind === "successful_coordination",
      ).length,
      interventions: [
        "live_communication",
        "shared_history",
        "human_visibility",
        "unknown",
      ].map((intervention) => ({
        intervention,
        confirmedCases: findings.filter(
          (f) =>
            f.review.verdict === "confirmed" &&
            f.kind !== "successful_coordination" &&
            f.intervention === intervention,
        ).length,
      })),
    },
    baseline: {
      sampledWorkstreams: baseline.length,
      reviewedWorkstreams: reviewed.length,
      pendingWorkstreams: baseline.length - reviewed.length,
      uncertainFindings: baselineFindings.filter(
        (f) => f.review.verdict === "uncertain",
      ).length,
      metrics: KINDS.map((kind) => {
        const cases = confirmed.filter((f) => f.kind === kind);
        const affected = new Set(cases.map((f) => f.workstreamId)).size;
        return {
          kind,
          confirmedIncidents: cases.length,
          affectedWorkstreams: affected,
          denominator: reviewed.length,
          affectedWorkstreamFraction: reviewed.length
            ? affected / reviewed.length
            : null,
        };
      }),
    },
    review: {
      pending: a.findings.filter((f) => f.review.verdict === "pending").length,
      confirmed: a.findings.filter((f) => f.review.verdict === "confirmed")
        .length,
      rejected: a.findings.filter((f) => f.review.verdict === "rejected")
        .length,
      uncertain: a.findings.filter((f) => f.review.verdict === "uncertain")
        .length,
    },
    sessions: a.sessions,
    workstreams,
    findings,
    evidence: a.evidence,
    readableEvidence: selectReadableEvidence(
      a.evidence,
      findings,
      observations,
      workstreams,
    ),
    limitations: [
      "Workstream summaries, hypotheses and reviewer judgments are supplied with the assessment. Structural validation does not independently establish their meaning. Unreviewed activity states remain unknown.",
      "Overlap observations show shared surfaces or activity; they are not proof of wasted work, conflicting changes or a missed conversation.",
      "Baseline fractions describe reviewed sampled workstreams only. Targeted cases are excluded; sample selection and capture gaps can still bias the baseline.",
      "Confirmed means the named reviewer accepted the evidence, not that Relay caused or would have prevented an outcome.",
      "Timing eligibility checks cited facts only. A replay still needs reconstructed task state, permission checks, message grounding, controls and repeated trials. No replay has run.",
      "Session time is not human labor or delivery time. No saved-hours, cost-savings or causal-uplift estimate is computed.",
      "Not observed means no communication is shown in the reviewed evidence; uncaptured communication may exist.",
    ],
  };
}

export type Report = ReturnType<typeof buildReport>;

const md = (value: string): string =>
  value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/([\\`*_{}\[\]()#+.!|~-])/g, "\\$1")
    .replace(/\s+/g, " ");

const INTERVENTION_LABELS: Record<string, string> = {
  live_communication: "Live communication",
  shared_history: "Shared session history",
  human_visibility: "Human visibility",
  unknown: "Intervention not classified",
};

export function renderMarkdown(r: Report): string {
  const labels = new Map(r.evidence.map((e, i) => [e.id, `E${i + 1}`]));
  const evidence = new Map(r.evidence.map((e) => [e.id, e]));
  const includedIds = new Set(r.readableEvidence.includedIds);
  const sessions = new Map(r.sessions.map((s) => [s.key, s]));
  const workstreams = new Map(r.workstreams.map((w) => [w.id, w]));
  const refs = (ids: string[]) =>
    ids
      .map((id) => {
        const label = labels.get(id)!;
        return includedIds.has(id)
          ? `[${label}](#${label.toLowerCase()})`
          : `${label} (evidence ID ${md(id)}; see report.json)`;
      })
      .join(", ");
  const lines = [
    `# ${r.synthetic ? "Synthetic example coordination report" : "Workspace coordination report"}`,
    "",
    ...(r.synthetic
      ? [
          "**SYNTHETIC DATA — this demonstrates the report format and is not a customer finding.**",
          "",
        ]
      : []),
    ...(r.reportStage === "draft"
      ? [
          "**DRAFT — analysis and review are pending or incomplete. Missing findings do not mean no coordination problems occurred.**",
          "",
        ]
      : [
          "Assessment review is complete for the supplied workstreams. Uncertain cases and capture gaps remain explicit below.",
          "",
        ]),
    `Workspace: ${md(r.scope.workspaceId)} · Organization: ${md(r.scope.organizationId)}`,
    "",
    `Window: ${md(r.window.since)} to ${md(r.window.until)} (end exclusive).`,
    "",
    `Capture coverage: **${r.coverage.status}**. ${md(r.coverage.notes)}`,
    "",
    `${r.counts.sessions} sessions across ${r.activity.sources.length} sources; ${r.counts.workstreams} workstreams; ${r.observations.length} overlap observations. These counts describe the supplied evidence, not necessarily the entire workspace.`,
    "",
    `${r.opportunities.confirmed} review-confirmed coordination opportunities; ${r.opportunities.successfulControls} successful coordination controls; ${r.review.pending} candidates awaiting review; ${r.review.uncertain} uncertain cases. Review confirmation is not measured Relay impact.`,
    "",
    "## Engineering activity",
    "",
    `Sources: ${r.activity.sources.length ? r.activity.sources.map(md).join(", ") : "none supplied"}. ${r.activity.reviewedWorkstreams} of ${r.counts.workstreams} workstreams have reviewed assessments. Unreviewed workstream states are shown as unknown.`,
    "",
    ...r.workstreams.flatMap((w) => [
      `### ${md(w.title)}`,
      "",
      `State: **${w.reportedState}** · Assessment: ${w.assessment} · Selection: ${w.selection} · Sessions: ${w.sessionKeys.length}.`,
      "",
      `${w.assessment === "pending" ? "Unreviewed summary: " : ""}${md(w.summary)}`,
      "",
      `Evidence: ${refs(w.summaryEvidenceIds)}.`,
      "",
    ]),
    ...(r.workstreams.length
      ? []
      : [
          "No workstreams supplied. This is not evidence that no work occurred.",
          "",
        ]),
    "## Overlapping work",
    "",
    "These observations locate shared work for inspection. Overlap alone does not establish a conflict, wasted effort or a need to communicate.",
    "",
    ...r.observations.flatMap((o) => [
      `### ${md(o.id)} — ${o.kind.replaceAll("_", " ")}`,
      "",
      md(o.description),
      "",
      `Workstreams: ${o.workstreamIds.map((id) => md(workstreams.get(id)!.title)).join(", ")}. Sessions: ${o.sessionKeys.length}. Evidence: ${refs(o.evidenceIds)}.`,
      "",
    ]),
    ...(r.observations.length
      ? []
      : [
          "No overlap observations supplied. Overlap has not been ruled out.",
          "",
        ]),
    "## Coordination opportunities",
    "",
    "Classify the useful intervention for each reviewed case: live communication between agents, reuse of shared session history, or visibility for a human decision. An unclassified intervention remains unknown.",
    "",
    "| Intervention | Review-confirmed opportunities |",
    "| --- | ---: |",
    ...r.opportunities.interventions.map(
      (i) => `| ${INTERVENTION_LABELS[i.intervention]} | ${i.confirmedCases} |`,
    ),
    "",
  ];
  const groups = [
    {
      title: "Reviewed opportunities",
      cases: r.findings.filter(
        (f) =>
          f.review.verdict === "confirmed" &&
          f.kind !== "successful_coordination",
      ),
    },
    {
      title: "Successful coordination controls",
      cases: r.findings.filter(
        (f) =>
          f.review.verdict === "confirmed" &&
          f.kind === "successful_coordination",
      ),
    },
    {
      title: "Candidates awaiting review",
      cases: r.findings.filter((f) => f.review.verdict === "pending"),
    },
    {
      title: "Uncertain cases",
      cases: r.findings.filter((f) => f.review.verdict === "uncertain"),
    },
    {
      title: "Rejected cases",
      cases: r.findings.filter((f) => f.review.verdict === "rejected"),
    },
  ];
  for (const group of groups) {
    if (!group.cases.length) continue;
    lines.push(`### ${group.title}`, "");
    for (const f of group.cases) {
      lines.push(
        `#### ${md(f.id)} — ${f.kind.replaceAll("_", " ")}`,
        "",
        md(f.description),
        "",
        `Verdict: **${f.review.verdict}**. Reviewer: ${f.review.reviewer ? md(f.review.reviewer) : "unassigned"}. ${md(f.review.reason)}`,
        "",
        `Intervention: ${INTERVENTION_LABELS[f.intervention]}.`,
        "",
        `Facts: ${refs(f.factEvidenceIds)}. Action: ${refs(f.actionEvidenceIds)}. Outcome: ${refs(f.outcomeEvidenceIds)}.`,
        "",
        `Intervention checkpoint: ${md(f.checkpoint)}. Communication: ${f.communication.replaceAll("_", " ")}.`,
        ...(f.proposedMessage
          ? ["", `Proposed message: ${md(f.proposedMessage)}`]
          : []),
        "",
        `Replay: not run. ${f.replay.timingEligible ? "Cited facts pass timing checks; task reconstruction and message-grounding review still required." : `Timing/control exclusions: ${f.replay.exclusions.join(", ").replaceAll("_", " ")}.`}`,
        "",
      );
    }
  }
  if (!r.findings.length)
    lines.push(
      "No findings supplied. No conclusion about coordination quality can be drawn yet.",
      "",
    );
  lines.push(
    "## Baseline from reviewed sampled workstreams",
    "",
    `Baseline selection: ${md(r.coverage.baselineSelection)}`,
    "",
    `${r.baseline.reviewedWorkstreams} of ${r.baseline.sampledWorkstreams} baseline workstreams reviewed; ${r.baseline.pendingWorkstreams} pending; ${r.baseline.uncertainFindings} uncertain findings in reviewed workstreams. Targeted examples do not enter these counts.`,
    "",
    "| Pattern | Confirmed incidents | Affected workstreams / reviewed workstreams |",
    "| --- | ---: | ---: |",
    ...r.baseline.metrics.map(
      (m) =>
        `| ${m.kind.replaceAll("_", " ")} | ${m.confirmedIncidents} | ${m.denominator ? `${m.affectedWorkstreams} / ${m.denominator}` : "Not available"} |`,
    ),
    "",
    "## Next review actions",
    "",
    ...r.workstreams
      .filter((w) => w.reportedState === "blocked")
      .map(
        (w) =>
          `- Resolve the reviewed blocker in **${md(w.title)}**. Start with ${refs(w.summaryEvidenceIds)}.`,
      ),
    ...(r.review.pending
      ? [
          `- Adjudicate ${r.review.pending} pending candidates against the complete surrounding sessions; record reviewer and reason.`,
        ]
      : []),
    ...(r.review.uncertain
      ? [
          `- Resolve missing evidence for ${r.review.uncertain} uncertain cases, or retain their uncertainty.`,
        ]
      : []),
    ...(r.coverage.status !== "complete"
      ? [
          "- Resolve capture gaps before generalizing these results to the whole workspace.",
        ]
      : []),
    ...(r.baseline.pendingWorkstreams
      ? [
          `- Finish review of ${r.baseline.pendingWorkstreams} sampled workstreams before interpreting baseline rates.`,
        ]
      : []),
    ...(r.opportunities.interventions.find((i) => i.intervention === "unknown")!
      .confirmedCases
      ? [
          "- Classify the intervention for reviewed opportunities that still have an unknown intervention.",
        ]
      : []),
    "- Review proposed messages for hindsight and permissions before selecting cases for replay.",
    "",
    "## Evidence",
    "",
    `Readable appendix: ${r.readableEvidence.includedCount} of ${r.readableEvidence.citedCount} cited excerpts shown; ${r.readableEvidence.omittedCitedCount} cited excerpts omitted by the ${r.readableEvidence.limit}-excerpt limit; ${r.readableEvidence.uncitedCount} uncited events not reproduced. Complete validated evidence remains in report.json, keyed by evidence ID.`,
    "",
    `Excerpts are selected in priority order: findings, overlap observations, then workstream summaries. Evidence labels retain their positions in the full report.json evidence array. Display text is limited to ${r.readableEvidence.excerptMaxChars} characters per excerpt; any display clipping is marked separately from source truncation.`,
    "",
  );
  for (const id of r.readableEvidence.includedIds) {
    const e = evidence.get(id)!;
    const session = sessions.get(e.sessionKey)!;
    const clipped = e.text.length > r.readableEvidence.excerptMaxChars;
    lines.push(
      `### ${labels.get(e.id)}`,
      "",
      `Source: ${md(session.source)}; session: ${md(session.sessionId)}; machine: ${md(e.machineId)}; kind: ${md(e.kind)}; event: ${md(e.eventId)}.`,
      "",
      `Occurred: ${md(e.occurredAt)}. Available to the system: ${e.availableAt ? md(e.availableAt) : "unknown"}. Truncated: ${e.truncated ? "yes" : "no"}.`,
      "",
      ...(clipped
        ? [
            `Display excerpt clipped; complete text for evidence ID ${md(e.id)} is in report.json.`,
            "",
          ]
        : []),
      `> ${md(e.text.slice(0, r.readableEvidence.excerptMaxChars))}${clipped ? "…" : ""}`,
      "",
    );
  }
  lines.push(
    "## Interpretation and reproducibility",
    "",
    ...r.limitations.map((s) => `- ${s}`),
    "",
    `Evaluator: ${r.evaluatorVersion}. Rubric: ${r.rubricVersion}. Input SHA-256: ${r.inputSha256}.`,
    "",
  );
  return lines.join("\n");
}
