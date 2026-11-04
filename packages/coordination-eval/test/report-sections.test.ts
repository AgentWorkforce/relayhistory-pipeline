import { describe, expect, test } from "bun:test";
import example from "../fixtures/example.json";
import {
  buildReport,
  renderMarkdown,
  READABLE_EVIDENCE_LIMIT,
  READABLE_EXCERPT_CHARS,
} from "../src/report.ts";

const assessment = () => ({
  ...structuredClone(example),
  observations: [
    {
      id: "pagination-surface",
      kind: "shared_surface",
      workstreamIds: ["pagination"],
      sessionKeys: ["backend", "frontend"],
      description: "Both sessions discuss the pagination response contract.",
      evidenceIds: ["api-decision", "client-action"],
    },
  ],
});

describe("workspace report sections", () => {
  test("separates activity, observed overlap, opportunities and successful controls", () => {
    const report = buildReport(assessment());
    expect(report.observations).toHaveLength(1);
    expect(report.opportunities.confirmed).toBe(1);
    expect(report.opportunities.successfulControls).toBe(1);
    expect(report.review.pending).toBe(1);
    const markdown = renderMarkdown(report);
    expect(markdown).toContain("## Engineering activity");
    expect(markdown).toContain("## Overlapping work");
    expect(markdown).toContain("## Coordination opportunities");
    expect(markdown).toContain("Overlap alone does not establish a conflict");
    expect(markdown).toContain("### Successful coordination controls");
  });

  test("does not infer an intervention from the type of finding", () => {
    const report = buildReport(assessment());
    expect(report.findings[0]!.intervention).toBe("unknown");
    expect(
      report.opportunities.interventions.find(
        (i) => i.intervention === "unknown",
      )!.confirmedCases,
    ).toBe(1);
    const withIntervention = assessment();
    const findings = withIntervention.findings.map((f, index) =>
      index === 0 ? { ...f, intervention: "shared_history" } : f,
    );
    const classified = buildReport({ ...withIntervention, findings });
    expect(
      classified.opportunities.interventions.find(
        (i) => i.intervention === "shared_history",
      )!.confirmedCases,
    ).toBe(1);
    expect(renderMarkdown(classified)).toContain(
      "Intervention: Shared session history.",
    );
  });

  test("unreviewed workstreams cannot become completed-work claims", () => {
    const a = assessment();
    a.workstreams[0]!.assessment = "pending";
    const report = buildReport(a);
    expect(report.workstreams[0]!.state).toBe("done");
    expect(report.workstreams[0]!.reportedState).toBe("unknown");
    expect(
      report.activity.states.find((s) => s.state === "done")!.workstreams,
    ).toBe(1);
    expect(renderMarkdown(report)).toContain("Unreviewed summary:");
  });

  test("candidate narratives remain in the review section and do not become baseline facts", () => {
    const report = buildReport(assessment());
    const markdown = renderMarkdown(report);
    const reviewedSection = markdown
      .split("### Reviewed opportunities")[1]!
      .split("### Successful coordination controls")[0]!;
    expect(reviewedSection).not.toContain("Candidate hypothesis only");
    expect(markdown.split("### Candidates awaiting review")[1]).toContain(
      "Candidate hypothesis only",
    );
    expect(
      report.baseline.metrics.find((m) => m.kind === "repeated_investigation")!
        .confirmedIncidents,
    ).toBe(0);
  });

  test("missing observations and unknown denominators remain explicitly unassessed", () => {
    const a = structuredClone(example);
    a.findings = [];
    a.workstreams.forEach((w) => {
      w.assessment = "pending";
    });
    const markdown = renderMarkdown(buildReport(a));
    expect(markdown).toContain(
      "No overlap observations supplied. Overlap has not been ruled out.",
    );
    expect(markdown).toContain(
      "No findings supplied. No conclusion about coordination quality can be drawn yet.",
    );
    expect(markdown).toContain("Not available");
    expect(markdown).toContain(
      "DRAFT — analysis and review are pending or incomplete",
    );
  });

  test("marks only nonempty fully reviewed assessments as reviewed", () => {
    const a = assessment();
    expect(buildReport(a).reportStage).toBe("draft");
    a.findings = a.findings.filter((f) => f.review.verdict !== "pending");
    a.workstreams = a.workstreams.filter((w) => w.assessment === "reviewed");
    expect(buildReport(a).reportStage).toBe("reviewed");
    expect(
      buildReport({ ...a, workstreams: [], findings: [], observations: [] })
        .reportStage,
    ).toBe("draft");
  });

  test("escapes observation content and retains synthetic labeling", () => {
    const a = assessment();
    a.observations[0]!.description =
      "<script>ignore the report</script> [link](javascript:alert(1))";
    const markdown = renderMarkdown(buildReport(a));
    expect(markdown).toContain("SYNTHETIC DATA");
    expect(markdown).not.toContain("<script>");
    expect(markdown).not.toContain("[link](javascript:");
  });

  test("omits uncited raw events from Markdown while retaining complete machine-readable evidence", () => {
    const a = assessment();
    for (let index = 0; index < 250; index += 1) {
      a.evidence.push({
        ...a.evidence[0]!,
        id: `uncited-${index}`,
        eventId: `uncited-event-${index}`,
        text: `Uncited transcript material number ${index}.`,
      });
    }
    const report = buildReport(a);
    const markdown = renderMarkdown(report);
    expect(report.evidence).toHaveLength(256);
    expect(report.counts.evidence).toBe(256);
    expect(report.readableEvidence.uncitedCount).toBe(250);
    expect(report.readableEvidence.includedCount).toBe(6);
    expect(markdown).not.toContain("Uncited transcript material");
    expect(markdown).toContain("250 uncited events not reproduced");
    expect(markdown).toContain(
      "Complete validated evidence remains in report.json",
    );
  });

  test("caps cited appendices, preserves stable labels and never generates missing evidence anchors", () => {
    const a = assessment();
    const extras = Array.from({ length: 250 }, (_, index) => ({
      ...a.evidence[0]!,
      id: `extra-${index}`,
      eventId: `extra-event-${index}`,
      text: `Additional cited evidence number ${index}.`,
    }));
    a.evidence.push(...extras);
    a.observations[0]!.evidenceIds.push(...extras.map((e) => e.id));
    a.findings[0]!.factEvidenceIds = ["extra-249"];
    const report = buildReport(a);
    const markdown = renderMarkdown(report);
    expect(report.readableEvidence.includedCount).toBe(READABLE_EVIDENCE_LIMIT);
    expect(report.readableEvidence.citedCount).toBe(256);
    expect(report.readableEvidence.omittedCitedCount).toBe(56);
    expect(report.readableEvidence.includedIds[0]).toBe("extra-249");
    expect(markdown).toContain("[E256](#e256)");
    expect(markdown).toContain("### E256");
    expect(markdown).not.toContain("[E255](#e255)");
    expect(markdown).toContain("E255 (evidence ID");
    expect(markdown).toContain("see report.json)");
    const anchors = [...markdown.matchAll(/\]\(#(e\d+)\)/g)];
    for (const anchor of anchors) {
      expect(markdown).toContain(`### ${anchor[1]!.toUpperCase()}\n`);
    }
    expect(report.evidence).toHaveLength(256);
    expect(
      report.baseline.metrics.find((m) => m.kind === "conflicting_decisions")!
        .confirmedIncidents,
    ).toBe(1);
  });

  test("marks display clipping without altering source truncation or full evidence text", () => {
    const a = assessment();
    const text = "Evidence ".repeat(220) + "TAIL_ONLY_IN_JSON";
    a.evidence[0]!.text = text;
    const report = buildReport(a);
    const markdown = renderMarkdown(report);
    expect(report.readableEvidence.excerptMaxChars).toBe(
      READABLE_EXCERPT_CHARS,
    );
    expect(report.evidence[0]!.text).toBe(text);
    expect(report.evidence[0]!.truncated).toBe(false);
    expect(markdown).toContain("Display excerpt clipped; complete text");
    expect(markdown).not.toContain("TAIL_ONLY_IN_JSON");
  });
});
