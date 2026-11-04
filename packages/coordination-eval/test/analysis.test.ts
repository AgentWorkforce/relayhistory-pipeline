import { describe, expect, test } from "bun:test";
import example from "../fixtures/example.json";
import sessions from "../fixtures/sessions-example.json";
import { prepareAssessment } from "../src/intake.ts";
import {
  executeAnalysis,
  parseAnalysisResult,
  type AnalysisPacket,
} from "../src/analysis.ts";
import { parseAssessment } from "../src/contract.ts";
import { buildAnalysisPackets } from "../src/packets.ts";
const assessment = () =>
  prepareAssessment(structuredClone(sessions)).assessment;
function response(p: AnalysisPacket) {
  return {
    packetId: p.packetId,
    assessmentSha256: p.assessmentSha256,
    workstreamId: p.subject.workstreamId,
    workstreamSummary: {
      state: "unknown",
      text: "Captured activity requires review.",
      evidenceIds: [p.evidence[0]!.id],
    },
    observationReviews: [],
    candidateFindings: [],
    missingContextRequests: [],
  };
}
describe("semantic analysis boundary", () => {
  test("rejects foreign citations, extra fields, mismatched provenance, and confirmed verdicts", () => {
    const p = buildAnalysisPackets(assessment()).packets[0]!;
    for (const mutate of [
      (r: any) => {
        r.workstreamSummary.evidenceIds = ["foreign"];
      },
      (r: any) => {
        r.confirmed = true;
      },
      (r: any) => {
        r.assessmentSha256 = "wrong";
      },
      (r: any) => {
        r.candidateFindings = [{ status: "confirmed" }];
      },
    ]) {
      const r = response(p);
      mutate(r);
      expect(() => parseAnalysisResult(r, p)).toThrow(
        "invalid structured result",
      );
    }
  });
  test("keeps missing outcome as a draft/request and leaves canonical summary untouched", async () => {
    const a = assessment();
    const run = await executeAnalysis(a, async ({ packet }) => ({
      ...response(packet),
      candidateFindings: [
        {
          status: "pending",
          kind: "missed_handoff",
          description: "Possible handoff requires more context.",
          factEvidenceIds: [packet.evidence[0]!.id],
          actionEvidenceIds: [],
          outcomeEvidenceIds: [],
          checkpoint: null,
          intervention: "unknown",
          communication: "unknown",
          proposedMessage: null,
          strongestAlternativeExplanation:
            "Work may have been deliberately separate.",
          uncertainty: "No captured outcome.",
        },
      ],
    }));
    expect(run.assessment.findings).toEqual(a.findings);
    expect(run.assessment.workstreams).toEqual(a.workstreams);
    expect(run.missingContextRequests.length).toBe(run.packetResults.length);
    expect(run.packetResults.every((r) => r.status === "completed")).toBe(true);
  });
  test("merges complete candidates deterministically as pending without changing confirmed facts", async () => {
    const normalized = parseAssessment(example);
    const run = await executeAnalysis(normalized, async ({ packet }) => {
      const source = normalized.findings.find(
        (f) => f.workstreamId === packet.subject.workstreamId,
      );
      if (!source) return response(packet);
      const { id, incidentId, workstreamId, review, ...candidate } = source;
      return {
        ...response(packet),
        candidateFindings: [
          {
            ...candidate,
            status: "pending",
            strongestAlternativeExplanation:
              "Intentional independent work remains possible.",
            uncertainty: "Captured sessions do not establish causation.",
          },
        ],
      };
    });
    expect(run.assessment.findings.slice(0, example.findings.length)).toEqual(
      normalized.findings,
    );
    const additions = run.assessment.findings.slice(example.findings.length);
    expect(additions.length).toBeGreaterThan(0);
    expect(
      additions.every(
        (f) => f.review.verdict === "pending" && f.review.reviewer === null,
      ),
    ).toBe(true);
  });
  test("bounds concurrent calls and retries failures without leaking exception text", async () => {
    let active = 0;
    let peak = 0;
    const run = await executeAnalysis(
      assessment(),
      async ({ packet, attempt }) => {
        active++;
        peak = Math.max(peak, active);
        await new Promise((resolve) => setTimeout(resolve, 2));
        active--;
        if (attempt === 1) throw new Error("private provider data");
        return response(packet);
      },
      { concurrency: 2, maxAttempts: 2 },
    );
    expect(peak).toBeLessThanOrEqual(2);
    expect(run.execution.calls).toBe(run.execution.totalPackets * 2);
    expect(run.execution.completedPackets).toBe(run.execution.totalPackets);
    expect(JSON.stringify(run)).not.toContain("private provider data");
  });
  test("enforces call budget and times out an uncooperative transport", async () => {
    const run = await executeAnalysis(
      assessment(),
      async () => new Promise(() => {}),
      { concurrency: 1, maxCalls: 1, maxAttempts: 1, timeoutMs: 5 },
    );
    expect(run.execution.calls).toBe(1);
    expect(run.packetResults[0]!.status).toBe("failed");
    expect(
      run.packetResults.slice(1).every((r) => r.status === "budget_exhausted"),
    ).toBe(true);
  });
  test("rejects future facts at a checkpoint", () => {
    const p = buildAnalysisPackets(assessment()).packets[0]!;
    const e = [...p.evidence]
      .sort((a, b) => a.occurredAt.localeCompare(b.occurredAt))
      .at(-1)!;
    if (e.occurredAt <= p.window.since)
      throw new Error("fixture needs later evidence");
    const r = {
      ...response(p),
      candidateFindings: [
        {
          status: "pending",
          kind: "missed_handoff",
          description: "Possible handoff.",
          factEvidenceIds: [e.id],
          actionEvidenceIds: [],
          outcomeEvidenceIds: [],
          checkpoint: p.window.since,
          intervention: "unknown",
          communication: "unknown",
          proposedMessage: null,
          strongestAlternativeExplanation: "Separate work.",
          uncertainty: "Unknown outcome.",
        },
      ],
    };
    expect(() => parseAnalysisResult(r, p)).toThrow(
      "invalid structured result",
    );
  });
});
