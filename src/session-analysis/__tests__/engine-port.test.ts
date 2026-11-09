// Ported from relayhistory-cloud c7de2163 packages/relayhistory/test/session-analysis/engine.test.ts:
// vitest -> bun:test and import paths only. It runs the vendored engine against upstream's own fixtures.
import { describe, expect, it } from "bun:test";
import {
  EVIDENCE_KINDS,
  type SessionSemanticAnalysis,
} from "../engine/analysis-contract.ts";
import { prepareAnalysisEvidence } from "../engine/analysis-evidence.ts";
import { buildSessionAnalysisSnapshot } from "../engine/snapshot.ts";
import { summaryInput } from "../../session-brief/engine/summary-input.ts";
import {
  validateSummaryInput,
  validateSummaryText,
} from "../../session-brief/engine/summary-contract.ts";
import {
  summarizeSessionInput,
  type ModelTransportRequest,
} from "../engine/model.ts";
import type {
  CollectedEvidence,
  NormalizedHydration,
} from "../../session-brief/engine/normalized-history.ts";

const evidence: CollectedEvidence = {
  tree: {
    nodes: [{ source: "codex", sessionId: "root", relationship: null }],
    unlinked: [],
    diagnostics: [],
    truncated: false,
  },
  events: [
    {
      id: 2,
      source: "codex",
      sessionId: "root",
      eventUid: "a1",
      tsMs: 2_000,
      role: "assistant",
      kind: "text",
      text: "Implemented it",
      model: "gpt-5.4",
      tokenUsage: { input_tokens: 100, output_tokens: 20 },
      messageId: "m1",
      parentId: null,
    },
    {
      id: 1,
      source: "codex",
      sessionId: "root",
      eventUid: "u1",
      tsMs: 1_000,
      role: "user",
      kind: "text",
      text: "Implement analysis",
      model: null,
      tokenUsage: null,
      messageId: null,
      parentId: null,
    },
  ],
  tools: [],
  edits: [],
};
const hydration: NormalizedHydration = {
  capability: "partial",
  status: "hydrated",
  discoveryState: "full",
  indexedThrough: { sourceStamp: "test", lastEventAtMs: 2_000 },
  diagnostics: [],
};

function unknownAnalysis(id: string): SessionSemanticAnalysis {
  const claim = {
    text: "Analyze the session",
    evidenceIds: [id],
    confidence: "high" as const,
    basis: "observed" as const,
  };
  const unknown = {
    value: "unknown" as const,
    explanation: "Unclear",
    evidenceIds: [] as string[],
    confidence: "low" as const,
    basis: "unknown" as const,
  };
  return {
    description: claim,
    intents: [claim],
    constraints: [],
    outcome: claim,
    outcomes: [],
    codeChangeAssessment: claim,
    decisions: [],
    testing: [],
    risks: [],
    blockers: [],
    skillsUsed: [],
    state: unknown,
    sessionType: unknown,
  };
}

describe("Skip analysis engine port", () => {
  it("preserves prompt, citation restoration, metrics and full snapshot", async () => {
    let seen: ModelTransportRequest | undefined;
    const snapshot = await buildSessionAnalysisSnapshot(
      evidence,
      hydration,
      {
        async complete(request) {
          seen = request;
          const input = JSON.parse(request.input) as {
            references: Array<{ id: string }>;
          };
          return {
            outputText: JSON.stringify({
              title: "Implement session analysis",
              ...unknownAnalysis(input.references[0]!.id),
            }),
            inputTokens: 40,
            outputTokens: 12,
            providerRequestId: "response-1",
            model: "gpt-5.4-2026-09-01",
          };
        },
      },
      {
        source: "codex",
        sessionId: "root",
        model: "gpt-5.4",
        analyzedAt: new Date("2026-09-01T00:00:00Z"),
      },
    );
    expect(seen?.instructions).toContain(
      "Tool-call arguments show an attempted action",
    );
    expect(seen?.schemaName).toBe("session_analysis");
    expect(snapshot.analyzerVersion).toBe("session-analysis-v2.4");
    expect(snapshot.understanding.description.evidenceIds).toEqual([
      "event:codex:root:u1",
    ]);
    expect(snapshot.metrics.usage).toMatchObject({
      inputTokens: 100,
      outputTokens: 20,
      totalTokens: 120,
    });
    expect(snapshot.metrics.interactionPeriods[0]).toMatchObject({
      durationMs: 1_000,
      triggerEvidenceId: "event:codex:root:u1",
    });
    expect(snapshot.evidence.coverage.byKind).toHaveLength(
      EVIDENCE_KINDS.length,
    );
    expect(snapshot.snapshotId).toMatch(/^session-analysis-[a-f0-9]{64}$/);
    // bun:test adaptation: bun's toMatchObject writes asymmetric matchers into the
    // actual object, so compare the durations before it (vitest order was after).
    expect(snapshot.backend!.totalDurationMs!).toBeGreaterThanOrEqual(
      snapshot.backend!.modelDurationMs!,
    );
    expect(snapshot.backend).toMatchObject({
      model: "gpt-5.4-2026-09-01",
      requestId: "response-1",
      modelDurationMs: expect.any(Number),
      totalDurationMs: expect.any(Number),
    });
  });

  it("rejects invented citations from model output", () => {
    const input = {
      source: "codex",
      sessionId: "root",
      references: [],
      coverage: {
        byKind: EVIDENCE_KINDS.map((kind) => ({
          kind,
          available: 0,
          included: 0,
          truncated: 0,
        })),
        metricsOmitted: {
          models: 0,
          tools: 0,
          filesChanged: 0,
          interactionPeriods: 0,
        },
        metricsShortened: 0,
        treeTruncated: false,
        unlinkedDelegationCount: 0,
        hydrationStatus: "hydrated",
        diagnosticCount: 0,
        sourceStamp: null,
        indexedThroughMs: null,
      },
      capability: "partial" as const,
      metrics: {
        models: [],
        usage: {
          observed: false,
          inputTokens: 0,
          outputTokens: 0,
          cachedInputTokens: 0,
          totalTokens: 0,
          costUSD: null,
        },
        tools: [],
        filesChanged: [],
        interactionPeriods: [],
        longestInteractionPeriodMs: null,
      },
    };
    const prepared = prepareAnalysisEvidence(input);
    expect(prepared.schema).not.toHaveProperty("$defs");
    expect(() => prepared.decode(unknownAnalysis("e1"))).toThrow(
      /unknown evidence e1/,
    );
  });

  it("briefs every source RelayHistory captures, and only those", () => {
    const input = (source: string) => ({
      source,
      sessionId: "root",
      messages: [{ role: "user", text: "Fix the build", truncated: false }],
    });
    for (const source of [
      "codex",
      "claude",
      "cursor",
      "grok",
      "opencode",
      "muse",
      "devin",
      "capy",
    ])
      expect(validateSummaryInput(input(source)).source).toBe(source);
    for (const source of ["relay", "trajectory", "unknown"])
      expect(() => validateSummaryInput(input(source))).toThrow(
        "Invalid summary input",
      );
  });

  it("requires one of the summary states", () => {
    const brief = {
      title: "Fix imports",
      description: "Fixed them. Tests pass.",
    };
    expect(validateSummaryText({ ...brief, state: "blocked" })).toEqual({
      ...brief,
      state: "blocked",
    });
    expect(() => validateSummaryText(brief)).toThrow("Invalid summary output");
    expect(() => validateSummaryText({ ...brief, state: "done" })).toThrow(
      "Invalid summary output",
    );
  });

  it("keeps quick summary separate and applies Skip's model budget", async () => {
    const input = summaryInput("codex", "root", evidence.events);
    let seen: ModelTransportRequest | undefined;
    const result = await summarizeSessionInput(
      input,
      {
        async complete(request) {
          seen = request;
          return {
            outputText: JSON.stringify({
              title: "Implement session analysis",
              description: "The agent implemented analysis.",
              state: "completed",
            }),
            inputTokens: 10,
            outputTokens: 5,
            providerRequestId: null,
          };
        },
      },
      "gpt-5.6-luna",
    );
    expect(input.messages.map((message) => message.role)).toEqual([
      "user",
      "assistant",
    ]);
    expect(seen).toMatchObject({
      schemaName: "session_summary",
      store: false,
      maxOutputTokens: 256,
      reasoningEffort: "none",
      timeoutMs: 20_000,
    });
    expect(result.summary.title).toBe("Implement session analysis");
    expect(result.summary.state).toBe("completed");
  });
});

it("decodes a produced snapshot whose full metrics exceed model input limits", async () => {
  const expanded = {
    ...evidence,
    events: Array.from({ length: 130 }, (_, index) => ({
      ...evidence.events[1]!,
      id: index + 1,
      eventUid: `event-${index}`,
      tsMs: index * 1000,
      model: `model-${index}`,
    })),
  };
  const snapshot = await buildSessionAnalysisSnapshot(
    expanded,
    hydration,
    {
      async complete(request) {
        const input = JSON.parse(request.input);
        return {
          outputText: JSON.stringify({
            title: "Many requests",
            ...unknownAnalysis(input.references[0].id),
          }),
          inputTokens: 1,
          outputTokens: 1,
          providerRequestId: "synthetic",
          model: "synthetic",
        };
      },
    },
    { source: "codex", sessionId: "root", model: "synthetic" },
  );
  expect(snapshot.metrics.models.length).toBeGreaterThan(32);
  expect(snapshot.metrics.interactionPeriods.length).toBeGreaterThan(100);
  const { decodeSessionAnalysisResult } =
    await import("../engine/session-analysis-result.ts");
  expect(
    decodeSessionAnalysisResult(
      {
        schemaVersion: 1,
        analysisVersion: "test",
        sourceFingerprint: "revision",
        analysis: snapshot,
        durationMs: 1,
      },
      { source: "codex", sessionId: "root" },
    ).status,
  ).toBe("ready");
});
