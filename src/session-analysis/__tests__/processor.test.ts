/**
 * The full analysis over canonical evidence: relayhistory stream records -> the
 * pipeline's own Neon-table projection -> relayhistory's evidence normalization ->
 * the vendored engine (fake model transport) -> a verified result object.
 * Nothing here reaches a network, a database or a real model.
 */
import { describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey } from "../../../packages/object-store/src/index.ts";
import { projectRelayhistorySession } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import type { RelayhistoryStreamRecord } from "@rhc/lib/pipeline/relayhistory-transform.ts";
import type {
  ModelTransportRequest,
  ModelTransportResponse,
} from "../engine/model.ts";
import { decodeSessionAnalysisResult } from "../engine/session-analysis-result.ts";
import { SessionAnalysisProviderError } from "../../session-brief/engine/transport.ts";
import { CanonicalReadError } from "../../session-read-api/canonical.ts";
import {
  apiEvent,
  apiTurn,
  loadSessionSource,
  SessionSourceIncompleteError,
  SessionSourceNotFoundError,
  type AnalysisEvidenceDeps,
  type CanonicalScopeRows,
} from "../evidence.ts";
import {
  classifyAnalysisFailure,
  processSessionAnalysis,
  runAnalysisJob,
  type AnalysisJob,
  type AnalysisJobStore,
} from "../processor.ts";
import {
  putAnalysisResult,
  analysisResultKey,
  analysisResultPrefix,
  readAnalysisResult,
  writeAnalysisResult,
} from "../results.ts";
import {
  SESSION_ANALYSIS_SUMMARY_MODEL_VERSION,
  SESSION_ANALYSIS_SUMMARY_VERSION,
  SESSION_ANALYSIS_MODEL_VERSION,
  SESSION_ANALYSIS_RESULT_SCHEMA_VERSION,
  SESSION_ANALYSIS_VERSION,
} from "../versions.ts";

const SCOPE = { orgId: "org-a", workspaceId: "ws-a", sessionId: "s-1" };
const JOB_ID = "0b6f3a3e-6a59-4d5c-9b1e-2f7f6c1d9a10";

let seq = 0;
const rec = (kind: string, payload: Record<string, unknown>): RelayhistoryStreamRecord => {
  seq += 1;
  return {
    kind,
    origin_id: "origin-1",
    record_id: `r-${String(seq).padStart(4, "0")}`,
    revision: "1",
    operation: "upsert",
    received_at: "2026-11-05T12:00:00.000000+00:00",
    payload,
  };
};

function records(): RelayhistoryStreamRecord[] {
  seq = 0;
  const t = Date.parse("2026-11-05T11:00:00Z");
  return [
    rec("session_event", { role: "user", text: "Fix the flaky queue test", ts_ms: t }),
    rec("session_event", {
      role: "assistant",
      text: "Looking at the queue test. data:image/png;base64,AAAA",
      ts_ms: t + 1_000,
      message_id: "msg-1",
      model: "claude-opus",
    }),
    rec("tool_call", {
      name: "Edit",
      target: "src/queue.ts",
      tool_use_id: "toolu_1",
      args_json: JSON.stringify({ file_path: "src/queue.ts" }),
      is_error: "false",
      ts_ms: t + 2_000,
    }),
    rec("file_edit", { file_path: "src/queue.ts", tool_use_id: "toolu_1", ts_ms: t + 2_000 }),
    rec("session_event", { role: "assistant", text: "Fixed the race in acks.", ts_ms: t + 3_000 }),
  ];
}

function projectedRows(recs = records()): CanonicalScopeRows {
  const projection = projectRelayhistorySession(recs, {
    workspaceId: SCOPE.workspaceId,
    source: "claude",
    sessionId: SCOPE.sessionId,
    machineId: "m-1",
    userId: "u-1",
  });
  return {
    exists: true,
    generation: "a".repeat(64),
    legacyCoverage: { version: 1, tables: ["conversation_turns", "session_links", "session_outcomes"] },
    rows: projection.rows.map(({ table, row }) => ({
      table,
      row: { org_id: SCOPE.orgId, workspace_id: SCOPE.workspaceId, ...row },
    })),
  };
}

const deps = (over: Partial<AnalysisEvidenceDeps> = {}, rows = projectedRows()): AnalysisEvidenceDeps => ({
  harnesses: async () => ["claude"],
  snapshot: async () => rows,
  ...over,
});

function analysisFor(firstId: string) {
  const claim = { text: "Fix the flaky queue test", evidenceIds: [firstId], confidence: "high", basis: "observed" };
  const unknown = { value: "unknown", explanation: "Unclear", evidenceIds: [], confidence: "low", basis: "unknown" };
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

function fakeTransport(seen: ModelTransportRequest[] = []) {
  return {
    async complete(request: ModelTransportRequest): Promise<ModelTransportResponse> {
      seen.push(request);
      if (request.schemaName === "session_summary")
        return {
          outputText: JSON.stringify({ title: "Fix the flaky queue test", description: "Fixed a race in acks.", state: "completed" }),
          inputTokens: 30,
          outputTokens: 10,
          providerRequestId: "resp-0",
          model: "gpt-5.6-luna-2026-10-01",
        };
      const input = JSON.parse(request.input) as { references: Array<{ id: string }> };
      return {
        outputText: JSON.stringify({ title: "Fix the flaky queue test", ...analysisFor(input.references[0]!.id) }),
        inputTokens: 120,
        outputTokens: 40,
        providerRequestId: "resp-1",
        model: "gpt-5.4-2026-09-01",
      };
    },
  };
}

const job = (over: Partial<AnalysisJob> = {}): AnalysisJob => ({
  id: JOB_ID,
  ...SCOPE,
  analysisVersion: SESSION_ANALYSIS_VERSION,
  schemaVersion: SESSION_ANALYSIS_RESULT_SCHEMA_VERSION,
  modelVersion: SESSION_ANALYSIS_MODEL_VERSION,
  summaryVersion: SESSION_ANALYSIS_SUMMARY_VERSION,
  summaryModelVersion: SESSION_ANALYSIS_SUMMARY_MODEL_VERSION,
  sourceRevision: "r".repeat(64),
  attempts: 1,
  leaseToken: "8d1c8f4e-1b0a-4f3e-9a51-0c2e3d4f5a6b",
  ...over,
});

const tempStore = () => createFsStore({ root: mkdtempSync(join(tmpdir(), "analysis-")) });
const PREFIX = analysisResultPrefix("org-a/ws-a/m-1/claude/2026-11-05/s-1/manifest.json");

describe("canonical evidence adapter", () => {
  test("apiEvent has exactly relayhistory shapeEvent's fields plus workspace provenance", () => {
    const row = projectedRows().rows.find((r) => r.table === "convergence_events")!.row;
    expect(Object.keys(apiEvent(row)).sort()).toEqual(
      [
        "eventId", "userId", "sessionId", "source", "lens", "kind", "type", "ts", "actorName",
        "actorRole", "subagentId", "machineId", "projectId", "trajectoryId", "taskRef", "taskTitle",
        "taskDescription", "taskStatus", "content", "contentTruncated", "significance", "confidence",
        "tags", "model", "provider", "usage", "costUsdMicros", "toolName", "toolStatus", "toolCalls",
        "filesTouched", "durationMs", "workspaceId",
      ].sort(),
    );
    expect(apiEvent({ ...row, ts: "2026-11-05T11:00:00.123456+00:00", confidence_basis_points: 2500 })).toMatchObject({
      ts: "2026-11-05T11:00:00.123Z",
      confidence: 0.25,
      contentTruncated: false,
    });
  });

  test("apiTurn is listConversationTurns' selection, with no workspace", () => {
    expect(
      apiTurn({
        id: 7, session_id: "s-1", session_owner: "o", turn_index: 2, role: "user", content: "hi",
        actor_name: "a", actor_role: "user", metadata: {}, ts: "2026-11-05T11:00:00+00:00", workspace_id: "ws-a",
      }),
    ).toEqual({
      id: "7", sessionId: "s-1", sessionOwner: "o", turnIndex: 2, role: "user", content: "hi",
      actorName: "a", actorRole: "user", metadata: {}, ts: "2026-11-05T11:00:00.000Z",
    });
  });

  test("loads the projected scope as relayhistory's evidence, scrubbing embedded images", async () => {
    const source = await loadSessionSource(deps(), SCOPE);
    expect(source.sourceProvider).toBe("claude");
    expect(source.sourceWorkspaceId).toBe("ws-a");
    expect(source.evidenceWorkspaceIds).toEqual(["ws-a"]);
    expect(source.sourceGeneration).toBe("a".repeat(64));
    expect(source.sourceFingerprint).toMatch(/^[0-9a-f]{64}$/);
    // relayhistory normalizedEvent's role/kind mapping over the projected rows.
    expect(source.evidence.events.map((e) => [e.role, e.kind, e.sourceKind])).toEqual([
      ["user", "text", "session_event"],
      ["assistant", "text", "session_event"],
      ["unknown", "tool_call", "tool_call"],
      ["unknown", "file_edit", "file_edit"],
      ["assistant", "text", "session_event"],
    ]);
    const texts = source.evidence.events.map((e) => e.text ?? "").join("\n");
    expect(texts).toContain("[embedded image omitted from text analysis]");
    expect(texts).not.toContain("base64,AAAA");
    expect(source.evidence.tools).toHaveLength(1);
    expect(source.evidence.tools[0]).toMatchObject({ name: "Edit", toolUseId: "toolu_1", isError: false });
    expect(source.evidence.edits.map((e) => e.filePath)).toEqual(["src/queue.ts"]);
  });

  test("the fingerprint is deterministic and moves with the evidence", async () => {
    const a = await loadSessionSource(deps(), SCOPE);
    const b = await loadSessionSource(deps(), SCOPE);
    expect(a.sourceFingerprint).toBe(b.sourceFingerprint);
    const more = records();
    more.push(rec("session_event", { role: "user", text: "Also add a test", ts_ms: Date.parse("2026-11-05T11:10:00Z") }));
    const c = await loadSessionSource(deps({}, projectedRows(more)), SCOPE);
    expect(c.sourceFingerprint).not.toBe(a.sourceFingerprint);
  });

  test("refuses an absent session and a harness collision as relayhistory did", async () => {
    await expect(loadSessionSource(deps({ harnesses: async () => [] }), SCOPE)).rejects.toBeInstanceOf(
      SessionSourceNotFoundError,
    );
    await expect(
      loadSessionSource(deps({ harnesses: async () => ["claude", "codex"] }), SCOPE),
    ).rejects.toBeInstanceOf(SessionSourceIncompleteError);
    await expect(
      loadSessionSource(deps({ snapshot: async () => ({ exists: false, generation: "x", legacyCoverage: null, rows: [] }) }), SCOPE),
    ).rejects.toBeInstanceOf(SessionSourceNotFoundError);
    await expect(loadSessionSource(deps(), { ...SCOPE, sessionId: " " })).rejects.toBeInstanceOf(
      SessionSourceNotFoundError,
    );
  });

  test("refuses a scope without the legacy coverage marker unless the operator assumes no turns", async () => {
    const rows = { ...projectedRows(), legacyCoverage: null };
    await expect(loadSessionSource(deps({}, rows), SCOPE)).rejects.toThrow("legacy evidence coverage is unknown");
    expect((await loadSessionSource(deps({}, rows), SCOPE, { assumeNoLegacyTurns: true })).sourceProvider).toBe("claude");
  });

  test("refuses a projected row from another source", async () => {
    const rows = projectedRows();
    rows.rows.find((r) => r.table === "convergence_events")!.row.source = "codex";
    await expect(loadSessionSource(deps({}, rows), SCOPE)).rejects.toBeInstanceOf(SessionSourceIncompleteError);
  });
});

describe("processSessionAnalysis", () => {
  test("produces relayhistory's v1 job result over canonical evidence", async () => {
    const seen: ModelTransportRequest[] = [];
    const out = await processSessionAnalysis(job(), { evidence: deps(), transport: fakeTransport(seen) });
    // Both halves from one evidence read: relayhistory's brief, then the full analysis.
    expect(seen.map((r) => [r.schemaName, r.model])).toEqual([
      ["session_summary", "gpt-5.6-luna"],
      ["session_analysis", "gpt-5.4"],
    ]);
    expect(out.result.brief).toMatchObject({
      schemaVersion: 1,
      promptVersion: "session-summary-v3",
      title: "Fix the flaky queue test",
      state: "completed",
      messageCount: 3,
      backend: { model: "gpt-5.6-luna-2026-10-01", inputTokens: 30, outputTokens: 10 },
    });
    expect(out.result.brief.inputFingerprint).toMatch(/^[0-9a-f]{64}$/);
    expect(out.harness).toBe("claude");
    expect(out.result.schemaVersion).toBe(1);
    expect(out.result.analysisVersion).toBe(SESSION_ANALYSIS_VERSION);
    expect(out.result.sourceFingerprint).toBe(out.sourceFingerprint);
    expect(out.result.analysis.title).toBe("Fix the flaky queue test");
    expect(out.result.analysis.backend?.requestId).toBe(JOB_ID);
    expect(out.inputTokens).toBe(150);
    // The v1 result is relayhistory's `a.result || {brief: b.result}`; the analysis channel
    // (what decodeSessionAnalysisResult reads) is the envelope without `brief`.
    const { brief: _brief, ...envelope } = out.result;
    expect(decodeSessionAnalysisResult(envelope, { source: "claude", sessionId: "s-1" }).status).toBe("ready");
  });

  test("refuses an unsupported version before any read or model call", async () => {
    let reads = 0;
    const seen: ModelTransportRequest[] = [];
    await expect(
      processSessionAnalysis(job({ summaryModelVersion: "gpt-4o" }), {
        evidence: deps({ harnesses: async () => (reads++, ["claude"]) }),
        transport: fakeTransport(seen),
      }),
    ).rejects.toThrow("unsupported_analysis_version");
    expect(reads).toBe(0);
    expect(seen).toHaveLength(0);
  });

  test("a session with no briefable message is ineligible", async () => {
    const recs = [rec("tool_call", { name: "Read", target: "a", tool_use_id: "t", ts_ms: 1 })];
    await expect(
      processSessionAnalysis(job(), { evidence: deps({}, projectedRows(recs)), transport: fakeTransport() }),
    ).rejects.toThrow("session_analysis_ineligible");
  });
});

describe("results in object storage", () => {
  test("write is create-only and verified; read refuses tampering", async () => {
    const store = tempStore();
    const out = await processSessionAnalysis(job(), { evidence: deps(), transport: fakeTransport() });
    const stored = await writeAnalysisResult(store, PREFIX, JOB_ID, out.result);
    expect(stored.key).toBe(analysisResultKey(PREFIX, JOB_ID, stored.sha256));
    expect(stored.key.startsWith("org-a/ws-a/m-1/claude/2026-11-05/s-1/normalized/session-analysis-v2/")).toBe(true);
    expect(await readAnalysisResult(store, stored)).toEqual(out.result);
    // Idempotent rewrite of identical bytes.
    expect(await writeAnalysisResult(store, PREFIX, JOB_ID, out.result)).toEqual(stored);
    await expect(readAnalysisResult(store, { ...stored, sha256: "0".repeat(64) })).rejects.toThrow(
      "analysis_result_mismatch",
    );
    await store.delete(objectKey(stored.key));
    await expect(readAnalysisResult(store, stored)).rejects.toThrow("analysis_result_missing");
  });

  test("refuses keys outside the layout", () => {
    expect(() => analysisResultPrefix("a/b/other.json")).toThrow("analysis_anchor_invalid");
    expect(() => analysisResultKey(PREFIX, "../x", "0".repeat(64))).toThrow("analysis_result_key_invalid");
  });
});

describe("runAnalysisJob", () => {
  function memoryStore(claimed: AnalysisJob | null, opts: { lose?: boolean; refuseReserve?: boolean } = {}) {
    const calls: Array<[string, unknown]> = [];
    const results = tempStore();
    const store: AnalysisJobStore = {
      async claim(o) {
        calls.push(["claim", o]);
        return claimed;
      },
      async reserve(j, _out, planned) {
        calls.push(["reserve", planned.sha256]);
        return opts.refuseReserve ? null : analysisResultKey(PREFIX, j.id, planned.sha256);
      },
      async put(key, bytes, sha) {
        calls.push(["put", key]);
        await putAnalysisResult(results, key, bytes, sha);
      },
      async complete(input) {
        calls.push(["complete", { jobId: input.job.id, key: input.stored.key, fp: input.output.sourceFingerprint }]);
        return !opts.lose;
      },
      async fail(input) {
        calls.push(["fail", input]);
        return !opts.lose;
      },
    };
    return { store, calls };
  }

  test("idle, completed, lease-lost", async () => {
    expect(await runAnalysisJob(memoryStore(null).store, { evidence: deps(), transport: fakeTransport() })).toBe("idle");
    const ok = memoryStore(job());
    expect(await runAnalysisJob(ok.store, { evidence: deps(), transport: fakeTransport() })).toBe("completed");
    expect(ok.calls[0]).toEqual(["claim", { leaseSeconds: 600 }]);
    expect(ok.calls.slice(1).map((c) => c[0])).toEqual(["reserve", "put", "complete"]);
    const lost = memoryStore(job(), { lose: true });
    expect(await runAnalysisJob(lost.store, { evidence: deps(), transport: fakeTransport() })).toBe("lease-lost");
  });

  test("erase-safe: nothing is written when the reservation fails (lease, liveness or revision)", async () => {
    const refused = memoryStore(job(), { refuseReserve: true });
    expect(await runAnalysisJob(refused.store, { evidence: deps(), transport: fakeTransport() })).toBe("lease-lost");
    expect(refused.calls.map((c) => c[0])).toEqual(["claim", "reserve"]);
  });

  test("persists only controlled codes", async () => {
    const failing = memoryStore(job());
    const transport = {
      async complete(): Promise<ModelTransportResponse> {
        throw new SessionAnalysisProviderError("model_rate_limited", true);
      },
    };
    expect(await runAnalysisJob(failing.store, { evidence: deps(), transport })).toBe("failed");
    expect(failing.calls.at(-1)).toEqual([
      "fail",
      { jobId: JOB_ID, leaseToken: job().leaseToken, error: "model_rate_limited", retryable: true },
    ]);
    const leaky = memoryStore(job());
    const secret = {
      async complete(): Promise<ModelTransportResponse> {
        throw new Error("sk-secret transcript text");
      },
    };
    await runAnalysisJob(leaky.store, { evidence: deps(), transport: secret });
    expect(JSON.stringify(leaky.calls)).not.toContain("secret");
    expect(leaky.calls.at(-1)![1]).toMatchObject({ error: "analysis_processing_failed", retryable: true });
  });

  test("failure classification matches relayhistory runSessionJob", () => {
    expect(classifyAnalysisFailure(new SessionSourceNotFoundError())).toEqual({ code: "session_not_found", retryable: false });
    expect(classifyAnalysisFailure(new SessionSourceIncompleteError("x"))).toEqual({
      code: "session_evidence_incomplete",
      retryable: false,
    });
    expect(
      classifyAnalysisFailure(new CanonicalReadError("canonical_projection_incomplete", 503, ["manifest_unscrubbed"])),
    ).toEqual({ code: "session_quarantined", retryable: false });
    expect(classifyAnalysisFailure(new CanonicalReadError("source_generation_changed"))).toEqual({
      code: "session_evidence_unavailable",
      retryable: true,
    });
    expect(classifyAnalysisFailure(new TypeError("bad"))).toEqual({ code: "model_output_invalid", retryable: false });
    expect(classifyAnalysisFailure(Object.assign(new Error("x"), { retryable: false }))).toEqual({
      code: "analysis_processing_failed",
      retryable: false,
    });
  });
});
