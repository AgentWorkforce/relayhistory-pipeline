import { describe, expect, test } from "bun:test";
import type { SessionAnalysisSnapshot } from "../engine/session-analysis-result.ts";
import type { AnalysisJob, AnalysisJobOutput } from "../processor.ts";
import type { ClaimsPort } from "../claims.ts";
import type { DecisionRecordInput, DecisionRecordsSink } from "../decision-records.ts";
import {
  PIPELINE_PLUGIN_API_VERSION,
  pipelinePluginManifest,
  RELAYFACTS_PLUGIN_MANIFEST,
  relayfactsPluginEnabled,
  relayfactsPluginHook,
  relayfactsPluginRuntime,
  type PipelinePluginPolicy,
} from "../pipeline-plugins.ts";
import { analysisTickDeps } from "../runtime-deps.ts";

const snapshot = {
  source: "claude",
  sessionId: "session-a",
  analyzedAt: "2026-11-26T12:00:00.000Z",
  evidence: {
    references: [{
      id: "r1", source: "claude", sessionId: "session-a", kind: "human-message",
      at: "2026-11-26T11:00:00.000Z", summary: "unused", detail: "unused", truncated: false,
      isError: null, toolCallId: null, locator: { eventUid: null, toolUseId: null, fileEditId: null },
    }],
  },
  metrics: { filesChanged: [{ path: "src/session-analysis/worker.ts", editCount: 1 }] },
  understanding: {
    decisions: [{ text: "Keep relayfacts on its existing tables.", evidenceIds: ["r1"], confidence: "high", basis: "observed" }],
    constraints: [{ text: "Never expose claims through service_role.", evidenceIds: ["r1"], confidence: "high", basis: "observed" }],
    intents: [],
  },
} as unknown as SessionAnalysisSnapshot;

const job = { orgId: "org-a", workspaceId: "workspace-a" } as AnalysisJob;
const output = { result: { analysis: snapshot } } as unknown as AnalysisJobOutput;

function allowPolicy(calls: string[] = []): PipelinePluginPolicy {
  return {
    enabled: async (scope, plugin) => { calls.push(`enabled:${plugin}:${scope.orgId}:${scope.workspaceId}`); return true; },
    admit: async ({ delivery, records, modelTokens, scope }) => {
      calls.push(`admit:${delivery}:${records}:${modelTokens}:${scope.orgId}:${scope.workspaceId}`);
      return true;
    },
  };
}

function claimsPort(over: Partial<ClaimsPort> = {}): ClaimsPort {
  return {
    enabled: async () => true,
    insert: async (_scope, records) => ({ inserted: records.length, merged: 0, evidence_added: records.length }),
    current: async () => [],
    supersede: async () => false,
    ...over,
  };
}

describe("pipeline plugin v1 manifest and activation", () => {
  test("the registry contains only the team-published current-view relayfacts adapter", () => {
    expect(pipelinePluginManifest("relayfacts")).toBe(RELAYFACTS_PLUGIN_MANIFEST);
    expect(pipelinePluginManifest("customer-upload")).toBeUndefined();
    expect(RELAYFACTS_PLUGIN_MANIFEST).toMatchObject({
      apiVersion: PIPELINE_PLUGIN_API_VERSION,
      metadata: { id: "relayfacts", publisher: "pipeline-team", manifestVersion: 1 },
      spec: {
        extractor: { kind: "managed-adapter", customerConfiguration: "none", modelExecution: "none" },
        output: {
          storage: "existing-relayfacts-tables", view: "current",
          logicalIdentity: ["workspace", "plugin", "topic", "record"],
          adapterIdentity: ["workspace", "topic", "normalized-title"],
          revisionAddressing: "unsupported",
        },
        exposure: { http: "legacy-relayfacts-routes", claims: "internal-only", databaseRole: "bulls96_read_api" },
        retention: { erase: "operator-session-erase-062090-062100", continuesWhenDisabled: true },
      },
    });
  });

  test("the per-plugin flag is exact-true and defaults off", () => {
    expect(relayfactsPluginEnabled({})).toBe(false);
    for (const value of ["1", "TRUE", "True", " true", "on", ""]) {
      expect(relayfactsPluginEnabled({ PIPELINE_PLUGIN_RELAYFACTS_ENABLED: value })).toBe(false);
    }
    expect(relayfactsPluginEnabled({ PIPELINE_PLUGIN_RELAYFACTS_ENABLED: "true" })).toBe(true);
  });

  const deps = (env: NodeJS.ProcessEnv, policy?: PipelinePluginPolicy) => analysisTickDeps({
    env, db: { query: async () => ({ rows: [] }) } as never, store: {} as never,
    bucket: "bucket", region: "region", keyPrefix: "prefix", modelKey: null,
    credentials: async () => ({}) as never, ...(policy ? { pluginPolicy: policy } : {}),
  } as never);

  test("adapter activation requires both the existing outer flag and the new plugin flag", () => {
    expect("relayfactsPlugin" in deps({ PIPELINE_PLUGIN_RELAYFACTS_ENABLED: "true" })).toBe(false);
    const legacy = deps({ SUMMARY_DECISION_RECORDS_ENABLED: "true" });
    expect(typeof legacy.decisionRecords).toBe("function");
    expect("relayfactsPlugin" in legacy).toBe(false);
    const adapter = deps({ SUMMARY_DECISION_RECORDS_ENABLED: "true", PIPELINE_PLUGIN_RELAYFACTS_ENABLED: "true" });
    expect("decisionRecords" in adapter).toBe(false);
    expect(adapter.relayfactsPlugin?.manifest).toBe(RELAYFACTS_PLUGIN_MANIFEST);
  });
});

describe("relayfacts plugin runtime contract", () => {
  test("plugin control off is fail-closed before quota or either writer", async () => {
    const calls: string[] = [];
    const policy: PipelinePluginPolicy = {
      enabled: async () => false,
      admit: async () => { calls.push("admit"); return true; },
    };
    const counts: Record<string, number> = {};
    await relayfactsPluginHook(relayfactsPluginRuntime({
      decisions: async () => { calls.push("decisions"); return null; },
      claims: claimsPort({ insert: async () => { calls.push("claims"); return null; } }),
      policy,
    }), counts)(job, output);
    expect(calls).toEqual([]);
    expect(counts).toEqual({ plugin_relayfacts_control_off: 1 });
  });

  test("claims control off leaves the completed decision delivery intact", async () => {
    const calls: string[] = [];
    const counts: Record<string, number> = {};
    await relayfactsPluginHook(relayfactsPluginRuntime({
      decisions: async () => { calls.push("decisions"); return { inserted: 1, merged: 0, evidence_added: 1 }; },
      claims: claimsPort({ enabled: async () => { calls.push("claims-control"); return false; } }),
      policy: allowPolicy(calls),
    }), counts)(job, output);
    expect(calls).toEqual([
      "enabled:relayfacts:org-a:workspace-a",
      "admit:decisions:1:0:org-a:workspace-a",
      "decisions",
      "admit:claims:1:0:org-a:workspace-a",
      "claims-control",
    ]);
    expect(counts).toEqual({ decision_records: 1, claim_records_control_off: 1 });
  });

  test("quota admission is per workspace and per delivery", async () => {
    const calls: string[] = [];
    const policy = allowPolicy(calls);
    policy.admit = async ({ delivery, scope, pluginId, modelTokens }) => {
      calls.push(`admit:${pluginId}:${delivery}:${scope.workspaceId}:${modelTokens}`);
      return delivery === "decisions";
    };
    const counts: Record<string, number> = {};
    await relayfactsPluginHook(relayfactsPluginRuntime({
      decisions: async () => { calls.push("decisions"); return { inserted: 1, merged: 0, evidence_added: 1 }; },
      claims: claimsPort({ insert: async () => { calls.push("claims"); return null; } }),
      policy,
    }), counts)(job, output);
    expect(calls).toContain("admit:relayfacts:decisions:workspace-a:0");
    expect(calls).toContain("admit:relayfacts:claims:workspace-a:0");
    expect(calls).toContain("decisions");
    expect(calls).not.toContain("claims");
    expect(counts).toEqual({ decision_records: 1, plugin_relayfacts_claims_quota_off: 1 });
  });

  test("a claims writer failure cannot roll back analysis or decisions", async () => {
    const written: string[] = [];
    const counts: Record<string, number> = { completed: 1 };
    await relayfactsPluginHook(relayfactsPluginRuntime({
      decisions: async () => { written.push("decision"); return { inserted: 1, merged: 0, evidence_added: 1 }; },
      claims: claimsPort({ insert: async () => { written.push("claim"); throw new Error("claims unavailable"); } }),
      policy: allowPolicy(),
    }), counts)(job, output);
    expect(written).toEqual(["decision", "claim"]);
    expect(counts).toEqual({ completed: 1, decision_records: 1, claim_records_failed: 1 });
  });

  test("replay delegates identical records to an idempotent current-view sink", async () => {
    const rows = new Map<string, DecisionRecordInput>();
    const sink: DecisionRecordsSink = async (scope, records) => {
      let inserted = 0;
      let merged = 0;
      for (const record of records) {
        const key = `${scope.workspaceId}\u0000${record.topic}\u0000${record.title.toLowerCase()}`;
        if (rows.has(key)) merged += 1;
        else { rows.set(key, record); inserted += 1; }
      }
      return { inserted, merged, evidence_added: inserted };
    };
    const counts: Record<string, number> = {};
    const hook = relayfactsPluginHook(relayfactsPluginRuntime({ decisions: sink, policy: allowPolicy() }), counts);
    await hook(job, output);
    await hook(job, output);
    expect(rows.size).toBe(1);
    expect(counts).toEqual({ decision_records: 2 });
  });

  test("RH016 on a duplicate with no new evidence remains a fenced delivery", async () => {
    let call = 0;
    const sink: DecisionRecordsSink = async () => {
      call += 1;
      if (call === 2) throw Object.assign(new Error("session erased"), { code: "RH016" });
      return { inserted: 1, merged: 0, evidence_added: 1 };
    };
    const counts: Record<string, number> = {};
    const hook = relayfactsPluginHook(relayfactsPluginRuntime({ decisions: sink, policy: allowPolicy() }), counts);
    await hook(job, output);
    await hook(job, output);
    expect(counts).toEqual({ decision_records: 1, decision_records_fenced: 1 });
  });
});
