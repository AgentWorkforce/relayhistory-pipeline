/**
 * Pipeline plugin runtime v1. Manifests are pipeline-team source artifacts in this release: there
 * is no customer manifest loader, prompt/schema input, webhook, or worker execution path.
 *
 * Relayfacts is an adapter over the existing 062080/062090 decision records and 062150 claims.
 * It deliberately does not invent generic record history: the tables are current views, their UUIDs
 * are row identities rather than revisions, and erase/fencing remains owned by 062090/062100.
 */
import { extractClaims, type ClaimsPort, claimsHook } from "./claims.ts";
import {
  type DecisionRecordsSink,
  decisionRecordsHook,
  extractDecisionRecords,
} from "./decision-records.ts";
import type { AnalysisJob, AnalysisJobOutput } from "./processor.ts";

export const PIPELINE_PLUGIN_API_VERSION = "pipeline.relayhistory.dev/v1alpha1" as const;
export const RELAYFACTS_PLUGIN_ID = "relayfacts" as const;

export interface PipelinePluginManifest {
  apiVersion: typeof PIPELINE_PLUGIN_API_VERSION;
  kind: "PipelinePlugin";
  metadata: {
    id: string;
    publisher: "pipeline-team";
    manifestVersion: number;
  };
  spec: {
    input: {
      source: "completed-session-analysis";
      workspaceBinding: "job";
    };
    extractor: {
      kind: "managed-adapter";
      implementation: string;
      customerConfiguration: "none";
      modelExecution: "none";
    };
    output: {
      storage: "existing-relayfacts-tables";
      view: "current";
      logicalIdentity: readonly ["workspace", "plugin", "topic", "record"];
      adapterIdentity: readonly ["workspace", "topic", "normalized-title"];
      revisionAddressing: "unsupported";
    };
    exposure: {
      http: "legacy-relayfacts-routes";
      mcp: "legacy-relayfacts-tools";
      claims: "internal-only";
      databaseRole: "bulls96_read_api";
    };
    retention: {
      erase: "operator-session-erase-062090-062100";
      continuesWhenDisabled: true;
    };
  };
}

/** The only v1 manifest. `satisfies` makes the source artifact the schema boundary. */
export const RELAYFACTS_PLUGIN_MANIFEST = Object.freeze({
  apiVersion: PIPELINE_PLUGIN_API_VERSION,
  kind: "PipelinePlugin",
  metadata: {
    id: RELAYFACTS_PLUGIN_ID,
    publisher: "pipeline-team",
    manifestVersion: 1,
  },
  spec: {
    input: {
      source: "completed-session-analysis",
      workspaceBinding: "job",
    },
    extractor: {
      kind: "managed-adapter",
      implementation: "relayfacts-current-view-v1",
      customerConfiguration: "none",
      modelExecution: "none",
    },
    output: {
      storage: "existing-relayfacts-tables",
      view: "current",
      logicalIdentity: ["workspace", "plugin", "topic", "record"],
      adapterIdentity: ["workspace", "topic", "normalized-title"],
      revisionAddressing: "unsupported",
    },
    exposure: {
      http: "legacy-relayfacts-routes",
      mcp: "legacy-relayfacts-tools",
      claims: "internal-only",
      databaseRole: "bulls96_read_api",
    },
    retention: {
      erase: "operator-session-erase-062090-062100",
      continuesWhenDisabled: true,
    },
  },
} as const satisfies PipelinePluginManifest);

const MANIFESTS: ReadonlyMap<string, PipelinePluginManifest> = new Map([
  [RELAYFACTS_PLUGIN_ID, RELAYFACTS_PLUGIN_MANIFEST],
]);

/** Static registry only: unknown/customer-supplied manifests are not executable in v1. */
export function pipelinePluginManifest(id: string): PipelinePluginManifest | undefined {
  return MANIFESTS.get(id);
}

/** Per-plugin activation is exact-true and therefore defaults off. */
export function relayfactsPluginEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.PIPELINE_PLUGIN_RELAYFACTS_ENABLED === "true";
}

export type RelayfactsDelivery = "decisions" | "claims";
export interface PipelinePluginScope {
  orgId: string;
  workspaceId: string;
}

/**
 * The operator-owned policy boundary for a plugin in one workspace. Implementations provide the
 * rollout control and quota/model-spend decision; numeric limits intentionally do not live here.
 * Any false result or error is a fail-closed refusal for this plugin delivery only.
 */
export interface PipelinePluginPolicy {
  enabled(scope: PipelinePluginScope, pluginId: typeof RELAYFACTS_PLUGIN_ID): Promise<boolean>;
  admit(input: {
    scope: PipelinePluginScope;
    pluginId: typeof RELAYFACTS_PLUGIN_ID;
    delivery: RelayfactsDelivery;
    records: number;
    /** Relayfacts reuses the completed analysis and runs no plugin model. */
    modelTokens: 0;
  }): Promise<boolean>;
}

/** Used when activation was requested without an operator policy. */
export const denyPipelinePluginPolicy: PipelinePluginPolicy = Object.freeze({
  enabled: async () => false,
  admit: async () => false,
});

export interface RelayfactsPluginRuntime {
  manifest: typeof RELAYFACTS_PLUGIN_MANIFEST;
  decisions: DecisionRecordsSink;
  claims?: ClaimsPort;
  policy: PipelinePluginPolicy;
}

export function relayfactsPluginRuntime(input: {
  decisions: DecisionRecordsSink;
  claims?: ClaimsPort;
  policy?: PipelinePluginPolicy;
}): RelayfactsPluginRuntime {
  return {
    manifest: RELAYFACTS_PLUGIN_MANIFEST,
    decisions: input.decisions,
    ...(input.claims ? { claims: input.claims } : {}),
    policy: input.policy ?? denyPipelinePluginPolicy,
  };
}

async function admitted(
  policy: PipelinePluginPolicy,
  input: Parameters<PipelinePluginPolicy["admit"]>[0],
): Promise<boolean> {
  try { return await policy.admit(input); }
  catch { return false; }
}

/**
 * One completion hook, two independent writer deliveries. The analysis is already committed before
 * this hook runs. Decisions are attempted first; a refused or failed claim delivery cannot roll back
 * them. Existing hooks retain their exact extraction, idempotency, RH016, and claims-control rules.
 */
export function relayfactsPluginHook(
  runtime: RelayfactsPluginRuntime,
  counts: Record<string, number>,
): (job: AnalysisJob, output: AnalysisJobOutput) => Promise<void> {
  const writeDecisions = decisionRecordsHook(runtime.decisions, counts);
  const writeClaims = runtime.claims ? claimsHook(runtime.claims, counts) : undefined;
  return async (job, output) => {
    const scope = { orgId: job.orgId, workspaceId: job.workspaceId };
    let enabled = false;
    try { enabled = await runtime.policy.enabled(scope, RELAYFACTS_PLUGIN_ID); }
    catch { enabled = false; }
    if (!enabled) {
      counts.plugin_relayfacts_control_off = (counts.plugin_relayfacts_control_off ?? 0) + 1;
      return;
    }

    const decisionCount = extractDecisionRecords(output.result.analysis).length;
    if (decisionCount > 0) {
      if (await admitted(runtime.policy, {
        scope, pluginId: RELAYFACTS_PLUGIN_ID, delivery: "decisions", records: decisionCount, modelTokens: 0,
      }))
        await writeDecisions(job, output);
      else counts.plugin_relayfacts_decisions_quota_off = (counts.plugin_relayfacts_decisions_quota_off ?? 0) + 1;
    }

    if (!writeClaims) return;
    const claimCount = extractClaims(output.result.analysis).records.length;
    if (claimCount === 0) return;
    if (await admitted(runtime.policy, {
      scope, pluginId: RELAYFACTS_PLUGIN_ID, delivery: "claims", records: claimCount, modelTokens: 0,
    }))
      await writeClaims(job, output);
    else counts.plugin_relayfacts_claims_quota_off = (counts.plugin_relayfacts_claims_quota_off ?? 0) + 1;
  };
}
