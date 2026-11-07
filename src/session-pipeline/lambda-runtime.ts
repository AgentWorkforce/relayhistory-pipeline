/** Bind a reviewed Lambda runtime to the existing service work. The adapter
 * owns platform ack/retry mechanics; the service retains pointer/search locks,
 * busy requeue, reconciliation and every existing invocation branch. */
import type { PipelineRuntimeAdapter, PipelineWork, WorkItem, WorkOutcome } from "@rhc/runtime/contracts.ts";
import type { BatchResponse } from "./consumer.ts";

export interface PipelineLambdaEvent {
  Records?: { messageId: string; body: string; eventSource?: string; attributes?: Record<string, string> }[];
  reconcile?: unknown; brief?: unknown; sessionAnalysis?: unknown; analysisCache?: unknown; analysisCacheReconcile?: unknown; semanticCopy?: unknown;
  schedule?: string; analysisCursor?: unknown; cursor?: string; since?: string; link?: number;
}
export interface LambdaRuntimeBinding {
  handler(event: unknown): Promise<BatchResponse | void>;
}
export type PipelineInvoker = (event: PipelineLambdaEvent) => Promise<BatchResponse | { ok: boolean }>;
const SCHEDULES = { reconcile: "reconcile", brief: "brief", "session-analysis": "sessionAnalysis", "analysis-cache": "analysisCache", "analysis-cache-reconcile": "analysisCacheReconcile", "semantic-copy": "semanticCopy" } as const;
type Schedule = keyof typeof SCHEDULES;

function queueBody(item: Extract<WorkItem, { type: "manifest-landed" }>): string {
  return JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: item.deleted ? "ObjectRemoved:Delete" : "ObjectCreated:Put", s3: {
    ...(item.bucket ? { bucket: { name: item.bucket } } : {}),
    object: { key: encodeURIComponent(item.key), ...(item.etag ? { eTag: item.etag } : {}), ...(item.version ? { versionId: item.version } : {}), ...(item.sequencer ? { sequencer: item.sequencer } : {}) },
  } }] });
}

export async function dispatchRegisteredLambda(adapter: PipelineRuntimeAdapter, event: PipelineLambdaEvent, invoke: PipelineInvoker): Promise<BatchResponse | { ok: boolean }> {
  const caps = adapter.capabilities();
  if (adapter.kind !== "lambda" || caps.kind !== "lambda" || caps.trigger !== "push" || !caps.partialBatchFailure || caps.scopeSerialization !== "advisory-lock") throw new Error("Lambda runtime composition incompatible");
  if (event.schedule !== undefined && (typeof event.schedule !== "string" || !Object.hasOwn(SCHEDULES, event.schedule))) throw new Error("Lambda invocation schedule invalid");
  const flags = Object.entries(SCHEDULES).filter(([, key]) => event[key] === true);
  if (Array.isArray(event.Records) && (flags.length || event.schedule) || flags.length > 1 ||
      event.schedule !== undefined && flags.length === 1 && event.schedule !== flags[0]![0]) throw new Error("Lambda invocation ambiguous");
  const job = event.schedule ?? flags[0]?.[0];
  if (!Array.isArray(event.Records) && (!job || !Object.hasOwn(SCHEDULES, job))) return invoke(event);
  const work: PipelineWork = {
    schedules: Object.keys(SCHEDULES).map(job => ({ job, everySeconds: 60 })),
    async handle(items) {
      const results = new Map<string, WorkOutcome>();
      const manifests = items.filter((item): item is Extract<WorkItem, { type: "manifest-landed" }> => item.type === "manifest-landed");
      if (manifests.length) {
        const answer = await invoke({ Records: manifests.map(item => ({ messageId: item.id, body: queueBody(item) })) });
        if (!("batchItemFailures" in answer)) throw new Error("pipeline batch response invalid");
        const failed = new Set(answer.batchItemFailures.map(f => f.itemIdentifier));
        for (const item of manifests) results.set(item.id, failed.has(item.id) ? { status: "retry", delaySeconds: 0, code: "service_retry" } : { status: "done" });
      }
      for (const item of items) if (item.type === "schedule") {
        if (!Object.hasOwn(SCHEDULES, item.job)) { results.set(item.id, { status: "dead", code: "unknown_schedule" }); continue; }
        // Continuation cursor/since/link and analysis cursor stay attached to
        // this invocation; no mutable global current-event slot can race.
        const answer = await invoke({ ...event, schedule: undefined, [SCHEDULES[item.job as Schedule]]: true });
        results.set(item.id, "ok" in answer && answer.ok ? { status: "done" } : { status: "retry", delaySeconds: 0, code: "schedule_retry" });
      }
      return results;
    },
  };
  const binding = adapter.bind(work) as Partial<LambdaRuntimeBinding> | undefined;
  if (typeof binding?.handler !== "function") throw new Error("Lambda runtime handler missing");
  const result = await binding.handler(Array.isArray(event.Records) ? event : { ...event, schedule: job });
  if (!Array.isArray(event.Records)) return { ok: true };
  const ids = new Set(event.Records.map(r => r.messageId));
  if (!result || !Array.isArray(result.batchItemFailures) || result.batchItemFailures.some(f => typeof f?.itemIdentifier !== "string" || !ids.has(f.itemIdentifier))) throw new Error("Lambda runtime partial response invalid");
  return result;
}
