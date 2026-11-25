#!/usr/bin/env bun
/**
 * The search-queue split's staged rollout and rollback, rendered from the stack's own props
 * (stacks/aws/session-pipeline.ts) with placeholder names. No AWS, no state, no credentials: each
 * stage's resources are rendered from its inputs and compared with the previous stage's, so the
 * listing shows what each capture's plan must contain (create, update with the changed keys, noop),
 * the pooler demand of the stage, and the most any moment of the apply into it can hold.
 *
 *   bun scripts/search-split-staged-render.ts            # print
 *   bun scripts/search-split-staged-render.ts --check    # exit 1 unless docs/handoffs/search-queue-split.staged-render.txt is current
 *
 * The deploy-guard phase printed for each stage is asserted against assertTranscriptStorePlanContract
 * in stacks/aws/__tests__/session-pipeline.test.ts ("search-queue split: staged rollout").
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  ALARM_DEFAULTS,
  POOLER_BUDGET,
  assertSessionPipelineInputs,
  sessionEventsMappingProps,
  sessionPipelineFunctionProps,
  sessionPipelineNames,
  sessionPoolDemand,
  sessionPoolSettings,
  sessionPoolTransitionDemand,
  sessionSearchAlarms,
  sessionSearchDlqProps,
  sessionSearchMappingProps,
  sessionSearchQueueProps,
  type SessionPipelineInputs,
} from "../stacks/aws/session-pipeline.ts";

export interface SearchSplitStage {
  id: string;
  title: string;
  /** The stage's sessionPipeline inputs (only the settings this rollout moves; every other live setting is carried unchanged). */
  p: SessionPipelineInputs;
  /** The deploy guard's verdict on this stage's capture (asserted in the stack tests). */
  phase: string;
}

/**
 * The live shape this rollout starts from: search on, the pointer mapping at 20, HOOPDB_POOL_MAX 3
 * (set by hand on the function; 20 x 3 = 60). The pool is never raised back to 4, in either direction.
 */
const LIVE: SessionPipelineInputs = { searchIndex: true, maxConcurrency: 20, hoopdbPoolMax: 3 };
const QUEUE = { enabled: true, maxConcurrency: 16 } as const;

export const SEARCH_SPLIT_ROLLOUT: readonly SearchSplitStage[] = [
  { id: "live", title: "Live before the split (pointer mapping 20, HOOPDB_POOL_MAX 3)", p: LIVE, phase: "post-apply" },
  { id: "A", title: "Provisioning only: search queue, DLQ, queue policy, alarms, search mapping created DISABLED", p: { ...LIVE, searchQueue: QUEUE }, phase: "session search queue rollout" },
  { id: "B", title: "Pointer mapping MaximumConcurrency 20 -> 10", p: { ...LIVE, maxConcurrency: 10, searchQueue: QUEUE }, phase: "session pipeline update" },
  { id: "C1", title: "Search mapping enabled (MaximumConcurrency 16)", p: { ...LIVE, maxConcurrency: 10, searchQueue: { ...QUEUE, mappingEnabled: true } }, phase: "session pipeline update" },
  { id: "C2", title: "Handoff on: SESSION_PIPELINE_SEARCH_QUEUE_URL set (function env only)", p: { ...LIVE, maxConcurrency: 10, searchQueue: { ...QUEUE, mappingEnabled: true, handoff: true } }, phase: "session pipeline update" },
];

export const SEARCH_SPLIT_ROLLBACK: readonly SearchSplitStage[] = [
  { id: "R1", title: "Handoff off: SESSION_PIPELINE_SEARCH_QUEUE_URL removed; the search mapping keeps draining the search queue", p: { ...LIVE, maxConcurrency: 10, searchQueue: { ...QUEUE, mappingEnabled: true } }, phase: "session pipeline update" },
  { id: "R2", title: "Search mapping disabled (once the search queue and its in-flight count are 0)", p: { ...LIVE, maxConcurrency: 10, searchQueue: QUEUE }, phase: "session pipeline update" },
  { id: "R3", title: "Pointer mapping MaximumConcurrency 10 -> 20 (HOOPDB_POOL_MAX stays 3; search resources stay, mapping disabled)", p: { ...LIVE, searchQueue: QUEUE }, phase: "session pipeline update" },
];

/** Shortcuts the stack or the guard refuses, with why (each is asserted in the stack tests). */
export const SEARCH_SPLIT_REFUSED: readonly { title: string; from: SessionPipelineInputs; to: SessionPipelineInputs; refusedBy: string }[] = [
  {
    title: "Enable search before lowering the pointer cap (pointer 20 + search 16 at HOOPDB_POOL_MAX 3)",
    from: SEARCH_SPLIT_ROLLOUT[1]!.p,
    to: { ...SEARCH_SPLIT_ROLLOUT[1]!.p, searchQueue: { ...QUEUE, mappingEnabled: true } },
    refusedBy: "the stack render (assertSessionPipelineInputs: over the budget)",
  },
  {
    title: "Lower the pointer cap and enable search in one apply (from A)",
    from: SEARCH_SPLIT_ROLLOUT[1]!.p,
    to: SEARCH_SPLIT_ROLLOUT[3]!.p,
    refusedBy: "the deploy guard (the events and search mappings change in separate plans)",
  },
  {
    title: "Raise the pool back to 4 at the split's caps (pointer 10 + search 16)",
    from: SEARCH_SPLIT_ROLLOUT[4]!.p,
    to: { ...SEARCH_SPLIT_ROLLOUT[4]!.p, hoopdbPoolMax: 4 },
    refusedBy: "the stack render (assertSessionPipelineInputs: over the budget)",
  },
];

const P = { stage: "<stage>", account: "<account>", region: "<region>", bucket: "<transcripts-bucket>", role: "<pipeline-role-arn>" } as const;

/** Every resource the split touches, rendered from `p` with placeholder names (absent when not declared). */
export function renderSearchSplitResources(p: SessionPipelineInputs): Record<string, unknown> {
  const names = sessionPipelineNames(P.stage, P.account, P.region);
  const tags = { "96bulls:component": "session-pipeline" };
  const fn = sessionPipelineFunctionProps({ stage: P.stage, accountId: P.account, region: P.region, bucket: P.bucket, p, tags: () => tags });
  const out: Record<string, unknown> = {
    "session-pipeline": { env: fn.env },
    "session-events-mapping": sessionEventsMappingProps({ p, functionName: names.function, queueArn: names.queueArn, tags }),
  };
  const q = p.searchQueue;
  if (q?.enabled === true) {
    out["session-search-dlq"] = sessionSearchDlqProps({ names, tags });
    out["session-search"] = sessionSearchQueueProps({ names, roleArn: P.role, dlqArn: names.searchDlqArn, tags });
    out["session-search-mapping"] = sessionSearchMappingProps({ q, functionName: names.function, queueArn: names.searchQueueArn, tags });
    for (const { id, props } of sessionSearchAlarms(names)) out[id] = { ...ALARM_DEFAULTS, ...props, tags };
  }
  return out;
}

/** The leaf values of a rendered value, by dotted path. */
function leaves(value: unknown, path = ""): Map<string, string> {
  const out = new Map<string, string>();
  if (Array.isArray(value)) value.forEach((v, n) => leaves(v, `${path}[${n}]`).forEach((x, k) => out.set(k, x)));
  else if (value !== null && typeof value === "object")
    for (const k of Object.keys(value).sort()) leaves((value as Record<string, unknown>)[k], path ? `${path}.${k}` : k).forEach((x, kk) => out.set(kk, x));
  else if (value !== undefined) out.set(path, JSON.stringify(value));
  return out;
}

function refusal(p: SessionPipelineInputs): string | undefined {
  try {
    assertSessionPipelineInputs(p, (why) => {
      throw new Error(why);
    });
    return undefined;
  } catch (e) {
    return (e as Error).message;
  }
}

function poolLine(p: SessionPipelineInputs): string {
  const s = sessionPoolSettings(p);
  const searchNote = p.searchQueue?.enabled === true && s.search === 0 ? " (search mapping disabled)" : "";
  return `(pointer ${s.pointer} + search ${s.search}${searchNote}) x HOOPDB_POOL_MAX ${s.pool} = ${sessionPoolDemand(p)}`;
}

function renderStage(prev: SearchSplitStage | undefined, stage: SearchSplitStage): string[] {
  const lines = [`== ${stage.id}: ${stage.title}`, `inputs: ${JSON.stringify(stage.p)}`];
  const refused = refusal(stage.p);
  if (refused) throw new Error(`stage ${stage.id} does not render: ${refused}`);
  const before = prev ? renderSearchSplitResources(prev.p) : undefined;
  const now = renderSearchSplitResources(stage.p);
  for (const resource of [...new Set([...Object.keys(before ?? {}), ...Object.keys(now)])].sort()) {
    if (!before) {
      lines.push(`[${resource}] ${resource in now ? "rendered" : "absent"}`);
      continue;
    }
    if (!(resource in now)) {
      lines.push(`[${resource}] delete`);
      continue;
    }
    const a = resource in before ? leaves(before[resource]) : undefined;
    const b = leaves(now[resource]);
    if (!a) {
      lines.push(`[${resource}] create`);
      for (const [k, v] of b) lines.push(`    ${k}: ${v}`);
      continue;
    }
    const changed = [...new Set([...a.keys(), ...b.keys()])].filter((k) => a.get(k) !== b.get(k));
    lines.push(`[${resource}] ${changed.length ? "update" : "noop"}`);
    for (const k of changed) lines.push(`    ${k}: ${a.get(k) ?? "(absent)"} -> ${b.get(k) ?? "(absent)"}`);
  }
  lines.push(`pooler demand: ${poolLine(stage.p)} <= ${POOLER_BUDGET}`);
  if (prev) lines.push(`apply worst case ${prev.id} -> ${stage.id}: ${sessionPoolTransitionDemand(prev.p, stage.p)} <= ${POOLER_BUDGET}`);
  lines.push(`deploy guard: ${stage.phase}`);
  return lines;
}

export function renderSearchSplitStages(): string {
  const out = [
    "Search-queue split: staged render (placeholders only; scripts/search-split-staged-render.ts)",
    `Budget: ${POOLER_BUDGET} pooler connections, never raised. Each capture changes one capacity setting.`,
    "Resources not listed are noop in every stage (store, grants, schedules, other alarms).",
    "Live HOOPDB_POOL_MAX is 3 (set by hand); hoopdbPoolMax: 3 is required in every stage and is never raised.",
    "A function env update (C2, R1) replaces the function's whole env with the rendered one: see the doc, hand-set variables.",
    "",
  ];
  const forward = SEARCH_SPLIT_ROLLOUT;
  forward.forEach((stage, n) => out.push(...renderStage(forward[n - 1], stage), ""));
  out.push("-- Rollback (from C2) --", "");
  const back = [forward[forward.length - 1]!, ...SEARCH_SPLIT_ROLLBACK];
  back.slice(1).forEach((stage, n) => out.push(...renderStage(back[n], stage), ""));
  out.push("-- Refused shortcuts --", "");
  for (const r of SEARCH_SPLIT_REFUSED) {
    const why = refusal(r.to);
    const worst = sessionPoolTransitionDemand(r.from, r.to);
    out.push(`x ${r.title}`);
    out.push(`    target ${poolLine(r.to)}${why ? ` > ${POOLER_BUDGET}` : ` <= ${POOLER_BUDGET}`}`);
    out.push(`    apply worst case ${worst} > ${POOLER_BUDGET}`);
    out.push(`    refused by ${r.refusedBy}`);
  }
  out.push("");
  return out.join("\n");
}

export const STAGED_RENDER_PATH = "docs/handoffs/search-queue-split.staged-render.txt";

if (import.meta.main) {
  const text = renderSearchSplitStages();
  if (process.argv.includes("--check")) {
    const file = readFileSync(fileURLToPath(new URL(`../${STAGED_RENDER_PATH}`, import.meta.url)), "utf8");
    if (file !== text) {
      console.error(`${STAGED_RENDER_PATH} is stale: rerun bun scripts/search-split-staged-render.ts > ${STAGED_RENDER_PATH}`);
      process.exit(1);
    }
  } else process.stdout.write(text);
}
