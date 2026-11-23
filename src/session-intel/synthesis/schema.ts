/**
 * The JSON Schema (draft 2020-12 subset) of workspace-synthesis/1, and a small validator for the subset it uses:
 * type (one or a list), const, enum, pattern, maxLength, minimum, maximum, properties with required and
 * additionalProperties false, propertyNames plus additionalProperties (maps), items and maxItems. It follows
 * analysis-contract.ts's approach (a hand-written walker over a const schema) rather than adding a dependency.
 * `checkSynthesis` adds the cross-field invariants a schema cannot state.
 */
import {
  CATEGORY_STATUSES, ISSUE_KINDS, SESSION_ID, SESSION_TYPES, SKIP_REASONS, SOURCE_ID, SYNTHESIS_BOUNDS,
  SYNTHESIS_CONTRACT, WORKSPACE_ID, type WorkspaceSynthesis,
} from "./synthesize.ts";
import { EVIDENCE_KINDS } from "../../session-analysis/engine/session-analysis-result.ts";

type S = Record<string, unknown>;
const ISO_MS = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$";
const obj = (properties: Record<string, S>, optional: string[] = []): S => ({
  type: "object", additionalProperties: false, properties, required: Object.keys(properties).filter((k) => !optional.includes(k)),
});
const count: S = { type: "integer", minimum: 0 };
const nullableCount: S = { type: ["integer", "null"], minimum: 0 };
const rate: S = { type: ["number", "null"], minimum: 0, maximum: 1 };
const iso: S = { type: "string", pattern: ISO_MS };
const nullableIso: S = { type: ["string", "null"], pattern: ISO_MS };
const text: S = { type: "string", maxLength: SYNTHESIS_BOUNDS.maxTextChars };
const pattern = (re: RegExp): S => ({ type: "string", pattern: re.source });
const sessionRef = obj({ source: pattern(SOURCE_ID), sessionId: pattern(SESSION_ID) });
const cap = obj({ limit: count, truncated: { type: "boolean" } });
const sourceMap = (value: S): S => ({ type: "object", propertyNames: pattern(SOURCE_ID), additionalProperties: value });

export const WORKSPACE_SYNTHESIS_SCHEMA = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  $id: "workspace-synthesis/1",
  ...obj({
    contract: { const: SYNTHESIS_CONTRACT },
    workspace: pattern(WORKSPACE_ID),
    window: obj({ since: iso, until: iso }),
    generatedAt: iso,
    inputs: obj({
      eligible: nullableCount,
      considered: { type: "integer", minimum: 0, maximum: SYNTHESIS_BOUNDS.maxSessions },
      analyzed: count,
      skipped: obj(Object.fromEntries(SKIP_REASONS.map((r) => [r, count]))),
      truncated: { type: "boolean" },
      caps: obj({
        sessions: obj({ limit: count, offered: count, truncated: { type: "boolean" } }),
        drilldown: cap, issues: cap, evidencePerIssue: cap, textChars: cap, artifactBytes: cap,
      }),
      notes: { type: "array", maxItems: 16, items: { type: "string", maxLength: SYNTHESIS_BOUNDS.maxTextChars } },
    }),
    freshness: obj({
      staleAfterMs: count,
      analyzedAtMin: nullableIso,
      analyzedAtMax: nullableIso,
      indexedThroughMs: { type: ["number", "null"], minimum: 0 },
      indexedThroughMissing: count,
      staleCount: count,
      analyzedAfterNow: count,
      bySource: sourceMap(obj({ analyzed: count, analyzedAtMin: iso, analyzedAtMax: iso, staleCount: count })),
    }),
    usageCoverage: sourceMap(obj({
      considered: count, analyzed: count, observed: count, unobserved: count, knownCost: count, nullCost: count,
      observedRate: rate, nullCostRate: rate,
    })),
    categories: {
      type: "array", maxItems: SESSION_TYPES.length * CATEGORY_STATUSES.length,
      items: obj({
        sessionType: { enum: [...SESSION_TYPES] },
        outcomeStatus: { enum: [...CATEGORY_STATUSES] },
        count: { type: "integer", minimum: 1 },
        truncated: { type: "boolean" },
        sessions: { type: "array", maxItems: SYNTHESIS_BOUNDS.maxDrilldown, items: sessionRef },
      }),
    },
    issues: {
      type: "array", maxItems: SYNTHESIS_BOUNDS.maxIssues,
      items: obj({
        id: { type: "string", pattern: `^(?:${ISSUE_KINDS.join("|")}):[0-9a-f]{16}$` },
        label: { const: "candidate" },
        kind: { enum: [...ISSUE_KINDS] },
        key: { ...text, minLength: 1 },
        keyTruncated: { type: "boolean" },
        text: { ...text, minLength: 1 },
        textTruncated: { type: "boolean" },
        roots: { type: "integer", minimum: 2 },
        sessionCount: { type: "integer", minimum: 2 },
        sessionsTruncated: { type: "boolean" },
        sessions: { type: "array", maxItems: SYNTHESIS_BOUNDS.maxDrilldown, items: sessionRef },
        evidenceTruncated: { type: "boolean" },
        evidence: {
          type: "array", maxItems: SYNTHESIS_BOUNDS.maxEvidencePerIssue,
          items: obj({
            source: pattern(SOURCE_ID), sessionId: pattern(SESSION_ID), eventId: { type: "string", minLength: 1, maxLength: 1024 },
            ts: iso, kind: { enum: [...EVIDENCE_KINDS] },
          }, ["ts", "kind"]),
        },
        basis: { const: "deterministic" },
      }),
    },
  }),
} as const;

function typeOf(v: unknown): string {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  if (typeof v === "number") return Number.isSafeInteger(v) ? "integer" : Number.isFinite(v) ? "number" : "nonfinite";
  return typeof v;
}

/** The validation errors of `value` against `schema` (the subset above), as `path: message`. */
export function schemaErrors(value: unknown, schema: S = WORKSPACE_SYNTHESIS_SCHEMA, path = "$"): string[] {
  const errors: string[] = [];
  const t = typeOf(value);
  if ("const" in schema && value !== schema.const) errors.push(`${path}: expected ${JSON.stringify(schema.const)}`);
  if (schema.enum && !(schema.enum as unknown[]).includes(value)) errors.push(`${path}: not in enum`);
  if (schema.type) {
    const types = Array.isArray(schema.type) ? (schema.type as string[]) : [schema.type as string];
    const ok = types.includes(t) || (t === "integer" && types.includes("number"));
    if (!ok) return [...errors, `${path}: expected ${types.join("|")}, got ${t}`];
  }
  if (t === "string") {
    const s = value as string;
    if (typeof schema.pattern === "string" && !new RegExp(schema.pattern, "u").test(s)) errors.push(`${path}: does not match ${schema.pattern}`);
    if (typeof schema.maxLength === "number" && [...s].length > schema.maxLength) errors.push(`${path}: longer than ${schema.maxLength}`);
    if (typeof schema.minLength === "number" && [...s].length < schema.minLength) errors.push(`${path}: shorter than ${schema.minLength}`);
  }
  if (t === "integer" || t === "number") {
    const n = value as number;
    if (typeof schema.minimum === "number" && n < schema.minimum) errors.push(`${path}: below ${schema.minimum}`);
    if (typeof schema.maximum === "number" && n > schema.maximum) errors.push(`${path}: above ${schema.maximum}`);
  }
  if (t === "array") {
    const a = value as unknown[];
    if (typeof schema.maxItems === "number" && a.length > schema.maxItems) errors.push(`${path}: more than ${schema.maxItems} items`);
    if (schema.items) a.forEach((v, i) => errors.push(...schemaErrors(v, schema.items as S, `${path}[${i}]`)));
  }
  if (t === "object") {
    const o = value as Record<string, unknown>;
    const props = (schema.properties ?? {}) as Record<string, S>;
    for (const k of (schema.required ?? []) as string[]) if (!Object.hasOwn(o, k)) errors.push(`${path}: missing ${k}`);
    for (const [k, v] of Object.entries(o)) {
      if (Object.hasOwn(props, k)) errors.push(...schemaErrors(v, props[k]!, `${path}.${k}`));
      else if (schema.additionalProperties === false) errors.push(`${path}: undeclared ${k}`);
      else if (schema.additionalProperties && typeof schema.additionalProperties === "object") {
        if (schema.propertyNames) errors.push(...schemaErrors(k, schema.propertyNames as S, `${path}{${k}}`));
        errors.push(...schemaErrors(v, schema.additionalProperties as S, `${path}.${k}`));
      }
    }
  }
  return errors;
}

/** The cross-field invariants of workspace-synthesis/1, as messages; empty when they all hold. */
export function invariantErrors(s: WorkspaceSynthesis): string[] {
  const e: string[] = [];
  const skipped = Object.values(s.inputs.skipped).reduce((a, b) => a + b, 0);
  if (s.inputs.considered !== s.inputs.analyzed + skipped) e.push("considered != analyzed + sum(skipped)");
  if (s.inputs.eligible !== null && s.inputs.eligible < s.inputs.caps.sessions.offered) e.push("eligible < offered");
  if (s.inputs.considered !== Math.min(s.inputs.caps.sessions.offered, s.inputs.caps.sessions.limit)) e.push("considered != min(offered, cap)");
  if (s.categories.reduce((a, c) => a + c.count, 0) !== s.inputs.analyzed) e.push("categories do not partition the analyzed sessions");
  const anyCap = Object.values(s.inputs.caps).some((c) => c.truncated);
  if (s.inputs.truncated !== anyCap) e.push("inputs.truncated disagrees with the caps");
  if (Date.parse(s.window.until) - Date.parse(s.window.since) > SYNTHESIS_BOUNDS.maxWindowMs || s.window.since >= s.window.until) e.push("window");
  const usage = Object.values(s.usageCoverage);
  for (const [src, u] of Object.entries(s.usageCoverage)) {
    if (u.observed + u.unobserved !== u.analyzed) e.push(`${src}: observed + unobserved != analyzed`);
    if (u.knownCost + u.nullCost !== u.analyzed) e.push(`${src}: knownCost + nullCost != analyzed`);
    if (u.analyzed > u.considered) e.push(`${src}: analyzed > considered`);
    if ((u.analyzed === 0) !== (u.observedRate === null) || (u.analyzed === 0) !== (u.nullCostRate === null)) e.push(`${src}: rate nullness`);
  }
  if (usage.reduce((a, u) => a + u.considered, 0) !== s.inputs.considered) e.push("usageCoverage.considered does not sum to considered");
  if (usage.reduce((a, u) => a + u.analyzed, 0) !== s.inputs.analyzed) e.push("usageCoverage.analyzed does not sum to analyzed");
  const f = s.freshness;
  if (s.inputs.analyzed === 0 && (f.analyzedAtMin !== null || f.indexedThroughMs !== null || f.indexedThroughMissing !== 0)) e.push("freshness of an empty set");
  if (f.indexedThroughMissing > 0 && f.indexedThroughMs !== null) e.push("indexedThroughMs must be null when any watermark is missing");
  for (const c of s.categories) if (c.truncated !== c.count > c.sessions.length) e.push(`category ${c.sessionType}/${c.outcomeStatus} truncated flag`);
  for (const i of s.issues) {
    if (i.roots > i.sessionCount) e.push(`${i.id}: roots > sessions`);
    if (i.sessionsTruncated !== i.sessionCount > i.sessions.length) e.push(`${i.id}: sessions truncated flag`);
  }
  return e;
}

/** Throws with every schema and invariant error; returns the value typed when there are none. */
export function validateWorkspaceSynthesis(value: unknown): WorkspaceSynthesis {
  const errors = schemaErrors(value);
  if (!errors.length) errors.push(...invariantErrors(value as WorkspaceSynthesis));
  if (errors.length) throw new Error(`workspace-synthesis/1 invalid: ${errors.slice(0, 20).join("; ")}`);
  return value as WorkspaceSynthesis;
}
