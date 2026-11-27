/**
 * Org waves for decision 0005's per-org cutover (docs/backlog-ops-workflow.md "Org waves"). A WAVE is one scoped
 * backlog-ops sequence over up to BACKLOG_OPS_MAX_ORGS (64) orgs: the scope secret BACKLOG_OPS_ORGS names them, every
 * run of the sequence (FULL, catch-up, reshard verify, marker, drain) binds that exact scope, and each org gets its own
 * verdict. This module holds what the per-org tools share:
 *
 *   waveOrgList   the wave's org list from untrusted input: lowercase org UUIDs, 1..64, no duplicate (a duplicate is
 *                 refused, never deduplicated: the scope's id would silently differ from what was approved), sorted.
 *   runWave       bounded parallel per-org work: at most `concurrency` orgs in flight (1..WAVE_MAX_CONCURRENCY), every
 *                 org settled independently (one org's refusal or throw never stops or changes another's), results
 *                 in the input order. A throw becomes that org's fixed code (`codeOf`), never its message.
 *   waveId        the identity an approval pins: a domain-separated sha256 over the commit, the scope id, every bound
 *                 run and each org's hash with its hold epoch's hash. `h_` + 32 hex, so it fits a counts-only receipt
 *                 and a confirmation token; it names no org, epoch or run by value.
 *
 * Single-org compatibility: a wave of one org is an ordinary scoped run (concurrency is capped at the org count).
 * Nothing here reads a store, a database or the environment, and nothing here logs: org ids stay in memory.
 */
import { createHash } from "node:crypto";

/** The most orgs one wave (one scoped run) may name: backlog-ops.ts BACKLOG_OPS_MAX_ORGS. */
export const WAVE_MAX_ORGS = 64;
/** The most orgs a wave tool works on at once (each holds its own database session and S3 requests). */
export const WAVE_MAX_CONCURRENCY = 16;
export const WAVE_DEFAULT_CONCURRENCY = 8;
const ORG_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const CODE = /^[a-z][a-z0-9_]{0,63}$/u;
const HASH = /^h_[0-9a-f]{32}$/u;
const SHA40 = /^[0-9a-f]{40}$/u;
const SCOPE_ID = /^orgs_[0-9a-f]{32}$/u;
const RUN_ID = /^[1-9][0-9]{0,19}$/u;

export class WaveRefusal extends Error {
  constructor(readonly code: string) { super(code); this.name = "WaveRefusal"; }
}
const refuse = (code: string): never => { throw new WaveRefusal(code); };

/**
 * The wave's orgs: an array of lowercase org UUIDs, 1..WAVE_MAX_ORGS, each once. Returns a sorted copy. Refuses
 * `wave_orgs_empty`, `wave_orgs_oversized`, `wave_org_invalid` or `wave_org_duplicate` (never the value).
 */
export function waveOrgList(orgs: unknown): string[] {
  if (!Array.isArray(orgs)) return refuse("wave_org_invalid");
  if (orgs.length === 0) refuse("wave_orgs_empty");
  if (orgs.length > WAVE_MAX_ORGS) refuse("wave_orgs_oversized");
  if (!orgs.every((o) => typeof o === "string" && ORG_UUID.test(o))) refuse("wave_org_invalid");
  if (new Set(orgs).size !== orgs.length) refuse("wave_org_duplicate");
  return [...(orgs as string[])].sort();
}

/** The effective concurrency: 1..WAVE_MAX_CONCURRENCY, never above the org count. */
export function waveConcurrency(requested: number, orgCount: number): number {
  if (!Number.isSafeInteger(requested) || requested < 1 || requested > WAVE_MAX_CONCURRENCY) refuse("wave_concurrency_invalid");
  return Math.max(1, Math.min(requested, orgCount));
}

/** A thrown value's fixed code: its own `code` when code-shaped and allowed, else `fallback`. Never its message. */
export function codeOf(error: unknown, allowed: ReadonlySet<string>, fallback: string): string {
  const c = (error as { code?: unknown } | null)?.code;
  return typeof c === "string" && CODE.test(c) && allowed.has(c) ? c : fallback;
}

export type WaveSettled<T> = { org: string; ok: true; value: T } | { org: string; ok: false; code: string };

/**
 * `fn` for every org, at most `concurrency` at once, each settled on its own. A throw is recorded as that org's
 * code (codeOf over `allowed`, else `fallback`) and the others go on. Results follow the input order. `orgs` must
 * already be a validated wave (waveOrgList); it is re-checked for duplicates and size, never deduplicated.
 */
export async function runWave<T>(orgs: readonly string[], concurrency: number, fn: (org: string, index: number) => Promise<T>,
  opts: { allowed?: ReadonlySet<string>; fallback?: string } = {}): Promise<WaveSettled<T>[]> {
  if (orgs.length === 0) refuse("wave_orgs_empty");
  if (orgs.length > WAVE_MAX_ORGS) refuse("wave_orgs_oversized");
  if (new Set(orgs).size !== orgs.length) refuse("wave_org_duplicate");
  const width = waveConcurrency(concurrency, orgs.length);
  const allowed = opts.allowed ?? new Set<string>(), fallback = opts.fallback ?? "wave_org_failed";
  const out: WaveSettled<T>[] = new Array(orgs.length);
  let next = 0;
  const worker = async () => {
    for (;;) {
      const i = next++;
      if (i >= orgs.length) return;
      const org = orgs[i]!;
      try { out[i] = { org, ok: true, value: await fn(org, i) }; }
      catch (error) { out[i] = { org, ok: false, code: codeOf(error, allowed, fallback) }; }
    }
  };
  await Promise.all(Array.from({ length: width }, worker));
  return out;
}

export interface WaveIdentity {
  /** The dispatched main commit. */
  sha: string;
  /** backlog-ops.ts backlogOpsScope(...).id of the wave's scope (`orgs_` + 32 hex). */
  scope: string;
  /** Every run the wave's step binds, by role (e.g. authority, backfill, full, reshard). */
  runs: Readonly<Record<string, string>>;
  /** Each org's hash and its hold epoch's hash (`h_` + 32 hex), any order. */
  orgs: readonly { orgHash: string; epochHash: string }[];
}

/**
 * The wave's identity: `h_` + the first 32 hex of sha256 over a versioned, line-delimited canonical form (roles and
 * orgs sorted). Two waves differ in it when they differ in the commit, the scope, any bound run, any org or any org's
 * hold epoch. Refuses `wave_identity_invalid` on any malformed part.
 */
export function waveId(w: WaveIdentity): string {
  const roles = Object.keys(w.runs).sort();
  if (!SHA40.test(w.sha) || !SCOPE_ID.test(w.scope) || roles.length === 0 || roles.length > 8 ||
      !roles.every((r) => /^[a-z]{1,16}$/u.test(r) && RUN_ID.test(w.runs[r]!)) ||
      w.orgs.length === 0 || w.orgs.length > WAVE_MAX_ORGS || !w.orgs.every((o) => HASH.test(o.orgHash) && HASH.test(o.epochHash)) ||
      new Set(w.orgs.map((o) => o.orgHash)).size !== w.orgs.length)
    refuse("wave_identity_invalid");
  const lines = ["org-wave/v1", `sha ${w.sha}`, `scope ${w.scope}`, ...roles.map((r) => `run ${r} ${w.runs[r]}`),
    ...[...w.orgs].sort((a, b) => (a.orgHash < b.orgHash ? -1 : a.orgHash > b.orgHash ? 1 : 0)).map((o) => `org ${o.orgHash} ${o.epochHash}`)];
  return "h_" + createHash("sha256").update(lines.join("\n")).digest("hex").slice(0, 32);
}
