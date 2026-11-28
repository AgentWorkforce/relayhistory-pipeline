/**
 * Per-org wave runs of the scoped backlog tools (docs/backlog-ops-workflow.md "Org waves"). With the dispatch input
 * `per_org` (BACKLOG_OPS_PER_ORG=true, PER_ORG_MODES only, a scoped run), the credential step (scripts/backlog-ops.ts)
 * runs the UNCHANGED tool once per scope org, as a single-org scoped run, at most waveConcurrencyFor(mode) at once.
 * Each org's bindings are checked on their own (waveView), and each org gets its own verdict. One org's refusal never
 * stops or changes another's. Without `per_org`, every receipt is byte-identical to before.
 *
 * The wave receipt: the usual top-level fields at the WAVE's scope, and
 *   summary = { kind: "wave", org_count, ok_count, per_org: [ { org_hash, exit_code, summary } ] }
 * where each slice's summary is exactly the single-org receipt summary the tool's run produced, after that mode's own
 * checks (countsOnly, and the exact backfill / catch-up schema where one exists). waveReceiptSummary enforces:
 *   - the exact shape;
 *   - per_org hashes equal the scope's, each exactly once;
 *   - org_count is the scope's size and ok_count is DERIVED (exit_code 0 and a summary that is not a refusal);
 *   - every slice passes its mode's check.
 * Anything else refuses. The arrays are deliberate (waveCountsOnly): only the per_org list of objects; each slice is
 * still countsOnly'd under its own mode's rules (no lists unless that mode's receipt allows them).
 *
 * Bindings. A consumer that binds a wave receipt checks it PER ORG, through waveView: the org's slice as a single-org
 * receipt (the wave's top-level fields with the org's single-org scope id, its exit_code and summary). A non-wave
 * receipt bound by a wave run gets the same per-org view: its scope must first be exactly the wave's, then it is
 * re-scoped to the org with its per-org lists (authority evidence, baseline, reshard verdicts) sliced to that org.
 * The existing per-receipt checks run unchanged on each view, so one org's binding failing refuses that org alone.
 * Views are for checking only. Every evidence hash (the marker attempt's, the bridge's) stays sha256 over the bound
 * file's exact bytes, never over a view.
 *
 * Leases. The export and the drain execute hold a one-at-a-time S3 lease, so their children run one after another.
 * Dry runs and the backfill run up to WAVE_TOOL_CONCURRENCY at once.
 */
import { createHash } from "node:crypto";
import { evidenceHash } from "./authority-evidence.ts";
import type { BacklogOpsMode } from "./backlog-ops.ts";

export { BACKLOG_OPS_PER_ORG_MODES as PER_ORG_MODES } from "./backlog-ops.ts";
/** Children at once for a tool without a lease (backfill, dry runs). */
export const WAVE_TOOL_CONCURRENCY = 2;
export const waveConcurrencyFor = (mode: BacklogOpsMode) => (mode === "export-execute" || mode === "drain-execute" ? 1 : WAVE_TOOL_CONCURRENCY);

const HASH = /^h_[0-9a-f]{32}$/u;
const isObj = (v: unknown): v is Record<string, any> => v !== null && typeof v === "object" && !Array.isArray(v);
export class WaveReceiptRefusal extends Error { constructor(readonly code: string) { super(code); this.name = "WaveReceiptRefusal"; } }
const refuse = (code: string): never => { throw new WaveReceiptRefusal(code); };

/** The single-org scope id backlogOpsScope gives for exactly [org]. */
export const orgScopeId = (org: string) => "orgs_" + createHash("sha256").update(org).digest("hex").slice(0, 32);

/**
 * One org's slice. `carried_ms`: present only on a slice carried from an earlier incomplete run of the same execute
 * (input previous_run): that run's created_at, so a consumer applies its freshness rule from when the org really ran.
 */
export interface WaveSlice { org_hash: string; exit_code: number; summary: unknown; carried_ms?: number }
export interface WaveSummary { kind: "wave"; org_count: number; ok_count: number; per_org: WaveSlice[] }
export const sliceOk = (s: WaveSlice) => s.exit_code === 0 && isObj(s.summary) && !Object.hasOwn(s.summary, "refused");

/** Build the wave summary from per-org slices (in scope order); ok_count derived. */
export function waveSummary(orgs: readonly string[], slices: readonly { org: string; exitCode: number; summary: unknown; carriedMs?: number }[]): WaveSummary {
  if (slices.length !== orgs.length || !slices.every((s, i) => s.org === orgs[i])) refuse("backlog_ops_wave_slices");
  const per_org = slices.map((s) => ({ org_hash: evidenceHash("org", s.org), exit_code: s.exitCode, summary: s.summary,
    ...(s.carriedMs !== undefined ? { carried_ms: s.carriedMs } : {}) }));
  return { kind: "wave", org_count: orgs.length, ok_count: per_org.filter(sliceOk).length, per_org };
}

/**
 * The wave summary a receipt may carry (before both sinks): exact keys, per_org hashes exactly the scope's each once,
 * counts derived, every slice summary accepted by `slice` (its mode's own check; returns the summary to keep or throws).
 * Returns the checked summary or {"refused": "backlog_ops_receipt_wave"}.
 */
export function waveReceiptSummary(summary: unknown, orgs: readonly string[] | null, slice: (s: unknown) => unknown,
  ctx: { previousRun?: string | null; writtenMs?: number } = {}): { summary: unknown; ok: boolean } {
  const bad = { summary: { refused: "backlog_ops_receipt_wave" }, ok: false };
  if (!orgs || orgs.length === 0 || !isObj(summary)) return bad;
  const keys = Object.keys(summary).sort();
  if (keys.join(",") !== "kind,ok_count,org_count,per_org" || summary.kind !== "wave" || !Array.isArray(summary.per_org)) return bad;
  const want = orgs.map((o) => evidenceHash("org", o)).sort();
  const per = summary.per_org as unknown[];
  if (per.length !== want.length || summary.org_count !== want.length) return bad;
  const out: WaveSlice[] = [];
  for (const p of per) {
    const pk = isObj(p) ? Object.keys(p).sort().join(",") : "";
    if (!isObj(p) || (pk !== "exit_code,org_hash,summary" && pk !== "carried_ms,exit_code,org_hash,summary") || typeof p.org_hash !== "string" || !HASH.test(p.org_hash) ||
        !Number.isSafeInteger(p.exit_code) || p.exit_code < 0 || p.exit_code > 255) return bad;
    // A carried slice is a completed org (only those are carried), with the earlier run's creation time.
    if (pk.startsWith("carried_ms") && (!Number.isSafeInteger(p.carried_ms) || p.carried_ms < 1 || p.exit_code !== 0)) return bad;
    let s: unknown;
    try { s = slice(p.summary); } catch { return bad; }
    out.push({ org_hash: p.org_hash, exit_code: p.exit_code, summary: s, ...(p.carried_ms !== undefined ? { carried_ms: p.carried_ms } : {}) });
    if (p.carried_ms !== undefined && !sliceOk(out[out.length - 1]!)) return bad;
    // A carried slice needs the receipt's previous_run, and ran no later than the receipt was written.
    if (p.carried_ms !== undefined && (typeof ctx.previousRun !== "string" || !/^[1-9][0-9]{0,19}$/u.test(ctx.previousRun) ||
        !Number.isSafeInteger(ctx.writtenMs) || p.carried_ms > ctx.writtenMs!)) return bad;
  }
  const got = out.map((p) => p.org_hash).sort();
  if (!got.every((h, i) => h === want[i])) return bad;
  const ok = out.filter(sliceOk).length;
  if (summary.ok_count !== ok) return bad;
  return { summary: { kind: "wave", org_count: want.length, ok_count: ok, per_org: out }, ok: ok > 0 };
}

/** True when a receipt (raw JSON) carries a wave summary. */
export function isWaveReceipt(raw: string): boolean {
  try { const r = JSON.parse(raw); return isObj(r) && isObj(r.summary) && r.summary.kind === "wave"; } catch { return false; }
}

/** A per-org list sliced to one org hash (`key` the hash field); an org that is absent leaves an empty list. */
const sliceList = (list: unknown, key: string, h: string) => (Array.isArray(list) ? list.filter((x) => isObj(x) && x[key] === h) : list);

/**
 * One org's view of a bound receipt (raw JSON), for the existing per-receipt checks. `waveScope` is the wave's scope
 * id: a non-wave receipt must carry exactly it (else the view keeps its own scope, so the check refuses). A wave
 * receipt must also carry the wave's scope; its view is the org's slice. Refuses `backlog_ops_wave_view` when the
 * receipt is not JSON, or a wave receipt has no slice for the org.
 */
export function waveView(raw: string, org: string, waveScope: string): string {
  let r: Record<string, any>;
  try { r = JSON.parse(raw); } catch { return refuse("backlog_ops_wave_view"); }
  if (!isObj(r)) refuse("backlog_ops_wave_view");
  const h = evidenceHash("org", org), scope = r.scope === waveScope ? orgScopeId(org) : r.scope;
  if (isObj(r.summary) && r.summary.kind === "wave") {
    const s = (Array.isArray(r.summary.per_org) ? r.summary.per_org : []).filter((p: unknown) => isObj(p) && p.org_hash === h);
    if (s.length !== 1) refuse("backlog_ops_wave_view");
    // Consumer hardening: a carried slice only in a retry's receipt, never later than the receipt was written.
    if (s[0].carried_ms !== undefined && (typeof r.previous_run !== "string" || !/^[1-9][0-9]{0,19}$/u.test(r.previous_run) ||
        !Number.isSafeInteger(r.written_ms) || !Number.isSafeInteger(s[0].carried_ms) || s[0].carried_ms > r.written_ms)) refuse("backlog_ops_wave_view");
    return JSON.stringify({ ...r, scope, exitCode: s[0].exit_code, summary: s[0].summary, ...(s[0].carried_ms !== undefined ? { carried_ms: s[0].carried_ms } : {}) });
  }
  const s = isObj(r.summary) ? { ...r.summary } : r.summary;
  if (isObj(s)) {
    // authority-dry: the evidence's per-org verdicts (and its count).
    if (isObj(s.evidence) && Array.isArray(s.evidence.orgs)) s.evidence = { ...s.evidence, orgs: sliceList(s.evidence.orgs, "org_hash", h), org_count: 1 };
    // reshard-dry: per-org verify verdicts.
    if (s.tool === "reshard_verify" && Array.isArray(s.per_org)) {
      s.per_org = sliceList(s.per_org, "org_hash", h);
      s.org_count = 1;
      s.verify_passed = (s.per_org as Record<string, unknown>[]).length === 1 && (s.per_org as Record<string, unknown>[])[0]!.verify_passed === true;
    }
    // a non-wave catch-up with a baseline: the baseline's per-org entries.
    if (isObj(s.authority_baseline) && Array.isArray(s.authority_baseline.orgs))
      s.authority_baseline = { ...s.authority_baseline, orgs: sliceList(s.authority_baseline.orgs, "org_hash", h), org_count: 1 };
  }
  return JSON.stringify({ ...r, scope, summary: s });
}

/** Per-org baselines (module shape), every one ready, merged into one baseline over those orgs (for the marker). */
export function mergeBaselines(baselines: readonly unknown[]): unknown {
  if (baselines.length === 0) refuse("backlog_ops_wave_view");
  const orgs: unknown[] = [];
  for (const b of baselines as Record<string, any>[]) {
    if (!isObj(b) || b.kind !== "authority_baseline" || b.status !== "authority_baseline_ready" || !Array.isArray(b.orgs) || b.orgs.length !== 1) refuse("backlog_ops_wave_view");
    orgs.push(b.orgs[0]);
  }
  return { kind: "authority_baseline", v: 1, status: "authority_baseline_ready", orgCount: orgs.length, orgs };
}
