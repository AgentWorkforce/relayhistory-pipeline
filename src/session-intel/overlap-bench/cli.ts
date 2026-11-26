/**
 * Overlap benchmark CLI (docs/session-intel-overlap-bench.md).
 *
 *   bun run src/session-intel/overlap-bench/cli.ts climb --harvest <dir>[,<dir>...] --forge <dir> --labels <labels.json> --out <dir>
 *   bun run src/session-intel/overlap-bench/cli.ts heldout --harvest <dir> --forge <dir> --labels <labels.json> --frozen <frozen.json> --out <dir>
 *
 * `climb` scores every grid point of every detector on the dev split only and writes the log and the best point
 * per detector (frozen.json, with a sha256 of params and sources). `heldout` scores exactly the frozen points once
 * and refuses to run twice for the same frozen file.
 */
import { createHash } from "node:crypto";
import { closeSync, existsSync, mkdirSync, openSync, readdirSync, readFileSync, readSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { basename, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import type { DirectoryRow, SessionFacts } from "../cloud-harvest/harvest.ts";
import type { PullRequest } from "../forge/types.ts";
import { type Alert, type Context, concurrentEdits, type ForgePr, openPrOverlap, sharedTrigger } from "./detectors.ts";
import { outcomeFactsBaseline } from "./baseline.ts";
import { buildModel, communicated, type ObservedPolicy } from "./model.ts";
import { type IncidentLabel, type NegativeLabel, score, type Score } from "./score.ts";
import { components, type Components, rank, rankerGrid } from "./ranker.ts";
import { adjudicationSample, type AlertVerdict, ceiling, evaluate } from "./eval.ts";
import { describe as describePair } from "./bundle.ts";

const readJsonl = <T>(p: string): T[] => readFileSync(p, "utf8").split("\n").filter(Boolean).flatMap((l) => { try { return [JSON.parse(l) as T]; } catch { return []; } });

/**
 * Merges several harvests of one workspace (the Cloud routes before the 2026-11-05 storage move, the Reader after
 * it). A session in both keeps the richer directory row (the first harvest's, which carries parents), the latest
 * `lastTs` and every owner, and the union of its events by event id.
 */
export function mergeHarvests(parts: Array<{ rows: DirectoryRow[]; facts: SessionFacts[] }>) {
  const rows = new Map<string, DirectoryRow>(), facts = new Map<string, SessionFacts>();
  const key = (x: { source: string; sessionId: string }) => `${x.source}\u0000${x.sessionId}`;
  for (const p of parts) {
    for (const r of p.rows) {
      const o = rows.get(key(r));
      if (!o) { rows.set(key(r), r); continue; }
      const users = [...new Set([...((o.userIds as string[] | null) ?? []), ...((r.userIds as string[] | null) ?? [])])];
      rows.set(key(r), { ...o, lastTs: r.lastTs > o.lastTs ? r.lastTs : o.lastTs, userIds: users, project: o.project ?? r.project });
    }
    for (const f of p.facts) {
      const o = facts.get(key(f));
      if (!o) { facts.set(key(f), f); continue; }
      const seen = new Set(o.events.map((e) => e.eventId));
      facts.set(key(f), { ...o, eventsRead: o.eventsRead + f.eventsRead, censored: o.censored || f.censored, error: o.error ?? f.error,
        events: [...o.events, ...f.events.filter((e) => !seen.has(e.eventId))].sort((a, b) => (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0)) });
    }
  }
  return { rows: [...rows.values()], facts: [...facts.values()] };
}

/** `harvest` is one harvest directory, or several separated by commas (see mergeHarvests). */
export function loadContext(harvest: string, forge: string, observed: ObservedPolicy = "all"): Context & { codingSessions: number } {
  const { rows, facts } = mergeHarvests(harvest.split(",").filter(Boolean).map((dir) => ({
    rows: readJsonl<DirectoryRow>(join(dir, "directory.jsonl")), facts: readJsonl<SessionFacts>(join(dir, "facts.jsonl")),
  })));
  const sessions = buildModel(rows, facts, observed);
  const prs: ForgePr[] = [];
  for (const f of readdirSync(join(forge, "prs")).filter((n) => n.endsWith(".json"))) {
    const repo = `AgentWorkforce/${f.slice(0, -5)}`;
    for (const p of JSON.parse(readFileSync(join(forge, "prs", f), "utf8")) as PullRequest[]) prs.push({ ...p, repo });
  }
  return { sessions, prs, communicated: communicated(sessions), codingSessions: facts.length };
}

type Run = (ctx: Context) => Alert[];
/** The ranker's component alerts are the same for every ranker grid point; computed once per context. */
const COMPONENTS = new WeakMap<Context, Components>();
const componentsOf = (c: Context) => { let v = COMPONENTS.get(c); if (!v) { v = components(c); COMPONENTS.set(c, v); } return v; };
export const WINDOW = { since: "2026-07-28T00:00:00.000Z", until: "2026-11-25T00:00:00.000Z" };
interface GridPoint { detector: string; params: Record<string, unknown>; run: Run }

export function grid(): GridPoint[] {
  const out: GridPoint[] = [];
  for (const minShared of [1, 2, 3]) for (const maxPrFiles of [30, 100, 400]) for (const skipHot of [true, false]) for (const suppressCommunicated of [true, false]) {
    const params = { minShared, maxPrFiles, skipHot, suppressCommunicated };
    out.push({ detector: "open-pr", params, run: (c) => openPrOverlap(c, params) });
  }
  for (const windowH of [2, 6, 24, 72]) for (const minShared of [1, 2, 3]) for (const maxRootsPerPath of [3, 8]) for (const skipHot of [true, false]) for (const crossOwnerOnly of [false, true]) {
    const params = { windowH, minShared, maxRootsPerPath, skipHot, crossOwnerOnly, suppressCommunicated: true };
    out.push({ detector: "concurrent", params, run: (c) => concurrentEdits(c, params) });
  }
  // The shipped detector, unchanged; its own internal caps apply. `limit` is its maximum of 200 findings.
  out.push({ detector: "outcome-facts", params: { limit: 200 }, run: (c) => outcomeFactsBaseline(c, WINDOW, { limit: 200 }) });
  for (const windowMin of [15, 60, 240]) for (const requireEdit of [false, true]) for (const suppressCommunicated of [true, false]) {
    const params = { windowMin, requireEdit, suppressCommunicated };
    out.push({ detector: "shared-trigger", params, run: (c) => sharedTrigger(c, params) });
  }
  for (const params of rankerGrid()) out.push({ detector: "ranker", params: { ...params }, run: (c) => rank(c, componentsOf(c), params) });
  return out;
}

const REPO_ROOT = realpathSync(resolve(fileURLToPath(new URL("../../..", import.meta.url))));
const inRepo = (p: string) => (p + "/").startsWith(REPO_ROOT + "/");
export function assertOutsideRepo(out: string) {
  const abs = resolve(out);
  if (inRepo(abs)) throw new Error("--out must be outside this repository");
  mkdirSync(abs, { recursive: true, mode: 0o700 });
  if (inRepo(realpathSync(abs))) throw new Error("--out must be outside this repository");
}

const SOURCES = ["detectors.ts", "model.ts", "paths.ts", "score.ts", "baseline.ts", "ranker.ts", "eval.ts", "bundle.ts", "cli.ts"].map((f) => new URL(f, import.meta.url));
const sourceHash = () => createHash("sha256").update(SOURCES.map((u) => readFileSync(u)).join("\u0000")).digest("hex");
/** sha256 over files, streamed (harvest facts are large), in the given order, each prefixed with its name and size. */
function digestFiles(files: string[]): string {
  const h = createHash("sha256"), buf = Buffer.alloc(1 << 20);
  for (const f of files) {
    h.update(`${basename(f)}\u0000${statSync(f).size}\u0000`);
    const fd = openSync(f, "r");
    try { for (let n; (n = readSync(fd, buf, 0, buf.length, null)) > 0;) h.update(buf.subarray(0, n)); } finally { closeSync(fd); }
  }
  return h.digest("hex");
}
/** Everything a frozen score depends on besides the points themselves: inputs, sources, labels, budget, split. */
export interface FreezeTerms { sourceHash: string; labelsHash: string; inputsHash: string; perHundred: number; split: "heldout"; observed: "all" }
export function freezeTerms(harvest: string, forge: string, labels: string, perHundred: number): FreezeTerms {
  const inputs = [
    ...harvest.split(",").filter(Boolean).flatMap((d) => ["directory.jsonl", "facts.jsonl"].map((f) => join(d, f))),
    ...readdirSync(join(forge, "prs")).filter((n) => n.endsWith(".json")).sort().map((n) => join(forge, "prs", n)),
  ];
  return { sourceHash: sourceHash(), labelsHash: digestFiles([labels]), inputsHash: digestFiles(inputs), perHundred, split: "heldout", observed: "all" };
}
const pointHash = (g: { detector: string; params: unknown }) => createHash("sha256").update(JSON.stringify([g.detector, g.params, sourceHash()])).digest("hex").slice(0, 16);

/** Ranking used for the climb: recall first, then fewer alerts on adjudicated non-overlaps, then lead time. */
export const better = (a: Score, b: Score) =>
  a.hits.length - b.hits.length || b.negativeHits - a.negativeHits || (a.medianLeadH ?? -1) - (b.medianLeadH ?? -1) || b.alerts - a.alerts;

async function main(argv: string[]) {
  const [cmd, ...rest] = argv;
  const { values } = parseArgs({ args: rest, options: {
    harvest: { type: "string" }, forge: { type: "string" }, labels: { type: "string" }, out: { type: "string" }, frozen: { type: "string" },
    "per-hundred": { type: "string" }, verdicts: { type: "string" }, "per-detector": { type: "string" }, seed: { type: "string" }, "owner-labels": { type: "string" },
  } });
  if (!values.harvest || !values.forge || !values.labels || !values.out) throw new Error("--harvest, --forge, --labels and --out are required");
  // Every output (logs, scores, bundles) carries real identifiers or PR text.
  assertOutsideRepo(values.out);
  const labels = JSON.parse(readFileSync(values.labels, "utf8")) as { incidents: IncidentLabel[]; negatives: NegativeLabel[] };
  const ctx = loadContext(values.harvest, values.forge);
  const perHundred = Number(values["per-hundred"] ?? 5);
  if (!(perHundred > 0) || !Number.isFinite(perHundred)) throw new Error("--per-hundred must be a positive number");
  const scoreOn = (split: "dev" | "heldout", g: GridPoint) =>
    score(g.detector, ctx, g.run(ctx), labels.incidents.filter((i) => i.split === split), labels.negatives, { sessions: ctx.codingSessions, perHundred });
  if (cmd === "climb") {
    const log: Array<{ hash: string; detector: string; params: unknown; score: Score }> = [];
    const best = new Map<string, { g: GridPoint; s: Score }>();
    for (const g of grid()) {
      const s = scoreOn("dev", g);
      log.push({ hash: pointHash(g), detector: g.detector, params: g.params, score: s });
      const cur = best.get(g.detector);
      if (!cur || better(s, cur.s) > 0) best.set(g.detector, { g, s });
    }
    writeFileSync(join(values.out, "climb-log.json"), JSON.stringify(log, null, 1), { mode: 0o600 });
    const frozen = [...best.values()].map(({ g, s }) => ({ detector: g.detector, params: g.params, hash: pointHash(g), dev: { hits: s.hits.length, linkable: s.linkable, negativeHits: s.negativeHits, alerts: s.alerts, medianLeadH: s.medianLeadH } }));
    const terms = freezeTerms(values.harvest, values.forge, values.labels, perHundred);
    writeFileSync(join(values.out, "frozen.json"), JSON.stringify({ format: "overlap-bench-frozen/2", ...terms, frozen }, null, 1), { mode: 0o600 });
    for (const f of frozen) console.log(JSON.stringify(f));
  } else if (cmd === "heldout") {
    if (!values.frozen) throw new Error("heldout needs --frozen");
    const frozen = JSON.parse(readFileSync(values.frozen, "utf8")) as FreezeTerms & { format?: string; frozen: Array<{ detector: string; params: Record<string, unknown>; hash: string }> };
    if (frozen.format !== "overlap-bench-frozen/2") throw new Error("frozen file predates input and budget freezing; climb again");
    const ledger = join(values.out, `heldout-${frozen.labelsHash.slice(0, 12)}.json`);
    if (existsSync(ledger)) throw new Error("held-out was already scored for these labels");
    // The held-out score must use exactly the frozen inputs, sources, labels, budget and split.
    if (values["per-hundred"] !== undefined && perHundred !== frozen.perHundred) throw new Error("--per-hundred differs from the frozen budget");
    const now = freezeTerms(values.harvest, values.forge, values.labels, frozen.perHundred);
    for (const k of ["sourceHash", "labelsHash", "inputsHash", "perHundred", "split", "observed"] as const) {
      if (now[k] !== frozen[k]) throw new Error(`${k} differs from the frozen file`);
    }
    const results = frozen.frozen.map((f) => {
      const g = grid().find((x) => x.detector === f.detector && JSON.stringify(x.params) === JSON.stringify(f.params));
      if (!g) throw new Error(`frozen point ${f.hash} is not in the grid`);
      if (pointHash(g) !== f.hash) throw new Error(`frozen point ${f.hash} does not hash to its parameters and sources`);
      return { hash: f.hash, ...score(g.detector, ctx, g.run(ctx), labels.incidents.filter((i) => i.split === frozen.split), labels.negatives, { sessions: ctx.codingSessions, perHundred: frozen.perHundred }) };
    });
    writeFileSync(ledger, JSON.stringify(results, null, 1), { mode: 0o600, flag: "wx" });
    for (const r of results) console.log(JSON.stringify({ detector: r.detector, hits: r.hits.length, linkable: r.linkable, recall: r.recall, negativeHits: r.negativeHits, medianLeadH: r.medianLeadH }));
  } else if (cmd === "eval" || cmd === "sample") {
    // Both read the frozen points and the dev split only; held-out stays behind `heldout`.
    if (!values.frozen) throw new Error(`${cmd} needs --frozen`);
    const frozen = JSON.parse(readFileSync(values.frozen, "utf8")) as { frozen: Array<{ detector: string; params: Record<string, unknown> }> };
    const points = frozen.frozen.map((f) => grid().find((x) => x.detector === f.detector && JSON.stringify(x.params) === JSON.stringify(f.params)) ?? (() => { throw new Error(`frozen ${f.detector} point is not in the grid`); })());
    const verdicts = values.verdicts ? new Map((JSON.parse(readFileSync(values.verdicts, "utf8")) as AlertVerdict[]).map((v) => [v.pair, v])) : undefined;
    const dev = labels.incidents.filter((i) => i.split === "dev");
    const alerts = new Map(points.map((g) => [g.detector, g.run(ctx)]));
    if (cmd === "eval") {
      const reports = points.map((g) => evaluate(g.detector, g.params, ctx, alerts.get(g.detector)!, dev, labels.negatives, verdicts));
      const union = new Set(reports.flatMap((r) => r.curve.at(-1)!.hitIds));
      const ceil = ceiling(ctx, dev);
      const summary = { reports, unionHitsUncapped: union.size, ceiling: Object.fromEntries(["no_session", "one_side", "both_sides"].map((c) => [c, ceil.filter((x) => x.cls === c).length])) };
      writeFileSync(join(values.out, "eval.json"), JSON.stringify(summary, null, 1), { mode: 0o600 });
      for (const r of reports) console.log(JSON.stringify({ detector: r.detector, linkable: r.linkable, curve: r.curve.map((c) => `${c.perHundred}:${c.hits}h/${c.negativeHits}n/${c.alerts}a`).join(" "), precision: r.precision }));
      console.log(JSON.stringify({ unionHitsUncapped: summary.unionHitsUncapped, ceiling: summary.ceiling }));
    } else {
      const sample = adjudicationSample(ctx, alerts, Number(values["per-detector"] ?? 15), values.seed ?? "v1", new Set(verdicts?.keys() ?? []));
      // Display names for owners come from a private file ({ "<user id>": "<label>" }); anyone else is "member".
      const names = values["owner-labels"] ? JSON.parse(readFileSync(values["owner-labels"], "utf8")) as Record<string, string> : {};
      const ownerLabel = (u: string | null) => u === null ? null : names[u] ?? "member";
      writeFileSync(join(values.out, "bundle.jsonl"), sample.map((s) => JSON.stringify(describePair(ctx, s.sides, s.at, ownerLabel))).join("\n") + "\n", { mode: 0o600 });
      console.log(JSON.stringify({ sampled: sample.length }));
    }
  } else throw new Error("command must be climb, heldout, eval or sample");
}

if (import.meta.main) main(process.argv.slice(2)).catch((e) => { console.error(`overlap-bench: ${(e as Error).message}`); process.exit(1); });
