/**
 * Overlap benchmark CLI (docs/session-intel-overlap-bench.md).
 *
 *   bun run src/session-intel/overlap-bench/cli.ts climb --harvest <dir>[,<dir>...] --forge <dir> --labels <labels.json> --out <dir>
 *   bun run src/session-intel/overlap-bench/cli.ts stage2-batch --harvest ... --forge ... --labels ... --frozen <frozen.json> --split dev|heldout \
 *     --prompt <prompt.txt> --cutoff 0.3 --seed <s> [--fresh 40] [--exclude <key.json>,...] [--verdicts <hindsight.json>] [--owner-labels <f>] --out <batch dir>
 *   bun run src/session-intel/overlap-bench/cli.ts stage2-score --out <batch dir>
 *   bun run src/session-intel/overlap-bench/cli.ts heldout --harvest <dir> --forge <dir> --labels <labels.json> --frozen <frozen.json> --out <dir>
 *
 * `climb` scores every grid point of every detector on the dev split only and writes the log and the best point
 * per detector (frozen.json, with a sha256 of params and sources). `heldout` scores exactly the frozen points once
 * and refuses to run twice for the same frozen file.
 */
import { createHash } from "node:crypto";
import { closeSync, existsSync, openSync, readdirSync, readFileSync, readSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import type { DirectoryRow, SessionFacts } from "../cloud-harvest/harvest.ts";
import type { PrHistory, PullRequest } from "../forge/types.ts";
import { type Alert, type Context, concurrentEdits, type ForgePr, openPrOverlap, sharedTrigger } from "./detectors.ts";
import { outcomeFactsBaseline } from "./baseline.ts";
import { buildModel, communicated, type ObservedPolicy } from "./model.ts";
import { type IncidentLabel, type NegativeLabel, score, type Score } from "./score.ts";
import { components, type Components, rank, rankerGrid, type RankerParams } from "./ranker.ts";
import { adjudicationSample, type AlertVerdict, ceiling, evaluate, pairId } from "./eval.ts";
import { describe as describePair } from "./bundle.ts";
import { LOOSE_RANKER, scoreStage2, stage2Batch, verifiedBatch } from "./stage2.ts";
import { claimOnce, finishOnce, noteOnce, pinScore } from "./once.ts";
import { assertPrivateFile, assertPrivateInput, outsideRoot, privateDir, readPrivate, writePrivate } from "../cloud-harvest/private-fs.ts";

/** Every record of a JSON-lines input; a malformed line or a partial last record is refused, never skipped. */
const readJsonl = <T>(p: string): T[] => {
  const text = readPrivate(p);
  if (text && !text.endsWith("\n")) throw new Error(`${p} ends in a partial record`);
  return text.split("\n").slice(0, -1).map((l, i) => { try { return JSON.parse(l) as T; } catch { throw new Error(`${p} is malformed at line ${i + 1}`); } });
};
/** A canonical instant: parses, and is an ISO-8601 UTC time (date, time, optional fraction, Z). */
const INSTANT = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$/;
const instant = (v: unknown, what: string): number => {
  const t = typeof v === "string" && INSTANT.test(v) ? Date.parse(v) : NaN;
  if (!Number.isFinite(t)) throw new Error(`${what} is not a canonical UTC timestamp`);
  return t;
};

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
/**
 * The scope a harvest was taken under, from its manifest. Merged harvests must be of one workspace, and none may be
 * past its delete-by date: reading expired data is refused, not warned about.
 */
export interface HarvestScope { dir: string; workspace: string; since: string; until: string; deleteBy: string; via: string }
export function harvestScopes(harvest: string, now = new Date().toISOString()): HarvestScope[] {
  const scopes = harvest.split(",").filter(Boolean).map((dir): HarvestScope => {
    assertPrivateInput(dir, "dir");
    const m = JSON.parse(readPrivate(join(dir, "manifest.json"))) as { params?: Record<string, string> };
    const p = m.params ?? {};
    if (typeof p.workspace !== "string" || !/^[0-9a-f-]{36}$|^[A-Za-z0-9._-]{1,64}$/.test(p.workspace)) throw new Error(`${dir}: the manifest does not record its workspace`);
    // Compared as instants, and fail closed: a scope that does not validate is not read.
    const since = instant(p.since, `${dir}: since`), until = instant(p.until, `${dir}: until`), deleteBy = instant(p.deleteBy, `${dir}: deleteBy`);
    if (!(since < until)) throw new Error(`${dir}: the window is empty or reversed`);
    if (!(deleteBy > instant(now, "now"))) throw new Error(`${dir}: past its delete-by date; it must be deleted, not read`);
    return { dir, workspace: p.workspace, since: p.since!, until: p.until!, deleteBy: p.deleteBy!, via: p.via ?? "cloud" };
  });
  if (new Set(scopes.map((x) => x.workspace)).size > 1) throw new Error("the harvests are of different workspaces");
  return scopes;
}

/** The earliest delete-by of several scopes, compared as instants (formats differ: whole or fractional seconds). */
export function earliestDeleteBy(scopes: Array<{ deleteBy: string }>): string {
  if (!scopes.length) throw new Error("no harvest scope");
  return new Date(Math.min(...scopes.map((x) => instant(x.deleteBy, "deleteBy")))).toISOString();
}

export function loadContext(harvest: string, forge: string, observed: ObservedPolicy = "all"): Context & { codingSessions: number; scopes: HarvestScope[] } {
  const scopes = harvestScopes(harvest);
  const { rows, facts } = mergeHarvests(scopes.map(({ dir }) => ({
    rows: readJsonl<DirectoryRow>(join(dir, "directory.jsonl")), facts: readJsonl<SessionFacts>(join(dir, "facts.jsonl")),
  })));
  const sessions = buildModel(rows, facts, observed);
  const prs: ForgePr[] = [];
  assertPrivateInput(forge, "dir"); assertPrivateInput(join(forge, "prs"), "dir");
  for (const f of readdirSync(join(forge, "prs")).filter((n) => n.endsWith(".json"))) {
    const repo = `AgentWorkforce/${f.slice(0, -5)}`;
    // Dated title and description histories, when fetched (forge `history`), sit beside the PR files.
    const hist = join(forge, "history", f);
    const byNumber = new Map(existsSync(hist) ? (JSON.parse(readPrivate(hist)) as PrHistory[]).map((h) => [h.number, h]) : []);
    for (const p of JSON.parse(readPrivate(join(forge, "prs", f))) as PullRequest[]) {
      const h = byNumber.get(p.number);
      prs.push({ ...p, repo, ...(h ? { history: { titles: h.titles, bodies: h.bodies } } : {}) });
    }
  }
  return { sessions, prs, communicated: communicated(sessions), codingSessions: facts.length, scopes };
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
export function assertOutsideRepo(out: string): string { return outsideRoot(out, REPO_ROOT); }

/**
 * Every source a score depends on, transitively: the bench, the shared code it imports (the harvest's facts and
 * directory types and its private I/O, the forge types), and the shipped detector the baseline runs.
 */
const SOURCES = [
  ...["detectors.ts", "model.ts", "paths.ts", "score.ts", "baseline.ts", "ranker.ts", "eval.ts", "bundle.ts", "stage2.ts", "once.ts", "cli.ts"],
  "../cloud-harvest/features.ts", "../cloud-harvest/harvest.ts", "../cloud-harvest/private-fs.ts", "../forge/types.ts",
  "../../../packages/coordination-detect/src/index.ts",
].map((f) => new URL(f, import.meta.url));
const sourceHash = () => createHash("sha256").update(SOURCES.map((u) => readFileSync(u)).join("\u0000")).digest("hex");
/** sha256 over files, streamed (harvest facts are large), in the given order, each prefixed with its name and size. */
function digestFiles(files: string[]): string {
  const h = createHash("sha256"), buf = Buffer.alloc(1 << 20);
  for (const f of files) {
    assertPrivateInput(f, "file");
    h.update(`${basename(f)}\u0000${statSync(f).size}\u0000`);
    const fd = openSync(f, "r");
    try { for (let n; (n = readSync(fd, buf, 0, buf.length, null)) > 0;) h.update(buf.subarray(0, n)); } finally { closeSync(fd); }
  }
  return h.digest("hex");
}
/**
 * The labels' identity: the sha256 of their bytes alone, so a renamed or moved copy of the same labels is the same
 * held-out set (the once-only registry is keyed by it).
 */
export function labelsIdentity(labels: string): string {
  assertPrivateInput(labels, "file");
  return createHash("sha256").update(readFileSync(labels)).digest("hex");
}
/** Everything a frozen score depends on besides the points themselves: inputs, sources, labels, budget, split. */
export interface FreezeTerms { sourceHash: string; labelsHash: string; inputsHash: string; perHundred: number; split: "heldout"; observed: "all" }
type Frozen = FreezeTerms & { format?: string; freezeHash?: string; frozen: Array<{ detector: string; params: Record<string, unknown>; hash: string }> };
/** The identity of a freeze: every term and point, so editing any of them (the budget, say) breaks it. */
const freezeHashOf = (body: object) => createHash("sha256").update(JSON.stringify(body)).digest("hex");
/** A frozen file whose every term (inputs, sources, labels, budget, split, policy) matches what is on disk now. */
function verifiedFrozen(file: string, values: { harvest?: string; forge?: string; labels?: string; "per-hundred"?: string }, perHundred: number): Frozen {
  const frozen = JSON.parse(readPrivate(file)) as Frozen;
  if (frozen.format !== "overlap-bench-frozen/3") throw new Error("frozen file predates the freeze identity; climb again");
  const { freezeHash, ...body } = frozen;
  if (freezeHash !== freezeHashOf(body)) throw new Error("the frozen file was changed after freezing");
  if (values["per-hundred"] !== undefined && perHundred !== frozen.perHundred) throw new Error("--per-hundred differs from the frozen budget");
  const now = freezeTerms(values.harvest!, values.forge!, values.labels!, frozen.perHundred);
  for (const k of ["sourceHash", "labelsHash", "inputsHash", "perHundred", "split", "observed"] as const) {
    if (now[k] !== frozen[k]) throw new Error(`${k} differs from the frozen file`);
  }
  return frozen;
}

export function freezeTerms(harvest: string, forge: string, labels: string, perHundred: number): FreezeTerms {
  const inputs = [
    ...harvest.split(",").filter(Boolean).flatMap((d) => ["manifest.json", "directory.jsonl", "facts.jsonl"].map((f) => join(d, f))),
    ...readdirSync(join(forge, "prs")).filter((n) => n.endsWith(".json")).sort().map((n) => join(forge, "prs", n)),
    ...(existsSync(join(forge, "history")) ? readdirSync(join(forge, "history")).filter((n) => n.endsWith(".json")).sort().map((n) => join(forge, "history", n)) : []),
  ];
  return { sourceHash: sourceHash(), labelsHash: labelsIdentity(labels), inputsHash: digestFiles(inputs), perHundred, split: "heldout", observed: "all" };
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
    split: { type: "string" }, prompt: { type: "string" }, cutoff: { type: "string" }, fresh: { type: "string" }, exclude: { type: "string" }, judge: { type: "string" },
  } });
  if (cmd === "stage2-score") {
    // Scores a batch at its frozen cutoff. The batch directory must be a private destination outside the repo and
    // the batch within its retention deadline before any derived input is opened; then the batch is verified (every
    // part, prompt and the key by hash, the manifest by its own hash) and the verdicts must cover it exactly.
    if (!values.out) throw new Error("stage2-score needs --out (the batch directory)");
    assertOutsideRepo(values.out);
    const { manifest, key, verdicts } = verifiedBatch(values.out);
    // The verdicts' identity: their validated content in pair order, independent of file layout and formatting.
    const verdictsHash = createHash("sha256").update(JSON.stringify([...verdicts].sort((a, b) => (a.pair < b.pair ? -1 : 1)).map((v) => [v.pair, v.verdict, v.p_overlap]))).digest("hex");
    let result: unknown = { ...scoreStage2(key, verdicts, manifest.cutoff), split: manifest.split, promptHash: manifest.promptHash, judge: manifest.judge, manifestHash: manifest.manifestHash, verdictsHash };
    const receipt = join(values.out, "key", "score.json");
    if (manifest.split === "heldout") {
      // Ownership is taken over from the batch's (finished) process under the batch's exact terms. The score is
      // pinned (verdicts digest and result) before the receipt is published; a resumed scoring must present the
      // same verdicts and republishes the pinned result, wherever its receipt goes.
      const rec = claimOnce("stage2", manifest.labelsHash, manifest.terms);
      if (rec.batch !== manifest.manifestHash) throw new Error("this held-out batch is not the one claimed for these labels");
      result = pinScore(rec, { verdictsHash, result }).score!.result;
      writePrivate(receipt, JSON.stringify(result, null, 1));
      finishOnce(rec, result);
    } else writePrivate(receipt, JSON.stringify(result, null, 1));
    console.log(JSON.stringify(result));
    return;
  }
  if (!values.harvest || !values.forge || !values.labels || !values.out) throw new Error("--harvest, --forge, --labels and --out are required");
  // Every output (logs, scores, bundles) carries real identifiers or PR text.
  assertOutsideRepo(values.out);
  const labels = JSON.parse(readPrivate(values.labels)) as { incidents: IncidentLabel[]; negatives: NegativeLabel[] };
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
    writePrivate(join(values.out, "climb-log.json"), JSON.stringify(log, null, 1));
    const frozen = [...best.values()].map(({ g, s }) => ({ detector: g.detector, params: g.params, hash: pointHash(g), dev: { hits: s.hits.length, linkable: s.linkable, negativeHits: s.negativeHits, alerts: s.alerts, medianLeadH: s.medianLeadH } }));
    const terms = freezeTerms(values.harvest, values.forge, values.labels, perHundred);
    const body = { format: "overlap-bench-frozen/3", ...terms, frozen };
    writePrivate(join(values.out, "frozen.json"), JSON.stringify({ ...body, freezeHash: freezeHashOf(body) }, null, 1));
    for (const f of frozen) console.log(JSON.stringify(f));
  } else if (cmd === "heldout") {
    if (!values.frozen) throw new Error("heldout needs --frozen");
    const frozen = verifiedFrozen(values.frozen, values, perHundred);
    // One evaluation per labels digest, recorded in the per-user registry (once.ts), whatever paths are used.
    const rec = claimOnce("stage1", frozen.labelsHash, { freezeHash: frozen.freezeHash });
    const results = frozen.frozen.map((f) => {
      const g = grid().find((x) => x.detector === f.detector && JSON.stringify(x.params) === JSON.stringify(f.params));
      if (!g) throw new Error(`frozen point ${f.hash} is not in the grid`);
      if (pointHash(g) !== f.hash) throw new Error(`frozen point ${f.hash} does not hash to its parameters and sources`);
      return { hash: f.hash, ...score(g.detector, ctx, g.run(ctx), labels.incidents.filter((i) => i.split === frozen.split), labels.negatives, { sessions: ctx.codingSessions, perHundred: frozen.perHundred }) };
    });
    // The receipt first: an exit before finishing leaves an interrupted claim that reruns to the same result.
    writePrivate(join(values.out, "heldout.json"), JSON.stringify({ freezeHash: frozen.freezeHash, results }, null, 1));
    finishOnce(rec, { freezeHash: frozen.freezeHash, results });
    for (const r of results) console.log(JSON.stringify({ detector: r.detector, hits: r.hits.length, linkable: r.linkable, recall: r.recall, negativeHits: r.negativeHits, medianLeadH: r.medianLeadH }));
  } else if (cmd === "eval" || cmd === "sample") {
    // Both read the frozen points and the dev split only; held-out stays behind `heldout`.
    if (!values.frozen) throw new Error(`${cmd} needs --frozen`);
    const frozen = JSON.parse(readPrivate(values.frozen)) as { frozen: Array<{ detector: string; params: Record<string, unknown> }> };
    const points = frozen.frozen.map((f) => grid().find((x) => x.detector === f.detector && JSON.stringify(x.params) === JSON.stringify(f.params)) ?? (() => { throw new Error(`frozen ${f.detector} point is not in the grid`); })());
    const verdicts = values.verdicts ? new Map((JSON.parse(readPrivate(values.verdicts)) as AlertVerdict[]).map((v) => [v.pair, v])) : undefined;
    const dev = labels.incidents.filter((i) => i.split === "dev");
    const alerts = new Map(points.map((g) => [g.detector, g.run(ctx)]));
    if (cmd === "eval") {
      const reports = points.map((g) => evaluate(g.detector, g.params, ctx, alerts.get(g.detector)!, dev, labels.negatives, verdicts));
      const union = new Set(reports.flatMap((r) => r.curve.at(-1)!.hitIds));
      const ceil = ceiling(ctx, dev);
      const summary = { reports, unionHitsUncapped: union.size, ceiling: Object.fromEntries(["no_session", "one_side", "both_sides"].map((c) => [c, ceil.filter((x) => x.cls === c).length])) };
      writePrivate(join(values.out, "eval.json"), JSON.stringify(summary, null, 1));
      for (const r of reports) console.log(JSON.stringify({ detector: r.detector, linkable: r.linkable, curve: r.curve.map((c) => `${c.perHundred}:${c.hits}h/${c.negativeHits}n/${c.alerts}a`).join(" "), precision: r.precision }));
      console.log(JSON.stringify({ unionHitsUncapped: summary.unionHitsUncapped, ceiling: summary.ceiling }));
    } else {
      const sample = adjudicationSample(ctx, alerts, Number(values["per-detector"] ?? 15), values.seed ?? "v1", new Set(verdicts?.keys() ?? []));
      // Display names for owners come from a private file ({ "<user id>": "<label>" }); anyone else is "member".
      const names = values["owner-labels"] ? JSON.parse(readPrivate(values["owner-labels"])) as Record<string, string> : {};
      const ownerLabel = (u: string | null) => u === null ? null : names[u] ?? "member";
      writePrivate(join(values.out, "bundle.jsonl"), sample.map((s) => JSON.stringify(describePair(ctx, s.sides, s.at, ownerLabel))).join("\n") + "\n");
      console.log(JSON.stringify({ sampled: sample.length }));
    }
  } else if (cmd === "stage2-batch") {
    // A blind stage-2 batch for one split, bound to the frozen stage 1, the prompt (by hash), the cutoff, the seed,
    // the exclusions and the judge. Judge-readable material (parts, rendered prompts) goes to <out>/judge; the key
    // and the manifest go to <out>/key, which no prompt names.
    if (!values.frozen || !values.prompt || !values.cutoff || !values.seed || !values.judge) throw new Error("stage2-batch needs --frozen, --prompt, --cutoff, --seed and --judge");
    const split = values.split === "heldout" ? "heldout" : values.split === "dev" ? "dev" : (() => { throw new Error("--split must be dev or heldout"); })();
    const cutoff = Number(values.cutoff);
    if (!(cutoff > 0 && cutoff <= 1)) throw new Error("--cutoff must be in (0, 1]");
    const fresh = Number(values.fresh ?? 40);
    if (!Number.isSafeInteger(fresh) || fresh < 1) throw new Error("--fresh must be a positive integer");
    const frozen = verifiedFrozen(values.frozen, values, perHundred);
    const ranker = frozen.frozen.find((f) => f.detector === "ranker");
    if (!ranker) throw new Error("the frozen file has no ranker point");
    const g = grid().find((x) => x.detector === "ranker" && JSON.stringify(x.params) === JSON.stringify(ranker.params));
    if (!g || pointHash(g) !== ranker.hash) throw new Error("the frozen ranker point does not hash to its parameters and sources");
    const prompt = readPrivate(values.prompt);
    if (!prompt.includes("{INPUT}") || !prompt.includes("{OUTPUT}")) throw new Error("the prompt must name {INPUT} and {OUTPUT}");
    const sha = (t: string) => createHash("sha256").update(t).digest("hex");
    const judged = new Set<string>();
    const exclusionFiles = (values.exclude ?? "").split(",").filter(Boolean);
    for (const f of exclusionFiles) for (const k of JSON.parse(readPrivate(f)) as Array<{ pair: string }>) judged.add(k.pair);
    const hindsight = values.verdicts ? (JSON.parse(readPrivate(values.verdicts)) as AlertVerdict[]) : [];
    for (const v of hindsight) judged.add(v.pair);
    const terms = { freezeHash: frozen.freezeHash, promptHash: sha(prompt), cutoff, seed: values.seed, fresh, judge: values.judge, exclusionsHash: sha([...judged].sort().join("\n")) };
    // Held-out: claimed (or an interrupted claim under identical terms resumed) before anything is drawn.
    const rec = split === "heldout" ? claimOnce("stage2", frozen.labelsHash, terms) : null;
    // A claim whose batch was already made is not drawn again (a resumed claim without one may finish).
    if (rec?.batch) throw new Error("the held-out batch for these labels was already made");
    const comps = componentsOf(ctx);
    const items = stage2Batch(ctx, {
      incidents: labels.incidents.filter((i) => i.split === split), loose: rank(ctx, comps, { ...LOOSE_RANKER }), frozen: rank(ctx, comps, ranker.params as unknown as RankerParams),
      hindsightOverlaps: split === "dev" ? hindsight.filter((v) => v.verdict === "overlap").map((v) => v.pair) : [], judged,
      fresh, seed: values.seed, perHundred: frozen.perHundred,
    });
    const names = values["owner-labels"] ? JSON.parse(readPrivate(values["owner-labels"])) as Record<string, string> : {};
    const ownerLabel = (u: string | null) => u === null ? null : names[u] ?? "member";
    const bundles = items.map((x) => JSON.stringify(describePair(ctx, x.sides, x.at, ownerLabel, true)));
    const judgeDir = privateDir(join(values.out, "judge")), keyDir = privateDir(join(values.out, "key"));
    const half = Math.ceil(bundles.length / 2), parts: Record<string, string> = {}, prompts: Record<string, string> = {};
    for (const [i, part] of [bundles.slice(0, half), bundles.slice(half)].entries()) {
      const id = String(i).padStart(2, "0"), body = part.length ? part.join("\n") + "\n" : "";
      const rendered = prompt.replaceAll("{INPUT}", join(judgeDir, `part-${id}`)).replaceAll("{OUTPUT}", join(judgeDir, `verdicts-${id}.json`));
      writePrivate(join(judgeDir, `part-${id}`), body); writePrivate(join(judgeDir, `prompt-${id}.txt`), rendered);
      parts[`part-${id}`] = sha(body); prompts[`prompt-${id}.txt`] = sha(rendered);
    }
    const keyText = JSON.stringify(items.map((x) => ({ pair: pairId(x.sides), truth: x.truth })), null, 1);
    writePrivate(join(keyDir, "key.json"), keyText);
    // The batch inherits the earliest delete-by of the data it was drawn from; scoring refuses it after that.
    const deleteBy = earliestDeleteBy(ctx.scopes);
    const body = { format: "overlap-bench-stage2/3", split, terms, ...terms, labelsHash: frozen.labelsHash, ranker: ranker.hash, perHundred: frozen.perHundred, deleteBy, items: items.length, keyHash: sha(keyText), parts, prompts };
    const manifest = { ...body, manifestHash: sha(JSON.stringify(body)) };
    writePrivate(join(keyDir, "stage2.json"), JSON.stringify(manifest, null, 1));
    if (rec) noteOnce(rec, manifest.manifestHash);
    console.log(JSON.stringify({ split, items: items.length, positives: items.filter((x) => x.truth !== "fresh").length, fresh: items.filter((x) => x.truth === "fresh").length, promptHash: terms.promptHash.slice(0, 16) }));
  } else throw new Error("command must be climb, heldout, eval, sample or stage2-batch");
}

if (import.meta.main) main(process.argv.slice(2)).catch((e) => { console.error(`overlap-bench: ${(e as Error).message}`); process.exit(1); });
