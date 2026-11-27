/**
 * The relayfacts benchmark CLI (docs/proposals/relayfacts-features.md). Offline: no network, database,
 * model or credentials. Reads a frozen corpus, writes only under --out, which must be outside this
 * repository (records hold quotes from real sessions).
 *
 *   bun run src/relayfacts/bench/cli.ts build   --corpus <dir> --out <dir> [--as-of <iso>]
 *   bun run src/relayfacts/bench/cli.ts score   --corpus <dir> --gold <file> --features <file> --out <dir>
 *   bun run src/relayfacts/bench/cli.ts packets --features <file> --out <dir> [--seed <n>] [--limit <n>]
 */
import { parseArgs } from "node:util";
import { buildFeatures } from "../features/build.ts";
import { extractSessionFacts } from "../features/extract.ts";
import { renderFeature, renderIndex } from "../features/render.ts";
import { utc, utcCut, type FeatureRecord } from "../features/types.ts";
import { loadCorpus } from "./corpus.ts";
import { PrivateDir, readPrivateFile } from "../out-path.ts";
import { printable, UsageError } from "../errors.ts";
import { CorpusError } from "./corpus.ts";
import { parseGold } from "./gold.ts";
import { adjudicationPackets, scoreRun } from "./score.ts";

/**
 * A built run, refused before anything is written unless its delete-by date is a valid instant still ahead, its
 * cut a valid millisecond cut, and every time in its records a valid instant (strict, never rolled over).
 */
export function readFeatures(path: string, now: Date = new Date()): { corpus: string; asOf: string; deleteBy: string; features: FeatureRecord[] } {
  const run = JSON.parse(readPrivateFile(path)) as { corpus: string; asOf: unknown; deleteBy?: unknown; features: FeatureRecord[] };
  const deleteBy = utc(typeof run.deleteBy === "string" ? run.deleteBy : null);
  if (deleteBy === null || !(Date.parse(deleteBy) > now.getTime())) throw new UsageError("--features is past its delete-by date (or has no valid one): delete it");
  const asOf = utcCut(typeof run.asOf === "string" ? run.asOf : null);
  if (asOf === null) throw new UsageError("--features has no valid as-of instant");
  if (!Array.isArray(run.features) || !run.features.every(validTimes)) throw new UsageError("--features holds a record with an invalid time");
  return { ...run, asOf, deleteBy } as { corpus: string; asOf: string; deleteBy: string; features: FeatureRecord[] };
}

/** Every time a record carries (its own, its sessions', its items' and their evidence's) is a valid instant or null where null is allowed. */
function validTimes(f: FeatureRecord): boolean {
  const ok = (t: unknown, nullable: boolean) => (t === null || t === undefined ? nullable : typeof t === "string" && utc(t) !== null);
  if (!f || typeof f !== "object" || !ok(f.firstAt, false) || !ok(f.updatedAt, false)) return false;
  if (!Array.isArray(f.sessions) || !f.sessions.every((s) => ok(s.startedAt, false))) return false;
  const walk = (v: unknown): boolean => {
    if (Array.isArray(v)) return v.every(walk);
    if (!v || typeof v !== "object") return true;
    for (const [k, x] of Object.entries(v)) {
      if ((k === "at" || k.endsWith("At")) && !ok(x, true)) return false;
      if (typeof x === "object" && !walk(x)) return false;
    }
    return true;
  };
  return walk(f);
}

export function main(argv: string[]): number {
  const [command, ...rest] = argv;
  const { values } = parseArgs({ args: rest, options: {
    corpus: { type: "string" }, out: { type: "string" }, "as-of": { type: "string" }, gold: { type: "string" },
    features: { type: "string" }, seed: { type: "string" }, limit: { type: "string" },
  } });
  if (command === "build") {
    if (!values.corpus) throw new UsageError("--corpus is required");
    const corpus = loadCorpus(values.corpus);
    // The cut is refused before anything is built or written.
    const asOf = utcCut(values["as-of"] ?? corpus.manifest.window.until);
    if (asOf === null) throw new UsageError("--as-of must be a valid zoned ISO 8601 instant, at most millisecond precision");
    const facts = corpus.sessions.map((s) => extractSessionFacts(s.snapshot, s.context, s.artifacts));
    const features = buildFeatures(facts, { asOf, forge: corpus.forge });
    const out = PrivateDir.open(values.out);
    const md = out.child("relayfacts");
    // Records hold quotes: they carry the corpus's delete-by date, and every later command refuses them after it.
    out.write("features.json", JSON.stringify({ format: "relayfacts-features/1", corpus: corpus.manifestSha256, deleteBy: corpus.manifest.deleteBy, asOf, features }, null, 2));
    for (const f of features) md.write(`${f.id}.md`, renderFeature(f));
    md.write("index.md", renderIndex(features, (f) => f.id));
    const dropped: Record<string, number> = {};
    for (const f of facts) for (const [k, v] of Object.entries(f.dropped)) dropped[k] = (dropped[k] ?? 0) + v;
    const states: Record<string, number> = {};
    for (const f of features) states[f.state] = (states[f.state] ?? 0) + 1;
    console.log(JSON.stringify({ sessions: facts.length, features: features.length, states, dropped }));
    return 0;
  }
  if (command === "score") {
    if (!values.corpus || !values.gold || !values.features) throw new UsageError("--corpus, --gold and --features are required");
    const corpus = loadCorpus(values.corpus);
    const gold = parseGold(JSON.parse(readPrivateFile(values.gold)), corpus.manifestSha256);
    const run = readFeatures(values.features);
    if (run.corpus !== corpus.manifestSha256) throw new UsageError("--features was built from a different corpus");
    const runAt = utcCut(run.asOf), goldAt = utcCut(gold.asOf);
    if (runAt === null || goldAt === null || runAt !== goldAt) throw new UsageError("--features and the gold labels are not as of the same valid instant");
    const report = scoreRun(corpus, gold, run.features);
    PrivateDir.open(values.out).write("score.json", JSON.stringify(report, null, 2));
    console.log(JSON.stringify({ grounding: report.grounding.pass, violations: report.grounding.violations.length, verdicts: report.verdicts, all: report.splits.all }));
    return report.grounding.pass ? 0 : 1;
  }
  if (command === "packets") {
    if (!values.features) throw new UsageError("--features is required");
    const run = readFeatures(values.features);
    const packets = adjudicationPackets(run.features, renderFeature, Number(values.seed ?? 1), Number(values.limit ?? 30));
    PrivateDir.open(values.out).write("packets.jsonl", packets.map((p) => JSON.stringify(p)).join("\n") + "\n");
    console.log(JSON.stringify({ packets: packets.length }));
    return 0;
  }
  console.error("usage: cli.ts build|score|packets ...");
  return 2;
}

if (import.meta.main) {
  try {
    process.exit(main(process.argv.slice(2)));
  } catch (error) {
    console.error(printable(error, [CorpusError]));
    process.exit(1);
  }
}
