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
import type { FeatureRecord } from "../features/types.ts";
import { loadCorpus } from "./corpus.ts";
import { PrivateDir, readPrivateFile } from "../out-path.ts";
import { printable, UsageError } from "../errors.ts";
import { CorpusError } from "./corpus.ts";
import { parseGold } from "./gold.ts";
import { adjudicationPackets, scoreRun } from "./score.ts";


/** A built run, refused after its delete-by date (or without one). */
function readFeatures(path: string, now: Date = new Date()): { corpus: string; asOf: string; deleteBy: string; features: FeatureRecord[] } {
  const run = JSON.parse(readPrivateFile(path)) as { corpus: string; asOf: string; deleteBy?: string; features: FeatureRecord[] };
  if (!run.deleteBy || !(Date.parse(run.deleteBy) > now.getTime())) throw new UsageError("--features is past its delete-by date (or has none): delete it");
  return run as { corpus: string; asOf: string; deleteBy: string; features: FeatureRecord[] };
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
    const asOf = values["as-of"] ?? corpus.manifest.window.until;
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
    if (run.asOf !== gold.asOf) throw new UsageError("--features and the gold labels are as of different times");
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
