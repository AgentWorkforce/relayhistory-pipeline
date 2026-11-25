/**
 * The relayfacts benchmark CLI (docs/proposals/relayfacts-features.md). Offline: no network, database,
 * model or credentials. Reads a frozen corpus, writes only under --out, which must be outside this
 * repository (records hold quotes from real sessions).
 *
 *   bun run src/relayfacts/bench/cli.ts build   --corpus <dir> --out <dir> [--as-of <iso>]
 *   bun run src/relayfacts/bench/cli.ts score   --corpus <dir> --gold <file> --features <file> --out <dir>
 *   bun run src/relayfacts/bench/cli.ts packets --features <file> --out <dir> [--seed <n>] [--limit <n>]
 */
import { mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { buildFeatures } from "../features/build.ts";
import { extractSessionFacts } from "../features/extract.ts";
import { renderFeature, renderIndex } from "../features/render.ts";
import type { FeatureRecord } from "../features/types.ts";
import { loadCorpus } from "./corpus.ts";
import { parseGold } from "./gold.ts";
import { adjudicationPackets, scoreRun } from "./score.ts";

const REPO_ROOT = realpathSync(resolve(fileURLToPath(new URL("../../..", import.meta.url))));

function outDir(path: string | undefined): string {
  if (!path) throw new Error("--out is required");
  mkdirSync(path, { recursive: true });
  const real = realpathSync(path);
  if (real === REPO_ROOT || real.startsWith(REPO_ROOT + sep)) throw new Error("--out must be outside this repository");
  return real;
}

export function main(argv: string[]): number {
  const [command, ...rest] = argv;
  const { values } = parseArgs({ args: rest, options: {
    corpus: { type: "string" }, out: { type: "string" }, "as-of": { type: "string" }, gold: { type: "string" },
    features: { type: "string" }, seed: { type: "string" }, limit: { type: "string" },
  } });
  if (command === "build") {
    if (!values.corpus) throw new Error("--corpus is required");
    const corpus = loadCorpus(values.corpus);
    const asOf = values["as-of"] ?? corpus.manifest.window.until;
    const facts = corpus.sessions.map((s) => extractSessionFacts(s.snapshot, s.context));
    const features = buildFeatures(facts, { asOf, forge: corpus.forge });
    const out = outDir(values.out);
    mkdirSync(join(out, "relayfacts"), { recursive: true });
    writeFileSync(join(out, "features.json"), JSON.stringify({ format: "relayfacts-features/1", corpus: corpus.manifestSha256, asOf, features }, null, 2));
    for (const f of features) writeFileSync(join(out, "relayfacts", `${f.id}.md`), renderFeature(f));
    writeFileSync(join(out, "relayfacts", "index.md"), renderIndex(features, (f) => f.id));
    const dropped: Record<string, number> = {};
    for (const f of facts) for (const [k, v] of Object.entries(f.dropped)) dropped[k] = (dropped[k] ?? 0) + v;
    const states: Record<string, number> = {};
    for (const f of features) states[f.state] = (states[f.state] ?? 0) + 1;
    console.log(JSON.stringify({ sessions: facts.length, features: features.length, states, dropped }));
    return 0;
  }
  if (command === "score") {
    if (!values.corpus || !values.gold || !values.features) throw new Error("--corpus, --gold and --features are required");
    const corpus = loadCorpus(values.corpus);
    const gold = parseGold(JSON.parse(readFileSync(values.gold, "utf8")), corpus.manifestSha256);
    const run = JSON.parse(readFileSync(values.features, "utf8")) as { corpus: string; asOf: string; features: FeatureRecord[] };
    if (run.corpus !== corpus.manifestSha256) throw new Error("--features was built from a different corpus");
    if (run.asOf !== gold.asOf) throw new Error(`--features is as of ${run.asOf}, the gold labels as of ${gold.asOf}`);
    const report = scoreRun(corpus, gold, run.features);
    const out = outDir(values.out);
    writeFileSync(join(out, "score.json"), JSON.stringify(report, null, 2));
    console.log(JSON.stringify({ grounding: report.grounding.pass, violations: report.grounding.violations.length, verdicts: report.verdicts, all: report.splits.all }));
    return report.grounding.pass ? 0 : 1;
  }
  if (command === "packets") {
    if (!values.features) throw new Error("--features is required");
    const run = JSON.parse(readFileSync(values.features, "utf8")) as { features: FeatureRecord[] };
    const packets = adjudicationPackets(run.features, renderFeature, Number(values.seed ?? 1), Number(values.limit ?? 30));
    const out = outDir(values.out);
    writeFileSync(join(out, "packets.jsonl"), packets.map((p) => JSON.stringify(p)).join("\n") + "\n");
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
    console.error((error as Error).message);
    process.exit(1);
  }
}
