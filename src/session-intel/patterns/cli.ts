/** Offline only. New private outputs, verified inputs, no model, database, runtime or network imports. */
import { existsSync, readFileSync, realpathSync } from "node:fs";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { loadSnapshot } from "../replay/snapshot.ts";
import { sha256Hex } from "../replay/util.ts";
import { analyseCorpus } from "./analyse.ts";
import { buildCorpus, PatternError } from "./corpus.ts";
import { reportMarkdown, reviewQueue } from "./report.ts";

export interface PatternIo { out(line: string): void; now?: Date }
const USAGE = "usage: patterns/cli.ts --snapshot <dir> [--snapshot <dir> --same-identity-space] --out <new-private-dir> [--nearby-ms N --max-comparisons N --max-candidates N --delete-by ISO]";
const serialise = (value: unknown) => `${JSON.stringify(value, null, 2)}\n`;

/** Refuse outputs under any checkout, including via a symlink, and never reuse an output directory. */
function outputPath(value: string | undefined): string {
  if (!value) throw new PatternError("--out is required");
  const target = resolve(value);
  const tail: string[] = [];
  let parent = target;
  while (!existsSync(parent)) { tail.unshift(basename(parent)); parent = dirname(parent); }
  let ancestor = realpathSync(parent);
  for (;;) {
    if (existsSync(join(ancestor, ".git"))) throw new PatternError("outputs belong outside every repository");
    const next = dirname(ancestor); if (next === ancestor) break; ancestor = next;
  }
  return join(realpathSync(parent), ...tail);
}

export async function main(argv: string[], io: PatternIo = { out: (line) => console.log(line) }): Promise<number> {
  let v;
  try { v = parseArgs({ args: argv, strict: true, allowPositionals: false, options: {
    snapshot: { type: "string", multiple: true }, out: { type: "string" }, "same-identity-space": { type: "boolean" },
    "nearby-ms": { type: "string" }, "max-comparisons": { type: "string" }, "max-candidates": { type: "string" }, "delete-by": { type: "string" }, help: { type: "boolean" },
  } }).values; } catch { throw new PatternError("invalid arguments; values are never echoed"); }
  if (v.help) { io.out(USAGE); return 0; }
  if (!v.snapshot?.length || v.snapshot.length > 16) throw new PatternError("provide 1 to 16 --snapshot values");
  const number = (raw: string | undefined) => { if (raw === undefined) return undefined; if (!/^\d{1,9}$/u.test(raw)) throw new PatternError("numeric options must be bounded nonnegative integers"); return Number(raw); };
  const out = outputPath(v.out), now = io.now ?? new Date();
  const snapshots = [];
  for (const path of v.snapshot) snapshots.push(await loadSnapshot(path, { now }));
  const corpus = buildCorpus(snapshots, { sameIdentitySpace: v["same-identity-space"], now });
  if (v["delete-by"] !== undefined) {
    const time = Date.parse(v["delete-by"]);
    if (!Number.isFinite(time) || time <= now.getTime() || time > Date.parse(corpus.deleteBy)) throw new PatternError("--delete-by must be in the future and may only shorten input retention");
    corpus.deleteBy = new Date(time).toISOString();
  }
  const report = analyseCorpus(corpus, { nearbyMs: number(v["nearby-ms"]), maxComparisons: number(v["max-comparisons"]), maxCandidates: number(v["max-candidates"]) });
  const data = { "report.json": serialise(report), "report.md": reportMarkdown(report), "review-queue.json": serialise(reviewQueue(report)) };
  if (Object.values(data).some((s) => Buffer.byteLength(s) > 16 * 1024 * 1024)) throw new PatternError("report exceeds output budget; reduce --max-candidates or input frame");
  await mkdir(dirname(out), { recursive: true, mode: 0o700 });
  await mkdir(out, { mode: 0o700 }); // Exclusive: existing outputs are never replaced.
  try {
    for (const [name, text] of Object.entries(data)) await writeFile(join(out, name), text, { mode: 0o600, flag: "wx" });
    await writeFile(join(out, "manifest.json"), serialise({ format: "session-intel-pattern-artifacts/1", createdAt: now.toISOString(), deleteBy: corpus.deleteBy,
      definitionHash: report.definition.hash, inputs: corpus.inputs,
      implementationSha256: sha256Hex(["./types.ts", "./corpus.ts", "./analyse.ts", "./report.ts", "./cli.ts", "../replay/snapshot.ts", "../replay/util.ts"]
        .map((name) => sha256Hex(readFileSync(new URL(name, import.meta.url)))).join("\n")),
      files: Object.fromEntries(Object.entries(data).map(([name, text]) => [name, sha256Hex(text)])),
    }), { mode: 0o600, flag: "wx" });
  } catch (error) { await rm(out, { recursive: true, force: true }); throw error; }
  io.out(`Analysed ${corpus.sessions.length} sessions; ${report.candidates.length} unreviewed candidates; workspace completeness and alert quality are not established.`);
  return report.coverage.scanComplete ? 0 : 3;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).then((code) => { process.exitCode = code; }).catch((error: unknown) => {
    // No argv, source identifiers, paths, filesystem messages or input content in diagnostics.
    console.error(error instanceof PatternError ? `patterns: ${error.message}` : "patterns: input or output could not be processed");
    process.exitCode = 1;
  });
}
