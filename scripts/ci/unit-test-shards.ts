// Deterministic shard planner for the root `bun test` suite (CI job `unit-tests-shard`).
//
// Discovers the same files `bun test` runs with bunfig.toml `[test] root = "src"`: every
// `*.test.*`, `*_test.*`, `*.spec.*` and `*_spec.*` file with a JavaScript or TypeScript
// extension under src/ (node_modules and dot-directories are skipped, as bun does). The
// sorted list is split into N shards by a greedy longest-first assignment over the
// estimated seconds in scripts/ci/unit-test-weights.json.
//
//   bun scripts/ci/unit-test-shards.ts --shard 2 --of 4   # shard 2's files, one per line
//   bun scripts/ci/unit-test-shards.ts --check --of 4     # every file in exactly one shard
//   bun scripts/ci/unit-test-shards.ts --plan --of 4      # estimated seconds per shard
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const TEST_FILE = /(\.test|_test|\.spec|_spec)\.(js|jsx|ts|tsx|mjs|cjs|mts|cts)$/;

export interface Weights {
  defaults: { pglite: number; other: number };
  files: Record<string, number>;
}

const byCodePoint = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/** Repo-relative, `/`-separated, sorted paths of every test file under `<root>/src`. */
export function discoverTestFiles(root: string, dir = "src"): string[] {
  const found: string[] = [];
  const walk = (rel: string) => {
    for (const entry of readdirSync(join(root, rel), { withFileTypes: true })) {
      if (entry.name.startsWith(".") || entry.name === "node_modules") continue;
      const path = `${rel}/${entry.name}`;
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile() && TEST_FILE.test(entry.name)) found.push(path);
    }
  };
  walk(dir);
  return found.sort(byCodePoint);
}

export function loadWeights(path: string): Weights {
  const raw = JSON.parse(readFileSync(path, "utf8")) as Partial<Weights>;
  const { defaults, files } = raw;
  if (!defaults || !(defaults.pglite > 0) || !(defaults.other > 0) || !files || typeof files !== "object") {
    throw new Error(`${path}: expected { defaults: { pglite, other }, files: { path: seconds } }`);
  }
  for (const [file, seconds] of Object.entries(files)) {
    if (!(typeof seconds === "number" && seconds > 0)) throw new Error(`${path}: ${file} needs a positive number of seconds`);
  }
  return { defaults, files };
}

export function weightOf(file: string, weights: Weights): number {
  return weights.files[file] ?? (file.endsWith(".pglite.test.ts") ? weights.defaults.pglite : weights.defaults.other);
}

export interface Shard { index: number; seconds: number; files: string[] }

/** Greedy longest-first: heaviest file to the currently lightest shard (ties: path, then lowest index). */
export function planShards(files: readonly string[], of: number, weights: Weights): Shard[] {
  if (!Number.isInteger(of) || of < 1) throw new Error(`--of must be a positive integer, got ${of}`);
  const shards: Shard[] = Array.from({ length: of }, (_, i) => ({ index: i + 1, seconds: 0, files: [] }));
  const ordered = [...new Set(files)].sort(byCodePoint).map((file) => ({ file, seconds: weightOf(file, weights) }));
  ordered.sort((a, b) => b.seconds - a.seconds || byCodePoint(a.file, b.file));
  for (const { file, seconds } of ordered) {
    let target = shards[0];
    for (const shard of shards) if (shard.seconds < target.seconds) target = shard;
    target.files.push(file);
    target.seconds += seconds;
  }
  for (const shard of shards) shard.files.sort(byCodePoint);
  return shards;
}

/** Problems with a plan: files missing, duplicated or unknown. Empty means every file is in exactly one shard. */
export function checkPlan(files: readonly string[], shards: readonly Shard[]): string[] {
  const seen = new Map<string, number>();
  for (const shard of shards) for (const file of shard.files) seen.set(file, (seen.get(file) ?? 0) + 1);
  const problems: string[] = [];
  const expected = new Set(files);
  for (const file of expected) {
    const count = seen.get(file) ?? 0;
    if (count !== 1) problems.push(`${file}: in ${count} shards`);
  }
  for (const file of seen.keys()) if (!expected.has(file)) problems.push(`${file}: planned but not discovered`);
  return problems;
}

function parseArgs(argv: readonly string[]) {
  const args = { mode: "", shard: 0, of: 0 };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--check" || arg === "--plan") args.mode = arg.slice(2);
    else if (arg === "--shard") { args.mode ||= "shard"; args.shard = Number(argv[++i]); }
    else if (arg === "--of") args.of = Number(argv[++i]);
    else throw new Error(`unknown argument ${arg}`);
  }
  if (!args.mode) throw new Error("one of --shard <i>, --check or --plan is required");
  if (!Number.isInteger(args.of) || args.of < 1) throw new Error("--of <N> must be a positive integer");
  if (args.mode === "shard" && !(Number.isInteger(args.shard) && args.shard >= 1 && args.shard <= args.of)) {
    throw new Error(`--shard must be an integer in 1..${args.of}`);
  }
  return args;
}

if (import.meta.main) {
  try {
    const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
    const args = parseArgs(process.argv.slice(2));
    const files = discoverTestFiles(root);
    if (files.length === 0) throw new Error("no test files discovered under src/");
    const weights = loadWeights(join(root, "scripts/ci/unit-test-weights.json"));
    const shards = planShards(files, args.of, weights);
    if (args.mode === "shard") {
      const shard = shards[args.shard - 1];
      if (shard.files.length === 0) throw new Error(`shard ${args.shard}/${args.of} is empty`);
      for (const file of shard.files) console.log(`./${file}`);
    } else if (args.mode === "check") {
      const problems = checkPlan(files, shards);
      // A stale weight only skews balance, so it warns rather than failing the check.
      for (const file of Object.keys(weights.files)) {
        if (!files.includes(file)) console.error(`warning: ${file} is in unit-test-weights.json but is not a discovered test file`);
      }
      for (const problem of problems) console.error(problem);
      if (problems.length > 0) process.exit(1);
      console.log(`ok: ${files.length} test files, each in exactly one of ${args.of} shards`);
    } else {
      for (const shard of shards) console.log(`shard ${shard.index}/${args.of}: ${shard.files.length} files, ~${Math.round(shard.seconds)} s`);
      console.log(`total: ${files.length} files, ~${Math.round(shards.reduce((s, x) => s + x.seconds, 0))} s`);
    }
  } catch (error) {
    console.error(`unit-test-shards: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(2);
  }
}
