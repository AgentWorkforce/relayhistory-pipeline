/** CI guard for the scrub-engine default flip (docs/scrub-engine.md, "Default flip").
 *
 * The pinned wasm-bindgen engine is the default scrub path. Three properties keep it so:
 *
 *  1. TS capability grants: `declareScrubTsCapability(` appears only in the hosted
 *     entries and the test preloads listed in TS_CAPABILITY_DECLARERS (and test files),
 *     the grant module is imported only by those files, the scrub-engine directory and tests
 *     (other files may import only the read-only scrubTsAllowed / scrubTsCapabilities), and no file
 *     re-exports it, so no Node self-host path can reach the TS engine.
 *  2. Preload isolation: no production source (src/, packages/<name>/src) imports a test preload.
 *  3. Entry inventory: every executable Node entry whose static import graph reaches the
 *     scrub seam calls initScrubEngine( or runWorker( itself, or is listed in
 *     ENTRY_EXEMPT with the reason it never scrubs or seals.
 *
 *   bun scripts/scrub-default-scan.ts      exit 1 and list each violation
 */
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const GRANT_MODULE = "packages/pipeline/src/lib/scrub-engine/ts-capability.ts";
export const SEAM_MODULES = ["packages/pipeline/src/lib/scrub-engine/index.ts", "packages/pipeline/src/lib/scrub.ts"] as const;
export const PRELOADS = ["scripts/test-preload/scrub-ts-capability.ts", "packages/pipeline/test/setup/scrub-ts-capability.ts"] as const;

/** The only files that may call declareScrubTsCapability(: the hosted (artifact-incapable)
 * entries, declared at handler entry, and the test preloads. */
export const TS_CAPABILITY_DECLARERS: readonly string[] = [
  "src/session-ingest/worker.ts",
  "packages/session-read-worker/src/index.ts",
  "src/session-pipeline/lambda.ts",
  "src/session-read-api/lambda.ts",
  "src/session-ingest/upload-admission-lambda.ts",
  "src/session-ingest/buffered-gc-lambda.ts",
  "src/session-ingest/rearm-sweeper-lambda.ts",
  ...PRELOADS,
  "packages/session-read-worker/test/setup-env.ts",
];

/** Executable entries that reach the seam's module graph but never scrub or seal, so they
 * need no engine. Each reason names what the entry actually uses. */
export const ENTRY_EXEMPT: Readonly<Record<string, string>> = {
  "scripts/index-prebuild-058000-github.ts": "GitHub API gate: imports contracts/constants only, prints with console",
  "scripts/ingest-admission-renewal-github.ts": "GitHub API gate: imports contracts/constants only, prints with console",
  "scripts/ingest-generation-overlap-github.ts": "GitHub API gate: imports contracts/constants only, prints with console",
  "scripts/frozen-source-custody-github.ts": "GitHub API gate: imports contracts/constants only, prints with console",
  "src/hoopdb/rds-activation.ts": "dynamic import of migrate.ts only for generation(), which hashes migration files",
  "scripts/build-canonical-publication.ts": "bundler: builds an artifact, runs none of it",
  "scripts/build-lambda-artifact.ts": "bundler: builds an artifact, runs none of it",
  "scripts/build-buffered-gc.ts": "bundler: builds an artifact, runs none of it",
  "packages/pipeline/scripts/technical-foul-manifest.ts": "manifest generator: hashes the ruleset and vendored files, imports only version/size constants",
  "scripts/ops/claim-records-archive.ts": "uses only rawZstdFrame (zstd framing) from raw-chunks; never scrubs or seals",
};

/** Files the entry heuristic flags that are not executable entries (with why). */
export const NOT_ENTRIES: Readonly<Record<string, string>> = {
  "src/proof/aws/checks.ts": "library for src/proof/aws/cli.ts (which initialises); its top-level await lines are inside a template string shipped to a pod",
};

const SCAN_ROOTS = ["src", "packages", "scripts", "e2e", "benchmarks"];
const isTestPath = (f: string) => /(^|\/)(__tests__|test|tests)\//.test(f) || /\.(test|spec)\.ts$/.test(f);

export type Sources = Map<string, string>;

/** Tracked and new (not ignored) .ts sources under the scan roots, relative to the repo root. */
export function trackedSources(root = ROOT): Sources {
  const out = execFileSync("git", ["ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", ...SCAN_ROOTS.map((r) => `${r}/*.ts`)], { cwd: root, encoding: "utf8" });
  const files = new Map<string, string>();
  for (const f of out.split("\0")) if (f && !f.endsWith(".d.ts") && existsSync(resolve(root, f))) files.set(f, readFileSync(resolve(root, f), "utf8"));
  return files;
}

const IMPORT_RE = /(?:^|\n)\s*(import|export)\s+(type\s+)?([^;]*?)\s*from\s*["']([^"']+)["']|import\(\s*["']([^"']+)["']\s*\)|(?:^|\n)\s*import\s+["']([^"']+)["']/g;

type Edge = { spec: string; names: string[] | "*"; typeOnly: boolean; reexport: boolean };
function edges(src: string): Edge[] {
  const out: Edge[] = [];
  for (const m of src.matchAll(IMPORT_RE)) {
    if (m[4]) {
      const clause = m[3] ?? "";
      const braces = clause.match(/\{([^}]*)\}/);
      const names = braces && !/\*|^\s*\w+\s*,/.test(clause.replace(braces[0], ""))
        ? braces[1]!.split(",").map((x) => x.trim()).filter((x) => x && !x.startsWith("type ")).map((x) => x.split(/\s+as\s+/)[0]!.trim())
        : "*";
      out.push({ spec: m[4], names, typeOnly: !!m[2] || (Array.isArray(names) && names.length === 0 && !!braces), reexport: m[1] === "export" });
    } else out.push({ spec: (m[5] ?? m[6])!, names: "*", typeOnly: false, reexport: false });
  }
  return out;
}

function resolveSpec(files: Sources, from: string, spec: string): string | undefined {
  let base: string;
  if (spec.startsWith("@rhc/")) base = `packages/pipeline/src/${spec.slice(5)}`;
  else if (spec.startsWith(".")) base = resolve("/", dirname(from), spec).slice(1);
  else return undefined;
  for (const c of [base, base.replace(/\.js$/, ".ts"), `${base}.ts`, `${base}/index.ts`]) if (files.has(c)) return c;
}

/** Names whose import alone never runs the engine (init, readiness, error classes, test reset, the size constant). */
const INERT = /^(initScrubEngine|scrubEngineReady|resetScrubEngineForTests|ScrubEngine\w*Error|ScrubEngineUnavailable|MAX_SCRUB_CHARS)$/;

/** First import chain from `entry` to a seam module that pulls in an engine-running name, or undefined. */
export function seamChain(files: Sources, entry: string): string[] | undefined {
  const parent = new Map<string, string | null>([[entry, null]]);
  const queue = [entry];
  while (queue.length) {
    const f = queue.shift()!;
    for (const e of edges(files.get(f) ?? "")) {
      if (e.typeOnly) continue;
      const r = resolveSpec(files, f, e.spec);
      if (!r || parent.has(r)) continue;
      if ((SEAM_MODULES as readonly string[]).includes(r)) {
        if (e.names !== "*" && e.names.every((n) => INERT.test(n))) continue;
        const chain = [r, f]; let c = parent.get(f);
        while (c) { chain.push(c); c = parent.get(c); }
        return chain.reverse();
      }
      parent.set(r, f); queue.push(r);
    }
  }
}

/** An executable Node entry: `import.meta.main`, top-level await, or any other top-level
 * statement that is not a declaration (a call, if/for/try block, process.exit, ...). */
export function isExecutableEntry(path: string, src: string): boolean {
  if (isTestPath(path) || !/\.ts$/.test(path) || path in NOT_ENTRIES) return false;
  if (/import\.meta\.main/.test(src)) return true;
  const code = src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/`(?:\\[\s\S]|[^`\\])*`/g, "``");
  return /^(?:(?:const|let|var)\s[^\n=]*=\s*)?await\s/m.test(code)
    || /^(?!(?:import|export|function|async\s+function|class|abstract\s+class|type|interface|const|let|var|declare|enum|namespace|return|else|case|default|break|continue)\b)[A-Za-z_$(]/m.test(code);
}

export type Violation = { rule: "ts-capability-declarer" | "ts-capability-importer" | "ts-capability-reexport" | "preload-import" | "entry-without-init" | "stale-exemption"; file: string; detail: string };

export function scan(files: Sources): Violation[] {
  const v: Violation[] = [];
  for (const [f, src] of files) {
    // 1. grants
    if (/declareScrubTsCapability\s*\(/.test(src.replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*/g, "")) && f !== GRANT_MODULE && !TS_CAPABILITY_DECLARERS.includes(f) && !isTestPath(f))
      v.push({ rule: "ts-capability-declarer", file: f, detail: "declares a TS capability outside the hosted entries, test preloads and tests" });
    for (const e of edges(src)) {
      const r = resolveSpec(files, f, e.spec);
      if (r === GRANT_MODULE && !e.typeOnly) {
        if (e.reexport) v.push({ rule: "ts-capability-reexport", file: f, detail: `re-exports ${GRANT_MODULE}` });
        const allowed = TS_CAPABILITY_DECLARERS.includes(f) || (SEAM_MODULES as readonly string[]).includes(f) || f.startsWith("packages/pipeline/src/lib/scrub-engine/") || isTestPath(f);
        const onlyReads = e.names !== "*" && e.names.every((n) => n === "scrubTsAllowed" || n === "scrubTsCapabilities");
        if (!allowed && !onlyReads) v.push({ rule: "ts-capability-importer", file: f, detail: `imports ${e.names === "*" ? "*" : e.names.join(", ")} from the grant module` });
      }
      // 2. preloads stay out of production sources
      if (r && (PRELOADS as readonly string[]).includes(r) && !isTestPath(f) && (f.startsWith("src/") || /^packages\/[^/]+\/src\//.test(f)))
        v.push({ rule: "preload-import", file: f, detail: `imports test preload ${r}` });
    }
  }
  // 3. entries
  for (const [f, src] of files) {
    if (!isExecutableEntry(f, src)) continue;
    const chain = seamChain(files, f);
    const exempt = ENTRY_EXEMPT[f];
    const inits = /\b(initScrubEngine|runWorker)\s*\(/.test(src.replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*/g, ""));
    if (chain && !inits && !exempt) v.push({ rule: "entry-without-init", file: f, detail: `reaches the seam (${chain.join(" -> ")}) without initScrubEngine( or runWorker(` });
    if (exempt && !chain) v.push({ rule: "stale-exemption", file: f, detail: "exempt but no longer reaches the seam; drop the exemption" });
  }
  for (const f of Object.keys(ENTRY_EXEMPT)) if (!files.has(f) || !isExecutableEntry(f, files.get(f)!)) v.push({ rule: "stale-exemption", file: f, detail: "exempt file is not an executable entry" });
  return v;
}

if (import.meta.main) {
  const v = scan(trackedSources());
  for (const x of v) console.error(`${x.rule}: ${x.file}: ${x.detail}`);
  console.log(v.length ? `scrub-default-scan: ${v.length} violation(s)` : "scrub-default-scan: ok");
  process.exit(v.length ? 1 : 0);
}
